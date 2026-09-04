#!/usr/bin/env python3
"""Fair six-window comparison: standalone S-gap (executable) vs CN S-3.

Both are independent A-share stock legs — NOT pick-strong and NOT twin-star mix.

  S-gap: build_sgap_timeline(skip_t1_limit=True)  # 涨停可能买不进
  S-3:   BacktestConfig + S3_CONFIG (strategy-params.md §1)

Windows match opportunity-twin-star freeze (core_satellite_frozen_2026-08-31.json):
  OOS2 / train / valid / past_year / aligned / long2y

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/compare_sgap_vs_s3.py
  PYTHONPATH=src python3 scripts/compare_sgap_vs_s3.py --windows OOS2,aligned

Writes data/backtest_reports/sgap_vs_s3_YYYY-MM-DD.json
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import UTC, date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    simulate,
)
from data_sync_service.service.state_bucket_track import build_sgap_timeline  # noqa: E402
from run_walk_forward import S3_CONFIG  # noqa: E402

# Same six windows as twin-star freeze (honest apples-to-apples with sat_exec).
WINDOWS: dict[str, tuple[str, str]] = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "past_year": ("2025-08-28", "2026-08-07"),
    "aligned": ("2025-08-28", "2026-08-28"),
    "long2y": ("2024-08-01", "2026-08-28"),
}

REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"


def _nav_metrics(nav: list[float]) -> dict[str, float | int | None]:
    if not nav:
        return {"total_pct": 0.0, "max_dd": 0.0, "sharpe": None, "n_days": 0}
    peak = nav[0]
    max_dd = 0.0
    for v in nav:
        peak = max(peak, v)
        if peak > 0:
            max_dd = max(max_dd, (peak - v) / peak)
    rets = [nav[i] / nav[i - 1] - 1.0 for i in range(1, len(nav)) if nav[i - 1] > 0]
    sharpe: float | None = None
    if len(rets) >= 2:
        mean_r = sum(rets) / len(rets)
        var = sum((x - mean_r) ** 2 for x in rets) / (len(rets) - 1)
        std_r = math.sqrt(var) if var > 0 else 0.0
        if std_r > 0:
            sharpe = round(mean_r / std_r * (252**0.5), 2)
    return {
        "total_pct": round((nav[-1] / nav[0] - 1.0) * 100.0, 2),
        "max_dd": round(max_dd * 100.0, 1),
        "sharpe": sharpe,
        "n_days": len(nav),
    }


def _run_sgap(start: str, end: str) -> dict[str, float | int | None]:
    sat = build_sgap_timeline(start=start, end=end, skip_t1_limit=True)
    nav = [float(r.get("satNav") or 1.0) for r in sat.get("rows") or []]
    m = _nav_metrics(nav)
    # Prefer engine summary for total/dd when present (matches Timeline).
    summary = sat.get("summary") or {}
    if "satPct" in summary:
        m["total_pct"] = float(summary["satPct"])
    if "satMaxDdPct" in summary:
        m["max_dd"] = float(summary["satMaxDdPct"])
    return m


def _run_s3(start: str, end: str) -> dict[str, float | int | None]:
    cfg = BacktestConfig(start_date=start, end_date=end, **S3_CONFIG)  # type: ignore[arg-type]
    data = BacktestData(cfg)
    run = simulate(cfg, data)
    s = run.summary
    return {
        "total_pct": float(s.total_net_pnl_pct or 0.0),
        "max_dd": float(s.max_drawdown_pct or 0.0),
        "sharpe": float(s.sharpe) if s.sharpe is not None else None,
        "n_days": len(run.nav_curve) if run.nav_curve else 0,
        "closed": int(s.closed or 0),
    }


def _md_table(results: dict[str, dict[str, dict[str, float | int | None]]]) -> str:
    lines = [
        "| 窗口 | S-gap total% | S-gap dd | S-gap sr | S-3 total% | S-3 dd | S-3 sr | Δ(sgap−s3) |",
        "|------|-------------:|---------:|---------:|-----------:|-------:|-------:|-----------:|",
    ]
    for w, (s, e) in WINDOWS.items():
        if w not in results:
            continue
        g = results[w]["sgap"]
        c = results[w]["s3"]
        delta = float(g["total_pct"] or 0) - float(c["total_pct"] or 0)
        g_sr = g["sharpe"] if g["sharpe"] is not None else "—"
        c_sr = c["sharpe"] if c["sharpe"] is not None else "—"
        lines.append(
            f"| {w} ({s}~{e}) | {g['total_pct']:+.1f} | {g['max_dd']:.1f} | {g_sr} | "
            f"{c['total_pct']:+.1f} | {c['max_dd']:.1f} | {c_sr} | {delta:+.1f}pt |"
        )
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--windows",
        default=",".join(WINDOWS),
        help="Comma-separated windows (default: all six)",
    )
    ap.add_argument("--json", help="Output path (default sgap_vs_s3_YYYY-MM-DD.json)")
    args = ap.parse_args()

    names = [w.strip() for w in args.windows.split(",") if w.strip()]
    missing = [w for w in names if w not in WINDOWS]
    if missing:
        print(f"ERROR: unknown windows {missing}; valid={list(WINDOWS)}", file=sys.stderr)
        return 2

    results: dict[str, dict[str, dict[str, float | int | None]]] = {}
    for w in names:
        start, end = WINDOWS[w]
        print(f"=== {w} ({start}~{end}) ===", flush=True)
        print("  S-gap (executable)…", flush=True)
        sgap = _run_sgap(start, end)
        print(
            f"    total={sgap['total_pct']:+.1f}% dd={sgap['max_dd']:.1f} "
            f"sr={sgap['sharpe']} n={sgap['n_days']}",
            flush=True,
        )
        print("  S-3 CN…", flush=True)
        s3 = _run_s3(start, end)
        print(
            f"    total={s3['total_pct']:+.1f}% dd={s3['max_dd']:.1f} "
            f"sr={s3['sharpe']} closed={s3.get('closed')}",
            flush=True,
        )
        results[w] = {
            "start": start,  # type: ignore[dict-item]
            "end": end,  # type: ignore[dict-item]
            "sgap": sgap,
            "s3": s3,
            "delta_pt": round(float(sgap["total_pct"] or 0) - float(s3["total_pct"] or 0), 2),
        }

    table = _md_table(results)  # type: ignore[arg-type]
    print("\n" + table)

    # Simple verdict: count windows where executable S-gap beats S-3 on total.
    wins = sum(1 for w in names if float(results[w]["delta_pt"]) > 0)  # type: ignore[arg-type]
    verdict = (
        f"Executable S-gap beats CN S-3 on total in {wins}/{len(names)} windows. "
        "Do NOT cite pre-audit +122.8% (R7/R8); that assumed limit-up fills."
    )
    print(f"\n{verdict}")

    today = date.today().isoformat()
    out_path = Path(args.json) if args.json else REPORT_DIR / f"sgap_vs_s3_{today}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "tag": f"sgap-vs-s3-{today}",
        "generated_at": datetime.now(UTC).isoformat(),
        "scheme": "Standalone S-gap (executable) vs CN S-3 (independent stock legs)",
        "sgap": {
            "engine": "state_bucket_track.build_sgap_timeline",
            "params": "bucket_q=3, max_pos=15, body=3, R-wide, skip_t1_limit=True",
            "note": "涨停可能买不进 → T-1 limit candidates skipped (same as 机会双子星 satellite)",
        },
        "s3": {
            "engine": "backtest_engine.simulate",
            "config": "run_walk_forward.S3_CONFIG (strategy-params.md §1)",
        },
        "windows": {k: {"start": v[0], "end": v[1]} for k, v in WINDOWS.items()},
        "per_window": results,
        "markdown_table": table,
        "verdict": verdict,
        "caveats": [
            "R7/R8 past_year +122.8% was pre limit-up fill audit — not truth for this compare",
            "S-gap costs COSTS_ROUNDTRIP=0.003; S-3 uses engine slippage/costs",
            "Not a pick-strong STOCK-leg replacement (R8 rejected argmax swap)",
            "Live default remains single_track; this is research/observation only",
        ],
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
