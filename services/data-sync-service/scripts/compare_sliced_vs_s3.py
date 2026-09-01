#!/usr/bin/env python3
"""Phase 1: per-state slice blends vs CN S-3 on walk-forward three windows.

Executable口径 (skip_t1_limit=True). See docs/designs/state-bucket-slice-stock-leg.md.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/compare_sliced_vs_s3.py
  PYTHONPATH=src python3 scripts/compare_sliced_vs_s3.py --variants slice2_LG,slice3_LGS,G

Writes data/backtest_reports/sliced_vs_s3_YYYY-MM-DD.json
"""
from __future__ import annotations

import argparse
import json
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
from data_sync_service.service.state_bucket_slice import (  # noqa: E402
    SLICE_VARIANTS,
    run_slice_variant,
)
from run_walk_forward import S3_CONFIG, WINDOWS  # noqa: E402

THREE_WINDOWS = ("OOS2", "train", "valid")
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
REJECT_PT = 5.0

# Default grid: singles + slice2/3 + L:G weight scan
DEFAULT_VARIANTS = (
    "L",
    "G",
    "S",
    "F",
    "slice2_LG",
    "slice3_LGS",
    "slice2_L70",
    "slice2_L60",
    "slice2_L50",
    "slice2_L40",
    "slice2_L30",
)


def _run_s3(start: str, end: str) -> dict[str, float | int | None]:
    cfg = BacktestConfig(start_date=start, end_date=end, **S3_CONFIG)  # type: ignore[arg-type]
    data = BacktestData(cfg)
    run = simulate(cfg, data)
    s = run.summary
    return {
        "total_pct": round(float(s.total_net_pnl_pct or 0.0), 2),
        "max_dd": round(float(s.max_drawdown_pct or 0.0), 1),
        "sharpe": float(s.sharpe) if s.sharpe is not None else None,
        "closed": int(s.closed or 0),
    }


def _passes_window(delta_pt: float) -> bool:
    return delta_pt >= -REJECT_PT


def _pick_best(results: dict[str, dict]) -> dict[str, Any]:
    """Best variant: most windows pass reject line, then highest mean delta."""
    scores: dict[str, tuple[int, float]] = {}
    for v in results:
        wins = sum(1 for w in THREE_WINDOWS if _passes_window(results[v][w]["delta_pt"]))
        mean_delta = sum(results[v][w]["delta_pt"] for w in THREE_WINDOWS) / len(THREE_WINDOWS)
        scores[v] = (wins, mean_delta)
    ranked = sorted(scores.items(), key=lambda x: (x[1][0], x[1][1]), reverse=True)
    best = ranked[0][0] if ranked else None
    return {"best_variant": best, "ranking": [{"variant": v, "pass_windows": s[0], "mean_delta_pt": round(s[1], 2)} for v, s in ranked[:5]]}


def _md_table(results: dict[str, dict], s3: dict[str, dict]) -> str:
    lines = [
        "| 窗口 | 变体 | total% | dd | sr | S-3 total% | Δ | 过线? |",
        "|------|------|-------:|---:|---:|-----------:|--:|:-----:|",
    ]
    for w in THREE_WINDOWS:
        s3t = s3[w]["total_pct"]
        for v in sorted(results.keys()):
            m = results[v][w]
            ok = "✓" if _passes_window(m["delta_pt"]) else "✗"
            lines.append(
                f"| {w} | {v} | {m['total_pct']:+.1f} | {m['max_dd']:.1f} | {m['sharpe']} | "
                f"{s3t:+.1f} | {m['delta_pt']:+.1f}pt | {ok} |"
            )
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--variants",
        default=",".join(DEFAULT_VARIANTS),
        help="Comma-separated SLICE_VARIANTS keys",
    )
    ap.add_argument("--json", help="Output path")
    args = ap.parse_args()

    variants = [v.strip() for v in args.variants.split(",") if v.strip()]
    bad = [v for v in variants if v not in SLICE_VARIANTS]
    if bad:
        print(f"ERROR: unknown variants {bad}; valid={list(SLICE_VARIANTS)}", file=sys.stderr)
        return 2

    s3_by_window: dict[str, dict] = {}
    for w in THREE_WINDOWS:
        start, end = WINDOWS[w]
        print(f"=== S-3 {w} ({start}~{end}) ===", flush=True)
        s3_by_window[w] = _run_s3(start, end)
        s = s3_by_window[w]
        print(f"  total={s['total_pct']:+.1f}% dd={s['max_dd']:.1f} sr={s['sharpe']}", flush=True)

    all_results: dict[str, dict] = {v: {} for v in variants}
    for v in variants:
        print(f"\n--- variant {v} ---", flush=True)
        for w in THREE_WINDOWS:
            start, end = WINDOWS[w]
            out = run_slice_variant(v, start=start, end=end, skip_t1_limit=True)
            m = out["metrics"]
            delta = float(m["total_pct"]) - float(s3_by_window[w]["total_pct"])
            all_results[v][w] = {
                **m,
                "delta_pt": round(delta, 2),
                "states": out["states"],
                "weights": out["weights"],
            }
            print(
                f"  {w}: total={m['total_pct']:+.1f}% dd={m['max_dd']:.1f} sr={m['sharpe']} "
                f"Δ={delta:+.1f}pt {'OK' if _passes_window(delta) else 'FAIL'}",
                flush=True,
            )

    pick = _pick_best(all_results)
    best = pick["best_variant"]
    best_pass = pick["ranking"][0]["pass_windows"] if pick["ranking"] else 0
    verdict = (
        f"Best slice candidate: {best!r} ({best_pass}/3 windows within −{REJECT_PT}pt of S-3). "
        "Executable skip_t1_limit=True. Phase 2 only if user approves after review."
    )
    print(f"\n{verdict}")
    print("\nTop ranking:", pick["ranking"])

    table = _md_table(all_results, s3_by_window)
    today = date.today().isoformat()
    out_path = Path(args.json) if args.json else REPORT_DIR / f"sliced_vs_s3_{today}.json"
    payload = {
        "tag": f"sliced-vs-s3-{today}",
        "generated_at": datetime.now(UTC).isoformat(),
        "design": "docs/designs/state-bucket-slice-stock-leg.md",
        "executable": True,
        "skip_t1_limit": True,
        "reject_line_pt": REJECT_PT,
        "windows": {w: {"start": WINDOWS[w][0], "end": WINDOWS[w][1]} for w in THREE_WINDOWS},
        "s3": s3_by_window,
        "variants": all_results,
        "best": pick,
        "verdict": verdict,
        "markdown_table": table,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
