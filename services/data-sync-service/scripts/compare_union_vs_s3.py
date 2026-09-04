#!/usr/bin/env python3
"""Reproduce R6/R8: four-state union-body vs CN S-3 on the walk-forward three windows.

Union-body = S-limit + S-gap + S-fresh + S-shrink OR-combined, shared 10 slots,
state priority, R-wide gate, body holds (3/3/15/15). Same engine as
``scout_state_bucket_pickstrong.simulate_state_bucket`` (state_filter=None).

S-3 = ``run_walk_forward.S3_CONFIG`` via ``backtest_engine.simulate``.

This is the *historical* fill model (no skip_t1_limit). Use alongside
``compare_sgap_vs_s3.py`` for executable S-gap single-state only.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/compare_union_vs_s3.py
  PYTHONPATH=src python3 scripts/compare_union_vs_s3.py --rerun-scout

Writes data/backtest_reports/union_vs_s3_YYYY-MM-DD.json
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
from run_walk_forward import S3_CONFIG, WINDOWS  # noqa: E402

REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
THREE_WINDOWS = ("OOS2", "train", "valid")
LOAD_S = "2024-04-01"
LOAD_E = "2026-09-10"


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
        "n_days": len(run.nav_curve) if run.nav_curve else 0,
    }


def _run_union_four_state(start: str, end: str) -> dict[str, float | int | None]:
    from scout_state_bucket_pickstrong import (  # noqa: E402
        _load_calendar,
        _load_daily,
        _load_list_dates,
        _load_mv_map,
        simulate_state_bucket,
        stats,
    )

    per_ts = _load_daily(LOAD_S, LOAD_E)
    mv_map = _load_mv_map(LOAD_S, LOAD_E)
    list_dates = _load_list_dates()
    date_idx = {ts: {r["date"]: i for i, r in enumerate(series)} for ts, series in per_ts.items()}
    cal = _load_calendar(start, end)
    nav, _ = simulate_state_bucket(cal, per_ts, mv_map, list_dates, date_idx)
    m = stats(cal, nav)
    return {
        "total_pct": round(float(m["total_pct"]), 2),
        "max_dd": round(float(m["max_dd"]), 1),
        "sharpe": round(float(m["sharpe"]), 2),
        "n_days": int(m["n_days"]),
    }


def _load_frozen_union() -> dict[str, dict[str, float]]:
    """Prior scout outputs for cross-check."""
    out: dict[str, dict[str, float]] = {}
    pk = REPORT_DIR / "state_bucket_pickstrong_latest.json"
    un = REPORT_DIR / "state_union_latest.json"
    if pk.exists():
        raw = json.loads(pk.read_text())
        for w in THREE_WINDOWS:
            sb = raw.get(w, {}).get("state_bucket", {})
            if sb:
                out[w] = {
                    "total_pct": round(float(sb["total_pct"]), 2),
                    "max_dd": round(float(sb["max_dd"]), 1),
                    "sharpe": round(float(sb["sharpe"]), 2),
                }
    if un.exists():
        raw = json.loads(un.read_text())
        for w in THREE_WINDOWS:
            u = raw.get("results", {}).get("False", {}).get(w, {})
            if u and w not in out:
                out[w] = {
                    "total_pct": round(float(u["total_pct"]), 2),
                    "max_dd": round(float(u["max_dd"]), 1),
                    "sharpe": round(float(u["sharpe"]), 2),
                }
    return out


def _md_table(results: dict) -> str:
    lines = [
        "| 窗口 | union total% | union dd | union sr | S-3 total% | S-3 dd | S-3 sr | Δ(union−s3) |",
        "|------|-------------:|---------:|---------:|-----------:|-------:|-------:|------------:|",
    ]
    for w in THREE_WINDOWS:
        if w not in results:
            continue
        u = results[w]["union"]
        s = results[w]["s3"]
        delta = float(u["total_pct"]) - float(s["total_pct"])
        lines.append(
            f"| {w} | {u['total_pct']:+.1f} | {u['max_dd']:.1f} | {u['sharpe']} | "
            f"{s['total_pct']:+.1f} | {s['max_dd']:.1f} | {s['sharpe']} | {delta:+.1f}pt |"
        )
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--rerun-scout",
        action="store_true",
        help="Re-run union simulation (DB); default also loads frozen JSON for diff",
    )
    args = ap.parse_args()

    frozen = _load_frozen_union()
    results: dict[str, dict] = {}

    for w in THREE_WINDOWS:
        start, end = WINDOWS[w]
        print(f"=== {w} ({start}~{end}) ===", flush=True)

        print("  union four-state (R6/R8 engine)…", flush=True)
        union = _run_union_four_state(start, end)
        print(
            f"    total={union['total_pct']:+.1f}% dd={union['max_dd']:.1f} sr={union['sharpe']}",
            flush=True,
        )

        print("  S-3 CN…", flush=True)
        s3 = _run_s3(start, end)
        print(
            f"    total={s3['total_pct']:+.1f}% dd={s3['max_dd']:.1f} sr={s3['sharpe']} "
            f"closed={s3.get('closed')}",
            flush=True,
        )

        diff_frozen = None
        if w in frozen:
            fr = frozen[w]
            diff_frozen = round(float(union["total_pct"]) - float(fr["total_pct"]), 2)
            print(
                f"    frozen union total={fr['total_pct']:+.1f}% "
                f"(Δ rerun−frozen {diff_frozen:+.2f}pt)",
                flush=True,
            )

        results[w] = {
            "start": start,
            "end": end,
            "union": union,
            "s3": s3,
            "delta_pt": round(float(union["total_pct"]) - float(s3["total_pct"]), 2),
            "frozen_union": frozen.get(w),
            "rerun_vs_frozen_pt": diff_frozen,
        }

    table = _md_table(results)
    print("\n" + table)

    wins = sum(1 for w in THREE_WINDOWS if results[w]["delta_pt"] > 0)
    repro_ok = all(
        results[w].get("rerun_vs_frozen_pt") is None
        or abs(results[w]["rerun_vs_frozen_pt"]) < 0.5
        for w in THREE_WINDOWS
    )
    verdict = (
        f"Four-state union beats S-3 on total in {wins}/3 walk-forward windows. "
        f"Rerun matches frozen scout JSON: {'yes' if repro_ok else 'CHECK DIFFS'}. "
        "Historical fill model (no skip_t1_limit) — not executable twin-star口径."
    )
    print(f"\n{verdict}")

    today = date.today().isoformat()
    out_path = REPORT_DIR / f"union_vs_s3_{today}.json"
    payload = {
        "tag": f"union-four-state-vs-s3-{today}",
        "generated_at": datetime.now(UTC).isoformat(),
        "scheme": "R6 union-body (4 states, shared 10 slots) vs CN S-3",
        "union": {
            "states": ["S-limit", "S-gap", "S-fresh", "S-shrink"],
            "engine": "scout_state_bucket_pickstrong.simulate_state_bucket",
            "params": "bucket_q=5, max_pos=10, body 3/3/15/15, R-wide, state priority",
            "fill_model": "historical (assumes limit-up open fills)",
        },
        "s3": {"config": "run_walk_forward.S3_CONFIG"},
        "windows": {w: {"start": WINDOWS[w][0], "end": WINDOWS[w][1]} for w in THREE_WINDOWS},
        "per_window": results,
        "markdown_table": table,
        "verdict": verdict,
        "caveats": [
            "R10: shared-slot union is S-limit-dominated; slice2 (L+G per-type slots) preferred structurally",
            "2026-09-01 limit-up audit: executable S-gap alone << union past_year +122.8%",
            "S-fresh deleted in R10; union repro uses original 4-state for historical fidelity",
        ],
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
