#!/usr/bin/env python3
"""Scout battery K: frozen S-3 CN realized trades cut by holding-days / entry-score.

Windows OOS2+train ONLY; valid NOT touched. Each BacktestTrade row (incl.
pyramid adds, own entry_date) is one observation. All cuts reported
(no cherry-pick). At most ONE forward candidate per pre-reg rules in
designs/scout-breakthrough-2026-09-05.md.

Read-only vs Postgres (via engine's own loaders). Saves nothing.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    simulate,
)
from run_walk_forward import S3_CONFIG  # noqa: E402

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
}
HOLD_BINS = ("<=5d", "6-15d", "16-30d", "31d+")
SCORE_BINS = ("65-70", "70-80", "80+", "unknown")


def _hold_bin(h: int) -> str:
    if h <= 5:
        return HOLD_BINS[0]
    if h <= 15:
        return HOLD_BINS[1]
    if h <= 30:
        return HOLD_BINS[2]
    return HOLD_BINS[3]


def _score_bin(s) -> str:
    try:
        s = float(s)
    except (TypeError, ValueError):
        return SCORE_BINS[3]
    if s < 70:
        return SCORE_BINS[0]
    if s < 80:
        return SCORE_BINS[1]
    return SCORE_BINS[2]


def main() -> int:
    acc_hold: dict[str, dict[str, list[float]]] = {
        w: {b: [] for b in HOLD_BINS} for w in WINDOWS
    }
    acc_score: dict[str, dict[str, list[float]]] = {
        w: {b: [] for b in SCORE_BINS} for w in WINDOWS
    }
    for w, (s, e) in WINDOWS.items():
        print(f"=== {w} ({s}~{e}) ===", flush=True)
        cfg = BacktestConfig(start_date=s, end_date=e, **S3_CONFIG)  # type: ignore[arg-type]
        data = BacktestData(cfg)
        run = simulate(cfg, data)
        print(f"  trades rows: {len(run.trades)}", flush=True)
        for tr in run.trades:
            pnl = float(tr.pnl_pct)
            acc_hold[w][_hold_bin(int(tr.holding_days or 0))].append(pnl)
            acc_score[w][_score_bin(tr.score_at_entry)].append(pnl)

    def _show(title: str, table: dict[str, dict[str, list[float]]], cols: tuple) -> None:
        print(f"\n## {title}")
        print("| window | " + " | ".join(cols) + " |")
        print("|" + "|".join(["------"] * (1 + len(cols))) + "|")
        for w in WINDOWS:
            cells = []
            for c in cols:
                v = table[w][c]
                m = float(np.mean(v)) if v else 0.0
                hit = float(np.mean([1.0 if x > 0 else 0.0 for x in v])) * 100 if v else 0.0
                cells.append(f"{m:+.2f}%/{hit:.0f}%/{len(v)}")
            print(f"| {w} | " + " | ".join(cells) + " |")

    _show("K-a holding days (realized net pnl)", acc_hold, HOLD_BINS)
    _show("K-b score at entry (realized net pnl)", acc_score, SCORE_BINS)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
