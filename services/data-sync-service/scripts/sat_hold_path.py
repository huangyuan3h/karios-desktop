#!/usr/bin/env python3
"""Frozen S-gap hold path: day-1/2/3 close vs T-open (clip4, body=3).

Observation only — does not change live exits. Answers: of names red at
day-2 close, how many are green (or improved) by day-3 close.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/sat_hold_path.py
  PYTHONPATH=src python3 scripts/sat_hold_path.py --save-report
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.service.sat_hold_path import paths_from_blotter, summarize_paths
from data_sync_service.service.state_bucket_track import (
    load_sgap_context,
    replay_sgap_from_context,
)

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}
FULL_START = "2024-08-01"
FULL_END = "2026-08-07"
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"


def _fmt_mean(v: float | None) -> str:
    return "—" if v is None else f"{v:+.2f}%"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    print("Sat hold path (frozen clip4 body=3, T-open fill, gross vs cost)\n", flush=True)
    print(f"loading context {FULL_START}~{FULL_END} ...", flush=True)
    ctx = load_sgap_context(FULL_START, FULL_END)
    print("  loaded.", flush=True)

    results: dict[str, dict] = {}
    for wname, (start, end) in WINDOWS.items():
        sat = replay_sgap_from_context(
            ctx,
            start=start,
            end=end,
            skip_t1_limit=True,
            pool_mode="strict",
            max_pos=4,
            position_pct=0.25,
        )
        paths = paths_from_blotter(ctx, sat.get("blotter") or [])
        stats = summarize_paths(paths)
        results[wname] = {"start": start, "end": end, **stats}
        d2 = stats["d2Red"]
        hit = stats["hitProtectByD2"]
        print(f"=== {wname} ({start}~{end}) fills={stats['n']} ===", flush=True)
        print(
            f"  mean d1/d2/d3  {_fmt_mean(stats['mean']['d1'])} / "
            f"{_fmt_mean(stats['mean']['d2'])} / {_fmt_mean(stats['mean']['d3'])}",
            flush=True,
        )
        print(
            f"  green d1/d2/d3 {stats['pctGreen']['d1']}% / "
            f"{stats['pctGreen']['d2']}% / {stats['pctGreen']['d3']}%",
            flush=True,
        )
        print(
            f"  d2 red {d2['n']} ({d2['pctOfFills']}%): "
            f"d3 green {d2['recoveredGreen']} ({d2['pctRecoveredGreen']}%), "
            f"improved {d2['improved']} ({d2['pctImproved']}%), "
            f"mean d3 {_fmt_mean(d2['meanD3'])} "
            f"(recovered {_fmt_mean(d2['meanD3Recovered'])} / stayed {_fmt_mean(d2['meanD3StayedRed'])})",
            flush=True,
        )
        print(
            f"  hit −5% by d2 close {hit['n']} ({hit['pctOfFills']}%): "
            f"d3 green {hit['d3Green']} ({hit['pctD3Green']}%), "
            f"mean d3 {_fmt_mean(hit['meanD3'])}",
            flush=True,
        )

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        out = REPORT_DIR / "sat_hold_path_day2_2026-09-03.json"
        payload = {
            "tag": "sat-hold-path-day2-2026-09-03",
            "protocol": (
                "window-local empty book; frozen clip4 sat (4×25% strict skip_t1 "
                "body=3); marks = close / T-open − 1, no costs; d2-red recovery "
                "is observation, not a live rule"
            ),
            "savedAt": datetime.now(tz=UTC).isoformat(),
            "windows": results,
        }
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
        print(f"\nsaved {out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
