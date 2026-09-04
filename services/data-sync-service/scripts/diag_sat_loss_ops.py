#!/usr/bin/env python3
"""S2 diagnostic: which loss-prone operations can be excluded? (selection only)

Selection windows OOS2+train; valid NOT touched. For all S-gap names passing
the habit fill-side skips (skip_t1 + C1 3% upside cap), report forward 3-day
net return (1430 entry -> day-3 1430 exit, minus costs) by:

1. runup bins (1430/open-1): <-3% / -3~-1% / -1~+1% / +1~+3%  (C3 candidate:
   skip names falling into the fill)
2. entry weekday: Mon..Fri  (weekend-occupancy hypothesis)
3. near-limit distance: (board_px - 1430px)/board_px bins  (C2 recheck)

Read-only vs Postgres. Prints tables, saves nothing.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.service.state_bucket_track import (  # noqa: E402
    COSTS_ROUNDTRIP,
    R_WIDE_THRESHOLD,
    _board_limit_pct,
    _cached_day_features,
    _intraday_px,
    _same_1430_skip_reason,
    load_sgap_context,
)

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
}
RUNUP_BINS = ("<-3%", "-3~-1%", "-1~+1%", "+1~+3%")
LIMIT_BINS = ("<1%", "1~3%", "3~6%", ">6%")


def _runup_bin(r: float) -> str:
    if r < -0.03:
        return RUNUP_BINS[0]
    if r < -0.01:
        return RUNUP_BINS[1]
    if r < 0.01:
        return RUNUP_BINS[2]
    return RUNUP_BINS[3]


def _limit_bin(dist: float) -> str:
    if dist < 0.01:
        return LIMIT_BINS[0]
    if dist < 0.03:
        return LIMIT_BINS[1]
    if dist < 0.06:
        return LIMIT_BINS[2]
    return LIMIT_BINS[3]


def main() -> int:
    print("loading context ...", flush=True)
    ctx = load_sgap_context("2024-08-01", "2026-02-01")
    per_ts = ctx["per_ts"]
    date_idx = ctx["date_idx"]
    cal = ctx["cal"]
    idx_by_day = ctx["idx_by_day"]
    from datetime import date as _date

    acc: dict[str, dict[str, dict[str, list[float]]]] = {
        "runup": {w: {b: [] for b in RUNUP_BINS} for w in WINDOWS},
        "weekday": {w: {str(i): [] for i in range(5)} for w in WINDOWS},
        "limit": {w: {b: [] for b in LIMIT_BINS} for w in WINDOWS},
    }
    for w, (s, e) in WINDOWS.items():
        for day in cal:
            if day <= s or day > e:
                continue
            ei = idx_by_day.get(day, -1)
            if ei < 0 or ei + 2 >= len(cal):
                continue
            exit_day = cal[ei + 2]
            feat_all, breadth = _cached_day_features(ctx, day)
            if breadth <= R_WIDE_THRESHOLD:
                continue
            wd = str(_date.fromisoformat(day).weekday())
            for ts, d in feat_all.items():
                if not d.get("is_gap"):
                    continue
                di = date_idx.get(ts, {}).get(day, -1)
                series = per_ts.get(ts)
                if di < 0 or not series:
                    continue
                bar = series[di]
                px = _intraday_px(ctx, ts, day, "1430")
                reason = _same_1430_skip_reason(
                    ts=ts,
                    px=px,
                    open_px=bar.get("open"),
                    pre_close=bar.get("pre_close"),
                    skip_t1_limit=True,
                    max_open_to_1430_pct=0.03,
                    near_limit_buffer_pct=None,
                )
                if reason or not px or px <= 0:
                    continue
                px_exit = _intraday_px(ctx, ts, exit_day, "1430")
                if not px_exit or px_exit <= 0:
                    continue
                open_px = bar.get("open") or 0
                pre_close = bar.get("pre_close") or 0
                if open_px <= 0 or pre_close <= 0:
                    continue
                fwd = px_exit / px - 1 - COSTS_ROUNDTRIP
                runup = px / open_px - 1
                acc["runup"][w][_runup_bin(runup)].append(fwd)
                acc["weekday"][w][wd].append(fwd)
                lim = _board_limit_pct(ts)
                dist = (pre_close * (1 + lim) - px) / px
                acc["limit"][w][_limit_bin(dist)].append(fwd)

    def _show(title: str, table: dict[str, dict[str, list[float]]], cols: tuple[str, ...]) -> None:
        print(f"\n## {title}")
        print("| window | " + " | ".join(f"{c} mean/hit/n" for c in cols) + " |")
        print("|" + "|".join(["------"] * (1 + len(cols))) + "|")
        for w in WINDOWS:
            cells = []
            for c in cols:
                v = table[w][c]
                m = float(np.mean(v)) * 100 if v else 0.0
                hit = float(np.mean([1.0 if x > 0 else 0.0 for x in v])) * 100 if v else 0.0
                cells.append(f"{m:+.2f}%/{hit:.0f}%/{len(v)}")
            print(f"| {w} | " + " | ".join(cells) + " |")

    _show("runup bins (C3 downside?)", acc["runup"], RUNUP_BINS)
    _show("entry weekday 0=Mon..4=Fri", acc["weekday"], tuple(str(i) for i in range(5)))
    _show("distance to limit board (C2 recheck)", acc["limit"], LIMIT_BINS)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
