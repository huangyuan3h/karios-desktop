#!/usr/bin/env python3
"""Realistic twin-star: mom_compare core (HK settle2+slip0.25) + habit sat, opp 50/50 blend.

Compares frozen twin vs realistic twin per window (OOS2/train/valid/past_year).
Read-only, saves nothing.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_walk_forward import HK_S3_CONFIG, S3_CONFIG, WINDOWS  # noqa: E402

from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    simulate,
)
from data_sync_service.service.pick_strong_track import (  # noqa: E402
    build_mom_compare_timeline,
    build_twin_star_timeline,
    fetch_etf_closes,
)
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    load_sgap_context,
    replay_sgap_from_context,
)

REAL = {"settle_lock_sessions": 2, "slippage_pct": 0.25}
WINDS = {**WINDOWS, "past_year": ("2025-08-01", "2026-08-07")}

HABIT = dict(
    skip_t1_limit=True, pool_mode="strict", max_pos=4, position_pct=0.25,
    fill_mode=FILL_SAME_1430, fill_hhmm="1430", exit_hhmm="1430",
    max_open_to_1430_pct=0.03,
)


def _merge(cn_run, cn_data, hk_run, hk_data):
    hk_by_day = {str(x.get("date")): x for x in hk_run.positions_by_day}
    merged = []
    for x in cn_run.positions_by_day:
        day = str(x.get("date"))
        poses = list(x.get("positions") or [])
        hk_x = hk_by_day.get(day)
        if hk_x:
            poses = poses + list(hk_x.get("positions") or [])
        merged.append({"date": day, "positions": poses})
    closes = dict(cn_data.close_by_ts_day)
    for ts, mp in hk_data.close_by_ts_day.items():
        if ts not in closes:
            closes[ts] = mp
        else:
            closes[ts].update(mp)
    cal = sorted(set(cn_data.calendar) | set(hk_data.calendar))
    return merged, closes, cal


def main() -> int:
    etf_close = fetch_etf_closes()
    print("loading sgap context ...", flush=True)
    ctx = load_sgap_context("2024-08-01", "2026-08-07")
    print("window | twin | fused% | core% | base% | maxDD | satDays")
    for w, (s, e) in WINDS.items():
        print(f"--- {w} ({s}~{e}) ...", flush=True)
        cfg = BacktestConfig(start_date=s, end_date=e, **S3_CONFIG)
        cn_data = BacktestData(cfg)
        cn_run = simulate(cfg, cn_data)
        cfg_hk = BacktestConfig(start_date=s, end_date=e, **HK_S3_CONFIG)
        hk_data = BacktestData(cfg_hk)
        hk_run = simulate(cfg_hk, hk_data)
        cfg_hr = BacktestConfig(start_date=s, end_date=e, **{**HK_S3_CONFIG, **REAL})
        hk_data_r = BacktestData(cfg_hr)
        hk_run_r = simulate(cfg_hr, hk_data_r)
        sat = replay_sgap_from_context(ctx, start=s, end=e, **HABIT)
        for name, hkr, hkd in (("frozen ", hk_run, hk_data), ("real   ", hk_run_r, hk_data_r)):
            merged, closes, cal = _merge(cn_run, cn_data, hkr, hkd)
            core = build_mom_compare_timeline(
                calendar=cal, positions_by_day=merged,
                close_by_ts_day=closes, etf_close=etf_close,
            )
            twin = build_twin_star_timeline(
                core_rows=core["rows"], core_summary=core["summary"],
                sat_rows=sat["rows"],
            )
            sm = twin["summary"]
            print(
                f"{w:9s} | {name} | {sm['fusedPct']:+8.1f} | {sm['corePct']:+8.1f} | "
                f"{sm['basePct']:+8.1f} | {sm['maxDdFusedPct']:5.1f} | {sm['satActiveDays']}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
