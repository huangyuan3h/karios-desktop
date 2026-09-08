#!/usr/bin/env python3
"""STOCK basket: CN-only vs CN+HK (strip-HK attribution for mom_compare timeline).

Baseline rebuilds the route logic (CN S-3 run + HK S-3 run merged);
variant drops the HK merge (CN-only basket, same builder). Compares
summary fused/base/maxDD + pick mix per window. Read-only, saves nothing.

Universe change needs a three-window pass to even consider (single-window
good = reject). Live untouched.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_walk_forward import HK_S3_CONFIG, S3_CONFIG  # noqa: E402

from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    simulate,
)
from data_sync_service.service.pick_strong_track import (  # noqa: E402
    build_mom_compare_timeline,
    fetch_etf_closes,
)

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "past_year": ("2025-08-01", "2026-08-07"),
}


def _is_hk(ts: str) -> bool:
    return ts.endswith(".HK") or ts.startswith("HK")


def build_pair(s: str, e: str, etf_close: dict) -> tuple[dict, dict]:
    cfg = BacktestConfig(start_date=s, end_date=e, **S3_CONFIG)
    data = BacktestData(cfg)
    run = simulate(cfg, data)
    cfg_hk = BacktestConfig(start_date=s, end_date=e, **HK_S3_CONFIG)
    data_hk = BacktestData(cfg_hk)
    run_hk = simulate(cfg_hk, data_hk)
    hk_by_day = {str(x.get("date")): x for x in run_hk.positions_by_day}
    merged = []
    for x in run.positions_by_day:
        day = str(x.get("date"))
        hk_x = hk_by_day.get(day)
        poses = list(x.get("positions") or [])
        if hk_x:
            poses = poses + list(hk_x.get("positions") or [])
        merged.append({"date": day, "positions": poses})
    closes = dict(data.close_by_ts_day)
    for ts, mp in data_hk.close_by_ts_day.items():
        if ts not in closes:
            closes[ts] = mp
        else:
            closes[ts].update(mp)
    cal = sorted(set(data.calendar) | set(data_hk.calendar))
    base = build_mom_compare_timeline(
        calendar=cal, positions_by_day=merged, close_by_ts_day=closes,
        etf_close=etf_close,
    )
    cn_only_snaps = []
    for x in run.positions_by_day:
        poses = [p for p in (x.get("positions") or [])
                 if not _is_hk(str(p.get("ts_code") or ""))]
        cn_only_snaps.append({"date": str(x.get("date")), "positions": poses})
    var = build_mom_compare_timeline(
        calendar=sorted(data.calendar), positions_by_day=cn_only_snaps,
        close_by_ts_day=data.close_by_ts_day, etf_close=etf_close,
    )
    return base, var


def _mix(rows: list[dict]) -> dict[str, int]:
    m = {"STOCK": 0, "ETF": 0, "REPO": 0, "STOCK_days_HKdom": 0}
    for r in rows:
        p = str(r.get("pick") or "")
        if p == "STOCK":
            m["STOCK"] += 1
            tot = int(r.get("positions") or 0)
            hk = int(r.get("hkPositions") or 0)
            if tot and hk * 2 >= tot:
                m["STOCK_days_HKdom"] += 1
        elif p == "REPO":
            m["REPO"] += 1
        else:
            m["ETF"] += 1
    return m


def main() -> int:
    etf_close = fetch_etf_closes()
    print("window | leg | fused% | base% | maxDD | picks(STOCK/ETF/REPO) | STOCK-HKdom-days")
    for w, (s, e) in WINDOWS.items():
        print(f"--- {w} ({s}~{e}) loading ...", flush=True)
        base, var = build_pair(s, e, etf_close)
        for name, t in (("A+H ", base), ("CN  ", var)):
            sm = t["summary"]
            print(
                f"{w:9s} | {name} | {sm['fusedPct']:+8.1f} | {sm['basePct']:+8.1f} | "
                f"{sm['maxDdFusedPct']:5.1f} | {_mix(t['rows'])}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
