#!/usr/bin/env python3
import sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData, simulate
from run_walk_forward import S3_CONFIG, HK_S3_CONFIG, WINDOWS
import pick_strong_grid as pg
import argparse
PAST_YEAR = ("2025-08-28", "2026-08-28")
ALL_WINDOWS = {**WINDOWS, "past_year": PAST_YEAR}
parser = argparse.ArgumentParser()
parser.add_argument("--variant", required=True)
parser.add_argument("--overrides", default="{}")
args = parser.parse_args()
overrides = json.loads(args.overrides)
vid = args.variant
print(f"=== Variant {vid} {overrides} ===", flush=True)
etf_close = pg.fetch_etf_closes()
# warm HK once
hk_cache = {}
for w in ["OOS2","train","valid","past_year"]:
    s,e = ALL_WINDOWS[w]
    cfg_hk = BacktestConfig(start_date=s, end_date=e, **HK_S3_CONFIG)
    data_hk = BacktestData(cfg_hk)
    run_hk = simulate(cfg_hk, data_hk)
    hk_cache[w] = {"data": data_hk, "run": run_hk, "snap": {str(r.get("date")): r for r in run_hk.positions_by_day}}
    print(f" HK warm {w} {run_hk.summary.closed}", flush=True)

for w in ["OOS2","train","valid","past_year"]:
    s,e = ALL_WINDOWS[w]
    cfg_cn = BacktestConfig(start_date=s, end_date=e, **{**S3_CONFIG, **overrides})
    data_cn = BacktestData(cfg_cn)
    run_cn = simulate(cfg_cn, data_cn)
    s3_total = run_cn.summary.total_net_pnl_pct
    s3_dd = run_cn.summary.max_drawdown_pct
    s3_sharpe = run_cn.summary.sharpe or 0
    # fused
    snap_cn = {str(r.get("date")): r for r in run_cn.positions_by_day}
    snap_hk = hk_cache[w]["snap"]
    hk_data = hk_cache[w]["data"]
    calendar = sorted(set(data_cn.calendar) | set(hk_data.calendar))
    close_by_ts = {**data_cn.close_by_ts_day, **hk_data.close_by_ts_day}
    etf_ret = {}
    for k, mp in etf_close.items():
        days = sorted(mp.keys())
        ret = {}
        for i in range(1, len(days)):
            d, prev = days[i], days[i-1]
            if mp[prev]!=0:
                ret[d]= mp[d]/mp[prev]-1.0
        etf_ret[k]=ret
    cache = {"calendar": calendar, "close_by_ts": close_by_ts, "etf_close": etf_close, "etf_ret": etf_ret, "snap_cn": snap_cn, "snap_hk": snap_hk, "ts_days": {ts: sorted(mp.keys()) for ts, mp in close_by_ts.items()}}
    fused = pg.build_nav_from_cache(cache, lookback=60, ma_window=200, min_hold=1, cost=0.0, score="mom", top2=False, trail_pct=8.0)
    print(f" {w:10s} S3 {s3_total:+6.1f}% dd{s3_dd:4.1f} sr{s3_sharpe:4.2f} n{run_cn.summary.closed:3d} | FUSED {fused['fusedPct']:+6.1f}% dd{fused['maxDdFusedPct']:4.1f} sw{fused['switches']:2d}", flush=True)
