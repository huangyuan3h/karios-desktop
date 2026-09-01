#!/usr/bin/env python3
import sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData, simulate
from run_walk_forward import S3_CONFIG, HK_S3_CONFIG, WINDOWS
import pick_strong_grid as pg
from data_sync_service.service.state_bucket_track import load_sgap_context, replay_sgap_from_context
from data_sync_service.service.ps_g50_blend import blend_nav_opportunity
import numpy as np
PAST_YEAR = ("2025-08-28","2026-08-28")
ALL_WINDOWS = {**WINDOWS, "past_year": PAST_YEAR, "aligned": ("2025-08-28","2026-08-28")}
TEST_WINDOWS = ["OOS2","train","valid","past_year","aligned"]
FULL_START="2024-08-01"
FULL_END="2026-08-28"
import argparse
parser=argparse.ArgumentParser()
parser.add_argument("--variant", required=True)
parser.add_argument("--overrides", default="{}")
args=parser.parse_args()
overrides=json.loads(args.overrides)
vid=args.variant
print(f"=== Variant {vid} {overrides} ===", flush=True)
etf_close=pg.fetch_etf_closes()
# load S-gap context once
ctx=load_sgap_context(FULL_START, FULL_END)
print(" S-gap context loaded", flush=True)
# HK cache
hk_cache={}
for w in TEST_WINDOWS:
    s,e=ALL_WINDOWS[w]
    cfg_hk=BacktestConfig(start_date=s, end_date=e, **HK_S3_CONFIG)
    data_hk=BacktestData(cfg_hk)
    run_hk=simulate(cfg_hk, data_hk)
    hk_cache[w]={"data": data_hk, "snap": {str(r.get("date")): r for r in run_hk.positions_by_day}}
    print(f" HK warm {w} {run_hk.summary.closed}", flush=True)

def stats(nav):
    n=len(nav)
    if n<2 or not nav[0]:
        return {"total":0,"dd":0,"sharpe":0}
    total=(nav[-1]/nav[0]-1)*100
    peak=nav[0]
    mdd=0
    for v in nav:
        if v>peak: peak=v
        if peak: mdd=max(mdd,(peak-v)/peak*100)
    rets=[nav[i]/nav[i-1]-1 for i in range(1,n) if nav[i-1]>0]
    sharpe=0
    if len(rets)>10:
        std=float(np.std(rets))
        if std>0:
            sharpe=float(np.mean(rets)/std*(252**0.5))
    return {"total": total, "dd": mdd, "sharpe": sharpe}

for w in TEST_WINDOWS:
    s,e=ALL_WINDOWS[w]
    cfg_cn=BacktestConfig(start_date=s, end_date=e, **{**S3_CONFIG, **overrides})
    data_cn=BacktestData(cfg_cn)
    run_cn=simulate(cfg_cn, data_cn)
    # build fused core NAV
    snap_cn={str(r.get("date")): r for r in run_cn.positions_by_day}
    snap_hk=hk_cache[w]["snap"]
    hk_data=hk_cache[w]["data"]
    calendar=sorted(set(data_cn.calendar) | set(hk_data.calendar))
    close_by_ts={**data_cn.close_by_ts_day, **hk_data.close_by_ts_day}
    etf_ret={}
    for k, mp in etf_close.items():
        days=sorted(mp.keys())
        ret={}
        for i in range(1,len(days)):
            d,prev=days[i],days[i-1]
            if mp[prev]!=0:
                ret[d]=mp[d]/mp[prev]-1.0
        etf_ret[k]=ret
    cache={"calendar": calendar, "close_by_ts": close_by_ts, "etf_close": etf_close, "etf_ret": etf_ret, "snap_cn": snap_cn, "snap_hk": snap_hk, "ts_days": {ts: sorted(mp.keys()) for ts,mp in close_by_ts.items()}}
    fused = pg.build_nav_from_cache(cache, lookback=60, ma_window=200, min_hold=1, cost=0.0, score="mom", top2=False, trail_pct=8.0)
    # Build core nav series for twin: need daily nav series, not just fusedPct. Reconstruct via build_nav_from_cache nav map?
    # fused returns nav map per day; we can build list
    core_map = fused["nav"]
    # Build sat nav series
    sat = replay_sgap_from_context(ctx, start=s, end=e, skip_t1_limit=True, pool_mode="strict")
    # Align dates: core_map and sat rows share same calendar but sat may have different start; align to calendar intersection
    # Use calendar as common, fill forward
    core_nav = []
    sat_nav = []
    sat_active=[]
    # Build sat dict date->nav
    sat_dict={r["date"]: float(r["satNav"]) for r in sat["rows"]}
    sat_active_dict={r["date"]: bool(r["satActive"]) for r in sat["rows"]}
    last_core=1.0
    last_sat=1.0
    for d in calendar:
        if d < s or d > e:
            continue
        v=core_map.get(d)
        if v is not None:
            last_core=v
        core_nav.append(last_core)
        sv=sat_dict.get(d)
        if sv is not None:
            last_sat=sv
            # normalize sat to start 1?
            # sat already starts 1 per window, but our ctx is continuous, need window-local normalization
            # replay gives window-local starting 1, so OK
        sat_nav.append(last_sat)
        sa=sat_active_dict.get(d, False)
        sat_active.append(sa)
    # Normalize sat to start same as core (both 1)
    if sat_nav and sat_nav[0]!=0:
        base=sat_nav[0]
        sat_nav=[x/base for x in sat_nav]
    # Blend opportunity 50
    # Need equal length
    n=min(len(core_nav), len(sat_nav))
    core_n=core_nav[:n]
    sat_n=sat_nav[:n]
    active_n=sat_active[:n]
    twin_nav = blend_nav_opportunity(core_n, sat_n, active_n, sat_weight=0.5)
    cs=stats(core_n)
    ss=stats(sat_n)
    ts=stats(twin_nav)
    print(f" {w:10s} core {cs['total']:+6.1f}/{cs['sharpe']:.2f}/{cs['dd']:.1f} sat {ss['total']:+6.1f}/{ss['sharpe']:.2f}/{ss['dd']:.1f} twin {ts['total']:+6.1f}/{ts['sharpe']:.2f}/{ts['dd']:.1f} Δtwin-core {ts['total']-cs['total']:+.1f}pt", flush=True)
