#!/usr/bin/env python3
"""Walk-forward for multi-asset rotation sleeve vs single NASDAQ vs baseline.

Compares:
 - base S-3 only (idle 0%)
 - single NASDAQ sleeve (T6 513100 + MA200)
 - multi-asset rotation sleeve (GOLD/OIL/NASDAQ/BOND10 mom60+MA200)

Usage: PYTHONPATH=src python3 scripts/multi_sleeve_walk_forward.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"src"))

from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData, simulate
from data_sync_service.service.portfolio_nav_sim import simulate_sleeve_nav, load_third_asset_cache
import json, psycopg
from data_sync_service.config import get_settings

# S3 config from run_walk_forward
from run_walk_forward import S3_CONFIG, WINDOWS

# Additional window: past year 2025-08-01~2026-08-21 (approx)
PAST_YEAR = ("2025-08-01", "2026-08-21")
ALL_WINDOWS = {**WINDOWS, "past_year": PAST_YEAR, "long": ("2021-08-01","2026-08-21")}

def load_multi_candidates():
    """Load daily closes for multi candidates from DB."""
    s=get_settings()
    conn=psycopg.connect(s.database_url)
    cur=conn.cursor()
    cands=["518880.SH","513350.SH","513100.SH","511260.SH"]
    out={}
    for ts in cands:
        cur.execute("select trade_date, close from daily where ts_code=%s order by trade_date", (ts,))
        rows=cur.fetchall()
        m={str(r[0]): float(r[1]) for r in rows}
        out[ts]=m
    conn.close()
    return out

def simulate_multi_sleeve(positions_by_day, close_by_ts_day, calendar, multi_close_by_ts, min_idle=20.0):
    """Daily pick = max mom60 among above MA200; hold next day."""
    MA_WINDOW=200
    LOOKBACK=60
    # Precompute MA200 and mom60 per candidate per day
    # Build etf_close_by_day for each candidate but pick logic needs combined
    # Simulate NAV similar to portfolio_nav_sim but sleeve_ret = return of picked asset (or repo if none)
    # For simplicity repo = 0 (cash) when no pick
    # Returns summary like simulate_sleeve_nav
    # Build daily pick map
    # Need closes list per candidate
    closes_lists={}
    for ts, mp in multi_close_by_ts.items():
        # mp is dict day->close, need ordered list
        sorted_days=sorted(mp.keys())
        closes_lists[ts]= (sorted_days, mp)
    # Precompute pick per calendar day (using t-1)
    pick_by_day={}
    for idx, day in enumerate(calendar):
        if idx==0: continue
        prev_day=calendar[idx-1]
        mom={}
        above={}
        for ts in multi_close_by_ts:
            mp=multi_close_by_ts[ts]
            # need 260 bars ending at prev_day
            # find closes up to prev_day
            # collect closes list
            days_sorted=sorted(mp.keys())
            # find index of prev_day
            try:
                pi=days_sorted.index(prev_day)
            except ValueError:
                continue
            if pi < MA_WINDOW+LOOKBACK-1: continue
            window_closes=[mp[days_sorted[j]] for j in range(pi-MA_WINDOW+1, pi+1)]
            ma200=sum(window_closes[-MA_WINDOW:])/MA_WINDOW
            close_t1=mp[prev_day]
            mom60=close_t1 / mp[days_sorted[pi-LOOKBACK]] -1
            mom[ts]=mom60
            above[ts]=close_t1 >= ma200
        filtered={k:v for k,v in mom.items() if above.get(k)}
        if filtered:
            pick_by_day[day]=max(filtered, key=lambda k: filtered[k])
        else:
            pick_by_day[day]=None
    # Now simulate NAV
    # Build daily ret map per ts
    ret_by_ts={}
    for ts, mp in multi_close_by_ts.items():
        days_sorted=sorted(mp.keys())
        ret={}
        for i in range(1, len(days_sorted)):
            d=days_sorted[i]
            prev=days_sorted[i-1]
            if mp[prev]!=0:
                ret[d]=mp[d]/mp[prev]-1
        ret_by_ts[ts]=ret
    snap_by_day={str(s.get("date")): s for s in positions_by_day}
    day_idx={d:i for i,d in enumerate(calendar)}
    nav_base=1.0
    nav_multi=1.0
    max_dd_base=0.0
    max_dd_multi=0.0
    peak_base=1.0
    peak_multi=1.0
    for day in calendar:
        snap=snap_by_day.get(day)
        deployed_ret=0.0
        deployed_pct=0.0
        if snap:
            for pos in snap.get("positions") or []:
                try: pct=float(pos.get("position_pct") or 0)
                except: continue
                if pct<=0: continue
                entry=str(pos.get("entry_date") or "")
                if entry and day <= entry: continue
                closes=close_by_ts_day.get(str(pos.get("ts_code") or "")) or {}
                today=closes.get(day)
                idx=day_idx.get(day)
                prev=closes.get(calendar[idx-1]) if idx and idx>0 else None
                if today is not None and prev:
                    deployed_ret += pct*(today/prev-1)
                deployed_pct+=pct
        deployed_pct=min(1.0, deployed_pct)
        idle_pct=max(0.0, 1.0-deployed_pct)
        pick_ts=pick_by_day.get(day)
        sleeve_ret=0.0
        if idle_pct>0 and pick_ts:
            # check min_idle
            if idle_pct*100 >= min_idle:
                sleeve_ret=ret_by_ts.get(pick_ts, {}).get(day, 0.0)
        nav_base*=1+deployed_ret
        nav_multi*=1+deployed_ret+idle_pct*sleeve_ret
        peak_base=max(peak_base, nav_base)
        peak_multi=max(peak_multi, nav_multi)
        if peak_base>0: max_dd_base=max(max_dd_base, (peak_base-nav_base)/peak_base)
        if peak_multi>0: max_dd_multi=max(max_dd_multi, (peak_multi-nav_multi)/peak_multi)
    return {
        "totalBasePct": round((nav_base-1)*100,1),
        "totalMultiPct": round((nav_multi-1)*100,1),
        "deltaPct": round((nav_multi-nav_base)*100,1),
        "maxDdBasePct": round(max_dd_base*100,1),
        "maxDdMultiPct": round(max_dd_multi*100,1),
    }

def main():
    multi_close=load_multi_candidates()
    # load single etf cache for comparison
    import json, pathlib
    cache_path=Path("data/third_asset_cache.json")
    etf_close, repo_rate=None, None
    if cache_path.exists():
        cache=json.loads(cache_path.read_text())
        from data_sync_service.service.portfolio_nav_sim import load_third_asset_cache
        etf_close, repo_rate = load_third_asset_cache(cache)
    else:
        etf_close, repo_rate = {}, {}
    windows=["OOS2","train","valid","past_year"]
    print("| 窗口 | 基线% | 单纳指套筒% | 多资产轮动% | 单增量 | 多增量 | 基线DD | 多DD |")
    print("|------|-------|------------|-----------|--------|--------|--------|------|")
    for w in windows:
        start,end = ALL_WINDOWS[w]
        cfg=BacktestConfig(start_date=start, end_date=end, **S3_CONFIG)
        data=BacktestData(cfg)
        run=simulate(cfg, data)
        # single sleeve
        from data_sync_service.service.portfolio_nav_sim import simulate_sleeve_nav
        single=simulate_sleeve_nav(positions_by_day=run.positions_by_day, close_by_ts_day=data.close_by_ts_day, calendar=data.calendar, etf_close_by_day=etf_close, repo_rate_by_day=repo_rate, min_idle_pct=20.0)
        multi=simulate_multi_sleeve(run.positions_by_day, data.close_by_ts_day, data.calendar, multi_close, min_idle=20.0)
        s=single["summary"]
        print(f"| {w:8s} | {s['totalBasePct']:5.1f} | {s['totalSleevePct']:5.1f} | {multi['totalMultiPct']:5.1f} | {s['deltaPct']:+4.1f} | {multi['deltaPct']:+4.1f} | {s['maxDdBasePct']:4.1f} | {multi['maxDdMultiPct']:4.1f} |")
    # also long if possible (may be heavy)
    # try long but with less calendar? skip if too heavy
    # Done

if __name__=="__main__":
    main()
