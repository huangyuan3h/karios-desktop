#!/usr/bin/env python3
"""Grid search for multi-asset sleeve expectation (idle enhancement, no stock sell).

Idle sleeve: when S-3 deployed <80% (idle>=20%), buy max mom60>MA among GOLD/OIL/NASDAQ/BOND.
Param grid: LOOKBACK 20/40/60/90/120 x MA 120/200/250 x COST 0.0005

Metric: delta vs base (idle=0) per window OOS2/train/valid, require tri-window all >0 and stable.

Uses BacktestData/BacktestData for CN only (S-3 A股), same as live sleeve (HK sleeve separate).
"""
import sys, json, psycopg
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"src"))
from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData, simulate
from data_sync_service.config import get_settings
from run_walk_forward import S3_CONFIG, WINDOWS

MULTI_TS = {"GOLD":"518880.SH","OIL":"513350.SH","NASDAQ":"513110.SH","BOND10":"511260.SH"}

def fetch_multi():
    s=get_settings(); conn=psycopg.connect(s.database_url); cur=conn.cursor()
    out={}
    for k, ts in MULTI_TS.items():
        cur.execute("select trade_date, close from daily where ts_code=%s order by trade_date", (ts,))
        out[k]={str(r[0]): float(r[1]) for r in cur.fetchall() if r[1] is not None}
    conn.close()
    return out

def simulate_sleeve_multi(positions_by_day, calendar, multi_close, lookback, ma_window, min_idle=20.0, cost=0.0005):
    # pick per day using t-1 mom/MA, returns per day of picked ETF
    # Build ret map
    ret_map={}
    for k, mp in multi_close.items():
        days=sorted(mp.keys())
        ret={}
        for i in range(1,len(days)):
            d=days[i]; prev=days[i-1]
            if mp[prev]!=0: ret[d]=mp[d]/mp[prev]-1
        ret_map[k]=ret
    snap={str(s.get("date")): s for s in positions_by_day}
    day_idx={d:i for i,d in enumerate(calendar)}
    nav_base=1.0; nav_sleeve=1.0; peak_base=1.0; peak_sleeve=1.0; max_dd_base=0; max_dd_sleeve=0
    for idx, day in enumerate(calendar):
        if idx==0: continue
        prev=calendar[idx-1]
        # deployed
        snap_t=snap.get(day)
        deployed_ret=0.0; deployed_pct=0.0
        if snap_t:
            for pos in snap_t.get("positions") or []:
                entry=str(pos.get("entry_date") or "")
                if entry and day <= entry: continue
                try: pct=float(pos.get("position_pct") or 0)
                except: continue
                if pct<=0: continue
                # need close for this pos's ts_code to calc ret; use close_by_ts from BacktestData? Instead approximate via positions return? Simpler approximate deployed_ret via summary? But we need per-pos return.
                # Fallback: use nav_base from run summary? Instead we approximate base as sum pct*stock daily ret via close_by_ts; need that map.
                # For now we use 0 for base (will be replaced by run.total) – we need close_by_ts.
                pass
        # Actually we need stock daily ret map – we will pass close_by_ts_day in outer call.
        # placeholder, will be filled by caller that has close_by_ts_day
        pass
    return {}

def run_window(window, lookback, ma_window):
    start,end=WINDOWS[window]
    cfg=BacktestConfig(start_date=start, end_date=end, **S3_CONFIG)
    data=BacktestData(cfg)
    run=simulate(cfg, data)
    # now sleeve sim with given params
    from data_sync_service.service.portfolio_nav_sim import simulate_sleeve_nav
    # single-asset sleeve sim is for 513100 only, not multi. For multi we call fused helper with idle logic.
    # Reuse fused's idle sleeve logic but with 100% idle -> best ETF.
    multi_close=fetch_multi()
    # Build ETF ret & pick map
    days_multi={}
    for k, mp in multi_close.items():
        days_multi[k]=sorted(mp.keys())
    # precompute pick per calendar day
    pick_by_day={}
    for idx, day in enumerate(data.calendar):
        if idx==0: continue
        prev=data.calendar[idx-1]
        mom={}; above={}
        for k, ts in MULTI_TS.items():
            mp=multi_close[k]
            if prev not in mp: continue
            days_k=days_multi[k]
            try: pi=days_k.index(prev)
            except: continue
            if pi < max(lookback, ma_window)-1: continue
            ma=sum(mp[days_k[j]] for j in range(pi-ma_window+1, pi+1))/ma_window
            above[k]=mp[prev] >= ma
            ago=days_k[pi-lookback]
            mom[k]=mp[prev]/mp[ago]-1 if mp[ago]!=0 else -1e9
        filt={k:v for k,v in mom.items() if above.get(k)}
        if filt:
            pick_by_day[day]=max(filt, key=lambda k: filt[k])
        else:
            pick_by_day[day]=None
    # ret map
    ret_map={}
    for k, mp in multi_close.items():
        days_k=days_multi[k]
        ret={}
        for i in range(1,len(days_k)):
            d=days_k[i]; prev=days_k[i-1]
            if mp[prev]!=0: ret[d]=mp[d]/mp[prev]-1
        ret_map[k]=ret
    snap={str(s.get("date")): s for s in run.positions_by_day}
    nav_base=1.0; nav_sleeve=1.0; peak_base=1.0; peak_sleeve=1.0; max_dd_base=0; max_dd_sleeve=0
    for idx, day in enumerate(data.calendar):
        if idx==0: continue
        prev=data.calendar[idx-1]
        s=snap.get(day)
        deployed_ret=0.0; deployed_pct=0.0
        if s:
            for pos in s.get("positions") or []:
                entry=str(pos.get("entry_date") or "")
                if entry and day <= entry: continue
                try: pct=float(pos.get("position_pct") or 0)/100.0
                except: continue
                if pct<=0: continue
                ts=str(pos.get("ts_code") or "")
                mp=data.close_by_ts_day.get(ts) or {}
                today=mp.get(day); yday=mp.get(prev)
                if today and yday and yday!=0:
                    deployed_ret += pct*(today/yday-1)
                deployed_pct+=pct
        deployed_pct=min(1.0, deployed_pct)
        idle=max(0.0, 1.0-deployed_pct)
        pick=pick_by_day.get(day)
        sleeve_ret=0.0
        if idle*100 >= 20.0 and pick:
            sleeve_ret=ret_map.get(pick,{}).get(day, 0.0)
        nav_base*=1+deployed_ret
        nav_sleeve*=1+deployed_ret + idle*sleeve_ret
        peak_base=max(peak_base, nav_base); peak_sleeve=max(peak_sleeve, nav_sleeve)
        if peak_base>0: max_dd_base=max(max_dd_base, (peak_base-nav_base)/peak_base)
        if peak_sleeve>0: max_dd_sleeve=max(max_dd_sleeve, (peak_sleeve-nav_sleeve)/peak_sleeve)
    base_pct=(nav_base-1)*100; sleeve_pct=(nav_sleeve-1)*100
    return {"basePct": round(base_pct,1), "sleevePct": round(sleeve_pct,1), "delta": round(sleeve_pct-base_pct,1), "maxDdBase": round(max_dd_base*100,1), "maxDdSleeve": round(max_dd_sleeve*100,1)}

if __name__=="__main__":
    import argparse
    ap=argparse.ArgumentParser()
    ap.add_argument("--lookback", type=int, default=60)
    ap.add_argument("--ma", type=int, default=200)
    args=ap.parse_args()
    multi_close=fetch_multi()
    # grid if no specific
    lbs=[20,40,60,90,120]
    mas=[120,200]
    print(f"| LB | MA | OOS2 base→sleeve Δ | train Δ | valid Δ | OOS2 DD | valid DD |")
    print(f"|----|----|-------------------|---------|---------|---------|----------|")
    for lb in lbs:
        for ma in mas:
            r_oos=run_window("OOS2", lb, ma)
            r_train=run_window("train", lb, ma)
            r_valid=run_window("valid", lb, ma)
            print(f"| {lb:3d} | {ma:3d} | {r_oos['basePct']:4.1f}→{r_oos['sleevePct']:4.1f} {r_oos['delta']:+4.1f} | {r_train['delta']:+4.1f} | {r_valid['delta']:+4.1f} | {r_oos['maxDdSleeve']:4.1f} | {r_valid['maxDdSleeve']:4.1f} |")
