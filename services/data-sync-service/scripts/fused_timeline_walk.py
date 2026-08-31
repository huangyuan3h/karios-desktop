#!/usr/bin/env python3
"""Fused single-track walk-forward: A+H stock basket vs GOLD/OIL/NASDAQ/BOND/REPO 100% hard switch.

Hard accurate (no sampling), reuses BacktestData/BacktestConfig for CN & HK.
Stock basket = union of CN S-3 + HK S-3 open positions at t-1 (each 10% sleeve, return = weighted avg).
Multi ETFs = 518880/513350/513110/511260 + GC001 repo.

Pick modes:
 - hard_stock: if any stock position exists at t-1 -> STOCK (current CN-only logic)
 - mom_compare: argmax among STOCK momAvg vs each ETF mom60 (>MA200), STOCK mom = avg mom60 of held stocks.

Grid: LOOKBACK 20/40/60/90 x MA 120/200 x mode hard/mom
Default run: 3 windows OOS2/train/valid; --windows past_year,long also available.

One config prints markdown row for tri-window verdict.
"""
import sys, json, psycopg
from pathlib import Path
from datetime import datetime, UTC
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"src"))
from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData, simulate
from data_sync_service.config import get_settings
from run_walk_forward import S3_CONFIG, HK_S3_CONFIG, WINDOWS

PAST_YEAR = ("2025-08-01","2026-08-21")
LONG = ("2021-08-01","2026-08-21")
ALL_WINDOWS = {**WINDOWS, "past_year": PAST_YEAR, "long": LONG}

MULTI_TS = {"GOLD":"518880.SH","OIL":"513350.SH","NASDAQ":"513110.SH","BOND10":"511260.SH"}
REPO_TS = "GC001.SH"  # placeholder, use 0 if missing

def fetch_etf_closes():
    s=get_settings(); conn=psycopg.connect(s.database_url); cur=conn.cursor()
    out={}
    for key, ts in MULTI_TS.items():
        cur.execute("select trade_date, close from daily where ts_code=%s order by trade_date", (ts,))
        rows=cur.fetchall()
        out[key]={str(r[0]): float(r[1]) for r in rows if r[1] is not None}
    # repo GC001 if exists else zero
    try:
        cur.execute("select trade_date, close from daily where ts_code=%s order by trade_date", (REPO_TS,))
        rows=cur.fetchall()
        out["REPO"]={str(r[0]): float(r[1]) for r in rows if r[1] is not None}
    except: out["REPO"]={}
    conn.close()
    return out

def build_fused_nav(cfg_cn, cfg_hk, etf_close, lookback=60, ma_window=200, mode="hard_stock"):
    # simulate both markets
    data_cn = BacktestData(cfg_cn)
    run_cn = simulate(cfg_cn, data_cn)
    data_hk = BacktestData(cfg_hk)
    run_hk = simulate(cfg_hk, data_hk)
    # calendar = union of both calendars (should be same SSE/HK? use CN calendar as master, HK supplement)
    calendar = sorted(set(data_cn.calendar) | set(data_hk.calendar))
    day_idx={d:i for i,d in enumerate(calendar)}
    # close maps
    close_by_ts = {**data_cn.close_by_ts_day, **data_hk.close_by_ts_day}
    # etf ret map
    etf_ret={}
    for k, mp in etf_close.items():
        days=sorted(mp.keys())
        ret={}
        for i in range(1,len(days)):
            d=days[i]; prev=days[i-1]
            if mp[prev]!=0: ret[d]=mp[d]/mp[prev]-1
        etf_ret[k]=ret
    # positions by day snapshots
    snap_cn={str(s.get("date")): s for s in run_cn.positions_by_day}
    snap_hk={str(s.get("date")): s for s in run_hk.positions_by_day}
    # precompute stock mom per stock per day: need daily closes per ts_code for mom calc
    # Build ts_close ordered for mom: use close_by_ts day dict -> sorted
    ts_days_close={}
    for ts, mp in close_by_ts.items():
        days=sorted(mp.keys())
        # keep mp as dict for fast lookup
        ts_days_close[ts]=(days, mp)
    # helper to get mom60 for a ts at day prev
    def mom60_at(ts, prev_day):
        mp=close_by_ts.get(ts)
        if not mp: return None
        days, _ = ts_days_close.get(ts, (None, None))
        # find index
        try: pi=days.index(prev_day)
        except: return None
        if pi < lookback: return None
        prev_close=mp.get(prev_day)
        ago_close=mp.get(days[pi-lookback])
        if not prev_close or not ago_close: return None
        return prev_close/ago_close -1
    # iterate days to compute fused single-track NAV
    nav_fused=1.0
    nav_base=1.0  # stock-only fused baseline (100% to stock basket when any stock held, else 0/repo? base = stock basket deployed)
    nav_multi_sleeve=1.0  # old sleeve (idle enhancement) for comparison? skip
    nav_map={}
    if calendar:
        nav_map[calendar[0]]=1.0
    peak_fused=1.0
    max_dd_fused=0.0
    peak_base=1.0
    max_dd_base=0.0
    for idx, day in enumerate(calendar):
        if idx==0: continue
        prev=calendar[idx-1]
        # collect open stock positions at prev (entry_date < day)
        stock_poses=[]
        for snap in (snap_cn.get(prev), snap_hk.get(prev)):
            if not snap: continue
            for pos in snap.get("positions") or []:
                entry=str(pos.get("entry_date") or "")
                if entry and day <= entry: continue
                # need ts_code for return/mom
                ts=str(pos.get("ts_code") or "")
                stock_poses.append(pos)
        # stock basket return at day = avg ret of held positions (equal weight) ; if none, 0
        stock_rets=[]
        stock_moms=[]
        for pos in stock_poses:
            ts=str(pos.get("ts_code") or "")
            closes=close_by_ts.get(ts) or {}
            today=closes.get(day)
            prev_c=closes.get(prev)
            if today and prev_c and prev_c!=0:
                stock_rets.append(today/prev_c-1)
            # mom for pick comparison
            m=mom60_at(ts, prev)
            if m is not None: stock_moms.append(m)
        stock_ret = sum(stock_rets)/len(stock_rets) if stock_rets else 0.0
        stock_mom = sum(stock_moms)/len(stock_moms) if stock_moms else -1e9
        # ETF mom/MA at prev
        etf_mom={}
        etf_above={}
        for k in MULTI_TS:
            mp=etf_close.get(k) or {}
            if prev not in mp: continue
            days_k=sorted(mp.keys())
            try: pi=days_k.index(prev)
            except: continue
            if pi < max(lookback, ma_window)-1: continue
            # MA
            ma=sum(mp[days_k[j]] for j in range(pi-ma_window+1, pi+1))/ma_window
            above=mp[prev] >= ma
            mom=mp[prev]/mp[days_k[pi-lookback]] -1 if mp[days_k[pi-lookback]]!=0 else -1e9
            etf_mom[k]=mom
            etf_above[k]=above
        filt={k:v for k,v in etf_mom.items() if etf_above.get(k)}
        # decide pick
        pick=None
        if mode=="hard_stock":
            if stock_poses:
                pick="STOCK"
            elif filt:
                pick=max(filt, key=lambda k: filt[k])
            else:
                pick="REPO"
        else: # mom_compare
            candidates={}
            if stock_poses:
                candidates["STOCK"]=stock_mom
            for k,v in filt.items(): candidates[k]=v
            if candidates:
                pick=max(candidates, key=lambda k: candidates[k])
            else:
                pick="REPO"
        # fused return = 100% to pick
        if pick=="STOCK":
            fused_ret=stock_ret
        elif pick=="REPO":
            fused_ret=etf_ret.get("REPO",{}).get(day, 0.0) * 0.0 + 0.00004  # GC001 ~1.5%年化 ≈0.004%/日, 简化0
            fused_ret=0.0
        else:
            fused_ret=etf_ret.get(pick,{}).get(day, 0.0)
        # base = stock-only (when stock exists, 100% stock basket else 0)
        base_ret=stock_ret if stock_poses else 0.0
        nav_fused*=1+fused_ret - 0.0005*0.0  # cost already in stock simulate? ETF cost 0.05% on switch not modeled yet
        nav_base*=1+base_ret
        peak_fused=max(peak_fused, nav_fused)
        peak_base=max(peak_base, nav_base)
        if peak_fused>0: max_dd_fused=max(max_dd_fused, (peak_fused-nav_fused)/peak_fused)
        if peak_base>0: max_dd_base=max(max_dd_base, (peak_base-nav_base)/peak_base)
        nav_map[day]=nav_fused
    return {
        "fusedPct": round((nav_fused-1)*100,2),
        "basePct": round((nav_base-1)*100,2),
        "deltaPct": round((nav_fused-nav_base)*100,2) if False else round((nav_fused-1)*100 - (nav_base-1)*100,2),
        "maxDdFusedPct": round(max_dd_fused*100,1),
        "maxDdBasePct": round(max_dd_base*100,1),
        "calendarDays": len(calendar),
        "nav": nav_map,
    }

def main():
    import argparse
    ap=argparse.ArgumentParser()
    ap.add_argument("--windows", default="OOS2,train,valid", help="comma windows")
    ap.add_argument("--lookback", type=int, default=60)
    ap.add_argument("--ma", type=int, default=200)
    ap.add_argument("--mode", choices=["hard_stock","mom_compare"], default="mom_compare")
    ap.add_argument("--grid", action="store_true", help="run 12-grid batch")
    ap.add_argument("--json", default="", help="output json path")
    args=ap.parse_args()
    etf_close=fetch_etf_closes()
    windows=[w.strip() for w in args.windows.split(",") if w.strip()]
    if args.grid:
        combos=[]
        for lb in [20,40,60,90]:
            for ma in [120,200]:
                for mode in ["hard_stock","mom_compare"]:
                    combos.append((lb,ma,mode))
        print(f"| 窗口 | LOOKBACK | MA | mode | fused% | base% | delta vs base | fusedDD | baseDD |")
        print(f"|------|----------|----|------|--------|-------|-------------|---------|--------|")
        results={}
        for lb,ma,mode in combos:
            row={}
            for w in windows:
                start,end=ALL_WINDOWS[w]
                cfg_cn=BacktestConfig(start_date=start, end_date=end, **S3_CONFIG)
                cfg_hk=BacktestConfig(start_date=start, end_date=end, **HK_S3_CONFIG)
                r=build_fused_nav(cfg_cn,cfg_hk, etf_close, lookback=lb, ma_window=ma, mode=mode)
                row[w]=r
            # print aggregated past? just train/valid?
            # Use valid delta as sorting
            vd=row.get("valid",{})
            print(f"| {','.join(windows):6s} | {lb:4d} | {ma:3d} | {mode:11s} | {vd.get('fusedPct',0):5.1f} | {vd.get('basePct',0):5.1f} | {vd.get('deltaPct',0):+5.1f} | {vd.get('maxDdFusedPct',0):4.1f} | {vd.get('maxDdBasePct',0):4.1f} |")
        return 0
    # single
    for w in windows:
        start,end=ALL_WINDOWS[w]
        cfg_cn=BacktestConfig(start_date=start, end_date=end, **S3_CONFIG)
        cfg_hk=BacktestConfig(start_date=start, end_date=end, **HK_S3_CONFIG)
        r=build_fused_nav(cfg_cn,cfg_hk, etf_close, lookback=args.lookback, ma_window=args.ma, mode=args.mode)
        print(f"[{w}] {start}..{end} lb{args.lookback} ma{args.ma} {args.mode} fused {r['fusedPct']}% base {r['basePct']}% delta {r['deltaPct']:+.1f}% DD {r['maxDdFusedPct']}%")
    return 0

if __name__=="__main__":
    raise SystemExit(main())
