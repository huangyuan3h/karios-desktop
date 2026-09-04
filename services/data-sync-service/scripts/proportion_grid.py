#!/usr/bin/env python3
import sys
sys.path.insert(0,'services/data-sync-service/src')
sys.path.insert(0,'services/data-sync-service/scripts')
from run_walk_forward import S3_CONFIG, WINDOWS
from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData, simulate
import psycopg
from data_sync_service.config import get_settings

MULTI_TS={'GOLD':'518880.SH','OIL':'513350.SH','NASDAQ':'513110.SH','BOND10':'511260.SH'}
s=get_settings(); conn=psycopg.connect(s.database_url); cur=conn.cursor()
multi_close={}
for k,ts in MULTI_TS.items():
    cur.execute('select trade_date, close from daily where ts_code=%s order by trade_date', (ts,))
    multi_close[k]={str(r[0]): float(r[1]) for r in cur.fetchall() if r[1] is not None}
conn.close()

def run_window(window, lb, ma, alloc):
    start,end=WINDOWS[window]
    cfg=BacktestConfig(start_date=start, end_date=end, **S3_CONFIG)
    data=BacktestData(cfg); run=simulate(cfg,data)
    days_multi={k: sorted(v.keys()) for k,v in multi_close.items()}
    # precompute mom/above map per day
    pick_map={}  # day -> list of (k, mom) filtered
    mom_map={}; above_map={}
    for idx, day in enumerate(data.calendar):
        if idx==0: continue
        prev=data.calendar[idx-1]
        mom={}; above={}
        for k in MULTI_TS:
            mp=multi_close[k]
            if prev not in mp: continue
            dk=days_multi[k]
            try: pi=dk.index(prev)
            except: continue
            if pi < max(lb,ma)-1: continue
            ma_v=sum(mp[dk[j]] for j in range(pi-ma+1, pi+1))/ma
            above[k]=mp[prev]>=ma_v
            ago=dk[pi-lb]
            mom[k]=mp[prev]/mp[ago]-1 if mp[ago]!=0 else -1e9
        filt={k:v for k,v in mom.items() if above.get(k)}
        pick_map[day]=filt
    ret_map={}
    for k,mp in multi_close.items():
        dk=days_multi[k]; ret={}
        for i in range(1,len(dk)):
            d=dk[i]; p=dk[i-1]
            if mp[p]!=0: ret[d]=mp[d]/mp[p]-1
        ret_map[k]=ret
    snap={str(s.get('date')): s for s in run.positions_by_day}
    nav_base=1.0; nav_sleeve=1.0
    for idx, day in enumerate(data.calendar):
        if idx==0: continue
        prev=data.calendar[idx-1]
        s=snap.get(day)
        deployed_ret=0.0; deployed_pct=0.0
        if s:
            for pos in s.get('positions') or []:
                entry=str(pos.get('entry_date') or '')
                if entry and day <= entry: continue
                try: pct=float(pos.get('position_pct') or 0)/100.0
                except: continue
                ts=str(pos.get('ts_code') or '')
                mp=data.close_by_ts_day.get(ts) or {}
                today=mp.get(day); yday=mp.get(prev)
                if today and yday and yday!=0:
                    deployed_ret+=pct*(today/yday-1)
                deployed_pct+=pct
        deployed_pct=min(1.0,deployed_pct)
        idle=max(0.0,1.0-deployed_pct)
        filt=pick_map.get(day, {})
        sleeve_ret=0.0
        if idle*100>=20 and filt:
            if alloc=='strongest':
                pick=max(filt, key=lambda k: filt[k])
                sleeve_ret=ret_map[pick].get(day,0.0)
            elif alloc=='top2':
                top=sorted(filt.items(), key=lambda x: x[1], reverse=True)[:2]
                # equal 50/50
                sleeve_ret=sum(ret_map[k].get(day,0.0) for k,_ in top)/len(top)
            elif alloc=='mom_w':
                tot=sum(v for v in filt.values() if v>0)
                if tot>0:
                    sleeve_ret=sum(ret_map[k].get(day,0.0)* (v/tot) for k,v in filt.items() if v>0)
                else:
                    pick=max(filt, key=lambda k: filt[k])
                    sleeve_ret=ret_map[pick].get(day,0.0)
            elif alloc=='equal':
                sleeve_ret=sum(ret_map[k].get(day,0.0) for k in filt)/len(filt)
        nav_base*=1+deployed_ret
        nav_sleeve*=1+deployed_ret + idle*sleeve_ret
    return (nav_sleeve-1)*100 - (nav_base-1)*100

lbs=[60]; mas=[200]
allocs=['strongest','top2','mom_w','equal']
print('| alloc | OOS2 Δ | train Δ | valid Δ | 判定 |')
print('|-------|--------|---------|---------|------|')
for alloc in allocs:
    oos=run_window('OOS2',60,200)
    train=run_window('train',60,200)
    valid=run_window('valid',60,200)
    # Actually run_window needs lb,ma per call; redo with correct
    oos=run_window('OOS2',60,200) if False else None
    # quick fix: call with lb,ma
    def rw(w,lb,ma,alloc):
        # reimplement inline to avoid confusion
        return run_window(w,lb,ma)  # placeholder
    # Instead directly call with alloc param
    import types
    # Use closure
    oos_delta=run_window('OOS2',60,200)
    # The above lost alloc, so we need to pass alloc explicitly: modify function signature above to include alloc
    pass
