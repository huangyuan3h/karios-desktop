#!/usr/bin/env python3
"""Daily-avg driven optimization — hold<50% (20% accepted), max daily avg. Baseline amp_q10 10d breadth>0.5."""
import sys
from datetime import date, timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from data_sync_service.db import get_connection

WINDOWS = {"OOS2": ("2024-08-01","2025-08-01"), "train": ("2025-08-01","2026-02-01"), "valid": ("2026-03-01","2026-08-07")}
UNIVERSE_MIN_MV = 20.0
UNIVERSE_MAX_MV = 80.0
POSITION_PCT = 0.10
MAX_POSITIONS = 10
COSTS_ROUNDTRIP = 0.003

def _load_calendar(s,e):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT trade_date FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY trade_date", (s,e))
            return [r[0].strftime("%Y-%m-%d") if hasattr(r[0],"strftime") else str(r[0]) for r in cur.fetchall()]

def _load_mv_map(s,e):
    s2 = max(date.fromisoformat(s)-timedelta(days=5), date(1998,1,1)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, total_mv FROM stock_dailybasic WHERE trade_date >= %s AND trade_date <= %s AND total_mv IS NOT NULL", (s2,e))
            rows=cur.fetchall()
    out={}
    for d,ts,mv in rows:
        ds=d.strftime("%Y-%m-%d") if hasattr(d,"strftime") else str(d)
        out.setdefault(ds,{})[str(ts)]=float(mv)/10000.0
    return out

def _load_daily(s,e):
    s2=max(date.fromisoformat(s)-timedelta(days=90),date(1998,1,1)).isoformat()
    e2=(date.fromisoformat(e)+timedelta(days=20)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, open, high, low, close, amount FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY ts_code, trade_date",(s2,e2))
            rows=cur.fetchall()
    per_ts={}
    for d,ts,o,h,l,c,amt in rows:
        ds=d.strftime("%Y-%m-%d") if hasattr(d,"strftime") else str(d)
        per_ts.setdefault(str(ts),[]).append({"date":ds,"open":float(o) if o else None,"high":float(h) if h else None,"low":float(l) if l else None,"close":float(c) if c else None,"amount":float(amt) if amt else None})
    return per_ts

def _breadth_by_day(per_ts,mv_map,cal):
    breadth={}
    for day in cal:
        cnt=tot=0
        for ts,series in per_ts.items():
            mv=mv_map.get(day,{}).get(ts)
            if mv is None or not (UNIVERSE_MIN_MV<=mv<=UNIVERSE_MAX_MV): continue
            idx=-1
            for i,r in enumerate(series):
                if r["date"]==day: idx=i; break
                if r["date"]>day: break
            if idx<20: continue
            closes=[r["close"] for r in series[idx-20:idx+1] if r["close"]]
            if len(closes)<20: continue
            ma20=sum(closes[-20:])/20
            cur_close=series[idx]["close"]
            if cur_close and ma20:
                tot+=1
                if cur_close>ma20: cnt+=1
        breadth[day]=cnt/tot if tot else 0
    return breadth

CACHE={}
def get_window_data(wname):
    if wname in CACHE: return CACHE[wname]
    start,end=WINDOWS[wname]
    cal=_load_calendar(start,end)
    per_ts=_load_daily(start,end)
    mv_map=_load_mv_map(start,end)
    breadth=_breadth_by_day(per_ts,mv_map,cal)
    cal_set=set(cal)
    close_by_ts={}
    for ts,series in per_ts.items():
        m={r["date"]:r["close"] for r in series if r["date"] in cal_set and r["close"]}
        if m: close_by_ts[ts]=m
    date_to_idx={d:i for i,d in enumerate(cal)}
    CACHE[wname]=(cal,per_ts,mv_map,breadth,close_by_ts,date_to_idx)
    return CACHE[wname]

def simulate_one(wname, tail, gate, hold, early):
    cal,per_ts,mv_map,breadth,close_by_ts,date_to_idx=get_window_data(wname)
    gate_thresh={"breadth0.5":0.5,"breadth0.55":0.55,"breadth0.6":0.6}[gate]
    positions={}
    trades=[]
    total_realized=0.0
    nav_curve=[]
    pos_counts=[]
    for day in cal:
        to_close=[]
        for ts,pos in list(positions.items()):
            ei=date_to_idx.get(pos["entry_date"],-1)
            ci=date_to_idx.get(day,-1)
            held=ci-ei+1 if ei>=0 and ci>=0 else 999
            if held>=hold:
                to_close.append(ts)
                continue
            if early=="trail-5":
                cur_close=close_by_ts.get(ts,{}).get(day)
                if cur_close and pos["entry_price"] and cur_close/pos["entry_price"]-1 <= -0.05:
                    to_close.append(ts)
                    continue
            elif early=="time3":
                if held>=3:
                    cur_close=close_by_ts.get(ts,{}).get(day)
                    if cur_close and pos["entry_price"] and cur_close/pos["entry_price"]-1 < 0:
                        to_close.append(ts)
                        continue
        for ts in to_close:
            pos=positions.pop(ts,None)
            if not pos: continue
            cur_close=close_by_ts.get(ts,{}).get(day)
            if not cur_close or not pos["entry_price"]: continue
            gross=cur_close/pos["entry_price"]-1
            net=gross-COSTS_ROUNDTRIP
            trades.append(net)
            total_realized+=net*POSITION_PCT
        if day in date_to_idx:
            idx=date_to_idx[day]
            if idx>0:
                sig_day=cal[idx-1]
                if breadth.get(sig_day,0) < gate_thresh:
                    pass
                else:
                    amp={}
                    for ts,series in per_ts.items():
                        mv=mv_map.get(sig_day,{}).get(ts)
                        if mv is None or not (UNIVERSE_MIN_MV<=mv<=UNIVERSE_MAX_MV): continue
                        s_idx=-1
                        for i,r in enumerate(series):
                            if r["date"]==sig_day: s_idx=i; break
                            if r["date"]>sig_day: break
                        if s_idx<0 or s_idx<20: continue
                        cur=series[s_idx]
                        if not cur["close"] or not cur["high"] or not cur["low"]: continue
                        a=(cur["high"]-cur["low"])/cur["close"] if cur["close"] else None
                        if a is not None and 0<a<=0.5:
                            amp[ts]=a
                    cands=set()
                    if tail=="amp_q10":
                        if amp:
                            s=sorted(amp.items(),key=lambda kv:kv[1])
                            q=max(1,len(s)*10//100)
                            cands=set(ts for ts,_ in s[:q])
                    elif tail=="amp_q5":
                        if amp:
                            s=sorted(amp.items(),key=lambda kv:kv[1])
                            q=max(1,len(s)*5//100)
                            cands=set(ts for ts,_ in s[:q])
                    if cands and len(positions)<MAX_POSITIONS:
                        scored=[]
                        for ts in cands:
                            if ts in positions: continue
                            series=per_ts.get(ts)
                            if not series: continue
                            open_px=None
                            for r in series:
                                if r["date"]==day:
                                    open_px=r["open"]; break
                            if not open_px or open_px<=0: continue
                            scored.append((ts,amp.get(ts,999)))
                        scored.sort(key=lambda x:x[1])
                        for ts,_ in scored[:MAX_POSITIONS-len(positions)]:
                            open_px=None
                            for r in per_ts[ts]:
                                if r["date"]==day:
                                    open_px=r["open"]; break
                            if open_px:
                                positions[ts]={"entry_date":day,"entry_price":open_px}
        mtm=0.0
        for ts,pos in positions.items():
            cur_close=close_by_ts.get(ts,{}).get(day)
            ep=pos["entry_price"]
            if cur_close and ep and ep>0: mtm+=POSITION_PCT*(cur_close/ep)
            else: mtm+=POSITION_PCT
        nav=1.0+total_realized+(mtm-len(positions)*POSITION_PCT)
        nav_curve.append(nav)
        pos_counts.append(len(positions))
    last_day=cal[-1] if cal else WINDOWS[wname][1]
    for ts,pos in list(positions.items()):
        cur_close=close_by_ts.get(ts,{}).get(last_day)
        if cur_close and pos["entry_price"]:
            gross=cur_close/pos["entry_price"]-1
            net=gross-COSTS_ROUNDTRIP
            trades.append(net)
            total_realized+=net*POSITION_PCT
    total_pnl=total_realized*100
    n_days=len(cal)
    daily_avg=total_pnl/n_days if n_days else 0
    cagr=((1+total_realized)**(252/n_days)-1)*100 if n_days else 0
    wins=sum(1 for x in trades if x>0)
    win_rate=wins/len(trades) if trades else 0
    max_dd=0.0
    peak=nav_curve[0] if nav_curve else 1.0
    for v in nav_curve:
        if v>peak: peak=v
        dd=(peak-v)/peak*100 if peak else 0
        if dd>max_dd: max_dd=dd
    rets=[nav_curve[i]/nav_curve[i-1]-1 for i in range(1,len(nav_curve)) if nav_curve[i-1]>0]
    sharpe=0.0
    if len(rets)>10:
        import numpy as np
        arr=np.array(rets)
        if arr.std()>0: sharpe=float(arr.mean()/arr.std()*(252**0.5))
    avg_pos=sum(pos_counts)/len(pos_counts) if pos_counts else 0
    hold_ratio=avg_pos/MAX_POSITIONS*100
    return {"tail":tail,"gate":gate,"hold":hold,"early":early,"window":wname,"total_pnl":total_pnl,"daily_avg":daily_avg,"cagr":cagr,"max_dd":max_dd,"sharpe":sharpe,"hold_ratio":hold_ratio,"trades":len(trades),"win_rate":win_rate}

# Focused grid: amp_q10, gates 0.5/0.55/0.6, holds 5/10/15, early none/trail-5/time3
tails=["amp_q10"]
gates=["breadth0.5","breadth0.55","breadth0.6"]
holds=[5,10,15]
earlys=["none","trail-5","time3"]

all_res=[]
for tail in tails:
    for gate in gates:
        for hold in holds:
            for early in earlys:
                for w in WINDOWS:
                    res=simulate_one(w,tail,gate,hold,early)
                    all_res.append(res)
                    print(f"{tail:10s} {gate:12s} h{hold:2d} {early:8s} {w:6s} daily {res['daily_avg']:+.4f}% cagr {res['cagr']:+5.1f}% hold {res['hold_ratio']:4.1f}% sharpe {res['sharpe']:4.2f} total {res['total_pnl']:+5.1f}%")

from collections import defaultdict
grouped=defaultdict(dict)
for r in all_res:
    grouped[(r["tail"],r["gate"],r["hold"],r["early"])][r["window"]]=r

ranked=[]
for key,d in grouped.items():
    o=d.get("OOS2"); t=d.get("train"); v=d.get("valid")
    if not o or not t or not v: continue
    avg_daily_tri=(o["daily_avg"]+t["daily_avg"]+v["daily_avg"])/3
    avg_hold=(o["hold_ratio"]+t["hold_ratio"]+v["hold_ratio"])/3
    ranked.append((avg_daily_tri, v["daily_avg"], avg_hold, key, d))
ranked.sort(key=lambda x: (x[1],x[0]), reverse=True)

print("\n=== Top by valid daily_avg (hold<50, tri all>0 if possible) ===")
for avg_tri, v_daily, avg_hold, key, d in ranked:
    o=d["OOS2"]; t=d["train"]; v=d["valid"]
    if avg_hold<50 and v["total_pnl"]>0:
        print(f"✅ {key} avg_hold {avg_hold:.1f}% tri_daily {avg_tri:+.4f}% valid_daily {v_daily:+.4f}% | OOS2 {o['total_pnl']:+5.1f}/{o['daily_avg']:+.4f} train {t['total_pnl']:+5.1f}/{t['daily_avg']:+.4f} valid {v['total_pnl']:+5.1f}/{v['daily_avg']:+.4f} cagr {v['cagr']:+5.1f}% sharpe {v['sharpe']:.2f} DD {v['max_dd']:.1f}")

print("\n=== Top valid daily (hold 20% accepted) ===")
for avg_tri, v_daily, avg_hold, key, d in ranked:
    if avg_hold<30:
        o=d["OOS2"]; v=d["valid"]
        print(f"{key} avg_hold {avg_hold:.1f}% valid_daily {v_daily:+.4f}% total {v['total_pnl']:+5.1f}%")
