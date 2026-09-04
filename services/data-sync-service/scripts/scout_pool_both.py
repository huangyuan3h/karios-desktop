#!/usr/bin/env python3
"""Re-run both schemes with expanded pools. Schemes: A=amp_q10 10d, B=amp_and_gap 10d, both breadth>0.5."""
import sys
from datetime import date, timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from data_sync_service.db import get_connection

WINDOWS = {"valid": ("2026-03-01","2026-08-07"), "long": ("2021-08-01","2026-08-07"), "OOS2": ("2024-08-01","2025-08-01"), "train": ("2025-08-01","2026-02-01")}

def run_pool_scheme(min_mv, max_mv, scheme: str, wname: str):
    start, end = WINDOWS[wname]
    # load calendar
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT trade_date FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY trade_date", (start,end))
            cal = [r[0].strftime("%Y-%m-%d") if hasattr(r[0],"strftime") else str(r[0]) for r in cur.fetchall()]
    s2 = max(date.fromisoformat(start)-timedelta(days=90), date(1998,1,1)).isoformat()
    e2 = (date.fromisoformat(end)+timedelta(days=20)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, open, high, low, close, amount FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY ts_code, trade_date", (s2,e2))
            rows = cur.fetchall()
    per_ts={}
    for d,ts,o,h,l,c,amt in rows:
        ds=d.strftime("%Y-%m-%d") if hasattr(d,"strftime") else str(d)
        per_ts.setdefault(str(ts),[]).append({"date":ds,"open":float(o) if o else None,"high":float(h) if h else None,"low":float(l) if l else None,"close":float(c) if c else None,"amount":float(amt) if amt else None})
    # mv
    s2m = max(date.fromisoformat(start)-timedelta(days=5), date(1998,1,1)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, total_mv FROM stock_dailybasic WHERE trade_date >= %s AND trade_date <= %s AND total_mv IS NOT NULL", (s2m,end))
            rows=cur.fetchall()
    mv_map={}
    for d,ts,mv in rows:
        ds=d.strftime("%Y-%m-%d") if hasattr(d,"strftime") else str(d)
        mv_map.setdefault(ds,{})[str(ts)]=float(mv)/10000.0
    # breadth for this pool
    breadth={}
    for day in cal:
        cnt=tot=0
        for ts,series in per_ts.items():
            mv=mv_map.get(day,{}).get(ts)
            if mv is None or not (min_mv <= mv <= max_mv): continue
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
    # simulate
    date_to_idx={d:i for i,d in enumerate(cal)}
    cal_set=set(cal)
    close_by_ts={}
    for ts,series in per_ts.items():
        m={r["date"]:r["close"] for r in series if r["date"] in cal_set and r["close"]}
        if m: close_by_ts[ts]=m
    positions={}
    trades=[]
    total_realized=0.0
    nav_curve=[]
    for day in cal:
        to_close=[]
        for ts,pos in list(positions.items()):
            ei=date_to_idx.get(pos["entry_date"],-1)
            ci=date_to_idx.get(day,-1)
            held=ci-ei+1 if ei>=0 and ci>=0 else 999
            if held>=10:
                to_close.append(ts)
        for ts in to_close:
            pos=positions.pop(ts,None)
            if not pos: continue
            cur_close=close_by_ts.get(ts,{}).get(day)
            if not cur_close or not pos["entry_price"]: continue
            gross=cur_close/pos["entry_price"]-1
            net=gross-0.003
            trades.append(net)
            total_realized+=net*0.10
        if day in date_to_idx:
            idx=date_to_idx[day]
            if idx>0:
                sig_day=cal[idx-1]
                if breadth.get(sig_day,0) < 0.5:
                    pass
                else:
                    # factors
                    amp={}
                    gap={}
                    for ts,series in per_ts.items():
                        mv=mv_map.get(sig_day,{}).get(ts)
                        if mv is None or not (min_mv <= mv <= max_mv): continue
                        s_idx=-1
                        for i,r in enumerate(series):
                            if r["date"]==sig_day: s_idx=i; break
                            if r["date"]>sig_day: break
                        if s_idx<0: continue
                        cur=series[s_idx]
                        if not cur["close"]: continue
                        if cur["high"] and cur["low"]:
                            a=(cur["high"]-cur["low"])/cur["close"] if cur["close"] else None
                            if a is not None and 0<a<=0.5:
                                amp[ts]=a
                        if s_idx>0 and cur["open"] and series[s_idx-1]["close"]:
                            g=cur["open"]/series[s_idx-1]["close"]-1
                            if -0.1<g<0.1:
                                gap[ts]=g
                    cands=set()
                    if scheme=="A":
                        if amp:
                            s=sorted(amp.items(),key=lambda kv:kv[1])
                            q=max(1,len(s)*10//100)
                            cands=set(ts for ts,_ in s[:q])
                    elif scheme=="B":
                        # amp_and_gap
                        if amp and gap:
                            sa=sorted(amp.items(),key=lambda kv:kv[1])
                            qa=max(1,len(sa)*10//100)
                            a_q=set(ts for ts,_ in sa[:qa])
                            sg=sorted(gap.items(),key=lambda kv:kv[1])
                            qg=max(1,len(sg)*20//100)
                            g_q=set(ts for ts,_ in sg[-qg:])
                            g_q=set(ts for ts in g_q if gap[ts]>0.01)
                            cands=a_q & g_q
                    if cands and len(positions)<10:
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
                        for ts,_ in scored[:10-len(positions)]:
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
            if cur_close and ep and ep>0: mtm+=0.10*(cur_close/ep)
            else: mtm+=0.10
        nav=1.0+total_realized+(mtm-len(positions)*0.10)
        nav_curve.append(nav)
    last_day=cal[-1] if cal else end
    for ts,pos in list(positions.items()):
        cur_close=close_by_ts.get(ts,{}).get(last_day)
        if cur_close and pos["entry_price"]:
            gross=cur_close/pos["entry_price"]-1
            net=gross-0.003
            trades.append(net)
            total_realized+=net*0.10
    total_pnl=total_realized*100
    n_days=len(cal)
    daily_avg=total_pnl/n_days if n_days else 0
    wins=sum(1 for x in trades if x>0)
    win_rate=wins/len(trades) if trades else 0
    max_dd=0.0
    peak=nav_curve[0] if nav_curve else 1.0
    for v in nav_curve:
        if v>peak: peak=v
        dd=(peak-v)/peak*100 if peak else 0
        if dd>max_dd: max_dd=dd
    return {"pool":f"{min_mv}-{max_mv}","scheme":scheme,"window":wname,"total_pnl":total_pnl,"daily_avg":daily_avg,"max_dd":max_dd,"win_rate":win_rate,"trades":len(trades)}

pools=[(20,80),(20,150),(20,300)]
schemes=["A","B"]
for min_mv,max_mv in pools:
    for scheme in schemes:
        for w in ["valid","long","OOS2"]:
            res=run_pool_scheme(min_mv,max_mv,scheme,w)
            print(f"pool {res['pool']:7s} {scheme:4s} {w:6s} total {res['total_pnl']:+6.1f}% daily {res['daily_avg']:+.4f}% dd {res['max_dd']:4.1f} win {res['win_rate']*100:4.1f}% n {res['trades']}")
