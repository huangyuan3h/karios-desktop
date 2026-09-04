#!/usr/bin/env python3
"""Full 8-factor industry IC per 144 industries + 4 style buckets."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from collections import defaultdict
from datetime import date, timedelta
import json, numpy as np
from data_sync_service.db import get_connection
WINDOWS={"OOS2": ("2024-08-01","2025-08-01"), "train": ("2025-08-01","2026-02-01"), "valid": ("2026-03-01","2026-08-07")}
HORIZONS=[5,10]
FACTOR_NAMES=["turnover_spike","amplitude","gap","ret1","ret5","dist_high5","down_cnt","neg_mv"]
def _load_industry():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ts_code, industry FROM stock_basic WHERE delist_date IS NULL")
            return {str(r[0]): (r[1] or "UNKNOWN") for r in cur.fetchall()}
def _load_mv_map(s,e):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, total_mv FROM stock_dailybasic WHERE trade_date >= %s AND trade_date <= %s AND total_mv IS NOT NULL",(s,e))
            rows=cur.fetchall()
    out=defaultdict(dict)
    for d,ts,mv in rows:
        ds=d.strftime("%Y-%m-%d") if hasattr(d,"strftime") else str(d)
        try: out[ds][str(ts)]=float(mv)/10000.0
        except: continue
    return out
def _load_daily(s,e):
    s2=max(date.fromisoformat(s)-timedelta(days=90),date(1998,1,1)).isoformat()
    e2=(date.fromisoformat(e)+timedelta(days=20)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, open, high, low, close, pre_close, amount FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY ts_code, trade_date",(s2,e2))
            rows=cur.fetchall()
    per_ts=defaultdict(list)
    for d,ts,o,h,l,c,pc,amt in rows:
        ds=d.strftime("%Y-%m-%d") if hasattr(d,"strftime") else str(d)
        per_ts[str(ts)].append({"date":ds,"open":float(o) if o is not None else None,"high":float(h) if h is not None else None,"low":float(l) if l is not None else None,"close":float(c) if c is not None else None,"pre_close":float(pc) if pc is not None else None,"amount":float(amt) if amt is not None else None})
    return per_ts
def _factors_for_day(per_ts, day, mv_by_day):
    out={}
    for ts, series in per_ts.items():
        idx=-1
        for i,r in enumerate(series):
            if r["date"]==day: idx=i;break
            if r["date"]>day: break
        if idx<0 or idx<20: continue
        mv=mv_by_day.get(ts)
        if mv is None: continue
        cur=series[idx]
        if not cur["close"] or cur["close"]<=0: continue
        amts=[r["amount"] for r in series[idx-20:idx+1] if r["amount"]]
        if len(amts)<15: continue
        avg20=sum(amts[:-1])/max(len(amts)-1,1) if len(amts)>1 else amts[0]
        amt=cur["amount"] or 0
        turnover_spike=(amt/avg20) if avg20 and avg20>0 else np.nan
        amplitude=(cur["high"]-cur["low"])/cur["close"] if cur["high"] and cur["low"] and cur["close"] else np.nan
        prev=series[idx-1]["close"] if idx>0 else None
        gap=(cur["open"]/prev-1) if cur["open"] and prev and prev>0 else np.nan
        ret1=(cur["close"]/prev-1) if prev and prev>0 else np.nan
        c5=series[idx-5]["close"] if idx>=5 else None
        ret5=(cur["close"]/c5-1) if c5 and c5>0 else np.nan
        highs5=[r["high"] for r in series[idx-5:idx+1] if r["high"]]
        max_h=max(highs5) if highs5 else np.nan
        dist_high5=(cur["close"]-max_h)/max_h if max_h and max_h>0 else np.nan
        down_cnt=0
        for k in range(5):
            j=idx-k
            if j<=0: break
            if series[j]["close"] and series[j-1]["close"] and series[j]["close"]<series[j-1]["close"]: down_cnt+=1
            else: break
        neg_mv=-mv
        out[ts]={"turnover_spike":turnover_spike,"amplitude":amplitude,"gap":gap,"ret1":ret1,"ret5":ret5,"dist_high5":dist_high5,"down_cnt":float(down_cnt),"neg_mv":neg_mv}
    return out
def _forward_returns(per_ts,h):
    out=defaultdict(dict)
    for ts, series in per_ts.items():
        d2i={r["date"]:i for i,r in enumerate(series)}
        for d,idx in d2i.items():
            j=idx+h
            if j>=len(series): continue
            c0=series[idx]["close"]; c1=series[j]["close"]
            if not c0 or not c1 or c0<=0: continue
            ret=c1/c0-1
            if abs(ret)>5: continue
            out[d][ts]=ret
    return out
def _spearman(x,y):
    if len(x)<10: return np.nan
    rx=np.argsort(np.argsort(x)); ry=np.argsort(np.argsort(y))
    xm=rx-rx.mean(); ym=ry-ry.mean()
    den=np.sqrt((xm*xm).sum()*(ym*ym).sum())
    return float((xm*ym).sum()/den) if den else np.nan
def run_window(wname, industry_map, horizons):
    s,e=WINDOWS[wname]
    per_ts=_load_daily(s,e); mv_map=_load_mv_map(s,e)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT trade_date FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY trade_date",(s,e))
            cal=[r[0].strftime("%Y-%m-%d") if hasattr(r[0],"strftime") else str(r[0]) for r in cur.fetchall()]
    fwd_by_h={h:_forward_returns(per_ts,h) for h in horizons}
    acc=defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for day in cal:
        fmap=_factors_for_day(per_ts, day, mv_map.get(day,{}))
        if not fmap: continue
        for h in horizons:
            fwd=fwd_by_h[h].get(day)
            if not fwd: continue
            for fname in FACTOR_NAMES:
                day_by_ind=defaultdict(dict)
                for ts,fv in fmap.items():
                    ind=industry_map.get(ts,"UNKNOWN")
                    v=fv.get(fname)
                    if v is not None and np.isfinite(v):
                        day_by_ind[ind][ts]=v
                for ind,xmap in day_by_ind.items():
                    common=set(xmap.keys())&set(fwd.keys())
                    if len(common)<10: continue
                    xs=np.array([xmap[ts] for ts in common],dtype=float); ys=np.array([fwd[ts] for ts in common],dtype=float)
                    ic=_spearman(xs,ys)
                    if np.isfinite(ic): acc[ind][fname][h].append(ic)
    res={"window":wname,"factors":{}}
    for ind,fdict in acc.items():
        res["factors"][ind]={}
        for fname in FACTOR_NAMES:
            res["factors"][ind][fname]={}
            for h in horizons:
                ics=np.array(fdict[fname][h])
                if len(ics)==0: continue
                ic_mean=float(np.nanmean(ics)); ic_std=float(np.nanstd(ics))
                ir=ic_mean/ic_std if ic_std and ic_std>0 else float("nan")
                res["factors"][ind][fname][f"h{h}"]={"n_days":len(ics),"ic_mean":ic_mean,"ic_std":ic_std,"ic_ir":ir,"hit_rate":float(np.mean(ics>0))}
    return res
def main():
    wins=["OOS2","train","valid"]
    industry_map=_load_industry()
    all_res={}
    for w in wins:
        print(f"[{w}] ...",flush=True)
        all_res[w]=run_window(w, industry_map, HORIZONS)
    out=Path(__file__).resolve().parents[1]/"data/backtest_reports/industry_ic_full_latest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"windows":wins,"horizons":HORIZONS,"factors":FACTOR_NAMES,"results":all_res},ensure_ascii=False,indent=2,default=str))
    print(f"report -> {out}")
if __name__=="__main__": raise SystemExit(main())
