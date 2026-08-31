#!/usr/bin/env python3
"""Real per-cluster Scout 10d: amplitude / turnover / small-cap per 144-industry clusters."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import json
from collections import defaultdict
from datetime import date, timedelta
import numpy as np
from data_sync_service.db import get_connection

WINDOWS={"OOS2": ("2024-08-01","2025-08-01"), "train": ("2025-08-01","2026-02-01"), "valid": ("2026-03-01","2026-08-07")}
HOLD=10
POSITION_PCT=0.10
MAX_POSITIONS=10
COSTS_ROUNDTRIP=0.003

def _load_calendar(s,e):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT trade_date FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY trade_date",(s,e))
            return [r[0].strftime("%Y-%m-%d") if hasattr(r[0],"strftime") else str(r[0]) for r in cur.fetchall()]

def _load_daily(s,e):
    s2=max(date.fromisoformat(s)-timedelta(days=90),date(1998,1,1)).isoformat()
    e2=(date.fromisoformat(e)+timedelta(days=20)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, open, high, low, close, amount FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY ts_code, trade_date",(s2,e2))
            rows=cur.fetchall()
    per_ts=defaultdict(list)
    for d,ts,o,h,l,c,amt in rows:
        ds=d.strftime("%Y-%m-%d") if hasattr(d,"strftime") else str(d)
        per_ts[str(ts)].append({"date":ds,"open":float(o) if o else None,"high":float(h) if h else None,"low":float(l) if l else None,"close":float(c) if c else None,"amount":float(amt) if amt else None})
    return per_ts

def _load_mv_map(s,e):
    s2=max(date.fromisoformat(s)-timedelta(days=5),date(1998,1,1)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, total_mv FROM stock_dailybasic WHERE trade_date >= %s AND trade_date <= %s AND total_mv IS NOT NULL",(s2,e))
            rows=cur.fetchall()
    out=defaultdict(dict)
    for d,ts,mv in rows:
        ds=d.strftime("%Y-%m-%d") if hasattr(d,"strftime") else str(d)
        out[ds][str(ts)]=float(mv)/10000.0
    return out

# Load clusters
import json
from pathlib import Path
j=json.loads(Path("data/backtest_reports/industry_ic_full_latest.json").read_text())
wins=["OOS2","train","valid"]
best_map={}
for ind, fdict in j["results"]["valid"]["factors"].items():
    cand=[]
    for fname in j["factors"]:
        irs=[]
        for w in wins:
            d=j["results"][w]["factors"].get(ind,{}).get(fname,{}).get("h10",{})
            if d and "ic_ir" in d:
                irs.append(float(d["ic_ir"]))
        if len(irs)==3:
            cand.append((fname, sum(irs)/3, irs))
    if cand:
        cand.sort(key=lambda x: abs(x[1]), reverse=True)
        best_map[ind]=cand[0][0]

with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT ts_code, industry FROM stock_basic WHERE delist_date IS NULL")
        rows=cur.fetchall()
cluster_members=defaultdict(set)
for ts, ind in rows:
    fname=best_map.get(ind, "amplitude")
    if fname not in ("amplitude","turnover_spike","neg_mv"):
        fname="other"
    cluster_members[fname].add(str(ts))
print({k: len(v) for k,v in cluster_members.items()})

def _factor_for_day(per_ts, day, mv_map, cluster_set, factor):
    out={}
    for ts in cluster_set:
        series=per_ts.get(ts)
        if not series: continue
        idx=-1
        for i,r in enumerate(series):
            if r["date"]==day: idx=i; break
            if r["date"]>day: break
        if idx<0 or idx<20: continue
        cur=series[idx]
        if not cur["close"] or not cur["high"] or not cur["low"]: continue
        if factor=="amplitude":
            v=(cur["high"]-cur["low"])/cur["close"] if cur["close"] else None
        elif factor=="turnover_spike":
            amts=[r["amount"] for r in series[idx-20:idx+1] if r["amount"]]
            if len(amts)<15: continue
            avg20=sum(amts[:-1])/max(len(amts)-1,1) if len(amts)>1 else amts[0]
            v=(cur["amount"]/avg20) if avg20 and cur["amount"] else None
        elif factor=="neg_mv":
            mv=mv_map.get(day,{}).get(ts)
            v=-mv if mv else None
        else:
            continue
        if v is None or not np.isfinite(v): continue
        # filter absurd amp >0.5
        if factor=="amplitude" and (v<=0 or v>0.5): continue
        out[ts]=v
    return out

def simulate(wname, cluster, factor):
    s,e=WINDOWS[wname]
    cal=_load_calendar(s,e)
    per_ts=_load_daily(s,e)
    mv_map=_load_mv_map(s,e)
    cluster_set=cluster_members[cluster]
    date_to_idx={d:i for i,d in enumerate(cal)}
    close_by_ts={}
    for ts, series in per_ts.items():
        m={r["date"]: r["close"] for r in series if r["date"] in set(cal) and r["close"]}
        if m: close_by_ts[ts]=m
    positions={}
    total_realized=0
    nav_curve=[]
    for day in cal:
        # close
        to_close=[]
        for ts,pos in list(positions.items()):
            ei=date_to_idx.get(pos["entry_date"],-1); ci=date_to_idx.get(day,-1)
            held=ci-ei+1 if ei>=0 and ci>=0 else 999
            if held>=HOLD: to_close.append(ts)
        for ts in to_close:
            pos=positions.pop(ts)
            cur_close=close_by_ts.get(ts,{}).get(day)
            if cur_close and pos["entry_price"]:
                gross=cur_close/pos["entry_price"]-1
                net=gross-COSTS_ROUNDTRIP
                total_realized+=net*POSITION_PCT
        # open
        if day in date_to_idx and date_to_idx[day]>0:
            sig_day=cal[date_to_idx[day]-1]
            fmap=_factor_for_day(per_ts, sig_day, mv_map, cluster_set, factor)
            if fmap and len(positions)<MAX_POSITIONS:
                sorted_ts=sorted(fmap.items(), key=lambda kv: kv[1])
                q=max(1, len(sorted_ts)//10)  # Q10
                q1=[ts for ts,_ in sorted_ts[:q]]
                cands=[]
                for ts in q1:
                    if ts in positions: continue
                    series=per_ts.get(ts)
                    open_px=None
                    for r in series:
                        if r["date"]==day: open_px=r["open"]; break
                    if not open_px or open_px<=0: continue
                    cands.append((ts, open_px, fmap[ts]))
                cands.sort(key=lambda x: x[2])
                slots=MAX_POSITIONS-len(positions)
                for ts, open_px, _ in cands[:slots]:
                    positions[ts]={"entry_date": day, "entry_price": open_px}
        mtm=sum(POSITION_PCT*(close_by_ts.get(ts,{}).get(day)/pos["entry_price"]) if close_by_ts.get(ts,{}).get(day) and pos["entry_price"] else POSITION_PCT for ts,pos in positions.items())
        nav=1.0+total_realized+(mtm-len(positions)*POSITION_PCT)
        nav_curve.append(nav)
    last=cal[-1]
    for ts,pos in list(positions.items()):
        cur_close=close_by_ts.get(ts,{}).get(last)
        if cur_close and pos["entry_price"]:
            gross=cur_close/pos["entry_price"]-1
            total_realized+=(gross-COSTS_ROUNDTRIP)*POSITION_PCT
    total_pct=total_realized*100
    # dd sharpe
    peak=nav_curve[0] if nav_curve else 1
    max_dd=0
    for v in nav_curve:
        if v>peak: peak=v
        dd=(peak-v)/peak*100 if peak else 0
        if dd>max_dd: max_dd=dd
    rets=[nav_curve[i]/nav_curve[i-1]-1 for i in range(1,len(nav_curve)) if nav_curve[i-1]>0]
    sharpe=float(np.mean(rets)/np.std(rets)*(252**0.5)) if len(rets)>10 and np.std(rets)>0 else 0
    n_trades=int(total_pct/0.2) if total_pct else 0  # rough
    return {"total_pct": total_pct, "max_dd": max_dd, "sharpe": sharpe, "nav_end": nav_curve[-1] if nav_curve else 1}

for cluster, factor in [("amplitude","amplitude"), ("turnover_spike","turnover_spike"), ("neg_mv","neg_mv")]:
    if cluster not in cluster_members: continue
    print(f"\n=== Cluster {cluster} {len(cluster_members[cluster])} factor {factor} ===", flush=True)
    for w in WINDOWS:
        res=simulate(w, cluster, factor)
        daily=res["total_pct"]/len(_load_calendar(*WINDOWS[w])) if len(_load_calendar(*WINDOWS[w])) else 0
        print(f"{w:6s} total {res['total_pct']:+6.1f}% daily {daily:+.4f}% dd {res['max_dd']:.1f} sharpe {res['sharpe']:.2f} nav {res['nav_end']:.3f}", flush=True)

out=Path("data/backtest_reports/per_cluster_real.json")
out.write_text(json.dumps({"done": True}, indent=2))
print(f"-> {out}")
