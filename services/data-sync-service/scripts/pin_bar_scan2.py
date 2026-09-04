"""Pin bar continuation vs reversal + base rate."""
import numpy as np, pandas as pd, psycopg
from data_sync_service.config import get_settings
def rollmean(a,w):
    a=np.asarray(a,float); cs=np.cumsum(np.concatenate([[0.0],a])); out=np.full(len(a),np.nan); idx=np.arange(w-1,len(a)); out[idx]=(cs[idx+1]-cs[idx-w+1])/w; return out
def fetch():
    s=get_settings(); conn=psycopg.connect(s.database_url); cur=conn.cursor()
    cur.execute("SELECT ts_code, trade_date, open, high, low, close, vol FROM daily WHERE trade_date >= '2021-08-01' ORDER BY ts_code, trade_date")
    rows=cur.fetchall(); conn.close()
    df=pd.DataFrame(rows, columns=["ts_code","trade_date","open","high","low","close","vol"]); df["trade_date"]=pd.to_datetime(df["trade_date"]); return df

def evaluate(data, masks, direction, tgt, stp, hold, cost=0.006):
    hits=0; rs=[]
    n=0
    for ts,d in data.items():
        closes=d["c"]; highs=d["h"]; lows=d["l"]; mask=masks[ts]
        for t in np.where(mask)[0]:
            if t+hold >= len(closes): continue
            entry=closes[t]
            if direction=="short": tgt_p=entry*(1-tgt); stp_p=entry*(1+stp)
            else: tgt_p=entry*(1+tgt); stp_p=entry*(1-stp)
            outcome=None; exit_price=None
            for k in range(1,hold+1):
                h=highs[t+k]; l=lows[t+k]
                if direction=="short":
                    if l<=tgt_p:
                        if h>=stp_p: outcome=False; exit_price=stp_p; break
                        outcome=True; exit_price=tgt_p; break
                    if h>=stp_p: outcome=False; exit_price=stp_p; break
                else:
                    if h>=tgt_p:
                        if l<=stp_p: outcome=False; exit_price=stp_p; break
                        outcome=True; exit_price=tgt_p; break
                    if l<=stp_p: outcome=False; exit_price=stp_p; break
            if outcome is None:
                exit_price=closes[t+hold]; outcome=False
            hit=bool(outcome)
            pnl=(entry-exit_price)/entry - cost if direction=="short" else (exit_price-entry)/entry - cost
            rs.append(pnl*100)
            if hit: hits+=1
            n+=1
    if n==0: return dict(n=0,hit=float('nan'),mean=float('nan'))
    return dict(n=n, hit=hits/n*100, mean=float(np.mean(rs)), median=float(np.median(rs)))

df=fetch()
groups=dict(tuple(df.groupby("ts_code")))
data={}
for ts,g in groups.items():
    g=g.sort_values("trade_date").reset_index(drop=True)
    o=g["open"].astype(float).values; h=g["high"].astype(float).values; l=g["low"].astype(float).values; c=g["close"].astype(float).values; v=g["vol"].astype(float).values
    if len(g)<70: continue
    rng=h-l; body=np.abs(c-o); up=h-np.maximum(o,c); lo=np.minimum(o,c)-l; ma20=rollmean(c,20); ma60=rollmean(c,60)
    data[ts]=dict(o=o,h=h,l=l,c=c,v=v,rng=rng,body=body,upper=up,lower=lo,ma20=ma20,ma60=ma60)

cfg=(2.5,0.60,0.33,0.20)
k,rr,br,op=cfg
# build masks
mask_up={}; mask_down={}; mask_up_trend={}; mask_down_trend={}; mask_rand_up={}; mask_rand_down={}
for ts,d in data.items():
    n=len(d["c"]); m_up=np.zeros(n,bool); m_down=np.zeros(n,bool); m_rand_up=np.zeros(n,bool); m_rand_down=np.zeros(n,bool)
    for t in range(60,n):
        if np.isnan(d["ma20"][t]) or np.isnan(d["ma60"][t]): continue
        rng=d["rng"][t]; body=d["body"][t]; up=d["upper"][t]; lo=d["lower"][t]
        if rng<=0: continue
        is_up=(up>=k*max(body,0.01)) and (up/rng>=rr) and (body/rng<=br) and (lo/rng<=op)
        is_down=(lo>=k*max(body,0.01)) and (lo/rng>=rr) and (body/rng<=br) and (up/rng<=op)
        if is_up: m_rand_up[t]=True
        if is_down: m_rand_down[t]=True
        if is_up and (d["ma20"][t]>d["ma60"][t] and d["c"][t]>d["ma60"][t]): m_up[t]=True
        if is_down and (d["ma20"][t]<d["ma60"][t] and d["c"][t]<d["ma60"][t]): m_down[t]=True
    mask_up[ts]=m_up; mask_down[ts]=m_down; mask_rand_up[ts]=m_rand_up; mask_rand_down[ts]=m_rand_down

# base rate: random entries within same trend (without pin)
trend_up_masks={}; trend_down_masks={}
for ts,d in data.items():
    n=len(d["c"]); mu=np.zeros(n,bool); md=np.zeros(n,bool)
    for t in range(60,n):
        if np.isnan(d["ma20"][t]): continue
        if d["ma20"][t]>d["ma60"][t] and d["c"][t]>d["ma60"][t]: mu[t]=True
        if d["ma20"][t]<d["ma60"][t] and d["c"][t]<d["ma60"][t]: md[t]=True
    trend_up_masks[ts]=mu; trend_down_masks[ts]=md

for hold,tgt,stp in [(5,0.02,0.02),(5,0.03,0.02),(5,0.015,0.015)]:
    print(f"\n-- hold{hold} tgt{tgt*100:.1f}% stp{stp*100:.1f}% --")
    for label, masks, direc in [
        ("pin_up→short(reversal)", mask_up, "short"),
        ("pin_up→long (continuation)", mask_up, "long"),
        ("pin_down→long (reversal)", mask_down, "long"),
        ("pin_down→short(continuation)", mask_down, "short"),
        ("naked pin_up→short", mask_rand_up, "short"),
        ("naked pin_down→long", mask_rand_down, "long"),
    ]:
        r=evaluate(data,masks,direc,tgt,stp,hold)
        print(f" {label:30s} n={r['n']:6d} hit={r['hit']:5.1f}% mean{r['mean']:+5.2f}% med{r['median']:+5.2f}%")
    # base: random in same trend, same n sampled? Use full trend as base
    for lbl, masks, direc in [("BASE uptrend→short", trend_up_masks,"short"), ("BASE uptrend→long",trend_up_masks,"long"), ("BASE downtrend→long",trend_down_masks,"long"), ("BASE downtrend→short",trend_down_masks,"short")]:
        # subsample to similar n? just report full trend performance
        r=evaluate(data,masks,direc,tgt,stp,hold)
        print(f" {lbl:30s} n={r['n']:6d} hit={r['hit']:5.1f}% mean{r['mean']:+5.2f}%")
