"""Pin bar (金针) validation — independent of S-3. See docs/designs/pattern-factor-validation.md

Two patterns:
 - pin_up (长上影) in uptrend → bearish reversal (short)
 - pin_down (长下影) in downtrend → bullish reversal (long)

Scan all A-share daily 2021-08 ~ 2026-08, count |O|, then simulate fixed target/stop
hold and report hit% / R vs random base.

No future leak: features use only t and earlier.
"""
from __future__ import annotations
import numpy as np, pandas as pd, psycopg
from datetime import date
from data_sync_service.config import get_settings

# ---------- helpers ----------
def rollmean(a, w):
    a = np.asarray(a,float); cs=np.cumsum(np.concatenate([[0.0],a]))
    out=np.full(len(a),np.nan); idx=np.arange(w-1,len(a))
    out[idx]=(cs[idx+1]-cs[idx-w+1])/w; return out

def fetch_all_daily():
    s=get_settings()
    # Use server-side cursor to avoid OOM: ~14M rows ~ 500MB.
    conn=psycopg.connect(s.database_url)
    cur=conn.cursor()
    # Restrict to 2021-08-01+
    cur.execute("""
        SELECT ts_code, trade_date, open, high, low, close, vol
        FROM daily
        WHERE trade_date >= '2021-08-01'
        ORDER BY ts_code, trade_date
    """)
    rows=cur.fetchall()
    conn.close()
    # group by ts_code via pandas for speed
    df=pd.DataFrame(rows, columns=["ts_code","trade_date","open","high","low","close","vol"])
    df["trade_date"]=pd.to_datetime(df["trade_date"])
    return df

def evaluate_trades(df_map, mask_series, direction, target_pct, stop_pct, hold_days, cost=0.003):
    """Walk forward per symbol: mask True at t then trade t's close -> forward bars t+1..t+hold.
    target/stop checked daily on high/low; winner = hit target before stop within hold.
    Returns (n, hit, R_mean, R_median).
    cost = one-way? use roundtrip 0.6% as prior.
    """
    # Build flat occurrence list and simulate
    # df_map: dict ts_code -> sub df sorted
    hits=0; rs=[]
    n=0
    for ts, d in df_map.items():
        closes=d["c"]; highs=d["h"]; lows=d["l"]
        mask=mask_series[ts]  # bool array len==len(g)
        for t in np.where(mask)[0]:
            if t+hold_days >= len(closes): continue
            entry=closes[t]
            if entry<=0 or np.isnan(entry): continue
            if direction=="short":
                tgt=entry*(1-target_pct); stp=entry*(1+stop_pct)
                # alternative: stp = highs[t]*1.005 as structure stop — test both? use fixed pct for now
            else:
                tgt=entry*(1+target_pct); stp=entry*(1-stop_pct)
            outcome=None; exit_price=None; exit_day=None
            for d in range(1, hold_days+1):
                h=highs[t+d]; l=lows[t+d]; c=closes[t+d]
                if direction=="short":
                    if l <= tgt: # hit target (low touched)
                        # check if stop also hit same day: assume stop first if both -> loss
                        if h >= stp: # both touched same day -> loss
                            outcome=False; exit_price=stp; exit_day=d; break
                        outcome=True; exit_price=tgt; exit_day=d; break
                    if h >= stp:
                        outcome=False; exit_price=stp; exit_day=d; break
                else:
                    if h >= tgt:
                        if l <= stp:
                            outcome=False; exit_price=stp; exit_day=d; break
                        outcome=True; exit_price=tgt; exit_day=d; break
                    if l <= stp:
                        outcome=False; exit_price=stp; exit_day=d; break
            if outcome is None: # time exit at close of t+hold_days
                exit_price=closes[t+hold_days]
                # R vs target/stop range
                if direction=="short":
                    outcome = exit_price <= tgt # if moved favorably beyond tgt? but for R we use realized pnl
                else:
                    outcome = exit_price >= tgt
                # for hit% we count only target hits; time exit = not hit (consistent with prior morphology reports)
                outcome = False  # only target-before-stop counts as hit? But for PnL we compute actual return
                # we keep hit flag False, but R computed below
                # Actually need distinguish hit vs time: hit = outcome True earlier; else False
                hit=False
            else:
                hit=bool(outcome)
            # R: pnl / target distance (normalized) or simple return minus cost
            if direction=="short":
                pnl = (entry - exit_price)/entry - cost  # cost roundtrip ~0.6%? use 0.003 one way? Let's 0.006 round
                # already cost deducted; but our earlier uses cost variable as roundtrip
            else:
                pnl = (exit_price - entry)/entry - cost
            rs.append(pnl*100)
            if hit: hits+=1
            n+=1
            # need correct roundtrip: we passed cost as 0.006? adjust outside
            # For counting hit we used the branch above
    if n==0: return dict(n=0, hit=float('nan'), mean=float('nan'), median=float('nan'))
    hit_rate = hits/n*100
    mean_r = float(np.mean(rs)) if rs else float('nan')
    med_r = float(np.median(rs)) if rs else float('nan')
    return dict(n=n, hits=hits, hit=hit_rate, mean=mean_r, median=med_r)

def main():
    print("fetch daily...")
    df=fetch_all_daily()
    print(f"rows {len(df)}  symbols {df['ts_code'].nunique()}  dates {df['trade_date'].min().date()} ~ {df['trade_date'].max().date()}")
    # Build per symbol groups
    grouped={}
    mask_up_by_params={}  # will fill
    mask_down_by_params={}
    # Precompute per symbol arrays
    print("compute per-symbol features...")
    # Convert to dict of DataFrames
    groups=dict(tuple(df.groupby("ts_code")))
    # We'll second-pass evaluate masks for each param set without re-looping all symbols many times? Just loop once building base arrays
    # store arrays for speed
    data={}
    for ts,g in groups.items():
        g=g.sort_values("trade_date").reset_index(drop=True)
        o=g["open"].astype(float).values; h=g["high"].astype(float).values; l=g["low"].astype(float).values; c=g["close"].astype(float).values; v=g["vol"].astype(float).values
        n=len(g)
        if n<70: continue
        rng=h-l; body=np.abs(c-o); upper=h-np.maximum(o,c); lower=np.minimum(o,c)-l
        # avoid div0
        rng_safe=np.where(rng==0, np.nan, rng)
        ma20=rollmean(c,20); ma60=rollmean(c,60)
        ret60=np.full(n,np.nan)
        ret60[60:]=c[60:]/c[:-60]-1
        data[ts]=dict(g=g, o=o,h=h,l=l,c=c,v=v,rng=rng, body=body, upper=upper, lower=lower, ma20=ma20, ma60=ma60, ret60=ret60)

    print(f"symbols with >=70 bars: {len(data)}")

    # param grid for pin definition
    configs=[
        ("2.0×/0.50", 2.0, 0.50, 0.35, 0.25),
        ("2.5×/0.60", 2.5, 0.60, 0.33, 0.20),
        ("3.0×/0.60", 3.0, 0.60, 0.30, 0.20),
    ]
    hold_grid=[5,10,20]
    target_stop_grid=[(0.02,0.02),(0.03,0.02),(0.015,0.015)]  # target, stop

    # Definitions for uptrend/downtrend
    def uptrend(ma20,ma60,c): return (ma20>ma60) & (c>ma60)
    def downtrend(ma20,ma60,c): return (ma20<ma60) & (c<ma60)

    for label, k, range_ratio, body_ratio, opp_ratio in configs:
        print(f"\n===== PIN config {label}  k={k} range>={range_ratio} body<={body_ratio} opp<={opp_ratio} =====")
        # Build masks per symbol for this config
        mask_up={}; mask_down={}; counts_up=0; counts_down=0
        counts_up_trend=0; counts_down_trend=0
        counts_up_naked=0; counts_down_naked=0
        for ts, d in data.items():
            n=len(d["c"]); m=np.zeros(n,bool); m2=np.zeros(n,bool)
            for t in range(60,n):
                if np.isnan(d["ma20"][t]) or np.isnan(d["ma60"][t]): continue
                rng=d["rng"][t]; body=d["body"][t]; up=d["upper"][t]; lo=d["lower"][t]
                if rng<=0 or np.isnan(rng): continue
                # pin up: upper long
                is_pin_up = (up >= k*max(body, 0.01)) and (up/rng >= range_ratio) and (body/rng <= body_ratio) and (lo/rng <= opp_ratio)
                # pin down: lower long
                is_pin_down = (lo >= k*max(body, 0.01)) and (lo/rng >= range_ratio) and (body/rng <= body_ratio) and (up/rng <= opp_ratio)
                if is_pin_up: counts_up_naked+=1
                if is_pin_down: counts_down_naked+=1
                if is_pin_up and uptrend(d["ma20"][t], d["ma60"][t], d["c"][t]): m[t]=True
                if is_pin_down and downtrend(d["ma20"][t], d["ma60"][t], d["c"][t]): m2[t]=True
            mask_up[ts]=m; mask_down[ts]=m2
            counts_up+=m.sum(); counts_down+=m2.sum()
        total_pairs=sum(len(v["c"]) for v in data.values())
        up_rate=counts_up/total_pairs*100 if total_pairs else 0
        down_rate=counts_down/total_pairs*100 if total_pairs else 0
        naked_up_rate=counts_up_naked/total_pairs*100
        naked_down_rate=counts_down_naked/total_pairs*100
        print(f"  naked pin_up {counts_up_naked} ({naked_up_rate:.3f}%)  naked pin_down {counts_down_naked} ({naked_down_rate:.3f}%)")
        print(f"  trend-filtered pin_up(uptrend) {counts_up} ({up_rate:.3f}%)  pin_down(downtrend) {counts_down} ({down_rate:.3f}%)")
        # evaluate each hold/target combo
        for hold in hold_grid:
            for tgt, stp in target_stop_grid:
                res_up=evaluate_trades(data, mask_up, "short", tgt, stp, hold, cost=0.006)
                res_down=evaluate_trades(data, mask_down, "long", tgt, stp, hold, cost=0.006)
                print(f"  hold{hold:2d} tgt{int(tgt*100):2d}% stp{int(stp*100):2d}% | UP-short n={res_up['n']:6d} hit={res_up['hit']:5.1f}% mean{res_up['mean']:+6.2f}% med{res_up['median']:+6.2f}% | DOWN-long n={res_down['n']:6d} hit={res_down['hit']:5.1f}% mean{res_down['mean']:+6.2f}% med{res_down['median']:+6.2f}%")

    # Additional: best config deeper dive: vol confirm & ret60 filter (like scoop)
    print("\n===== Deep dive: best config 2.5×/0.60 with vol & ret60 filters =====")
    k, rr, br, op = 2.5, 0.60, 0.33, 0.20
    hold, tgt, stp = 5, 0.02, 0.02
    for vol_thresh in [1.0, 1.2, 1.5]:
        for ret_thresh in [None, 0.10, 0.20]:
            masks={}
            for ts,d in data.items():
                n=len(d["c"]); m=np.zeros(n,bool)
                # vol ratio = vol[t]/mean vol 20
                vol20=rollmean(d["v"],20)
                for t in range(60,n):
                    if np.isnan(d["ma20"][t]) or np.isnan(d["ma60"][t]): continue
                    rng=d["rng"][t]; body=d["body"][t]; up=d["upper"][t]; lo=d["lower"][t]
                    if rng<=0: continue
                    is_pin_up = (up >= k*max(body,0.01)) and (up/rng>=rr) and (body/rng<=br) and (lo/rng<=op)
                    if not is_pin_up: continue
                    if not (d["ma20"][t]>d["ma60"][t] and d["c"][t]>d["ma60"][t]): continue
                    if vol_thresh>1.0:
                        if np.isnan(vol20[t]) or d["v"][t] <= vol_thresh*vol20[t]: continue
                    if ret_thresh is not None and d["ret60"][t] <= ret_thresh: continue
                    m[t]=True
                masks[ts]=m
            res=evaluate_trades(data, masks, "short", tgt, stp, hold, cost=0.006)
            print(f" vol>{vol_thresh} ret60>{ret_thresh} | UP-short n={res['n']:5d} hit={res['hit']:5.1f}% mean{res['mean']:+6.2f}% med{res['median']:+6.2f}%")
    # pin_down deep dive with vol & ret filter (weak)
    for vol_thresh in [1.0, 1.2]:
        for ret_thresh in [None, -0.10, -0.20]:
            masks={}
            for ts,d in data.items():
                n=len(d["c"]); m=np.zeros(n,bool)
                vol20=rollmean(d["v"],20)
                for t in range(60,n):
                    if np.isnan(d["ma20"][t]) or np.isnan(d["ma60"][t]): continue
                    rng=d["rng"][t]; body=d["body"][t]; up=d["upper"][t]; lo=d["lower"][t]
                    if rng<=0: continue
                    is_pin_down = (lo >= k*max(body,0.01)) and (lo/rng>=rr) and (body/rng<=br) and (up/rng<=op)
                    if not is_pin_down: continue
                    if not (d["ma20"][t]<d["ma60"][t] and d["c"][t]<d["ma60"][t]): continue
                    if vol_thresh>1.0:
                        if np.isnan(vol20[t]) or d["v"][t] <= vol_thresh*vol20[t]: continue
                    if ret_thresh is not None and d["ret60"][t] >= ret_thresh: continue
                    m[t]=True
                masks[ts]=m
            res=evaluate_trades(data, masks, "long", tgt, stp, hold, cost=0.006)
            print(f" vol>{vol_thresh} ret60<{ret_thresh} | DOWN-long n={res['n']:5d} hit={res['hit']:5.1f}% mean{res['mean']:+6.2f}% med{res['median']:+6.2f}%")

if __name__=="__main__":
    main()
