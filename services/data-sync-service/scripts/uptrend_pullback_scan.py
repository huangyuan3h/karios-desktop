"""Uptrend pullback entry test — does buying the dip in uptrend have excess?
Method same as §22.7 box studies: ahead 10/20/60d, CN liquid1500 aggregation, 2021-08~2026-08.
"""
from __future__ import annotations
import numpy as np, pandas as pd, psycopg
from data_sync_service.config import get_settings

def rollmean(a, w):
    a = np.asarray(a,float); cs=np.cumsum(np.concatenate([[0.0],a]))
    out=np.full(len(a),np.nan); idx=np.arange(w-1,len(a))
    out[idx]=(cs[idx+1]-cs[idx-w+1])/w; return out

def fetch_all_daily():
    s=get_settings()
    conn=psycopg.connect(s.database_url)
    cur=conn.cursor()
    cur.execute("""
        SELECT ts_code, trade_date, open, high, low, close, vol, amount
        FROM daily
        WHERE trade_date >= '2021-08-01'
        ORDER BY ts_code, trade_date
    """)
    rows=cur.fetchall()
    conn.close()
    df=pd.DataFrame(rows, columns=["ts_code","trade_date","open","high","low","close","vol","amount"])
    df["trade_date"]=pd.to_datetime(df["trade_date"])
    return df

def compute_forward(d, ahead):
    c=d["c"]
    f=np.full(len(c), np.nan)
    # ahead day close / today close -1
    f[:-ahead]=c[ahead:]/c[:-ahead]-1
    return f

def main():
    print("fetch...")
    df=fetch_all_daily()
    print(f"rows {len(df)} symbols {df['ts_code'].nunique()} {df['trade_date'].min().date()}~{df['trade_date'].max().date()}")
    groups=dict(tuple(df.groupby("ts_code")))
    # select liquid 1500 by avg amount last 60d? simplify: total amount
    amt_sum={}
    for ts,g in groups.items():
        amt_sum[ts]=g["amount"].astype(float).sum()
    top=sorted(amt_sum, key=lambda k: amt_sum[k], reverse=True)[:1500]
    top_set=set(top)
    print(f"liquid1500 selected")
    data={}
    for ts in top:
        g=groups[ts].sort_values("trade_date").reset_index(drop=True)
        n=len(g)
        if n<120: continue
        c=g["close"].astype(float).values; h=g["high"].astype(float).values; l=g["low"].astype(float).values; o=g["open"].astype(float).values; v=g["vol"].astype(float).values
        ma20=rollmean(c,20); ma60=rollmean(c,60); ma200=rollmean(c,200)
        # ma slopes
        ma60_slope=np.full(n,np.nan); ma60_slope[70:]=ma60[70:]-ma60[60:-10] # 10d slope approx (t vs t-10)
        # rsi14
        delta=np.diff(c, prepend=np.nan)
        gain=np.where(delta>0, delta, 0); loss=np.where(delta<0, -delta, 0)
        avg_gain=rollmean(gain,14); avg_loss=rollmean(loss,14)
        rs=np.where(avg_loss==0, np.nan, avg_gain/avg_loss)
        rsi=100 - 100/(1+rs)
        # max high 20
        # rolling max
        high20=np.full(n,np.nan)
        for i in range(19,n):
            high20[i]=np.max(h[i-19:i+1])
        data[ts]=dict(c=c, h=h,l=l,o=o,ma20=ma20,ma60=ma60,ma200=ma200, rsi=rsi, high20=high20, ma60_slope=ma60_slope, n=n)

    print(f"usable {len(data)}")

    # base forward stats for 10/20/60
    for ahead in [10,20,60]:
        all_rets=[]
        for ts,d in data.items():
            f=compute_forward(d,ahead)
            # valid where not nan and ma60 defined? use all
            valid=f[~np.isnan(f)]
            all_rets.append(valid)
        all_rets=np.concatenate(all_rets)
        print(f"base ahead{ahead} n={len(all_rets)} mean={np.mean(all_rets)*100:.2f}% median={np.median(all_rets)*100:.2f}% win={(all_rets>0).mean()*100:.1f}% >10%={(all_rets>0.10).mean()*100:.1f}%")

    # definitions
    def up_A(d, t): # close>ma20>ma60
        return d["c"][t]>d["ma20"][t] and d["ma20"][t]>d["ma60"][t]
    def up_B(d, t): # ma20>ma60 and ma60 rising
        return d["ma20"][t]>d["ma60"][t] and d["ma60_slope"][t]>0
    def up_C(d, t): # close>ma60 and ma60 rising
        return d["c"][t]>d["ma60"][t] and d["ma60_slope"][t]>0
    def up_D(d, t): # close>ma200 (long trend)
        return not np.isnan(d["ma200"][t]) and d["c"][t]>d["ma200"][t]

    up_defs={"A_close>20>60":up_A, "B_20>60+slope>0":up_B, "C_close>60+slope>0":up_C, "D_close>200":up_D}

    def pull_P1_touch20(d,t, thr=0.02): # |c-ma20|/ma20 < thr
        return abs(d["c"][t]-d["ma20"][t])/d["ma20"][t] < thr
    def pull_P2_between(d,t):
        return d["ma60"][t] < d["c"][t] < d["ma20"][t]  # between
    def pull_P3_drawdown(d,t, thr=0.03): # c/high20 < 1-thr
        return d["c"][t]/d["high20"][t] < 1-thr
    def pull_P4_rsi_cool(d,t, thr=50):
        return d["rsi"][t] < thr
    def pull_P5_below20_above60(d,t):
        return d["c"][t] < d["ma20"][t] and d["c"][t] > d["ma60"][t]

    pull_defs={
        "P1_touch20±2%": lambda d,t: pull_P1_touch20(d,t,0.02),
        "P1_touch20±3%": lambda d,t: pull_P1_touch20(d,t,0.03),
        "P2_between20_60": pull_P2_between,
        "P3_dd3%_from20h": lambda d,t: pull_P3_drawdown(d,t,0.03),
        "P3_dd5%": lambda d,t: pull_P3_drawdown(d,t,0.05),
        "P4_rsi<50": lambda d,t: pull_P4_rsi_cool(d,t,50),
        "P4_rsi<45": lambda d,t: pull_P4_rsi_cool(d,t,45),
        "P5_below20_above60": pull_P5_below20_above60,
    }

    # also combo: pull + narrow? we test combos later
    for ahead in [10,20,60]:
        print(f"\n===== ahead {ahead}d =====")
        # compute base for this ahead
        all_rets=np.concatenate([compute_forward(d,ahead)[~np.isnan(compute_forward(d,ahead))] for d in data.values()])
        base_mean=np.mean(all_rets)*100; base_win=(all_rets>0).mean()*100
        print(f"base mean {base_mean:.2f}% win {base_win:.1f}%")
        for up_name, up_fn in up_defs.items():
            for pull_name, pull_fn in pull_defs.items():
                rets=[]
                n=0
                for ts,d in data.items():
                    c=d["c"]
                    # iterate t from 70 to n-ahead-1
                    for t in range(70, d["n"]-ahead):
                        if np.isnan(d["ma20"][t]) or np.isnan(d["ma60"][t]) or np.isnan(d["high20"][t]): continue
                        if not up_fn(d,t): continue
                        if not pull_fn(d,t): continue
                        f = c[t+ahead]/c[t]-1
                        if np.isnan(f): continue
                        rets.append(f)
                if len(rets)<500:
                    continue
                arr=np.array(rets)
                mean=np.mean(arr)*100; median=np.median(arr)*100; win=(arr>0).mean()*100; gt10=(arr>0.10).mean()*100
                excess=mean-base_mean
                flag=" **" if excess>1.0 and win>50 else ""
                print(f" {up_name:18s} + {pull_name:20s} n={len(rets):6d} mean{mean:+6.2f}% med{median:+6.2f}% win{win:4.1f}% >10%{gt10:4.1f}% excess{excess:+5.2f}%{flag}")

    # combo deep dive: best candidates with volume filter?
    print("\n===== deep dive: up_B + pull variants with vol filter (vol >1.2*20d) =====")
    for ahead in [10,20]:
        print(f"-- ahead {ahead} --")
        for pull_name in ["P1_touch20±2%", "P5_below20_above60", "P3_dd3%_from20h"]:
            pull_fn=pull_defs[pull_name]
            # collect with vol filter vs without for comparison already above, just show extra
            pass

if __name__=="__main__":
    main()
