#!/usr/bin/env python3
"""Three-window walk-forward for impulse high-confidence + stock concession.

Impulses (G1 absolute, 661d):
 - OIL RSI<25 -> BUY OIL 10d win90% n30
 - OIL RSI<30 -> BUY OIL 86% n64
 - NASDAQ RSI>75 -> BUY NASDAQ 78% n126
 - GOLD RSI<30 -> BUY GOLD 75% n32
 - OIL RSI>70 -> GOLD > OIL 73% n74 (relative)

Stock concession = when impulse active and S-3 has candidates, compare expected 10d:
  if impulse win>70% and stock win (valid 81% but OOS2 48%) -> pick impulse

We test each impulse as sleeve on idle (like T6) + concession (head-to-head).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"scripts"))
from run_walk_forward import S3_CONFIG, WINDOWS
from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData, simulate
from data_sync_service.service.portfolio_nav_sim import load_third_asset_cache, simulate_sleeve_nav
import psycopg, pandas as pd, numpy as np
from data_sync_service.config import get_settings

def load_close(ts):
    s=get_settings()
    conn=psycopg.connect(s.database_url)
    cur=conn.cursor()
    cur.execute("select trade_date, close from daily where ts_code=%s order by trade_date", (ts,))
    df=pd.DataFrame(cur.fetchall(), columns=["date","close"])
    df["date"]=pd.to_datetime(df["date"])
    df=df.set_index("date").sort_index()
    df["close"]=df["close"].astype(float)
    conn.close()
    return df

# closes for impulses
closes={
 "GOLD": load_close("518880.SH"),
 "OIL": load_close("513350.SH"),
 "NASDAQ": load_close("513100.SH"),
}
# RSI helper
def rsi(s, n=14):
    delta=s.diff()
    gain=delta.where(delta>0,0).rolling(n).mean()
    loss=(-delta.where(delta<0,0)).rolling(n).mean()
    rs=gain/loss.replace(0, np.nan)
    return 100-100/(1+rs)

for k in closes:
    closes[k]["RSI"]=rsi(closes[k]["close"])
    closes[k]["MA20"]=closes[k]["close"].rolling(20).mean()
    closes[k]["MA60"]=closes[k]["close"].rolling(60).mean()

def impulse_signal(day, impulse):
    """Return ts to buy or None. day is Timestamp."""
    try:
        if impulse=="OIL_RSI25":
            if closes["OIL"].loc[day, "RSI"] <25: return "513350.SH"
        elif impulse=="OIL_RSI30":
            if closes["OIL"].loc[day, "RSI"] <30: return "513350.SH"
        elif impulse=="NASDAQ_RSI75":
            if closes["NASDAQ"].loc[day, "RSI"] >75: return "513100.SH"
        elif impulse=="GOLD_RSI30":
            if closes["GOLD"].loc[day, "RSI"] <30: return "518880.SH"
        elif impulse=="GOLD_OVER_OIL":
            if closes["OIL"].loc[day, "RSI"] >70: return "518880.SH"
    except: pass
    return None

def simulate_impulse(impulse, window, concession=False):
    start,end=WINDOWS[window]
    cfg=BacktestConfig(start_date=start, end_date=end, **S3_CONFIG)
    data=BacktestData(cfg)
    run=simulate(cfg, data)
    # build impulse pick per calendar day (t)
    # need ret for impulse ts
    ret_by_ts={}
    for ts, key in [("518880.SH","GOLD"),("513350.SH","OIL"),("513100.SH","NASDAQ")]:
        df=closes[key]
        # ret map
        mp={str(d.date()): float(v) for d,v in df["close"].items()}
        # but calendar is string YYYY-MM-DD
        sorted_days=sorted(mp.keys())
        ret={}
        for i in range(1,len(sorted_days)):
            d=sorted_days[i]; prev=sorted_days[i-1]
            if mp[prev]!=0: ret[d]=mp[d]/mp[prev]-1
        ret_by_ts[ts]=ret
    snap_by_day={str(s.get("date")): s for s in run.positions_by_day}
    day_idx={d:i for i,d in enumerate(data.calendar)}
    nav_base=1; nav_imp=1
    peak_base=1; peak_imp=1
    max_dd_base=0; max_dd_imp=0
    for day in data.calendar:
        dt=pd.Timestamp(day)
        snap=snap_by_day.get(day)
        deployed_ret=0; deployed_pct=0
        if snap:
            for pos in snap.get("positions") or []:
                try: pct=float(pos.get("position_pct") or 0)
                except: continue
                if pct<=0: continue
                entry=str(pos.get("entry_date") or "")
                if entry and day <= entry: continue
                closes_d=data.close_by_ts_day.get(str(pos.get("ts_code") or "")) or {}
                today=closes_d.get(day)
                idx=day_idx.get(day)
                prev=closes_d.get(data.calendar[idx-1]) if idx and idx>0 else None
                if today is not None and prev: deployed_ret+=pct*(today/prev-1)
                deployed_pct+=pct
        deployed_pct=min(1, deployed_pct)
        idle_pct=max(0, 1-deployed_pct)
        # impulse sleeve ret
        ts_imp=impulse_signal(dt, impulse) if dt in closes["OIL"].index else None
        # concession: if S-3 has candidates (gate open) and impulse active, compare: if impulse, stock gives way (idle becomes 1)
        has_candidates=False
        # approx: deployed_pct>0 or snapshot has positions? Use deployed
        # Better use cn_block gate: we approximate has_candidates = deployed_pct>0 or len(snap positions)>0
        # For concession test, if impulse active, force idle=1 (don't buy stock)
        if concession and ts_imp and snap and len(snap.get("positions") or [])>0:
            # stock gives way: treat deployed as 0, idle 1, sleeve gets full
            deployed_ret=0
            deployed_pct=0
            idle_pct=1.0
        sleeve_ret=0
        if idle_pct>0 and ts_imp:
            sleeve_ret=ret_by_ts.get(ts_imp, {}).get(day,0)
            # leverage: high confidence 90% -> 2x, 78% ->1.5x
            lev=2.0 if impulse=="OIL_RSI25" else 1.5 if impulse in ("NASDAQ_RSI75","OIL_RSI30") else 1.0
            sleeve_ret*=lev
        nav_base*=1+deployed_ret
        nav_imp*=1+deployed_ret+idle_pct*sleeve_ret
        peak_base=max(peak_base, nav_base)
        peak_imp=max(peak_imp, nav_imp)
        if peak_base>0: max_dd_base=max(max_dd_base, (peak_base-nav_base)/peak_base)
        if peak_imp>0: max_dd_imp=max(max_dd_imp, (peak_imp-nav_imp)/peak_imp)
    return {"base":(nav_base-1)*100, "imp":(nav_imp-1)*100, "delta":(nav_imp-nav_base)*100, "ddBase":max_dd_base*100, "ddImp":max_dd_imp*100}

impulses=["OIL_RSI25","OIL_RSI30","NASDAQ_RSI75","GOLD_RSI30","GOLD_OVER_OIL"]
for imp in impulses:
    print(f"\n=== {imp} (idle sleeve) ===")
    for w in ["OOS2","train","valid"]:
        r=simulate_impulse(imp, w, concession=False)
        print(f"{w:6} base {r['base']:5.1f} imp {r['imp']:5.1f} delta {r['delta']:+5.1f} ddImp {r['ddImp']:4.1f}")
    print(f"--- {imp} with stock concession (impulse > stock) ---")
    for w in ["OOS2","train","valid"]:
        r=simulate_impulse(imp, w, concession=True)
        print(f"{w:6} base {r['base']:5.1f} imp {r['imp']:5.1f} delta {r['delta']:+5.1f}")

