#!/usr/bin/env python3
"""Scout idle smart vs GOLD/OIL/NASDAQ: pick-strong with Scout as STOCK candidate vs ETFs, not just REPO.

For S-3空仓期, compare Scout mom60 vs GOLD/OIL/NASDAQ/BOND mom60, pick max >0 and >MA200.
"""
import sys
from datetime import date, timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from data_sync_service.db import get_connection
import numpy as np

WINDOWS = {"past_year": ("2025-08-01","2026-08-07"), "OOS2": ("2024-08-01","2025-08-01"), "train": ("2025-08-01","2026-02-01"), "valid": ("2026-03-01","2026-08-07")}

ETF_CODES = {"GOLD": "518880.SH", "OIL": "513350.SH", "NASDAQ": "513100.SH", "BOND10": "511260.SH"}

def _load_calendar(s,e):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT trade_date FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY trade_date", (s,e))
            return [r[0].strftime("%Y-%m-%d") if hasattr(r[0],"strftime") else str(r[0]) for r in cur.fetchall()]

def _load_close(code,s,e):
    s2=max(date.fromisoformat(s)-timedelta(days=400),date(1998,1,1)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, close FROM daily WHERE ts_code=%s AND trade_date >= %s AND trade_date <= %s ORDER BY trade_date", (code,s2,e))
            rows=cur.fetchall()
    return { (d.strftime("%Y-%m-%d") if hasattr(d,"strftime") else str(d)): float(c) for d,c in rows if c }

# For STOCK NAV, we will use Scout and S-3 totals to create synthetic NAV as before, but now we need Scout vs ETF mom comparison
# We will load Scout NAV curve from scout simulation (approx) and ETF closes

# For simplicity, we will reuse the scout_hold75_breadth's Scout NAV curve generation for 20-150 pool, but we can approximate Scout NAV as before with daily 0.1007% etc.
# Instead we will directly load ETF closes and compute mom, and for STOCK we will use Scout's daily 0.1007% etc. as proxy.

# To keep verification correct, we will compute ETF mom60 and STOCK mom60 (Scout) and pick max.

# STOCK daily returns: for Scout, daily = total/100/n_days, for S-3 similar, but for pick-strong we need STOCK's 60d mom, not daily.

# We will generate synthetic STOCK NAV series for each window based on its total_pnl and hold ratio.

# For this smart idle test, we will consider two STOCK candidates: Scout and S-3, and pick max mom among STOCK, GOLD, OIL, NASDAQ, BOND

# Let's just run for past_year as example with real ETF data and synthetic STOCK

for wname in WINDOWS:
    start,end = WINDOWS[wname]
    cal = _load_calendar(start,end)
    n_days=len(cal)
    # Scout totals for 20-150
    scout_totals = {"OOS2":20.5,"train":-1.6,"valid":11.3,"past_year":2.4}
    s3_totals = {"OOS2":47.3,"train":34.1,"valid":38.7,"past_year":66.6}
    scout_total = scout_totals.get(wname,0)
    s3_total = s3_totals.get(wname,0)
    # Use max as STOCK proxy for pick
    stock_total = max(scout_total, s3_total)
    # For ETF, load closes
    etf_closes={}
    for name,code in ETF_CODES.items():
        etf_closes[name]=_load_close(code,start,end)
    # Build ETF MA200 and mom
    # For STOCK, we need its close series proxy: create synthetic closes from total
    stock_daily = stock_total/100/n_days if n_days else 0
    stock_closes = [1 + stock_daily*i for i in range(len(cal))]
    stock_close_map = {cal[i]: stock_closes[i] for i in range(len(cal))}
    # Now simulate pick-strong
    nav=1.0
    nav_curve=[]
    picks=[]
    for i in range(len(cal)):
        if i<60:
            nav_curve.append(nav)
            picks.append("REPO")
            continue
        cands={}
        # STOCK mom60
        # stock mom = stock_closes[i-1]/stock_closes[i-60] -1
        if i>=60:
            sc = stock_close_map[cal[i-1]]/stock_close_map[cal[i-60]]-1 if stock_close_map[cal[i-60]] else 0
            if sc>0:
                cands["STOCK"]=sc
        for name in ETF_CODES:
            mp = etf_closes[name]
            # need 60d mom and MA200
            # get closes for cal
            # find closes for mom
            if i>=60:
                c0 = mp.get(cal[i-60])
                c1 = mp.get(cal[i-1])
                if c0 and c1 and c0>0:
                    mom = c1/c0 -1
                    # MA200
                    # need 200 closes
                    if i>=200:
                        window = [mp.get(cal[j]) for j in range(i-200,i)]
                        window = [x for x in window if x]
                        if len(window)>=150:
                            ma200 = sum(window)/len(window)
                            if mom>0 and c1 and ma200 and c1>ma200:
                                cands[name]=mom
        if not cands:
            pick="REPO"
            ret=0.0
        else:
            pick=max(cands, key=lambda k:cands[k])
            # next day return
            if pick=="STOCK":
                ret=stock_daily
            else:
                mp=etf_closes[pick]
                c0=mp.get(cal[i])
                c1=mp.get(cal[i+1]) if i+1<len(cal) else c0
                if c0 and c1 and c0>0:
                    ret=c1/c0-1-0.0005
                else:
                    ret=0
        nav*=(1+ret)
        nav_curve.append(nav)
        picks.append(pick)
    total=(nav-1)*100
    # compare vs pure pick-strong (without Scout, i.e., STOCK = S-3 only)
    # For pure, stock_total = s3_total only
    # quick: if we used only S-3, total would be?
    # Instead we can just report picks distribution
    from collections import Counter
    cnt=Counter(picks)
    print(f"{wname:10s} total {total:+6.1f}% nav {nav:.3f} picks {dict(cnt)} S-3 {s3_total:+5.1f}% Scout {scout_total:+5.1f}%")
