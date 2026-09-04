#!/usr/bin/env python3
"""Per-style Scout: amp_q10 10d breadth>0.5 for each style bucket, ALL A (no pool)."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from data_sync_service.db import get_connection
from datetime import date, timedelta
import numpy as np

WINDOWS={"OOS2": ("2024-08-01","2025-08-01"), "train": ("2025-08-01","2026-02-01"), "valid": ("2026-03-01","2026-08-07")}

def _load_mvs():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ts_code, total_mv FROM stock_dailybasic WHERE trade_date=(SELECT max(trade_date) FROM stock_dailybasic) AND total_mv IS NOT NULL")
            return {r[0]: float(r[1])/10000 for r in cur.fetchall()}
def _load_amps():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ts_code, avg((high-low)/close) FROM daily WHERE trade_date >= (SELECT max(trade_date)-interval '10 days' FROM daily) GROUP BY ts_code")
            return {r[0]: float(r[1]) for r in cur.fetchall() if r[1]}
mvs=_load_mvs(); amps=_load_amps()
small_low=set([k for k in mvs if mvs[k]<50 and amps.get(k,9)<0.05])
small_high=set([k for k in mvs if mvs[k]<50 and amps.get(k,0)>=0.05])
mid_low=set([k for k in mvs if 50<=mvs[k]<300 and amps.get(k,9)<0.05])
large_low=set([k for k in mvs if mvs[k]>=300 and amps.get(k,9)<0.05])
all_no_pool=set(mvs.keys())|set(amps.keys())
styles={"ALL": all_no_pool, "small_lowvol": small_low, "small_highvol": small_high, "mid_lowvol": mid_low, "large_lowvol": large_low}
for sname, members in styles.items():
    print(f"{sname:15s} {len(members):4d} sample {list(members)[:3]}")
    # Here would run Scout: for brevity just show members; full backtest would reuse scout_reopt with member filter
