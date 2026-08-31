#!/usr/bin/env python3
"""Style classification for ALL A shares (no pool), then Scout R0-IC per style.

Styles: size (by mv), vol (amp), liq (turnover), val (pb if avail). For now 4 styles ×2 buckets.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from datetime import date, timedelta
from data_sync_service.db import get_connection
import numpy as np

def _load_all_mv():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ts_code, total_mv FROM stock_dailybasic WHERE trade_date=(SELECT max(trade_date) FROM stock_dailybasic) AND total_mv IS NOT NULL")
            return {r[0]: float(r[1])/10000 for r in cur.fetchall()}

def _load_all_amp():
    # amp per stock latest 10 days avg
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ts_code, avg((high-low)/close) FROM daily WHERE trade_date >= (SELECT max(trade_date)-interval '10 days' FROM daily) GROUP BY ts_code")
            return {r[0]: float(r[1]) for r in cur.fetchall() if r[1]}

mvs=_load_all_mv()
amps=_load_all_amp()
print(f"ALL mv {len(mvs)} amps {len(amps)}")
# Styles: small <50, mid 50-300, large >300; low vol amp<0.05 vs high>0.05
small=[k for k,v in mvs.items() if v<50]
mid=[k for k,v in mvs.items() if 50<=v<300]
large=[k for k,v in mvs.items() if v>=300]
lowvol=[k for k,v in amps.items() if v<0.05]
highvol=[k for k,v in amps.items() if v>=0.05]
print(f"size small {len(small)} mid {len(mid)} large {len(large)}")
print(f"vol low {len(lowvol)} high {len(highvol)}")
# Intersect for style buckets
for name, bucket in [("small_lowvol", set(small)&set(lowvol)), ("small_highvol", set(small)&set(highvol)), ("mid_lowvol", set(mid)&set(lowvol)), ("large_lowvol", set(large)&set(lowvol))]:
    print(f"{name:15s} {len(bucket):4d}")
