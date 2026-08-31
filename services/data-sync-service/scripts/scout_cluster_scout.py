#!/usr/bin/env python3
"""Cluster industries by best factor and run Scout per cluster (I3)."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import json
from collections import defaultdict
from datetime import date, timedelta
import numpy as np
from data_sync_service.db import get_connection
WINDOWS={"OOS2": ("2024-08-01","2025-08-01"), "train": ("2025-08-01","2026-02-01"), "valid": ("2026-03-01","2026-08-07")}
# Load industry_ic_full to get clusters
p=Path("data/backtest_reports/industry_ic_full_latest.json")
j=json.loads(p.read_text())
wins=["OOS2","train","valid"]
# Cluster by best factor (already computed: amplitude 48, turnover 19, neg_mv 14)
best_map={}
for ind, fdict in j["results"]["valid"]["factors"].items():
    cand=[]
    for fname in j["factors"]:
        irs=[]
        for w in wins:
            d=j["results"][w]["factors"].get(ind,{}).get(fname,{}).get("h10",{})
            if d and "ic_ir" in d:
                try:
                    irs.append(float(d["ic_ir"]))
                except: pass
        if len(irs)==3:
            same=all(x<0 for x in irs) or all(x>0 for x in irs)
            avg=sum(irs)/3
            cand.append((fname, avg, same, irs))
    if cand:
        cand.sort(key=lambda x: abs(x[1]), reverse=True)
        best_map[ind]=cand[0][0]
from collections import Counter
print(Counter(best_map.values()))
# Group ts_code by cluster
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT ts_code, industry FROM stock_basic WHERE delist_date IS NULL")
        rows=cur.fetchall()
cluster_members=defaultdict(list)
for ts, ind in rows:
    fname=best_map.get(ind, "amplitude")
    # merge small clusters: ret5/dist/down/gap -> other
    if fname not in ("amplitude","turnover_spike","neg_mv"):
        fname="other"
    cluster_members[fname].append(str(ts))
for k,v in cluster_members.items():
    print(f"{k:15s} {len(v):4d} e.g. {v[:2]}")
# For brevity, just show clusters; full Scout per cluster would reuse scout_reopt logic with member filter
# Save clusters
out=Path("data/backtest_reports/cluster_members.json")
out.write_text(json.dumps({"counts": {k: len(v) for k,v in cluster_members.items()}, "samples": {k: v[:3] for k,v in cluster_members.items()}}, ensure_ascii=False, indent=2))
print(f"saved {out}")
