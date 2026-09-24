#!/usr/bin/env python3
"""H-SAT-COHORT-STRUCT read-only (S2): are same-day satellite baskets
same-industry, and does industry concentration predict cohort outcome?

Read-only; no strategy change. Uses frozen body=3 habit fills + EM industry.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_sat_industry.py
"""

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from eval_sat_idle_parking import _sat_book  # noqa: E402
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.db.stock_eastmoney_industry import lookup_by_ts_codes  # noqa: E402


def main() -> int:
    s, e = WINDOWS["long"]
    print(f"H-SAT-COHORT-STRUCT long {s}~{e}\n", flush=True)
    sat = _sat_book(s, e)
    fills = [
        b
        for b in (sat.get("blotter") or [])
        if b.get("kind") == "fill" and b.get("contribPct") is not None
    ]
    tss = sorted({str(b["ts"]) for b in fills})
    ind = lookup_by_ts_codes(tss)
    missed = sum(1 for ts in tss if not ind.get(ts))
    print(f"fills {len(fills)}  symbols {len(tss)}  industry missing {missed} ({100*missed/len(tss):.0f}%)")

    cohorts: dict[str, list] = defaultdict(list)
    for b in fills:
        cohorts[str(b["entryDate"])].append(b)

    # cohort industry concentration
    multi = [(d, c) for d, c in cohorts.items() if len(c) >= 2]
    same_ind, mixed_ind = [], []
    for _d, c in multi:
        inds = [ind.get(str(b["ts"])) for b in c]
        known = [x for x in inds if x]
        mc = float(np.mean([float(b["contribPct"]) for b in c]))
        one = len(known) >= 2 and len(set(known)) == 1
        (same_ind if one else mixed_ind).append(mc)
    print(
        f"\nMULTI-FILL COHORTS n={len(multi)}  "
        f"all-same-industry {len(same_ind)}  mixed {len(mixed_ind)}"
    )
    if same_ind:
        print(f"  same-industry cohort mean contrib {np.mean(same_ind):+5.2f}%")
    if mixed_ind:
        print(f"  mixed-industry  cohort mean contrib {np.mean(mixed_ind):+5.2f}%")

    # distribution of distinct industries per cohort
    dist = defaultdict(int)
    for _d, c in multi:
        known = {ind.get(str(b["ts"])) for b in c} - {None}
        dist[len(known) if known else 0] += 1
    print("  distinct industries per multi-cohort:", dict(sorted(dist.items())))

    # does a dominant industry (>=3 of 4) hurt?
    dom, nondom = [], []
    for _d, c in multi:
        inds = [ind.get(str(b["ts"])) for b in c if ind.get(str(b["ts"]))]
        if not inds:
            continue
        top = max(set(inds), key=inds.count)
        frac = inds.count(top) / len(inds)
        mc = float(np.mean([float(b["contribPct"]) for b in c]))
        (dom if frac >= 0.75 else nondom).append(mc)
    print(
        f"\n  dominant(>=75% one ind) n={len(dom)} mean {np.mean(dom) if dom else float('nan'):+5.2f}%  "
        f"diverse n={len(nondom)} mean {np.mean(nondom) if nondom else float('nan'):+5.2f}%"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
