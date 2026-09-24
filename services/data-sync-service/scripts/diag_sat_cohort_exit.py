#!/usr/bin/env python3
"""H-SAT-COHORT-EXIT read-only (S3): when a same-day basket is jointly weak at
day-2 14:30, is it better to exit then instead of holding to day-3 14:30?

Read-only, no engine change, zero grid. Uses frozen body=3 habit fills and the
14:30 bar prints. Feeds a possible prereg.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_sat_cohort_exit.py
"""

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from eval_sat_idle_parking import SLOT_PCT  # noqa: E402
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    load_sgap_context,
    replay_sgap_from_context,
)

WINS = {k: WINDOWS[k] for k in ("OOS2", "train", "valid", "long")}


def main() -> int:
    ls, le = WINS["long"]
    print(f"H-SAT-COHORT-EXIT long {ls}~{le}\n", flush=True)
    ctx = load_sgap_context(ls, le)
    sat = replay_sgap_from_context(
        ctx,
        start=ls,
        end=le,
        skip_t1_limit=True,
        pool_mode="strict",
        max_pos=4,
        position_pct=SLOT_PCT,
        body=3,
        fill_mode=FILL_SAME_1430,
        fill_hhmm="1430",
        exit_hhmm="1430",
        max_open_to_1430_pct=0.03,
        rank_key="amp_1430",
        gate_1430=True,
    )
    px = ctx.get("px_1430") or {}
    cal = ctx["cal"]
    ci = ctx["idx_by_day"]
    fills = [
        b
        for b in (sat.get("blotter") or [])
        if b.get("kind") == "fill" and b.get("contribPct") is not None
    ]

    recs = []
    for b in fills:
        ts, e0 = str(b["ts"]), str(b["entryDate"])
        i = ci.get(e0, -1)
        if i < 0 or i + 2 >= len(cal):
            continue
        mid, day3 = cal[i + 1], cal[i + 2]
        p0 = (px.get(ts) or {}).get(e0)
        p1 = (px.get(ts) or {}).get(mid)
        p2 = (px.get(ts) or {}).get(day3)
        if not p0 or not p1 or not p2:
            continue
        recs.append(
            {
                "day": e0,
                "ts": ts,
                "r_mid": p1 / p0 - 1,
                "r_exit": p2 / p0 - 1,
                "r_mid_to_exit": p2 / p1 - 1,
            }
        )

    cohorts: dict[str, list] = defaultdict(list)
    for r in recs:
        cohorts[r["day"]].append(r)

    print(f"usable fills {len(recs)} (px_1430 complete)\n")
    print("EARLY-EXIT RULE: if >=k of the basket is red at day-2 14:30, exit all at day-2")
    for k in (2, 3, 4):
        d_early, d_hold, n_aff = 0.0, 0.0, 0
        for c in cohorts.values():
            n = len(c)
            if n < k:
                continue
            kred = sum(1 for r in c if r["r_mid"] < 0)
            if kred < k:
                continue
            n_aff += len(c)
            for r in c:
                d_early += r["r_mid"] * SLOT_PCT
                d_hold += r["r_exit"] * SLOT_PCT
        delta = d_early - d_hold
        print(
            f"  k={k}: affected fills {n_aff:>4}  early sum {100*d_early:+8.1f}pt  "
            f"hold sum {100*d_hold:+8.1f}pt  delta {100*delta:+7.1f}pt"
        )

    # per-window for k=3
    print("\nPER-WINDOW (k=3)")
    for w, (s, e) in WINS.items():
        de = dh = 0.0
        for c in cohorts.values():
            if not (s <= c[0]["day"] <= e):
                continue
            if len(c) < 3 or sum(1 for r in c if r["r_mid"] < 0) < 3:
                continue
            for r in c:
                de += r["r_mid"] * SLOT_PCT
                dh += r["r_exit"] * SLOT_PCT
        print(f"  {w:<6} n_baskets-affected early {100*de:+7.1f} vs hold {100*dh:+7.1f}  delta {100*(de-dh):+7.1f}pt")

    # unconditional: for fills whose mid is red, is mid->exit positive?
    red = [r for r in recs if r["r_mid"] < 0]
    grn = [r for r in recs if r["r_mid"] >= 0]
    print(
        f"\nALL fills: red@mid n={len(red)} mid->exit mean {100*np.mean([r['r_mid_to_exit'] for r in red]):+.2f}%  |  "
        f"green@mid n={len(grn)} mid->exit mean {100*np.mean([r['r_mid_to_exit'] for r in grn]):+.2f}%"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
