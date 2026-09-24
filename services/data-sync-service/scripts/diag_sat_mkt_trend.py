#!/usr/bin/env python3
"""H-SAT-MKT-TREND read-only: does the satellite buy at market highs?

User intuition: "the buy point is exactly the high". Test on the frozen
body=3 fills: is the market (CSI300) extended above its MA20/MA60 and/or
overbought (ret5) on entry days, and do such cohorts perform worse?

Features use the PRIOR session close (fully knowable before the 14:30 entry) —
no look-ahead. No strategy change, zero grid.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_sat_mkt_trend.py
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

from data_sync_service.service.homeport import _series_on_cal, load_risk_closes  # noqa: E402


def main() -> int:
    s, e = WINDOWS["long"]
    print(f"H-SAT-MKT-TREND long {s}~{e}\n", flush=True)
    sat = _sat_book(s, e)
    rows = sat["rows"]
    dates = [str(r["date"]) for r in rows]
    idx = {d: i for i, d in enumerate(dates)}
    s300 = _series_on_cal(load_risk_closes()["510300.SH"], dates)
    n = len(dates)

    fills = [
        b
        for b in (sat["blotter"] or [])
        if b.get("kind") == "fill" and b.get("contribPct") is not None
    ]
    cohorts: dict[str, list] = defaultdict(list)
    for b in fills:
        cohorts[str(b["entryDate"])].append(b)

    def dist_ma(i: int, lb: int) -> float | None:
        j = i - 1  # prior session close
        if j - lb < 0:
            return None
        ma = float(np.mean(s300[j - lb + 1 : j + 1]))
        return s300[j] / ma - 1 if ma > 0 else None

    # 1. is the market extended on entry days vs all days?
    def collect(vals):
        return np.array([v for v in vals if v is not None])

    entry_i = [idx[d] for d in cohorts if d in idx]
    d20_entry = collect([dist_ma(i, 20) for i in entry_i])
    d60_entry = collect([dist_ma(i, 60) for i in entry_i])
    all_i = list(range(60, n))
    d20_all = collect([dist_ma(i, 20) for i in all_i])
    d60_all = collect([dist_ma(i, 60) for i in all_i])
    print("MARKET STATE ON ENTRY DAYS (prior-close CSI300 vs MA)")
    print(
        f"  dist MA20: entry {100 * np.mean(d20_entry):+.2f}%  all days {100 * np.mean(d20_all):+.2f}%"
    )
    print(
        f"  dist MA60: entry {100 * np.mean(d60_entry):+.2f}%  all days {100 * np.mean(d60_all):+.2f}%"
    )

    # 2. forward 2-day market return after entry days vs all days
    def fwd2(i):
        return s300[min(i + 2, n - 1)] / s300[i] - 1 if s300[i] else 0.0

    fe = np.array([fwd2(i) for i in entry_i])
    fa = np.array([fwd2(i) for i in all_i])
    print(
        f"  CSI300 fwd2d after entry days {100 * np.mean(fe):+.2f}%  all days {100 * np.mean(fa):+.2f}%"
    )

    # 3. cohort mean contrib bucketed by MA state / extension
    def cohort_rows():
        out = []
        for d, c in cohorts.items():
            if d not in idx:
                continue
            out.append((idx[d], float(np.mean([float(b["contribPct"]) for b in c]))))
        return out

    cr = cohort_rows()
    d20 = {i: dist_ma(i, 20) for i, _ in cr}
    d60 = {i: dist_ma(i, 60) for i, _ in cr}
    print("\nCOHORT MEAN CONTRIB BY MARKET STATE")
    for label, fn in (
        ("MA20 above/below", lambda i: "above" if (d20[i] or 0) > 0 else "below"),
        ("MA60 above/below", lambda i: "above" if (d60[i] or 0) > 0 else "below"),
    ):
        buckets: dict[str, list[float]] = defaultdict(list)
        for i, mc in cr:
            buckets[fn(i)].append(mc)
        for k, v in buckets.items():
            print(f"  {label} {k:<6} n={len(v):>3} mean {np.mean(v):+5.2f}%")

    # dist20 quintiles
    valid = [(d20[i], mc) for i, mc in cr if d20[i] is not None]
    valid.sort()
    q = len(valid) // 5
    print("\nCOHORT MEAN CONTRIB BY CSI300 dist-from-MA20 QUINTILE")
    for b in range(5):
        seg = valid[b * q : (b + 1) * q] if b < 4 else valid[4 * q :]
        lo, hi = seg[0][0], seg[-1][0]
        print(
            f"  Q{b + 1} dist {100 * lo:+5.2f}%..{100 * hi:+5.2f}%  n={len(seg):>3}  mean {np.mean([m for _, m in seg]):+5.2f}%"
        )

    # per-window consistency (entry dates sliced from the long book)
    print("\nPER-WINDOW: cohort mean contrib by MA20 state (above/below)")
    for w in ("OOS2", "train", "valid"):
        ws, we = WINDOWS[w]
        sub = [(i, mc) for i, mc in cr if ws <= dates[i] <= we and d20[i] is not None]
        ab = [mc for i, mc in sub if d20[i] > 0]
        be = [mc for i, mc in sub if d20[i] <= 0]
        print(
            f"  {w:<6} above n={len(ab):>3} {np.mean(ab) if ab else float('nan'):+5.2f}%  "
            f"below n={len(be):>3} {np.mean(be) if be else float('nan'):+5.2f}%"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
