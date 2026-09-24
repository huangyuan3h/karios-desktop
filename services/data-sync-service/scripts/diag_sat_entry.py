#!/usr/bin/env python3
"""H-SAT-ENTRY read-only: do clean entry-day features predict the satellite's
2-day hold outcome?  (S1: "the buy point is the high")

Features are knowable at/before the 14:30 fill (no look-ahead), on the frozen
body=3 habit fills (C1 3% same_1430 amp_1430 clip4 gate_1430):
  - gap      = entry-day open / prev close - 1   (known at the open)
  - amp1430  = (bar5 high-low up to 14:30) / 14:30 px  (the ranking key, clean)

No strategy change, zero grid. S1a=gap, S1b=amp1430.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_sat_entry.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from eval_sat_idle_parking import SLOT_PCT  # noqa: E402
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    _cached_day_features,
    _intraday_px,
    load_sgap_context,
    replay_sgap_from_context,
)

WINDOWS_SV = {k: WINDOWS[k] for k in ("OOS2", "train", "valid")}


def _report(name: str, feats: list[tuple[str, float, float]]) -> None:
    g = np.array([x[1] for x in feats])
    c = np.array([x[2] for x in feats])
    med = float(np.median(g))
    print(f"\n=== {name} ===  n={len(feats)}  range {100*g.min():.2f}..{100*g.max():.2f}%")
    print(
        f"  corr({name}, contrib) {float(np.corrcoef(g, c)[0,1]):+.3f}   "
        f"below-median {c[g<=med].mean():+.2f}%  above-median {c[g>med].mean():+.2f}%"
    )
    for k, idx in enumerate(np.array_split(np.argsort(g), 5)):
        print(f"  Q{k+1} n={idx.size:>4} mean {c[idx].mean():+5.2f}%  win {100*(c[idx]>0).mean():.0f}%")
    qlo, qhi = np.quantile(g, 0.2), np.quantile(g, 0.8)
    for w, (ws, we) in WINDOWS_SV.items():
        sub = [(gg, cc) for d, gg, cc in feats if ws <= d <= we]
        if len(sub) < 10:
            continue
        wg = np.array([x[0] for x in sub])
        wc = np.array([x[1] for x in sub])
        lo, hi = wc[wg <= qlo], wc[wg >= qhi]
        print(
            f"  {w:<6} n={len(sub):>4} corr {float(np.corrcoef(wg,wc)[0,1]):+.3f}  "
            f"low {lo.mean() if lo.size else float('nan'):+5.2f}%(n{lo.size})  "
            f"high {hi.mean() if hi.size else float('nan'):+5.2f}%(n{hi.size})"
        )


def main() -> int:
    s, e = WINDOWS["long"]
    print(f"H-SAT-ENTRY long {s}~{e}\n", flush=True)
    ctx = load_sgap_context(s, e)
    sat = replay_sgap_from_context(
        ctx,
        start=s,
        end=e,
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
    fills = [
        b
        for b in (sat.get("blotter") or [])
        if b.get("kind") == "fill" and b.get("contribPct") is not None
    ]
    hl = ctx.get("px_hl_1430") or {}

    recs = []
    for b in fills:
        ts, day, c = str(b["ts"]), str(b["entryDate"]), float(b["contribPct"])
        day_all, _ = _cached_day_features(ctx, day)
        gv = (day_all.get(ts) or {}).get("gap")
        px = _intraday_px(ctx, ts, day, "1430")
        hi = (hl.get(ts) or {}).get(day)
        amp = float(hi[0] - hi[1]) / float(px) if (px and px > 0 and hi) else None
        recs.append((day, ts, c, float(gv) if gv is not None and gv == gv else None, amp))

    gap_f = [(d, g, c) for d, _, c, g, _ in recs if g is not None]
    amp_f = [(d, a, c) for d, _, c, _, a in recs if a is not None]
    _report("gap", gap_f)
    _report("amp1430", amp_f)

    # cross-sectional (within-day demeaned) test: is it entry quality on the
    # SAME day, or just a day-level (market) effect?
    by_day: dict[str, list[tuple[float, float]]] = {}
    for d, _, c, _, a in recs:
        if a is not None:
            by_day.setdefault(d, []).append((a, c))
    xs, ys, npair = [], [], 0
    for _d, lst in by_day.items():
        if len(lst) < 2:
            continue
        ma = float(np.mean([a for a, _ in lst]))
        mc = float(np.mean([c for _, c in lst]))
        for a, c in lst:
            xs.append(a - ma)
            ys.append(c - mc)
        npair += 1
    print(
        f"\nWITHIN-DAY demeaned corr(amp1430, contrib) {float(np.corrcoef(xs,ys)[0,1]):+.3f} "
        f"over {npair} multi-fill days ({len(xs)} fills)"
    )
    both = [(a, g, c) for _, _, c, g, a in recs if g is not None and a is not None]
    if len(both) > 10:
        aa = np.array([x[0] for x in both])
        gg = np.array([x[1] for x in both])
        print(f"corr(amp1430, gap) {float(np.corrcoef(aa,gg)[0,1]):+.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
