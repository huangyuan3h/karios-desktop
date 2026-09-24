#!/usr/bin/env python3
"""H-SAT-CLUSTER read-only: can we block "same-day basket all loses"?

Groups satellite fills into entry-day cohorts and asks:
  1. how often does a cohort lose all/most trades (vs independence)?
  2. is outcome explained by entry-day or hold-window market return?
  3. is the cohort internally correlated (same beta/trade) or independent?

No strategy change, zero grid. Feeds a possible prereg.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_sat_cluster.py
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
    print(f"H-SAT-CLUSTER long {s}~{e}\n", flush=True)
    sat = _sat_book(s, e)
    rows = sat["rows"]
    sat_nav = sat["nav"]
    dates = [str(r["date"]) for r in rows]
    idx = {d: i for i, d in enumerate(dates)}
    risk = load_risk_closes()
    s300 = _series_on_cal(risk["510300.SH"], dates)
    r300 = [0.0] + [s300[i] / s300[i - 1] - 1 if s300[i - 1] else 0.0 for i in range(1, len(dates))]

    fills = [
        b
        for b in (sat["blotter"] or [])
        if b.get("kind") == "fill" and b.get("contribPct") is not None
    ]
    cohorts: dict[str, list] = defaultdict(list)
    for b in fills:
        cohorts[str(b["entryDate"])].append(b)

    p = sum(1 for f in fills if float(f["contribPct"]) < 0) / len(fills)
    print(f"fills {len(fills)} over {len(cohorts)} entry-days · marginal loss rate p={p:.3f}\n")

    # cohort outcome by size
    print("COHORT OUTCOME BY SIZE (all-lose / mixed / all-win)")
    exp_all_lose = 0.0
    obs_all_lose = 0
    for size in range(1, 5):
        cs = [c for c in cohorts.values() if len(c) == size]
        if not cs:
            continue
        al = sum(1 for c in cs if all(float(b["contribPct"]) < 0 for b in c))
        aw = sum(1 for c in cs if all(float(b["contribPct"]) >= 0 for b in c))
        exp = len(cs) * p**size
        exp_all_lose += exp
        obs_all_lose += al
        print(
            f"  n={size}: {len(cs):>3} cohorts  all-lose {al:>2} (indep exp {exp:5.1f})  "
            f"all-win {aw:>3} (exp {len(cs) * (1 - p) ** size:5.1f})  mixed {len(cs) - al - aw}"
        )
    print(f"  TOTAL all-lose observed {obs_all_lose} vs independence {exp_all_lose:.1f}")

    # market context: entry-day and hold-window CSI300 return by cohort outcome
    def mkt_hold(c: list) -> float:
        i, j = idx[str(c[0]["entryDate"])], idx[str(c[0]["exitDate"])]
        return float(np.prod([1 + r300[k] for k in range(i + 1, j + 1)]) - 1)

    def mkt_entry(c: list) -> float:
        return r300[idx[str(c[0]["entryDate"])]]

    groups = {
        "all-lose": [
            c
            for c in cohorts.values()
            if len(c) >= 2 and all(float(b["contribPct"]) < 0 for b in c)
        ],
        "all-win": [
            c
            for c in cohorts.values()
            if len(c) >= 2 and all(float(b["contribPct"]) >= 0 for b in c)
        ],
        "mixed": [
            c
            for c in cohorts.values()
            if len(c) >= 2 and 0 < sum(1 for b in c if float(b["contribPct"]) < 0) < len(c)
        ],
    }
    # mean cohort contribution + entry-day gapCount by size
    row_by_date = {str(r["date"]): r for r in rows}
    print("\nBY COHORT SIZE (mean cohort contrib%, entry-day gapCount)")
    for size in range(1, 5):
        cs = [c for c in cohorts.values() if len(c) == size]
        if not cs:
            continue
        mc = [float(np.mean([float(b["contribPct"]) for b in c])) for c in cs]
        gc = [float(row_by_date.get(str(c[0]["entryDate"]), {}).get("gapCount") or 0) for c in cs]
        print(
            f"  n={size}: mean contrib {np.mean(mc):+5.2f}%  mean entry gapCount {np.mean(gc):5.1f}"
        )

    print("\nMARKET CONTEXT (CSI300) by cohort (size>=2)")
    for name, cs in groups.items():
        if not cs:
            continue
        hold = [100 * mkt_hold(c) for c in cs]
        entry = [100 * mkt_entry(c) for c in cs]
        mcontrib = [np.mean([float(b["contribPct"]) for b in c]) for c in cs]
        amp = [
            np.mean([float(b["amp"]) for b in c if b.get("amp") is not None])
            for c in cs
            if any(b.get("amp") is not None for b in c)
        ]
        print(
            f"  {name:<9} cohorts {len(cs):>3}  hold-window {np.mean(hold):+6.2f}%  "
            f"entry-day {np.mean(entry):+5.2f}%  cohort mean contrib {np.mean(mcontrib):+5.2f}%  "
            f"mean amp {np.mean(amp):5.2f}"
        )

    # within-cohort correlation of trade returns (are the 4 the same trade?)
    print("\nWITHIN-COHORT RETURN SPREAD")
    for name, cs in groups.items():
        spreads = []
        for c in cs:
            v = [float(b["contribPct"]) for b in c]
            if len(v) >= 2:
                spreads.append(np.std(v))
        if spreads:
            print(f"  {name:<9} mean within-cohort std {np.mean(spreads):.2f}  (n={len(spreads)})")

    # ---------------- adjacent-entry-day co-movement ----------------
    from datetime import date as _date

    cmean = {d: float(np.mean([float(b["contribPct"]) for b in c])) for d, c in cohorts.items()}
    callose = {d: all(float(b["contribPct"]) < 0 for b in c) for d, c in cohorts.items()}
    cds = sorted(cmean)
    lagc = np.array([cmean[cds[i]] for i in range(len(cds) - 1)])
    leadc = np.array([cmean[cds[i + 1]] for i in range(len(cds) - 1)])
    gap = np.array(
        [
            (_date.fromisoformat(cds[i + 1]) - _date.fromisoformat(cds[i])).days
            for i in range(len(cds) - 1)
        ]
    )
    print("\nADJACENT ENTRY-DAY COHORTS (next entry day)")
    print(
        f"  consecutive cohort-mean corr {float(np.corrcoef(lagc, leadc)[0, 1]):+.3f}  "
        f"(gap=1d: {float(np.corrcoef(lagc[gap == 1], leadc[gap == 1])[0, 1]):+.3f}, n={(gap == 1).sum()})"
    )
    same = (np.sign(lagc) == np.sign(leadc)).mean()
    cond_allose = np.mean([callose[cds[i + 1]] for i in range(len(cds) - 1) if callose[cds[i]]])
    uncond_after = np.mean([callose[cds[i + 1]] for i in range(len(cds) - 1)])
    print(
        f"  same-sign fraction {same:.2f} (indep 0.51)  "
        f"P(neg|prev neg) {np.mean(leadc[lagc < 0] < 0):.2f} vs base {np.mean(leadc < 0):.2f}"
    )
    print(f"  P(all-lose | prev all-lose) {cond_allose:.2f} vs unconditional {uncond_after:.2f}")

    # satellite daily autocorrelation + conditional loss, with overlap caveat
    sret = np.array(
        [sat_nav[i] / sat_nav[i - 1] - 1 for i in range(1, len(sat_nav)) if sat_nav[i - 1]]
    )
    print("\nSATELLITE DAILY (body=3 -> overlapping holds; lag1-2 partly mechanical)")
    print(
        f"  down-day base {100 * (sret < 0).mean():.0f}%  "
        f"P(down|prev down) {100 * (sret[1:][sret[:-1] < 0] < 0).mean():.0f}%  "
        f"P(down|prev up) {100 * (sret[1:][sret[:-1] >= 0] < 0).mean():.0f}%"
    )
    big = sret < -0.01
    print(
        f"  big-loss(<-1%) base {100*big.mean():.0f}%  "
        f"P(big|prev big) {100*big[1:][big[:-1]].mean() if big[:-1].sum() else float('nan'):.0f}%  "
        f"P(big|prev>0) {100*big[1:][sret[:-1]>=0].mean():.0f}%"
    )

    print("\nPERSISTENCE lags 1..10 (satellite daily; lag1-2 confounded by body=3 overlap)")
    print(f"  {'lag':>3} {'corr':>7} {'P(down|down)':>13} {'P(big|big)':>11} {'n':>5}")
    for k in range(1, 11):
        a, b = sret[:-k], sret[k:]
        corr = float(np.corrcoef(a, b)[0, 1])
        p_down = (b[a < 0] < 0).mean()
        pk = big[:-k].sum()
        p_big = big[k:][big[:-k]].mean() if pk else float("nan")
        print(f"  {k:>3} {corr:>+7.3f} {100*p_down:>12.0f}% {100*p_big:>10.0f}% {a.size:>5}")
    return 0



if __name__ == "__main__":
    raise SystemExit(main())
