#!/usr/bin/env python3
"""Alpha101 CANDIDATE family — S2 tradability test (read-only).

Follows pre-registration §8 in docs/designs/alpha101-screen-prereg-2026-09-12.md

Factor = A16 residual after full S1c orthogonalization (size/liquidity/reversal/
momentum/vol/industry).  Non-overlapping rebalance at h in {5,10,20}:
  V1 long-short Q5-Q1, V2 long-only Q5 (excess vs universe EW).
Cost = 0.5*sum|dw| * 30bp roundtrip per rebalance.

Usage:
  PYTHONPATH=src python3 scripts/alpha101_candidates_s2.py --windows OOS2,train,valid
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import alpha101_candidates_diag as diag  # noqa: E402
import alpha101_screen as base  # noqa: E402

H_LIST = [5, 10, 20]
COST = base.COST_ROUNDTRIP
REPORT_DIR = base.REPORT_DIR


def backtest(factor, fwd, tradable, win_dates, h, long_only):
    rebal = win_dates[::h]
    rows = []
    prev = None
    for t in rebal:
        f = factor.loc[t].where(tradable.loc[t])
        y = fwd.loc[t].where(tradable.loc[t])
        m = f.notna() & y.notna()
        if int(m.sum()) < 50:
            continue
        fv, yv = f[m], y[m]
        qb = np.ceil(fv.rank(pct=True) * 5).clip(1, 5)
        w = pd.Series(0.0, index=fv.index)
        if long_only:
            sel = qb == 5
            if int(sel.sum()) == 0:
                continue
            w[sel] = 1.0 / int(sel.sum())
            gross = float(yv[sel].mean() - yv.mean())
        else:
            q5, q1 = qb == 5, qb == 1
            n5, n1 = int(q5.sum()), int(q1.sum())
            if n5 == 0 or n1 == 0:
                continue
            w[q5] = 1.0 / n5
            w[q1] = -1.0 / n1
            gross = float(yv[q5].mean() - yv[q1].mean())
        if prev is None:
            turn = 0.5 * float(w.abs().sum())
        else:
            idx = prev.index.union(w.index)
            diff = prev.reindex(idx, fill_value=0) - w.reindex(idx, fill_value=0)
            turn = 0.5 * float(diff.abs().sum())
        rows.append((gross, gross - turn * COST, turn))
        prev = w
    if not rows:
        return None
    gross = np.array([r[0] for r in rows])
    net = np.array([r[1] for r in rows])
    turn = np.array([r[2] for r in rows])
    sd = float(net.std(ddof=1)) if len(net) > 1 else 0.0
    return {
        "n_rebal": len(rows),
        "gross_period": float(gross.mean()),
        "net_period": float(net.mean()),
        "net_ir": float(net.mean() / sd) if sd > 0 else None,
        "turn_per_rebal": float(turn.mean()),
        "ann_net": float((1.0 + net.mean()) ** (250.0 / h) - 1.0) if net.mean() > -1 else None,
    }


def run_window(name, start, end):
    print(f"[{name}] {start}..{end}", flush=True)
    t0 = time.time()
    panel, groups = base.load_panel(start, end)
    close = panel["close"]
    avg20 = panel["amount"].rolling(20, min_periods=10).mean()
    tradable = (avg20 >= base.LIQ_FLOOR_QIAN) & close.notna()
    win_dates = [d for d in close.index if start <= d <= end]
    alphas = base.alpha101_lib(panel, groups)
    controls = diag.build_controls(panel)
    ctrl_cols = diag.STAGES["S1b_rev_mom_vol"]

    out = {}
    for rep in [diag.PRIMARY, diag.SECONDARY]:
        res = diag.residualize(
            alphas[rep], {c: controls[c] for c in ctrl_cols},
            win_dates, tradable, groups=groups, industry=True,
        )
        fwds = {h: base._forward_returns(close, h) for h in H_LIST}
        out[rep] = {}
        for h in H_LIST:
            out[rep][f"ls_h{h}"] = backtest(res, fwds[h], tradable, win_dates, h, False)
            out[rep][f"lo_h{h}"] = backtest(res, fwds[h], tradable, win_dates, h, True)
        del res
        print(
            f"[{name}] {rep} " + " ".join(
                f"h{h}: LS {out[rep][f'ls_h{h}']['net_period']:+.4f}/"
                f"LO {out[rep][f'lo_h{h}']['net_period']:+.4f}"
                for h in H_LIST
            ) + f" ({round(time.time() - t0, 1)}s)",
            flush=True,
        )
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--windows", default="OOS2,train,valid")
    ap.add_argument("--json", default="")
    args = ap.parse_args()
    wins = [w.strip() for w in args.windows.split(",") if w.strip()]

    allw = {}
    for w in wins:
        if w not in base.WINDOWS:
            print(f"unknown window {w}", file=sys.stderr)
            return 2
        allw[w] = run_window(w, *base.WINDOWS[w])

    variants = [f"{kind}_h{h}" for h in H_LIST for kind in ["ls", "lo"]]
    agg = {}
    for rep in [diag.PRIMARY, diag.SECONDARY]:
        agg[rep] = {}
        for v in variants:
            agg[rep][v] = {w: allw[w][rep][v] for w in wins}

    # verdict on primary per pre-registered kill line
    verdict = "S2-REJECT"
    detail = []
    for v in variants:
        recs = agg[diag.PRIMARY][v]
        if any(recs[w] is None for w in wins):
            continue
        nets = [recs[w]["net_period"] for w in wins]
        irs = [recs[w]["net_ir"] for w in wins]
        pos = all(x > 0 for x in nets)
        ir_ok = all(x is not None and x >= 0.3 for x in irs)
        ir_strong = all(x is not None and x >= 0.5 for x in irs)
        detail.append({"variant": v, "net": nets, "net_ir": irs,
                       "pos3": pos, "ir03": ir_ok, "ir05": ir_strong})
        if pos and ir_strong:
            verdict = "S2-SURVIVOR"
        elif pos and ir_ok and verdict != "S2-SURVIVOR":
            verdict = "S2-PASS"

    payload = {
        "generated_at": __import__("datetime").datetime.now(
            __import__("datetime").UTC
        ).isoformat(),
        "windows": wins,
        "primary": diag.PRIMARY,
        "secondary": diag.SECONDARY,
        "h_list": H_LIST,
        "cost_roundtrip": COST,
        "agg": agg,
        "verdict": verdict,
        "detail": detail,
    }
    out_path = Path(args.json) if args.json else REPORT_DIR / "alpha101_s2_latest.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {out_path}\n")

    print("| variant | net/period OOS2/train/valid | net IR OOS2/train/valid | ann_net OOS2/train/valid |")
    print("|---------|------------------------------|-------------------------|--------------------------|")
    for v in variants:
        recs = agg[diag.PRIMARY][v]
        if any(recs[w] is None for w in wins):
            continue
        net = "/".join(f"{recs[w]['net_period']:+.4f}" for w in wins)
        ir = "/".join(f"{recs[w]['net_ir']:+.2f}" if recs[w]['net_ir'] is not None else "na" for w in wins)
        ann = "/".join(f"{recs[w]['ann_net']:+.1%}" if recs[w]['ann_net'] is not None else "na" for w in wins)
        print(f"| {v:8} | {net} | {ir} | {ann} |")
    print(f"\nverdict: {verdict}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
