#!/usr/bin/env python3
"""Alpha101 CANDIDATE family — S0 collapse + S1 orthogonalization (read-only).

Follows pre-registration addendum §7 in
  docs/designs/alpha101-screen-prereg-2026-09-12.md

S0: cross-sectional Rank correlation among the 11 L0 CANDIDATEs (confirm one family).
S1: staged cross-sectional OLS residualization of the representative factor
    (A16 primary, A13 secondary) against size/liquidity/reversal/momentum/vol
    and (optionally) industry fixed effects; re-run identical three-window h=5 IC.

Usage:
  PYTHONPATH=src python3 scripts/alpha101_candidates_diag.py --windows OOS2,train,valid
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
import alpha101_screen as base  # noqa: E402

CANDIDATES = ["A13", "A16", "A44", "A26", "A15", "A50", "A3", "A6", "A55", "A2", "A27"]
PRIMARY = "A16"
SECONDARY = "A13"
STAGES = {
    "S1a_size_liq": ["log_mv", "log_amt20"],
    "S1b_rev_mom_vol": ["log_mv", "log_amt20", "ret5", "ret20", "vol20"],
}
REPORT_DIR = base.REPORT_DIR


def build_controls(panel: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    close, amount, r, cap = panel["close"], panel["amount"], panel["returns"], panel["cap"]
    return {
        "log_mv": np.log(cap),
        "log_amt20": np.log(amount.rolling(20, min_periods=10).mean()),
        "ret5": close / close.shift(5) - 1,
        "ret20": close / close.shift(20) - 1,
        "vol20": r.rolling(20).std(),
    }


def _cross_rank(factor, win_dates, tradable):
    return factor.reindex(win_dates).where(tradable.reindex(win_dates)).rank(axis=1)


def s0_correlation(alphas, win_dates, tradable) -> pd.DataFrame:
    ranks = {a: _cross_rank(alphas[a], win_dates, tradable) for a in CANDIDATES}
    mat = pd.DataFrame(np.nan, index=CANDIDATES, columns=CANDIDATES)
    for i, a in enumerate(CANDIDATES):
        for j, b in enumerate(CANDIDATES):
            if j < i:
                continue
            c = ranks[a].corrwith(ranks[b], axis=1).mean()
            mat.loc[a, b] = c
            mat.loc[b, a] = c
    return mat


def residualize(factor, controls, win_dates, tradable, groups=None, industry=False):
    f = factor.reindex(win_dates).where(tradable.reindex(win_dates))
    ctrl = {
        k: v.reindex(win_dates).where(tradable.reindex(win_dates))
        for k, v in controls.items()
    }
    out = pd.DataFrame(np.nan, index=f.index, columns=f.columns)
    for t in f.index:
        y = f.loc[t]
        X = pd.DataFrame({k: v.loc[t] for k, v in ctrl.items()}, index=y.index)
        m = y.notna() & X.notna().all(axis=1)
        n = int(m.sum())
        if n < 50:
            continue
        data = pd.concat([y[m].rename("y"), X[m]], axis=1)
        if industry and groups is not None:
            g = groups.reindex(data.index).fillna("UNKNOWN")
            data = data - data.groupby(g).transform("mean")
        A = np.column_stack([np.ones(n), data[list(ctrl)].to_numpy(dtype=float)])
        yv = data["y"].to_numpy(dtype=float)
        beta, *_ = np.linalg.lstsq(A, yv, rcond=None)
        out.loc[t, data.index] = yv - A @ beta
    return out


def run_window(name: str, start: str, end: str):
    print(f"[{name}] {start}..{end}", flush=True)
    t0 = time.time()
    panel, groups = base.load_panel(start, end)
    close = panel["close"]
    fwd5 = base._forward_returns(close, base.PRIMARY_H)
    avg20 = panel["amount"].rolling(20, min_periods=10).mean()
    tradable = (avg20 >= base.LIQ_FLOOR_QIAN) & close.notna()
    win_dates = [d for d in close.index if start <= d <= end]
    alphas = base.alpha101_lib(panel, groups)
    controls = build_controls(panel)

    s0 = s0_correlation(alphas, win_dates, tradable)
    tri = np.triu_indices(len(CANDIDATES), 1)
    s0_mean = float(np.nanmean(np.abs(s0.to_numpy()[tri])))
    print(f"[{name}] S0 mean pairwise |corr| = {s0_mean:.3f} ({round(time.time() - t0, 1)}s)", flush=True)

    stages: dict[str, dict] = {}
    for rep in [PRIMARY, SECONDARY]:
        f = alphas[rep]
        stages[rep] = {"raw": base.evaluate(f, fwd5, tradable, win_dates)}
        for sname, cols in STAGES.items():
            res = residualize(f, {c: controls[c] for c in cols}, win_dates, tradable)
            stages[rep][sname] = base.evaluate(res, fwd5, tradable, win_dates)
            del res
        res = residualize(
            f, {c: controls[c] for c in STAGES["S1b_rev_mom_vol"]},
            win_dates, tradable, groups=groups, industry=True,
        )
        stages[rep]["S1c_industry"] = base.evaluate(res, fwd5, tradable, win_dates)
        del res
        print(
            f"[{name}] {rep} raw IR={stages[rep]['raw']['ic_ir']:+.2f} "
            f"S1a={stages[rep]['S1a_size_liq']['ic_ir']:+.2f} "
            f"S1b={stages[rep]['S1b_rev_mom_vol']['ic_ir']:+.2f} "
            f"S1c={stages[rep]['S1c_industry']['ic_ir']:+.2f} "
            f"({round(time.time() - t0, 1)}s)",
            flush=True,
        )
    return {"s0": s0, "stages": stages}


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

    # aggregate S0 mean |corr|
    s0_means = {}
    for w in wins:
        m = allw[w]["s0"].to_numpy()
        s0_means[w] = float(np.nanmean(np.abs(m[np.triu_indices(len(CANDIDATES), 1)])))

    # aggregate stages for primary
    agg = {}
    for rep in [PRIMARY, SECONDARY]:
        agg[rep] = {}
        for stage in ["raw", "S1a_size_liq", "S1b_rev_mom_vol", "S1c_industry"]:
            agg[rep][stage] = {
                w: allw[w]["stages"][rep][stage] for w in wins
            }

    # verdict per pre-registered kill line (primary only)
    def irs(rep, stage):
        return [agg[rep][stage][w].get("ic_ir") for w in wins]

    verdict = []
    for stage in ["S1a_size_liq", "S1b_rev_mom_vol", "S1c_industry"]:
        v = irs(PRIMARY, stage)
        if any(x is None or abs(x) < 0.3 for x in v):
            verdict.append({"stage": stage, "result": "REJECT", "ic_ir": v})
            break
        if stage == "S1c_industry":
            if all(x is not None and abs(x) >= 0.5 for x in v):
                verdict.append({"stage": stage, "result": "SURVIVOR", "ic_ir": v})
            else:
                verdict.append({"stage": stage, "result": "WEAK", "ic_ir": v})
        else:
            verdict.append({"stage": stage, "result": "PASS-stage", "ic_ir": v})

    payload = {
        "generated_at": __import__("datetime").datetime.now(
            __import__("datetime").UTC
        ).isoformat(),
        "windows": wins,
        "candidates": CANDIDATES,
        "primary": PRIMARY,
        "secondary": SECONDARY,
        "s0_mean_abs_corr": s0_means,
        "s0_matrix": {w: allw[w]["s0"].to_dict() for w in wins},
        "stages": agg,
        "verdict": verdict,
    }
    out_path = Path(args.json) if args.json else REPORT_DIR / "alpha101_candidates_diag_latest.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {out_path}")

    print("\nS0 mean pairwise |rank corr|:", {w: round(s0_means[w], 3) for w in wins})
    print("\n| rep | stage | ICIR OOS2/train/valid | IC OOS2/train/valid | net OOS2/train/valid |")
    print("|-----|-------|----------------------|---------------------|----------------------|")
    for rep in [PRIMARY, SECONDARY]:
        for stage in ["raw", "S1a_size_liq", "S1b_rev_mom_vol", "S1c_industry"]:
            recs = agg[rep][stage]
            ir = "/".join(f"{recs[w].get('ic_ir'):+.2f}" if recs[w].get("ic_ir") is not None else "na" for w in wins)
            ic = "/".join(f"{recs[w].get('ic_mean'):+.3f}" if recs[w].get("ic_mean") is not None else "na" for w in wins)
            net = "/".join(f"{recs[w].get('net_spread'):+.4f}" if recs[w].get("net_spread") is not None else "na" for w in wins)
            print(f"| {rep} | {stage} | {ir} | {ic} | {net} |")
    print("\nverdict:", json.dumps(verdict, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
