"""Orthogonality check for the holder-number signal.

Is holder_chg incremental, or just a proxy for size / short-term reversal /
medium momentum? We residualize the signal (rank-space) against size (log amt),
prior-month return, and 6-month return, then recompute the 1m rank-IC.
Read-only.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alt_alpha_ic import monthly_returns  # noqa: E402
from alt_holder_refine import holder_signals, grid  # noqa: E402


def ic_series(df: pd.DataFrame, col: str, fwd: str) -> pd.Series:
    out = {}
    for ym, g in df.groupby("ym"):
        if len(g) < 100:
            continue
        out[ym] = g[col].rank().corr(g[fwd].rank())
    return pd.Series(out).dropna()


def report(label: str, ics: pd.Series) -> None:
    ir = ics.mean() / ics.std() if ics.std() else 0
    t = ir * np.sqrt(len(ics))
    print(f"  {label:34s} IC {ics.mean():+.4f}  IR {ir:+.3f}  t {t:+.2f}  n={len(ics)}")


def main() -> int:
    mo = monthly_returns().sort_values(["ts_code", "ym"])
    all_ym = sorted(mo["ym"].unique())
    # controls
    mo["logamt"] = np.log(mo["amt"].clip(lower=1.0))
    mo["ret_prev"] = mo.groupby("ts_code")["ret"].shift(1)
    mo["mom6"] = (1 + mo.groupby("ts_code")["ret"].shift(2)) * (1 + mo.groupby("ts_code")["ret"].shift(3)) \
        * (1 + mo.groupby("ts_code")["ret"].shift(4)) * (1 + mo.groupby("ts_code")["ret"].shift(5)) \
        * (1 + mo.groupby("ts_code")["ret"].shift(6)) * (1 + mo.groupby("ts_code")["ret"].shift(7)) - 1
    mo["fwd1"] = mo.groupby("ts_code")["ret"].shift(-1)

    for col in ["chg1", "chg2"]:
        sig = grid(holder_signals(), col, all_ym)
        df = mo.merge(sig, on=["ts_code", "ym"], how="inner").dropna(subset=[col, "fwd1"])
        print(f"\n[{col}]")
        report("raw", ic_series(df, col, "fwd1"))
        # rank-residualize against each control (cross-sectionally per month)
        for ctrl in ["logamt", "ret_prev", "mom6"]:
            d = df.dropna(subset=[ctrl, col, "fwd1"]).copy()
            d["s_r"] = d.groupby("ym")[col].rank(pct=True)
            d["c_r"] = d.groupby("ym")[ctrl].rank(pct=True)
            # simple per-month orthogonalization: rank-resid via OLS on ranks
            res = []
            for _ym, g in d.groupby("ym"):
                x = np.column_stack([np.ones(len(g)), g["c_r"].values])
                beta, *_ = np.linalg.lstsq(x, g["s_r"].values, rcond=None)
                g = g.assign(s_res=g["s_r"].values - x @ beta)
                res.append(g)
            d = pd.concat(res)
            report(f"resid vs {ctrl}", ic_series(d, "s_res", "fwd1"))
        # residualize against all three jointly
        d = df.dropna(subset=["logamt", "ret_prev", "mom6", col, "fwd1"]).copy()
        d["s_r"] = d.groupby("ym")[col].rank(pct=True)
        res = []
        for _ym, g in d.groupby("ym"):
            X = np.column_stack([np.ones(len(g))] + [g[c].rank(pct=True).values
                                                      for c in ["logamt", "ret_prev", "mom6"]])
            beta, *_ = np.linalg.lstsq(X, g["s_r"].values, rcond=None)
            res.append(g.assign(s_res=g["s_r"].values - X @ beta))
        report("resid vs size+rev+mom6", ic_series(pd.concat(res), "s_res", "fwd1"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
