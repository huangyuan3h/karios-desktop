"""Refine the holder-number (股东户数) signal: variants x horizons x universe.

Oriented so HIGHER signal = expected HIGHER forward return:
  chg1   = -1-period holder change (concentration)
  chg2   = -2-period holder change
  level  = -log(holder_num) (fewer holders = concentrated)
Horizons 1m / 3m. Universe all vs liquid (amt >= cross-sectional median).
Read-only.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_sync_service.db import get_connection  # noqa: E402
from alt_alpha_ic import monthly_returns  # noqa: E402

START = "2019-06-01"


def holder_signals() -> pd.DataFrame:
    with get_connection() as conn:
        h = pd.read_sql(
            "SELECT ts_code, ann_date, holder_num FROM cn_holder_number "
            "WHERE ann_date >= '%s' AND holder_num > 0" % START, conn)
    h["ann"] = pd.to_datetime(h["ann_date"])
    h["ym"] = h["ann"].dt.to_period("M")
    h = h.sort_values(["ts_code", "ann"])
    g = h.groupby("ts_code")
    h["prev1"] = g["holder_num"].shift(1)
    h["prev2"] = g["holder_num"].shift(2)
    h["chg1"] = -(h["holder_num"] / h["prev1"] - 1.0)
    h["chg2"] = -(h["holder_num"] / h["prev2"] - 1.0)
    h["level"] = -np.log(h["holder_num"])
    # collapse to (ts_code, ym): last disclosure in the month
    out = h.groupby(["ts_code", "ym"]).agg(chg1=("chg1", "last"), chg2=("chg2", "last"),
                                           level=("level", "last")).reset_index()
    return out


def grid(sig: pd.DataFrame, col: str, all_ym: list) -> pd.DataFrame:
    return (sig.pivot(index="ts_code", columns="ym", values=col)
            .reindex(columns=all_ym).ffill(axis=1).stack().rename(col).reset_index())


def evaluate(df: pd.DataFrame, col: str, fwd: str) -> None:
    ics, q = [], {i: [] for i in range(5)}
    for _ym, g in df.groupby("ym"):
        if len(g) < 100:
            continue
        ics.append(g[col].rank().corr(g[fwd].rank()))
        qq = pd.qcut(g[col].rank(method="first"), 5, labels=False)
        m = g.groupby(qq)[fwd].mean()
        for i in range(5):
            q[i].append(m.get(i, np.nan))
    ics = pd.Series(ics).dropna()
    if len(ics) < 5:
        print(f"  {col:8s} {fwd}: too few months")
        return
    ir = ics.mean() / ics.std() if ics.std() else 0
    qm = {i: np.nanmean(q[i]) * 100 for i in range(5)}
    mono = all(qm[i] <= qm[i + 1] for i in range(4)) or all(qm[i] >= qm[i + 1] for i in range(4))
    print(f"  {col:8s} {fwd}: IC {ics.mean():+.4f} IR {ir:+.3f} t {ir*np.sqrt(len(ics)):+.2f} "
          f"%pos {(ics>0).mean()*100:3.0f}% | Q1..Q5 " + " ".join(f"{qm[i]:+.2f}" for i in range(5))
          + f" | Q5-Q1 {qm[4]-qm[0]:+.2f}" + (" MONO" if mono else ""))


def main() -> int:
    mo = monthly_returns()
    all_ym = sorted(mo["ym"].unique())
    # forward 1m and 3m (compound), computed per stock from monthly returns
    mo = mo.sort_values(["ts_code", "ym"])
    mo["fwd1"] = mo.groupby("ts_code")["ret"].shift(-1)
    r1 = mo.groupby("ts_code")["ret"].shift(-1)
    r2 = mo.groupby("ts_code")["ret"].shift(-2)
    r3 = mo.groupby("ts_code")["ret"].shift(-3)
    mo["fwd3"] = (1 + r1) * (1 + r2) * (1 + r3) - 1
    sig = holder_signals()

    for col in ["chg1", "chg2", "level"]:
        s = grid(sig, col, all_ym)
        df = mo.merge(s, on=["ts_code", "ym"], how="inner")
        print(f"\n[{col}] all universe")
        for fwd in ["fwd1", "fwd3"]:
            d = df.dropna(subset=[col, fwd])
            evaluate(d, col, fwd)
        # liquid subset: amt >= same-month median
        med = df.groupby("ym")["amt"].transform("median")
        dfl = df[df["amt"] >= med]
        print(f"[{col}] liquid (amt>=median)")
        for fwd in ["fwd1", "fwd3"]:
            d = dfl.dropna(subset=[col, fwd])
            evaluate(d, col, fwd)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
