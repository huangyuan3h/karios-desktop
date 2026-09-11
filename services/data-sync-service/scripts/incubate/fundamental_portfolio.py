"""inv+accr composite: quintile monotonicity, liquidity filter, per-year. Read-only."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fundamental_ic import annual_metrics, add_factors  # noqa: E402
from data_sync_service.db import get_connection  # noqa: E402


def main() -> int:
    m = add_factors(annual_metrics())
    with get_connection() as conn:
        px = pd.read_sql(
            "SELECT ts_code, trade_date, close, amount FROM daily WHERE trade_date>='2007-01-01' "
            "AND close>0 AND (ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ' OR ts_code LIKE '%.BJ')", conn)
    px["trade_date"] = pd.to_datetime(px["trade_date"])
    px["ym"] = px["trade_date"].dt.to_period("M")
    mo = px.groupby(["ts_code", "ym"]).agg(close=("close", "last"), amt=("amount", "mean")).reset_index()
    mo["yr"] = mo["ym"].dt.year
    may = mo[mo["ym"].dt.month == 5][["ts_code", "yr", "close", "amt"]].copy()
    may["fwd12"] = may.groupby("ts_code")["close"].shift(-1) / may["close"] - 1.0

    def build(y):
        t = pd.Timestamp(f"{y}-05-01")
        ann = m[m["ann_date"] <= t].sort_values(["ts_code", "end_date"]).drop_duplicates("ts_code", keep="last")
        ann["yr"] = y
        d = ann.merge(may[may["yr"] == y], on=["ts_code", "yr"], how="inner").dropna(subset=["fwd12"])
        d["score"] = (d["asset_growth"].rank(pct=True) + d["accruals"].rank(pct=True)) / 2
        d["score"] = 1 - d["score"]  # higher = better (low growth, low accruals)
        return d

    print("=== quintile mean fwd12 (%) + monotonicity ===")
    rows = []
    for y in range(2008, 2025):
        d = build(y)
        if len(d) < 200:
            continue
        q = pd.qcut(d["score"].rank(method="first"), 5, labels=False)
        gm = d.groupby(q)["fwd12"].mean() * 100
        ew = d["fwd12"].mean() * 100
        rows.append([y, len(d)] + [gm.get(i, np.nan) for i in range(5)] + [ew])
    r = pd.DataFrame(rows, columns=["yr", "n", "Q1", "Q2", "Q3", "Q4", "Q5", "EW"])
    print(f"  mean: " + " ".join(f"Q{i+1} {r['Q'+str(i+1)].mean():+5.1f}" for i in range(5))
          + f"  EW {r['EW'].mean():+5.1f}  | Q5-Q1 {r[chr(81)+'5'].mean()-r['Q1'].mean():+.1f}  Q5>EW {((r['Q5']>r['EW']).mean()*100):.0f}%")
    print("  per year (Q5 / EW):")
    for _, x in r.iterrows():
        print(f"    {int(x.yr)} n={int(x.n):4d}  Q1 {x.Q1:+6.1f} Q5 {x.Q5:+6.1f}  EW {x.EW:+6.1f}  {'win' if x.Q5 > x.EW else 'lose'}")

    print("\n=== liquid subset (amt >= same-year median) ===")
    rows2 = []
    for y in range(2008, 2025):
        d = build(y)
        med = d["amt"].median()
        d = d[d["amt"] >= med]
        if len(d) < 150:
            continue
        q = pd.qcut(d["score"].rank(method="first"), 5, labels=False)
        gm = d.groupby(q)["fwd12"].mean() * 100
        rows2.append([y] + [gm.get(i, np.nan) for i in range(5)] + [d["fwd12"].mean() * 100])
    r2 = pd.DataFrame(rows2, columns=["yr", "Q1", "Q2", "Q3", "Q4", "Q5", "EW"])
    print(f"  mean: Q1 {r2['Q1'].mean():+5.1f} Q2 {r2['Q2'].mean():+5.1f} Q3 {r2['Q3'].mean():+5.1f} "
          f"Q4 {r2['Q4'].mean():+5.1f} Q5 {r2['Q5'].mean():+5.1f}  EW {r2['EW'].mean():+5.1f} "
          f" | Q5-EW {r2['Q5'].mean()-r2['EW'].mean():+.1f}  win {((r2['Q5']>r2['EW']).mean()*100):.0f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
