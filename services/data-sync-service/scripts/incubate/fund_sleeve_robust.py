"""Robustness of the inv+accr liquid sleeve: is the industry-neutral edge real?

The industry map (stock_eastmoney_industry) is a CURRENT snapshot, not PIT.
Tests:
  1. raw (no neutralization)
  2. industry-neutral, eastmoney map
  3. industry-neutral, tushare stock_basic.industry (independent map)
  4. placebo: SHUFFLED random groups (should NOT improve over raw)
  5. size-neutral (residualize score on log amount) instead of industry
  6. sub-period split (2008-2015 vs 2016-2024) for the eastmoney version
Metric: liquid-pool top-quintile minus pool-EW, and IC, per year. Read-only.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fundamental_ic import annual_metrics, add_factors  # noqa: E402
from data_sync_service.db import get_connection  # noqa: E402


def read(sql: str) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql(sql, conn)


def top_q_spread(d: pd.DataFrame, col: str) -> float:
    dd = d.dropna(subset=[col, "fwd12"])
    if len(dd) < 100:
        return np.nan
    q = pd.qcut(dd[col].rank(method="first"), 5, labels=False)
    return dd[q == 4]["fwd12"].mean() - dd["fwd12"].mean()


def ic(d: pd.DataFrame, col: str) -> float:
    dd = d.dropna(subset=[col, "fwd12"])
    if len(dd) < 100:
        return np.nan
    return float(np.corrcoef(dd[col].rank().values, dd["fwd12"].rank().values)[0, 1])


def main() -> int:
    m = add_factors(annual_metrics())
    em = read("SELECT ts_code, industry_name FROM stock_eastmoney_industry")
    ts = read("SELECT ts_code, industry FROM stock_basic")
    px = read("SELECT ts_code, trade_date, close, amount FROM daily WHERE trade_date>='2006-06-01' "
              "AND close>0 AND (ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ' OR ts_code LIKE '%.BJ')")
    px["trade_date"] = pd.to_datetime(px["trade_date"]); px["ym"] = px["trade_date"].dt.to_period("M")
    mo = px.groupby(["ts_code", "ym"]).agg(close=("close", "last"), amt=("amount", "mean")).reset_index()
    mo["yr"] = mo["ym"].dt.year; mo["mon"] = mo["ym"].dt.month
    may = mo[mo["mon"] == 5][["ts_code", "yr", "close"]].copy()
    may["fwd12"] = may.groupby("ts_code")["close"].shift(-1) / may["close"] - 1.0
    liqm = mo[mo["mon"].isin([2, 3, 4])].groupby(["ts_code", "yr"])["amt"].mean().rename("liq").reset_index()

    rng = np.random.default_rng(42)
    res = {k: [] for k in ["raw", "em", "ts", "shuf", "sizeneu"]}
    icres = {k: [] for k in ["raw", "em", "ts", "shuf", "sizeneu"]}
    yearly = []
    for y in range(2008, 2025):
        t = pd.Timestamp(f"{y}-05-01")
        ann = m[m["ann_date"] <= t].sort_values(["ts_code", "end_date"]).drop_duplicates("ts_code", keep="last")
        ann["yr"] = y
        d = ann.merge(may[may["yr"] == y], on=["ts_code", "yr"], how="inner")
        d = d.merge(liqm[liqm["yr"] == y], on=["ts_code", "yr"], how="left")
        d = d.merge(em, on="ts_code", how="left").merge(ts, on="ts_code", how="left")
        d = d.dropna(subset=["fwd12", "liq", "asset_growth", "accruals"])
        if len(d) < 200:
            continue
        d["industry_name"] = d["industry_name"].fillna("UNK")
        d["industry"] = d["industry"].fillna("UNK")
        base = 1 - (d["asset_growth"].rank(pct=True) + d["accruals"].rank(pct=True)) / 2
        d["raw"] = base
        d["em"] = d.groupby("industry_name")["raw"].rank(pct=True)
        d["ts"] = d.groupby("industry")["raw"].rank(pct=True)
        # placebo: shuffle into ~k groups
        g = rng.integers(0, 30, len(d))
        d["shuf"] = d.assign(_g=g).groupby("_g")["raw"].rank(pct=True)
        # size-neutral: residual of score ranks on log(liq) ranks
        x = np.column_stack([np.ones(len(d)), np.log(d["liq"].clip(lower=1)).rank(pct=True)])
        yv = d["raw"].rank(pct=True).values
        beta, *_ = np.linalg.lstsq(x, yv, rcond=None)
        d["sizeneu"] = yv - x @ beta

        med = d["liq"].median()
        liq = d[d["liq"] >= med]
        for k in res:
            res[k].append(top_q_spread(liq, k))
            icres[k].append(ic(liq, k))
        yearly.append({"yr": y, "raw": top_q_spread(liq, "raw") * 100,
                       "em": top_q_spread(liq, "em") * 100, "ts": top_q_spread(liq, "ts") * 100})

    def rep(key):
        a = pd.Series(res[key]).dropna()
        i = pd.Series(icres[key]).dropna()
        print(f"  {key:9s} Q5-EW {a.mean()*100:+.2f}%/yr  win {(a>0).mean()*100:3.0f}%  "
              f"| IC {i.mean():+.4f}  t {i.mean()/i.std()*np.sqrt(len(i)):+.2f}  n={len(a)}")
    print("=== liquid-pool inv+accr composite: neutralization variants ===")
    for k in ["raw", "em", "ts", "shuf", "sizeneu"]:
        rep(k)
    y = pd.DataFrame(yearly)
    print("\n=== sub-periods (Q5-EW liquid, %) ===")
    for lo, hi, name in [(2008, 2016, "2008-2015"), (2016, 2025, "2016-2024")]:
        s = y[(y.yr >= lo) & (y.yr < hi)]
        print(f"  {name}: raw {s.raw.mean():+.1f} | eastmoney {s.em.mean():+.1f} | tushare {s.ts.mean():+.1f} "
              f"(win em {(s.em>0).mean()*100:.0f}%)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
