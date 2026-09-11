"""Long-hold sleeve prototype: liquid pool + industry-neutral inv+accr, annual.

At each May formation (2008-2024):
  liquidity  = mean amount over Feb-Apr of the formation year (as-of, causal)
  pool       = liquidity >= cross-sectional median
  score      = industry-neutral rank of (-asset_growth, -accruals)
  hold       = pool & top quintile score, equal weight, 12m
Benchmarks: liquid-pool EW and 中证500 (000905, May-to-May).
Cost: annual rebalance, turnover * 30bps round-trip. Read-only.
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


def metrics(r: np.ndarray) -> str:
    nav = np.cumprod(1 + r)
    total = nav[-1] - 1
    yrs = len(r)
    cagr = nav[-1] ** (1 / yrs) - 1
    vol = r.std(ddof=1) * np.sqrt(1)  # annual points
    dd = float((nav / np.maximum.accumulate(nav) - 1).min())
    sh = r.mean() / r.std(ddof=1) if r.std(ddof=1) else 0
    return (f"NAV x{nav[-1]:.2f} ({cagr*100:+.1f}%/yr) vol {vol*100:.1f}% "
            f"DD {dd*100:.0f}% sharpe {sh:.2f} win {np.mean(r > 0)*100:.0f}%")


def main() -> int:
    m = add_factors(annual_metrics())
    ind = read("SELECT ts_code, industry_name FROM stock_eastmoney_industry")
    px = read("SELECT ts_code, trade_date, close, amount FROM daily WHERE trade_date>='2006-06-01' "
              "AND close>0 AND (ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ' OR ts_code LIKE '%.BJ')")
    px["trade_date"] = pd.to_datetime(px["trade_date"])
    px["ym"] = px["trade_date"].dt.to_period("M")
    mo = px.groupby(["ts_code", "ym"]).agg(close=("close", "last"), amt=("amount", "mean")).reset_index()
    mo["yr"] = mo["ym"].dt.year
    mo["mon"] = mo["ym"].dt.month
    may = mo[mo["mon"] == 5][["ts_code", "yr", "close"]].copy()
    may["fwd12"] = may.groupby("ts_code")["close"].shift(-1) / may["close"] - 1.0
    liqm = mo[mo["mon"].isin([2, 3, 4])].groupby(["ts_code", "yr"])["amt"].mean().rename("liq").reset_index()

    idx = read("SELECT trade_date, close FROM index_daily WHERE ts_code='000905.SH' AND close>0")
    idx["trade_date"] = pd.to_datetime(idx["trade_date"]); idx["ym"] = idx["trade_date"].dt.to_period("M")
    imay = idx[idx["ym"].dt.month == 5].groupby(idx["ym"].dt.year)["close"].last()
    idx_ret = imay.shift(-1) / imay - 1.0

    rows = []
    prev = set()
    for y in range(2008, 2025):
        t = pd.Timestamp(f"{y}-05-01")
        ann = m[m["ann_date"] <= t].sort_values(["ts_code", "end_date"]).drop_duplicates("ts_code", keep="last")
        ann["yr"] = y
        d = ann.merge(may[may["yr"] == y], on=["ts_code", "yr"], how="inner")
        d = d.merge(liqm[liqm["yr"] == y], on=["ts_code", "yr"], how="left")
        d = d.merge(ind, on="ts_code", how="left")
        d = d.dropna(subset=["fwd12", "liq", "asset_growth", "accruals"])
        if len(d) < 200:
            continue
        d["industry_name"] = d["industry_name"].fillna("UNK")
        d["score"] = 1 - (d["asset_growth"].rank(pct=True) + d["accruals"].rank(pct=True)) / 2
        d["score"] = d.groupby("industry_name")["score"].rank(pct=True)
        med = d["liq"].median()
        pool = d[d["liq"] >= med]
        if len(pool) < 100:
            continue
        q = pd.qcut(pool["score"].rank(method="first"), 5, labels=False)
        hold = set(pool[q == 4]["ts_code"])
        gross = pool[q == 4]["fwd12"].mean()
        turn = 1 - (len(hold & prev) / max(len(hold), 1)) if prev else 1.0
        net = (1 + gross) * (1 - turn * 0.003) - 1
        prev = hold
        rows.append({"yr": y, "n_pool": len(pool), "n_hold": len(hold), "gross": gross,
                     "net": net, "pool_ew": pool["fwd12"].mean(), "idx500": idx_ret.get(y, np.nan),
                     "turn": turn})
    r = pd.DataFrame(rows).set_index("yr")
    print(r.assign(gross=lambda x: (x.gross*100).round(1), net=lambda x: (x.net*100).round(1),
                   pool_ew=lambda x: (x.pool_ew*100).round(1), idx500=lambda x: (x.idx500*100).round(1),
                   turn=lambda x: x.turn.round(2)).to_string())
    print()
    print(f"  strategy (net)   {metrics(r['net'].values)}")
    print(f"  liquid-pool EW   {metrics(r['pool_ew'].values)}")
    ri = r["idx500"].dropna().values
    print(f"  中证500 (000905)  {metrics(ri)}")
    print(f"  excess vs pool EW: mean {(r['net']-r['pool_ew']).mean()*100:+.1f}%/yr  "
          f"win {((r['net']>r['pool_ew']).mean()*100):.0f}%  years={len(r)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
