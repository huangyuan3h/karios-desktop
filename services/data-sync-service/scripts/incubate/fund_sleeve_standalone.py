#!/usr/bin/env python3
"""X3 standalone: inv+accr liquid sleeve + parameter-free vol-management.

Read-only. Pre-registration: docs/designs/x3-sleeve-standalone-prereg-2026-09-12.md

Frozen rules (no grid): annual May formation; liquidity = Feb-Apr avg amount,
pool = >= cross-sectional median; score = industry-neutral -(asset_growth, accruals);
hold top quintile, equal weight, 12m; cost = turnover * 30bp (stress 50bp).

Single intervention: vol-managed overlay  s_t = clip(expanding_median(vol)/vol_{t-1}, 0, 1),
vol = trailing 24m realized monthly vol of the strategy gross return. No leverage, cash 0%.

Usage:
  PYTHONPATH=src python3 scripts/incubate/fund_sleeve_standalone.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fundamental_ic import add_factors, annual_metrics  # noqa: E402

from data_sync_service.db import get_connection  # noqa: E402

REPORT = Path(__file__).resolve().parents[2] / "data" / "backtest_reports" / "fund_sleeve_standalone.json"
YEARS = list(range(2008, 2025))


def read(sql: str) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql(sql, conn)


def monthly_panel() -> pd.DataFrame:
    return read(
        """
        SELECT ts_code, to_char(trade_date,'YYYY-MM') AS ym,
               (array_agg(close ORDER BY trade_date DESC))[1] AS close,
               avg(amount) AS amt
        FROM daily
        WHERE trade_date >= '2006-01-01' AND close > 0
          AND (ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ' OR ts_code LIKE '%.BJ')
        GROUP BY ts_code, ym
        """
    )


def build_holdings(m, mo, ind, cost):
    mo["yr"] = mo["ym"].str[:4].astype(int)
    mo["mon"] = mo["ym"].str[5:7].astype(int)
    liqm = (
        mo[mo["mon"].isin([2, 3, 4])]
        .groupby(["ts_code", "yr"])["amt"].mean().rename("liq").reset_index()
    )
    holdings = {}
    for y in YEARS:
        t = pd.Timestamp(f"{y}-05-01")
        ann = (
            m[m["ann_date"] <= t]
            .sort_values(["ts_code", "end_date"])
            .drop_duplicates("ts_code", keep="last")
        )
        ann["yr"] = y
        d = ann.merge(liqm[liqm["yr"] == y], on=["ts_code", "yr"], how="inner")
        d = d.merge(ind, on="ts_code", how="left")
        d = d.dropna(subset=["liq", "asset_growth", "accruals"])
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
        adv_yi = float((pool[q == 4]["liq"] / 1e5).sum())  # 千元 -> 亿元
        holdings[y] = {"hold": hold, "pool": set(pool["ts_code"]), "adv_yi": adv_yi}
    return holdings


def yearly_returns(monthly: pd.Series) -> pd.Series:
    """Formation-year (May..Apr) compounded returns, indexed by formation year."""
    out = {}
    for y in YEARS:
        idx = [f"{y}-{m:02d}" for m in range(5, 13)] + [f"{y + 1}-{m:02d}" for m in range(1, 5)]
        vals = monthly.reindex(idx).dropna()
        if len(vals) >= 6:
            out[y] = float((1 + vals).prod() - 1)
    return pd.Series(out)


def metrics(r: pd.Series) -> dict:
    r = r.dropna()
    if len(r) == 0:
        return {}
    nav = (1 + r).cumprod()
    n = len(r)
    cagr = nav.iloc[-1] ** (12.0 / n) - 1
    vol = r.std(ddof=1) * np.sqrt(12)
    dd = float((nav / nav.cummax() - 1).min())
    sharpe = r.mean() / r.std(ddof=1) * np.sqrt(12) if r.std(ddof=1) else 0.0
    win = float((r > 0).mean())
    return {
        "cagr": float(cagr), "vol": float(vol), "maxDD": dd,
        "sharpe": float(sharpe), "win": win,
        "calmar": float(cagr / abs(dd)) if dd else None,
    }


def main() -> int:
    m = add_factors(annual_metrics())
    ind = read("SELECT ts_code, industry_name FROM stock_eastmoney_industry")
    print("loading monthly panel...", flush=True)
    mo = monthly_panel()
    holdings = build_holdings(m, mo, ind, 0.003)
    print(f"formation years: {sorted(holdings)}", flush=True)

    close_m = mo.pivot(index="ym", columns="ts_code", values="close").sort_index()
    rets = close_m / close_m.shift(1) - 1
    months = list(close_m.index)

    def month_set(ym: str) -> str:
        y, mm = int(ym[:4]), int(ym[5:7])
        return y if mm >= 5 else y - 1

    # monthly gross returns for strategy and pool EW; turnover at each May
    strat, pool, turn = [], [], []
    prev_h = set()
    for ym in months:
        y = month_set(ym)
        if y not in holdings:
            strat.append(np.nan)
            pool.append(np.nan)
            turn.append(np.nan)
            continue
        h, p = holdings[y]["hold"], holdings[y]["pool"]
        r = rets.loc[ym]
        strat.append(float(r.reindex(list(h)).mean()))
        pool.append(float(r.reindex(list(p)).mean()))
        if ym[5:7] == "05":
            t = 1 - len(h & prev_h) / max(len(h), 1) if prev_h else 1.0
            prev_h = h
        else:
            t = 0.0
        turn.append(t)
    strat = pd.Series(strat, index=months)
    pool = pd.Series(pool, index=months)
    turn = pd.Series(turn, index=months)

    def apply_cost(g: pd.Series, rate: float) -> pd.Series:
        return (1 + g) * (1 - turn.fillna(0) * rate) - 1

    strat_net = apply_cost(strat, 0.003)
    pool_net = apply_cost(pool, 0.003)
    strat_net50 = apply_cost(strat, 0.005)
    pool_net50 = apply_cost(pool, 0.005)

    # vol-managed overlay (parameter-free, as-of)
    vol = strat.rolling(24).std()
    target = vol.expanding().median().shift(1)
    scale = (target / vol.shift(1)).clip(0, 1).fillna(1.0)
    rebal_cost = turn.fillna(0) * 0.003
    vt = scale * strat - scale * rebal_cost - scale.diff().abs().fillna(0) * 0.003

    idx = read("SELECT to_char(trade_date,'YYYY-MM') AS ym, close FROM index_daily "
               "WHERE ts_code='000905.SH' AND close>0")
    idx = idx.groupby("ym")["close"].last().sort_index()
    idx_ret = idx / idx.shift(1) - 1

    series = {
        "strategy_net30": strat_net,
        "strategy_net50": strat_net50,
        "strategy_volmanaged": vt,
        "pool_ew_net30": pool_net,
        "pool_ew_net50": pool_net50,
        "csi500": idx_ret,
    }
    print("\n=== full window 2008-2024 (monthly NAV) ===")
    for k, s in series.items():
        mt = metrics(s.loc["2008-05":])
        print(f"  {k:20s} CAGR {mt.get('cagr', 0)*100:+5.1f}%  vol {mt.get('vol', 0)*100:4.1f}%  "
              f"maxDD {mt.get('maxDD', 0)*100:5.1f}%  Sharpe {mt.get('sharpe', 0):+.2f}  "
              f"Calmar {mt.get('calmar') or 0:+.2f}")

    # annual excess (formation years)
    print("\n=== annual (formation-year) excess, net 30bp ===")
    ys = {k: yearly_returns(v) for k, v in series.items()}
    tab = pd.DataFrame({k: v for k, v in ys.items()})
    for period, lo, hi in [("2008-2015", 2008, 2016), ("2016-2024", 2016, 2025)]:
        sub = tab[(tab.index >= lo) & (tab.index < hi)]
        se = sub["strategy_net30"] - sub["pool_ew_net30"]
        ve = sub["strategy_volmanaged"] - sub["pool_ew_net30"]
        print(f"  {period}: strategy-pool {se.mean()*100:+.1f}%/yr win {(se>0).mean()*100:.0f}% | "
              f"volman-pool {ve.mean()*100:+.1f}%/yr win {(ve>0).mean()*100:.0f}%")
    se_all = tab["strategy_net30"] - tab["pool_ew_net30"]
    print(f"  full: strategy-pool {se_all.mean()*100:+.1f}%/yr win {(se_all>0).mean()*100:.0f}%")

    print("\n=== capacity (10% of top-quintile Feb-Apr ADV) ===")
    for y in sorted(holdings):
        print(f"  {y}: n={len(holdings[y]['hold']):4d}  cap {holdings[y]['adv_yi']*0.10:8.1f} 亿元")

    summary = {
        "full": {k: metrics(v.loc["2008-05":]) for k, v in series.items()},
        "annual": tab.to_dict(),
        "capacity_yi": {y: holdings[y]["adv_yi"] * 0.10 for y in holdings},
        "avg_turnover": float(turn[turn > 0].mean()),
    }
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
