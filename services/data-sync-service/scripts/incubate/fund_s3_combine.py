"""Combine the annual fundamental sleeve with S-3 at the portfolio level.

Builds the sleeve's DAILY NAV over the S-3 audit windows (2024-05..2026-08)
from the 2024/2025/2026 May vintages (liquid + industry-neutral inv+accr top
quintile, buy-and-hold EW), then blends with the S-3 engine daily NAV:

  S1 fixed blend     w*sleeve + (1-w)*S3, w in {0.2,0.3,0.4,0.5}
  S2 risk parity     trailing-vol inverse weights
  S3 idle -> sleeve  S-3 uninvested cash earns the sleeve
Read-only.
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
from data_sync_service.service.backtest_engine import BacktestConfig, simulate  # noqa: E402
from run_walk_forward import S3_CONFIG, WINDOWS  # noqa: E402


def read(sql: str) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql(sql, conn)


def build_sleeve_daily() -> pd.Series:
    m = add_factors(annual_metrics())
    em = read("SELECT ts_code, industry_name FROM stock_eastmoney_industry")
    px = read("SELECT ts_code, trade_date, close, amount FROM daily WHERE trade_date>='2020-09-01' "
              "AND close>0 AND (ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ' OR ts_code LIKE '%.BJ')")
    px["trade_date"] = pd.to_datetime(px["trade_date"]); px["ym"] = px["trade_date"].dt.to_period("M")
    mo = px.groupby(["ts_code", "ym"]).agg(close=("close", "last"), amt=("amount", "mean")).reset_index()
    mo["yr"] = mo["ym"].dt.year; mo["mon"] = mo["ym"].dt.month
    liqm = mo[mo["mon"].isin([2, 3, 4])].groupby(["ts_code", "yr"])["amt"].mean().rename("liq").reset_index()

    holdings = {}
    for y in (2021, 2022, 2023, 2024, 2025, 2026):
        t = pd.Timestamp(f"{y}-05-01")
        ann = m[m["ann_date"] <= t].sort_values(["ts_code", "end_date"]).drop_duplicates("ts_code", keep="last")
        d = ann.merge(liqm[liqm["yr"] == y], on="ts_code", how="left").merge(em, on="ts_code", how="left")
        d = d.dropna(subset=["liq", "asset_growth", "accruals"])
        d["industry_name"] = d["industry_name"].fillna("UNK")
        d["score"] = 1 - (d["asset_growth"].rank(pct=True) + d["accruals"].rank(pct=True)) / 2
        d["score"] = d.groupby("industry_name")["score"].rank(pct=True)
        pool = d[d["liq"] >= d["liq"].median()]
        q = pd.qcut(pool["score"].rank(method="first"), 5, labels=False)
        holdings[y] = set(pool[q == 4]["ts_code"])

    closes = px.pivot_table(index="trade_date", columns="ts_code", values="close")
    parts = []
    bounds = {2021: ("2021-05-01", "2022-05-01"), 2022: ("2022-05-01", "2023-05-01"),
              2023: ("2023-05-01", "2024-05-01"), 2024: ("2024-05-01", "2025-05-01"),
              2025: ("2025-05-01", "2026-05-01"), 2026: ("2026-05-01", "2026-09-11")}
    for y, (lo, hi) in bounds.items():
        cols = [c for c in holdings[y] if c in closes.columns]
        sub = closes.loc[(closes.index >= lo) & (closes.index < hi), cols].copy()
        sub = sub.ffill().dropna(axis=1, how="all")
        norm = sub / sub.iloc[0]
        v = norm.mean(axis=1)
        r = v.pct_change()
        r.iloc[0] = 0.0  # rebalance at the formation close → no return that day
        parts.append(r)
    return pd.concat(parts).sort_index().fillna(0.0)


def s3_series(run):
    n = len(run.positions_by_day)
    dates = pd.to_datetime([p["date"] for p in run.positions_by_day])
    navs = np.array([1.0] + list(run.nav_curve[:n]))
    r = pd.Series(navs[1:] / navs[:-1] - 1, index=dates)
    invest = pd.Series([sum(x["position_pct"] for x in p["positions"]) for p in run.positions_by_day],
                       index=dates)
    return r, invest


def metrics(r: pd.Series) -> str:
    r = r.fillna(0.0)
    nav = (1 + r).cumprod()
    total = (nav.iloc[-1] - 1) * 100
    v = r.std() * np.sqrt(252) * 100
    dd = float((nav / nav.cummax() - 1).min()) * 100
    sh = (r.mean() * 252) / (r.std() * np.sqrt(252)) if r.std() else 0
    return f"tot {total:+6.1f}% vol {v:5.1f}% DD {dd:6.1f}% sr {sh:5.2f}"


def main() -> int:
    sleeve = build_sleeve_daily()
    print(f"sleeve daily: {len(sleeve)} days {sleeve.index.min().date()}..{sleeve.index.max().date()}")
    print()
    for w in ("OOS2", "train", "valid", "long"):
        start, end = WINDOWS[w]
        run = simulate(BacktestConfig(start_date=start, end_date=end, **S3_CONFIG))
        r3, inv = s3_series(run)
        sl = sleeve.reindex(r3.index).fillna(0.0)
        r3 = r3.fillna(0.0)
        idle = (1.0 - inv).shift(1).fillna(1.0 - inv.iloc[0])
        corr = np.corrcoef(sl.values, r3.values)[0, 1]
        print(f"== {w} ==")
        print(f"  S-3 alone            {metrics(r3)}")
        print(f"  sleeve alone         {metrics(sl)}")
        print(f"  corr(sleeve,S3)={corr:+.2f}")
        for ww in (0.2, 0.3, 0.4, 0.5):
            print(f"  S1 blend w={ww:.1f}       {metrics(ww * sl + (1 - ww) * r3)}")
        v3 = r3.rolling(60, min_periods=20).std()
        vsl = sl.rolling(60, min_periods=20).std()
        wt = ((1 / vsl) / (1 / vsl + 1 / v3)).shift(1).fillna(0.4).clip(0.0, 0.8)
        print(f"  S2 risk-parity       {metrics(wt * sl + (1 - wt) * r3)}  | avgW_slv {wt.mean():.2f}")
        print(f"  S3 idle->sleeve      {metrics(r3 + idle * sl)}  | avgIdle {idle.mean():.2f}")
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
