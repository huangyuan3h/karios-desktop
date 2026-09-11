"""Alt-alpha IC screen — untested CN data sources vs next-month returns.

Signals (computed AS-OF end of month M, predicting month M+1 return):
  holder_chg  股东户数环比变化 (cn_holder_number)   — down = 筹码集中 (bullish)
  north_d     陆股通持股比率 月内变化 (cn_hk_hold)   — up = 北向增持 (bullish)
  lgbuy       大单+超大单净流入占比 (cn_moneyflow)   — up = 主力买入 (bullish)

Metrics per signal: monthly rank-IC mean / IR / t-stat / %positive,
plus quintile mean forward return (monotonicity). Read-only.

Usage:
  PYTHONPATH=src python3 scripts/incubate/alt_alpha_ic.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.db import get_connection  # noqa: E402

START = "2019-06-01"


def _read(sql: str) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql(sql, conn)


def monthly_returns() -> pd.DataFrame:
    px = _read(
        "SELECT ts_code, trade_date, close, amount FROM daily "
        "WHERE trade_date >= '%s' AND close > 0 "
        "AND (ts_code LIKE '%%.SH' OR ts_code LIKE '%%.SZ' OR ts_code LIKE '%%.BJ')" % START
    )
    px["trade_date"] = pd.to_datetime(px["trade_date"])
    px["ym"] = px["trade_date"].dt.to_period("M")
    mo = px.groupby(["ts_code", "ym"]).agg(
        first=("close", "first"), last=("close", "last"), amt=("amount", "mean")
    ).reset_index()
    mo["ret"] = mo["last"] / mo["first"] - 1.0
    return mo


def to_grid(s: pd.Series, all_ym: list) -> pd.DataFrame:
    """(ts_code, ym) signal → wide, reindex cols to all_ym, forward-fill, long."""
    wide = s.unstack("ym").reindex(columns=all_ym).ffill(axis=1)
    return wide.stack().rename("v").reset_index()


def sig_holder(all_ym: list) -> pd.DataFrame:
    h = _read(
        "SELECT ts_code, ann_date, holder_num FROM cn_holder_number "
        "WHERE ann_date >= '%s' AND holder_num > 0" % START
    )
    h["ann"] = pd.to_datetime(h["ann_date"])
    h["ym"] = h["ann"].dt.to_period("M")
    h = h.sort_values(["ts_code", "ann"])
    h["prev"] = h.groupby("ts_code")["holder_num"].shift(1)
    h["chg"] = h["holder_num"] / h["prev"] - 1.0
    h = h.dropna(subset=["chg"])
    return to_grid(h.groupby(["ts_code", "ym"])["chg"].last(), all_ym).rename(columns={"v": "holder_chg"})


def sig_north(all_ym: list) -> pd.DataFrame:
    nh = _read(
        "SELECT ts_code, trade_date, ratio FROM cn_hk_hold "
        "WHERE trade_date >= '%s'" % START
    )
    nh["trade_date"] = pd.to_datetime(nh["trade_date"])
    nh["ym"] = nh["trade_date"].dt.to_period("M")
    g = nh.groupby(["ts_code", "ym"])["ratio"].agg(["first", "last"])
    g["d"] = g["last"] - g["first"]
    return to_grid(g["d"], all_ym).rename(columns={"v": "north_d"})


def sig_lgbuy(all_ym: list) -> pd.DataFrame:
    mf = _read(
        "SELECT ts_code, trade_date, buy_lg_amount, sell_lg_amount, buy_elg_amount, "
        "sell_elg_amount, buy_sm_amount, sell_sm_amount, buy_md_amount, sell_md_amount "
        "FROM cn_moneyflow WHERE trade_date >= '%s'" % START
    )
    mf["trade_date"] = pd.to_datetime(mf["trade_date"])
    mf["ym"] = mf["trade_date"].dt.to_period("M")
    for c in ["buy_lg_amount", "sell_lg_amount", "buy_elg_amount", "sell_elg_amount",
              "buy_sm_amount", "sell_sm_amount", "buy_md_amount", "sell_md_amount"]:
        mf[c] = pd.to_numeric(mf[c], errors="coerce").fillna(0.0)
    mf["net"] = (mf["buy_lg_amount"] + mf["buy_elg_amount"]) - (mf["sell_lg_amount"] + mf["sell_elg_amount"])
    mf["tot"] = (mf["buy_lg_amount"] + mf["buy_elg_amount"] + mf["buy_sm_amount"]
                 + mf["sell_sm_amount"] + mf["buy_md_amount"] + mf["sell_md_amount"]
                 + mf["sell_lg_amount"] + mf["sell_elg_amount"])
    g = mf.groupby(["ts_code", "ym"]).agg(net=("net", "sum"), tot=("tot", "sum"))
    g["r"] = np.where(g["tot"] > 0, g["net"] / g["tot"], np.nan)
    return to_grid(g["r"], all_ym).rename(columns={"v": "lgbuy"})


def screen(name: str, sig: pd.DataFrame, mo: pd.DataFrame) -> None:
    # signal known at end of month M predicts month M+1 return
    s = sig.copy()
    s["ym"] = s["ym"] + 1
    df = mo.merge(s, on=["ts_code", "ym"], how="inner").dropna(subset=[name, "ret"])
    ics, rows = [], []
    for ym, g in df.groupby("ym"):
        if len(g) < 100:
            continue
        ic = g[name].rank().corr(g["ret"].rank())
        ics.append(ic)
        q = pd.qcut(g[name].rank(method="first"), 5, labels=False)
        qm = g.groupby(q)["ret"].mean()
        rows.append([ym] + [qm.get(i, np.nan) for i in range(5)])
    ics = pd.Series(ics)
    ir = ics.mean() / ics.std() if ics.std() else 0
    t = ir * np.sqrt(len(ics))
    qdf = pd.DataFrame(rows, columns=["ym", "Q1", "Q2", "Q3", "Q4", "Q5"]).set_index("ym")
    qm = {c: qdf[c].mean() * 100 for c in ["Q1", "Q2", "Q3", "Q4", "Q5"]}
    print(f"\n=== {name} ===  months={len(ics)} avg_n={int(df.groupby('ym').size().mean())}")
    print(f"  rank-IC mean {ics.mean():+.4f}  IR {ir:+.3f}  t {t:+.2f}  %pos {(ics > 0).mean()*100:.0f}%")
    print(f"  quintile fwd ret (%): Q1 {qm['Q1']:+.2f}  Q2 {qm['Q2']:+.2f}  Q3 {qm['Q3']:+.2f} "
          f"Q4 {qm['Q4']:+.2f}  Q5 {qm['Q5']:+.2f}  | Q5-Q1 {qm['Q5'] - qm['Q1']:+.2f}")


def main() -> int:
    mo = monthly_returns()
    all_ym = sorted(mo["ym"].unique())
    print(f"monthly panel {len(mo)} rows; months {all_ym[0]}..{all_ym[-1]}")
    for fn, name in [(sig_holder, "holder_chg"), (sig_north, "north_d"), (sig_lgbuy, "lgbuy")]:
        try:
            sig = fn(all_ym)
            screen(name, sig, mo)
        except Exception as e:  # noqa: BLE001
            print(f"\n=== {name} === ERROR {e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
