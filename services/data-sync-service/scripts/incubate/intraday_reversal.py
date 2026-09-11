"""Overnight-reversal tradability check for the last-30-min signal.

At close_t rank by last30 (14:30->15:00 return); next overnight return is
open_{t+1}/close_t - 1. Test: quintile monotonicity, liquid subset, and a
long-short / long-only net of cost. Also compare 14:55 (avoid closing auction).
Read-only.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.db import get_connection  # noqa: E402


def read(sql: str) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql(sql, conn)


def analyze(df: pd.DataFrame, col: str, label: str, cost: float = 0.003) -> None:
    d = df.dropna(subset=[col, "tgt_gap"]).copy()
    ls, q_means = [], {i: [] for i in range(5)}
    for _t, g in d.groupby("trade_date"):
        if len(g) < 200:
            continue
        q = pd.qcut(g[col].rank(method="first"), 5, labels=False)
        m = g.groupby(q)["tgt_gap"].mean()
        for i in range(5):
            if i in m:
                q_means[i].append(m[i])
        if 0 in m and 4 in m:
            ls.append(m[0] - m[4])
    qm = {i: np.nanmean(q_means[i]) * 100 for i in range(5)}
    ls = pd.Series(ls)
    nav_gross = (1 + ls).prod()
    nav_net = (1 + ls - cost).prod()
    print(f"  {label:26s} Q1 {qm[0]:+.3f} Q2 {qm[1]:+.3f} Q3 {qm[2]:+.3f} Q4 {qm[3]:+.3f} Q5 {qm[4]:+.3f}"
          f" | Q1-Q5 {qm[0]-qm[4]:+.3f}%/day | L/S nav gross x{nav_gross:.2f} net(-{cost*1e4:.0f}bp) x{nav_net:.2f}"
          f" | n_days {len(ls)}")


def main() -> int:
    px = read(
        "SELECT ts_code, trade_date, open, close, pre_close, amount FROM daily "
        "WHERE trade_date >= '2021-01-01' AND close > 0 "
        "AND (ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ' OR ts_code LIKE '%.BJ')")
    px["trade_date"] = px["trade_date"].astype(str)
    px = px.sort_values(["ts_code", "trade_date"])
    px["overnight"] = px["open"] / px["pre_close"] - 1.0
    px["intraday"] = px["close"] / px["open"] - 1.0
    px["tgt_gap"] = px.groupby("ts_code")["open"].shift(-1) / px["close"] - 1.0

    bars = read(
        "SELECT ts_code, trade_date, trade_time, close FROM bar_5min "
        "WHERE trade_time IN ('1430','1455','1500') AND trade_date >= '2021-01-01'")
    b = bars.pivot_table(index=["ts_code", "trade_date"], columns="trade_time",
                         values="close", aggfunc="last").reset_index()
    b["trade_date"] = b["trade_date"].astype(str)
    b["last30"] = b["1500"] / b["1430"] - 1.0
    b["last25"] = b["1455"] / b["1430"] - 1.0
    df = px.merge(b, on=["ts_code", "trade_date"], how="inner")

    print("=== all-stock, target = next open gap ===")
    for col in ["last30", "last25", "intraday", "overnight"]:
        analyze(df, col, col)
    print("\n=== liquid subset (amount >= same-day median) ===")
    df["med"] = df.groupby("trade_date")["amount"].transform("median")
    liq = df[df["amount"] >= df["med"]]
    for col in ["last30", "last25", "intraday", "overnight"]:
        analyze(liq, col, col)
    print("\n=== large liquid (amount >= 80th pct) ===")
    p80 = df.groupby("trade_date")["amount"].transform(lambda s: s.quantile(0.8))
    big = df[df["amount"] >= p80]
    for col in ["last30", "last25", "intraday", "overnight"]:
        analyze(big, col, col)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
