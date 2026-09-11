"""Intraday-microstructure IC probe on the AVAILABLE bar_5min slices.

bar_5min is sparse: 14:30-15:00 since 2021, +10:00/13:30/14:00 since 2024.
Signals known at day-t close, vs next-session targets.
  overnight  = open_t/prev_close_t - 1
  intraday   = close_t/open_t - 1
  last30     = bar1500/bar1430 - 1
  firsthour  = bar1000/open_t - 1   (2024+)
Targets:
  tgt_gap    = open_{t+1}/close_t - 1
  tgt_ret    = close_{t+1}/close_t - 1
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


def main() -> int:
    px = read(
        "SELECT ts_code, trade_date, open, close, pre_close FROM daily "
        "WHERE trade_date >= '2021-01-01' AND close > 0 "
        "AND (ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ' OR ts_code LIKE '%.BJ')")
    px["trade_date"] = px["trade_date"].astype(str)
    px = px.sort_values(["ts_code", "trade_date"])
    px["overnight"] = px["open"] / px["pre_close"] - 1.0
    px["intraday"] = px["close"] / px["open"] - 1.0
    px["tgt_ret"] = px.groupby("ts_code")["close"].shift(-1) / px["close"] - 1.0
    px["tgt_gap"] = px.groupby("ts_code")["open"].shift(-1) / px["close"] - 1.0

    bars = read(
        "SELECT ts_code, trade_date, trade_time, close FROM bar_5min "
        "WHERE trade_time IN ('1430','1500','1000') AND trade_date >= '2021-01-01'")
    b = bars.pivot_table(index=["ts_code", "trade_date"], columns="trade_time",
                         values="close", aggfunc="last").reset_index()
    b["trade_date"] = b["trade_date"].astype(str)
    if {"1430", "1500"}.issubset(b.columns):
        b["last30"] = b["1500"] / b["1430"] - 1.0
    if "1000" in b.columns:
        b["firsthour"] = np.nan  # filled after merge with open
    df = px.merge(b, on=["ts_code", "trade_date"], how="inner")
    df["firsthour"] = df["1000"] / df["open"] - 1.0

    def ic(col: str, tgt: str, label: str) -> None:
        d = df.dropna(subset=[col, tgt])
        ics = []
        for _t, g in d.groupby("trade_date"):
            if len(g) < 200:
                continue
            ics.append(g[col].rank().corr(g[tgt].rank()))
        ics = pd.Series(ics).dropna()
        if len(ics) < 10:
            print(f"  {label:26s} too few days ({len(ics)})")
            return
        ir = ics.mean() / ics.std() if ics.std() else 0
        print(f"  {label:26s} IC {ics.mean():+.4f} IR {ir:+.3f} t {ir*np.sqrt(len(ics)):+.1f} "
              f"n_days {len(ics)} n_obs {len(d)}")

    print("=== signals vs NEXT-DAY close ret / next OPEN gap (2021+) ===")
    for col in ["overnight", "intraday", "last30"]:
        ic(col, "tgt_ret", f"{col} -> tgt_ret")
        ic(col, "tgt_gap", f"{col} -> tgt_gap")
    print("=== first-hour (2024+ only) ===")
    d24 = df[df["trade_date"] >= "2024-01-01"]
    for col in ["firsthour"]:
        for tgt in ["tgt_ret", "tgt_gap"]:
            dd = d24.dropna(subset=[col, tgt])
            ics = []
            for _t, g in dd.groupby("trade_date"):
                if len(g) < 200:
                    continue
                ics.append(g[col].rank().corr(g[tgt].rank()))
            ics = pd.Series(ics).dropna()
            ir = ics.mean() / ics.std() if ics.std() else 0
            print(f"  {col} -> {tgt}       IC {ics.mean():+.4f} IR {ir:+.3f} "
                  f"t {ir*np.sqrt(len(ics)):+.1f} n_days {len(ics)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
