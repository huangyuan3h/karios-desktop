"""Composite of the surviving long-history fundamental factors + yearly playback.

Survivors (oriented: higher = better):
  -asset_growth  (investment anomaly)
  -accruals      (Sloan accrual anomaly)
  +ocf_assets    (cash profitability)
Test: composite rank-IC (raw / orth size), Q1..Q5 forward-12m, and a long-only
top-quintile portfolio (annual rebalance, EW) vs the equal-weight universe.
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


def pct(x: pd.Series) -> pd.Series:
    return x.rank(pct=True)


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

    variants = {
        "inv+accr": ["-asset_growth", "-accruals"],
        "inv+accr+ocf": ["-asset_growth", "-accruals", "ocf_assets"],
        "inv+accr+roa": ["-asset_growth", "-accruals", "roa"],
    }
    summary = {k: {"top": [], "ew": [], "ls": [], "n": [], "ics": [], "ics_o": []} for k in variants}
    for y in range(2008, 2025):
        t = pd.Timestamp(f"{y}-05-01")
        ann = m[m["ann_date"] <= t].sort_values(["ts_code", "end_date"]).drop_duplicates("ts_code", keep="last")
        ann["yr"] = y
        d = ann.merge(may[may["yr"] == y], on=["ts_code", "yr"], how="inner").dropna(subset=["fwd12"])
        if len(d) < 200:
            continue
        d["logamt"] = np.log(d["amt"].clip(lower=1.0))
        for name, fs in variants.items():
            parts = []
            for f in fs:
                col = f[1:] if f.startswith("-") else f
                parts.append(-pct(d[col]) if f.startswith("-") else pct(d[col]))
            d["score"] = sum(parts) / len(parts)
            dd = d.dropna(subset=["score", "fwd12"]).copy()
            q = pd.qcut(dd["score"].rank(method="first"), 5, labels=False)
            top = dd[q == 4]["fwd12"].mean()
            bot = dd[q == 0]["fwd12"].mean()
            ew = dd["fwd12"].mean()
            summary[name]["top"].append(top)
            summary[name]["ew"].append(ew)
            summary[name]["ls"].append(top - bot)
            summary[name]["n"].append(len(dd))
            summary[name]["ics"].append(np.corrcoef(dd["score"].rank().values, dd["fwd12"].rank().values)[0, 1])
            x = np.column_stack([np.ones(len(dd)), dd["logamt"].rank(pct=True).values])
            yv = dd["score"].rank(pct=True).values
            beta, *_ = np.linalg.lstsq(x, yv, rcond=None)
            summary[name]["ics_o"].append(np.corrcoef(yv - x @ beta, dd["fwd12"].rank().values)[0, 1])

    print(f"{'variant':16s} {'IC':>8s} {'t':>6s} {'IC_o':>8s} {'t_o':>6s} | "
          f"{'top/yr':>7s} {'ew/yr':>7s} {'L/S/yr':>7s} {'top win':>8s} {'top nav':>8s} {'ew nav':>7s}")
    for name in variants:
        s = summary[name]
        ic = np.array(s["ics"]); ico = np.array(s["ics_o"])
        top = np.array(s["top"]); ew = np.array(s["ew"]); ls = np.array(s["ls"])
        yrs = len(top)
        tir = ic.mean() / ic.std() if ic.std() else 0
        toir = ico.mean() / ico.std() if ico.std() else 0
        nav_t = np.prod(1 + top); nav_e = np.prod(1 + ew)
        print(f"{name:16s} {ic.mean():+.4f} {tir*np.sqrt(yrs):+6.2f} {ico.mean():+.4f} "
              f"{toir*np.sqrt(yrs):+6.2f} | {top.mean()*100:+6.1f}% {ew.mean()*100:+6.1f}% "
              f"{ls.mean()*100:+6.1f}% {(top>ew).mean()*100:6.0f}% {nav_t:8.2f} {nav_e:7.2f}")
    print(f"\n(years={yrs}, equal-weight universe ~= market; top = composite best quintile)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
