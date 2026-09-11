"""Deepen the inv+accr composite: industry-neutral + within-liquid IC.

Also a 2020+ valuation fold (B/M, E/P from cn_financial bps/eps).
Industry mapping is a CURRENT snapshot (stock_eastmoney_industry) — not
point-in-time, so treat the neutralization as indicative, not definitive.
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


def read(sql: str) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql(sql, conn)


def ic_of(d: pd.DataFrame, col: str) -> tuple[float, int]:
    dd = d.dropna(subset=[col, "fwd12"])
    if len(dd) < 100:
        return np.nan, len(dd)
    return float(np.corrcoef(dd[col].rank().values, dd["fwd12"].rank().values)[0, 1]), len(dd)


def main() -> int:
    m = add_factors(annual_metrics())
    ind = read("SELECT ts_code, industry_name FROM stock_eastmoney_industry")
    px = read("SELECT ts_code, trade_date, close, amount FROM daily WHERE trade_date>='2007-01-01' "
              "AND close>0 AND (ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ' OR ts_code LIKE '%.BJ')")
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
        d = d.merge(ind, on="ts_code", how="left")
        d["score"] = 1 - (d["asset_growth"].rank(pct=True) + d["accruals"].rank(pct=True)) / 2
        d["score_indneu"] = d.groupby("industry_name")["score"].rank(pct=True)
        d["logamt"] = np.log(d["amt"].clip(lower=1.0))
        return d

    print("=== inv+accr composite: raw vs industry-neutral vs within-liquid (IC / t over years) ===")
    rows = {"raw": [], "indneu": [], "raw_liq": [], "indneu_liq": [], "score_q5_ew": [], "indneu_q5_ew": []}
    for y in range(2008, 2025):
        d = build(y)
        if len(d) < 200:
            continue
        ic, _ = ic_of(d, "score")
        icn, _ = ic_of(d, "score_indneu")
        med = d["amt"].median()
        dl = d[d["amt"] >= med]
        icl, _ = ic_of(dl, "score")
        icnl, _ = ic_of(dl, "score_indneu")
        # Q5-EW within liquid, raw and industry-neutral
        q = pd.qcut(dl["score"].rank(method="first"), 5, labels=False)
        rows["score_q5_ew"].append(dl[q == 4]["fwd12"].mean() - dl["fwd12"].mean())
        q2 = pd.qcut(dl["score_indneu"].rank(method="first"), 5, labels=False)
        rows["indneu_q5_ew"].append(dl[q2 == 4]["fwd12"].mean() - dl["fwd12"].mean())
        rows["raw"].append(ic); rows["indneu"].append(icn)
        rows["raw_liq"].append(icl); rows["indneu_liq"].append(icnl)

    def rep(key, label):
        a = pd.Series(rows[key]).dropna()
        ir = a.mean() / a.std() if a.std() else 0
        print(f"  {label:34s} IC {a.mean():+.4f}  t {ir*np.sqrt(len(a)):+.2f}  n={len(a)}")
    rep("raw", "raw (all)")
    rep("indneu", "industry-neutral (all)")
    rep("raw_liq", "raw (liquid)")
    rep("indneu_liq", "industry-neutral (liquid)")
    for k, l in [("score_q5_ew", "Q5-EW liquid (raw)"), ("indneu_q5_ew", "Q5-EW liquid (ind-neu)")]:
        a = pd.Series(rows[k]).dropna()
        print(f"  {l:34s} mean {a.mean()*100:+.1f}%/yr  win {(a>0).mean()*100:.0f}%")

    # --- 2020+ valuation fold (B/M, E/P) ---
    fin = read("SELECT ts_code, ann_date, end_date, bps, eps FROM cn_financial WHERE bps IS NOT NULL")
    fin["ann_date"] = pd.to_datetime(fin["ann_date"]); fin["end_date"] = pd.to_datetime(fin["end_date"])
    fin = fin.sort_values(["ts_code", "ann_date"])
    print("\n=== 2020+ valuation fold (5 formations) ===")
    vrows = {"value": [], "val+inv+accr": []}
    for y in range(2021, 2026):
        t = pd.Timestamp(f"{y}-05-01")
        f = fin[fin["ann_date"] <= t].drop_duplicates("ts_code", keep="last")[["ts_code", "bps", "eps"]]
        d = build(y).merge(f, on="ts_code", how="inner")
        if len(d) < 200:
            continue
        d["bm"] = d["bps"] / d["close"]
        d["ep"] = d["eps"] / d["close"]
        d["value"] = (d["bm"].rank(pct=True) + d["ep"].rank(pct=True)) / 2
        d["val_ia"] = (d["value"] + d["score"]) / 2
        vrows["value"].append(ic_of(d, "value")[0])
        vrows["val+inv+accr"].append(ic_of(d, "val_ia")[0])
    for k in vrows:
        a = pd.Series(vrows[k]).dropna()
        ir = a.mean() / a.std() if a.std() else 0
        print(f"  {k:16s} IC {a.mean():+.4f}  t {ir*np.sqrt(len(a)):+.2f}  n={len(a)} "
              f"(2021-2025, industry snapshot caveat)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
