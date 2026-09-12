#!/usr/bin/env python3
"""GARP (quality + cheap) long-hold sleeve pilot. Read-only.

Pre-registration: docs/designs/garp-sleeve-prereg-2026-09-12.md

Annual May formation, liquid pool (Feb-Apr avg amount >= median), industry-neutral
scores; hold top quintile equal weight 12m; cost = turnover*30bp.
Arms (pre-registered controls): value(EP,BP) / quality(ROE,CCR) / GARP(all four).
BPS is reconstructed from income statement (shares = NI/basic_eps) and validated
against cn_financial.bps (2019+); if Spearman < 0.8 -> fall back to 2020-2024.

Usage:
  PYTHONPATH=src python3 scripts/incubate/garp_sleeve.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.db import get_connection  # noqa: E402

REPORT = Path(__file__).resolve().parents[2] / "data" / "backtest_reports" / "garp_sleeve.json"
SUF = "(ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ' OR ts_code LIKE '%.BJ')"


def read(sql: str) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql(sql, conn)


def load_fundamentals() -> pd.DataFrame:
    bs = read(
        "SELECT ts_code, ann_date, end_date, total_assets, total_liab, "
        "total_hldr_eqy_inc_min_int FROM cn_balance_sheet "
        "WHERE to_char(end_date,'MM-DD')='12-31' AND report_type='1' AND total_assets>0"
    )
    inc = read(
        "SELECT ts_code, ann_date, end_date, basic_eps, n_income_attr_p, revenue "
        "FROM cn_income_stmt WHERE to_char(end_date,'MM-DD')='12-31' AND report_type='1'"
    )
    cf = read(
        "SELECT ts_code, ann_date, end_date, n_cashflow_act FROM cn_cashflow_stmt "
        "WHERE to_char(end_date,'MM-DD')='12-31' AND report_type='1'"
    )
    inc = inc.rename(columns={"ann_date": "ann_date_inc"})
    cf = cf.rename(columns={"ann_date": "ann_date_cf"})
    for d in (bs, inc, cf):
        d["end_date"] = pd.to_datetime(d["end_date"])
    m = bs.merge(inc, on=["ts_code", "end_date"], how="inner").merge(
        cf, on=["ts_code", "end_date"], how="inner"
    )
    for c in ["total_assets", "total_liab", "total_hldr_eqy_inc_min_int", "basic_eps",
              "n_income_attr_p", "revenue", "n_cashflow_act"]:
        m[c] = pd.to_numeric(m[c], errors="coerce")
    for c in ["ann_date", "ann_date_inc", "ann_date_cf"]:
        m[c] = pd.to_datetime(m[c])
    m["ann_date"] = m[["ann_date", "ann_date_inc", "ann_date_cf"]].max(axis=1)
    eq = m["total_hldr_eqy_inc_min_int"]
    m["roe"] = m["n_income_attr_p"] / eq.where(eq > 0)
    m["ccr"] = m["n_cashflow_act"] / m["n_income_attr_p"].where(m["n_income_attr_p"] > 0)
    shares = m["n_income_attr_p"] / m["basic_eps"].where(m["basic_eps"] > 0)
    m["bps_est"] = eq / shares
    return m.sort_values(["ts_code", "end_date"])


def load_prices() -> tuple[pd.DataFrame, pd.Series]:
    may = read(
        f"SELECT ts_code, to_char(trade_date,'YYYY') AS yr, "
        f"(array_agg(close ORDER BY trade_date DESC))[1] AS close, "
        f"(array_agg(adj_factor ORDER BY trade_date DESC))[1] AS adj "
        f"FROM daily WHERE to_char(trade_date,'MM')='05' AND close>0 AND {SUF} "
        f"GROUP BY ts_code, yr"
    )
    may["yr"] = may["yr"].astype(int)
    for c in ["close", "adj"]:
        may[c] = pd.to_numeric(may[c], errors="coerce")
    nxt = may[["ts_code", "yr", "close"]].copy()
    nxt["yr"] = nxt["yr"] - 1
    may = may.merge(nxt, on=["ts_code", "yr"], how="left", suffixes=("", "_next"))
    may["fwd12"] = may["close_next"] / may["close"] - 1.0
    adj_latest = read(
        "SELECT DISTINCT ON (ts_code) ts_code, adj_factor FROM daily ORDER BY ts_code, trade_date DESC"
    )
    adj_latest = adj_latest.set_index("ts_code")["adj_factor"].astype(float)
    return may, adj_latest


def main() -> int:
    m = load_fundamentals()
    may, adj_latest = load_prices()
    liq = read(
        f"SELECT ts_code, to_char(trade_date,'YYYY') AS yr, avg(amount) AS amt FROM daily "
        f"WHERE to_char(trade_date,'MM') IN ('02','03','04') AND close>0 AND {SUF} "
        f"GROUP BY ts_code, yr"
    )
    liq["yr"] = liq["yr"].astype(int)
    ind = read("SELECT ts_code, industry_name FROM stock_eastmoney_industry")

    # --- BPS reconstruction validation gate (2019+ true bps) ---
    fin = read("SELECT ts_code, end_date, bps FROM cn_financial "
               "WHERE to_char(end_date,'MM-DD')='12-31' AND bps IS NOT NULL")
    fin["end_date"] = pd.to_datetime(fin["end_date"])
    chk = m.merge(fin, on=["ts_code", "end_date"], how="inner").dropna(subset=["bps_est", "bps"])
    chk = chk[chk["bps"] > 0]
    if len(chk) > 500:
        sp = float(np.corrcoef(chk["bps_est"].rank(), chk["bps"].rank())[0, 1])
        rel = float(((chk["bps_est"] - chk["bps"]).abs() / chk["bps"]).median())
    else:
        sp, rel = 0.0, 1.0
    gate_ok = sp >= 0.8
    print(f"BPS recon gate: spearman {sp:.3f}  median|relerr| {rel:.2f}  n={len(chk)} "
          f"-> {'long 2008-2024' if gate_ok else 'fallback 2020-2024 (underpowered)'}", flush=True)
    years = list(range(2008, 2025)) if gate_ok else list(range(2020, 2025))

    ARMS = {
        "value": ["ep", "bp"],
        "quality": ["roe", "ccr"],
        "garp": ["ep", "bp", "roe", "ccr"],
    }

    rows = []
    prev = {k: set() for k in ARMS}
    pool_prev = set()
    for y in years:
        t = pd.Timestamp(f"{y}-05-01")
        ann = m[m["ann_date"] <= t].sort_values(["ts_code", "end_date"]).drop_duplicates("ts_code", keep="last")
        d = ann.merge(may[may["yr"] == y], on="ts_code", how="inner")
        d = d.merge(liq[liq["yr"] == y], on="ts_code", how="left")
        d = d.merge(ind, on="ts_code", how="left")
        d["adj_latest"] = d["ts_code"].map(adj_latest)
        d = d.dropna(subset=["fwd12", "amt", "adj", "adj_latest", "total_assets"])
        d = d[(d["close"] > 0) & (d["adj"] > 0) & (d["adj_latest"] > 0)]
        if len(d) < 200:
            continue
        d["raw"] = d["close"] * d["adj_latest"] / d["adj"]
        d["ep"] = d["basic_eps"] / d["raw"]
        d["bp"] = d["bps_est"] / d["raw"]
        d["industry_name"] = d["industry_name"].fillna("UNK")
        d = d.dropna(subset=["ep", "bp", "roe", "ccr"])
        if len(d) < 200:
            continue
        for c in ["ep", "bp", "roe", "ccr"]:
            d[c] = d.groupby("industry_name")[c].rank(pct=True)
        med = d["amt"].median()
        pool = d[d["amt"] >= med]
        if len(pool) < 100:
            continue
        rec = {"yr": y, "n": len(d), "n_pool": len(pool)}
        for arm, cols in ARMS.items():
            pool = pool.copy()
            pool["score"] = pool[cols].mean(axis=1)
            q = pd.qcut(pool["score"].rank(method="first"), 5, labels=False)
            hold = set(pool[q == 4]["ts_code"])
            gross = float(pool[q == 4]["fwd12"].mean())
            turn = 1 - len(hold & prev[arm]) / max(len(hold), 1) if prev[arm] else 1.0
            rec[f"{arm}_gross"] = gross
            rec[f"{arm}_net"] = (1 + gross) * (1 - turn * 0.003) - 1
            rec[f"{arm}_turn"] = turn
            prev[arm] = hold
        pool_set = set(pool["ts_code"])
        pturn = 1 - len(pool_set & pool_prev) / max(len(pool_set), 1) if pool_prev else 1.0
        pool_prev = pool_set
        rec["pool_gross"] = float(pool["fwd12"].mean())
        rec["pool_net"] = (1 + rec["pool_gross"]) * (1 - pturn * 0.003) - 1
        rec["n_hold"] = len(prev["garp"])
        rec["cap_yi"] = float(pool["amt"].sum() / 1e5 * 0.10)
        rows.append(rec)

    r = pd.DataFrame(rows)
    print(r[["yr", "n", "n_pool", "n_hold", "value_net", "quality_net", "garp_net",
             "pool_net", "cap_yi"]].round(4).to_string(index=False))

    print("\n=== excess vs pool EW (net, formation-year) ===")
    res = {}
    for arm in ARMS:
        ex = r[f"{arm}_net"] - r["pool_net"]
        res[arm] = ex
        print(f"  {arm:8s} full {ex.mean()*100:+.1f}%/yr  win {(ex>0).mean()*100:.0f}%")
    if gate_ok:
        for lo, hi, name in [(2008, 2016, "2008-2015"), (2016, 2025, "2016-2024")]:
            msk = (r["yr"] >= lo) & (r["yr"] < hi)
            line = " | ".join(
                f"{a} {(r.loc[msk, f'{a}_net'] - r.loc[msk, 'pool_net']).mean()*100:+.1f}%"
                for a in ARMS
            )
            print(f"  {name}: {line}")
    print("\n  garp vs value corr of yearly excess: "
          f"{res['garp'].corr(res['value']):+.2f} | garp vs quality {res['garp'].corr(res['quality']):+.2f}")
    print("  avg turnover: " + ", ".join(f"{a} {r[f'{a}_turn'].mean():.2f}" for a in ARMS))

    summary = {
        "gate": {"spearman": sp, "median_rel_err": rel, "n": int(len(chk)), "long_history": gate_ok},
        "years": years,
        "arms": {a: {"excess_full": float(res[a].mean()), "win": float((res[a] > 0).mean())} for a in ARMS},
        "yearly": r.to_dict(orient="list"),
    }
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
