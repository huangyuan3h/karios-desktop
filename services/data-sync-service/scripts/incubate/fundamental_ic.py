"""Long-history fundamental factor IC screen (2008-2025, annual as-of).

Form portfolios each May from the latest annual report known by then; forward
12m return to next May. Factors: profitability (ROE/ROA/gross margin/OCF),
accruals, leverage, asset growth, fundamental growth, Piotroski F-score.
Cross-sectional rank-IC, plus orthogonality vs size (log amount). Read-only.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.db import get_connection  # noqa: E402

FORMATION_MONTH = "05-01"


def read(sql: str) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql(sql, conn)


def annual_metrics() -> pd.DataFrame:
    bs = read("SELECT ts_code, ann_date, end_date, total_assets, total_liab, total_cur_assets, "
              "total_cur_liab, total_hldr_eqy_inc_min_int FROM cn_balance_sheet "
              "WHERE to_char(end_date,'MM-DD')='12-31' AND report_type='1' AND total_assets>0")
    inc = read("SELECT ts_code, end_date, revenue, oper_cost, n_income_attr_p, "
               "operate_profit FROM cn_income_stmt "
               "WHERE to_char(end_date,'MM-DD')='12-31' AND report_type='1'")
    cf = read("SELECT ts_code, end_date, n_cashflow_act FROM cn_cashflow_stmt "
              "WHERE to_char(end_date,'MM-DD')='12-31' AND report_type='1'")
    bs["ann_date"] = pd.to_datetime(bs["ann_date"])
    for d in (bs, inc, cf):
        if "ann_date" in d.columns:
            d["ann_date"] = pd.to_datetime(d["ann_date"])
        d["end_date"] = pd.to_datetime(d["end_date"])
        d.sort_values(["ts_code", "end_date"], inplace=True)
        d.drop_duplicates(["ts_code", "end_date"], keep="last", inplace=True)
    return bs.merge(inc, on=["ts_code", "end_date"], how="inner") \
             .merge(cf, on=["ts_code", "end_date"], how="left")


def add_factors(m: pd.DataFrame) -> pd.DataFrame:
    m = m.sort_values(["ts_code", "end_date"]).copy()
    g = m.groupby("ts_code")
    for c in ["total_assets", "total_liab", "total_cur_assets", "total_cur_liab",
              "total_hldr_eqy_inc_min_int", "revenue", "oper_cost", "n_income_attr_p",
              "operate_profit", "n_cashflow_act"]:
        m[c] = pd.to_numeric(m[c], errors="coerce")
    eq = m["total_hldr_eqy_inc_min_int"]
    m["roe"] = m["n_income_attr_p"] / eq.where(eq > 0)
    m["roa"] = m["n_income_attr_p"] / m["total_assets"]
    m["gross_margin"] = (m["revenue"] - m["oper_cost"]) / m["revenue"].where(m["revenue"] > 0)
    m["ocf_assets"] = m["n_cashflow_act"] / m["total_assets"]
    m["accruals"] = (m["n_income_attr_p"] - m["n_cashflow_act"]) / m["total_assets"]
    m["leverage"] = m["total_liab"] / m["total_assets"]
    m["cur_ratio"] = m["total_cur_assets"] / m["total_cur_liab"].where(m["total_cur_liab"] > 0)
    m["asset_turnover"] = m["revenue"] / m["total_assets"]
    m["asset_growth"] = m["total_assets"] / g["total_assets"].shift(1) - 1.0
    m["rev_growth"] = m["revenue"] / g["revenue"].shift(1) - 1.0
    m["ni_growth"] = m["n_income_attr_p"] / g["n_income_attr_p"].shift(1).where(
        g["n_income_attr_p"].shift(1) > 0) - 1.0
    m["d_roe"] = m["roe"] - g["roe"].shift(1)
    m["d_gm"] = m["gross_margin"] - g["gross_margin"].shift(1)
    m["d_lev"] = m["leverage"] - g["leverage"].shift(1)
    m["d_cur"] = m["cur_ratio"] - g["cur_ratio"].shift(1)
    m["d_turn"] = m["asset_turnover"] - g["asset_turnover"].shift(1)
    ni, ocf, ta = m["n_income_attr_p"], m["n_cashflow_act"], m["total_assets"]
    pni, p_roa, p_lev, p_cur, p_gm, p_turn = (
        g["n_income_attr_p"].shift(1), g["roa"].shift(1), g["leverage"].shift(1),
        g["cur_ratio"].shift(1), g["gross_margin"].shift(1), g["asset_turnover"].shift(1))
    m["f_score"] = (
        (m["roa"] > 0).astype(float) + (ocf > 0).astype(float)
        + (m["roa"] > p_roa).astype(float) + (ocf > ni).astype(float)
        + (m["leverage"] < p_lev).astype(float) + (m["cur_ratio"] > p_cur).astype(float)
        + (m["gross_margin"] > p_gm).astype(float) + (m["asset_turnover"] > p_turn).astype(float)
    ).where(pni.notna() | p_roa.isna())
    return m


def main() -> int:
    m = add_factors(annual_metrics())
    # monthly prices + amount for returns / size
    px = read("SELECT ts_code, trade_date, close, amount FROM daily WHERE trade_date >= '2007-01-01' "
              "AND close>0 AND (ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ' OR ts_code LIKE '%.BJ')")
    px["trade_date"] = pd.to_datetime(px["trade_date"])
    px["ym"] = px["trade_date"].dt.to_period("M")
    mo = px.groupby(["ts_code", "ym"]).agg(close=("close", "last"), amt=("amount", "mean")).reset_index()
    mo["yr"] = mo["ym"].dt.year
    may = mo[mo["ym"].dt.month == 5][["ts_code", "yr", "close", "amt"]].copy()
    may["fwd12"] = may.groupby("ts_code")["close"].shift(-1) / may["close"] - 1.0

    factors = ["roe", "roa", "gross_margin", "ocf_assets", "accruals", "leverage",
               "asset_growth", "rev_growth", "ni_growth", "f_score"]
    forms = [f"{y}-{FORMATION_MONTH}" for y in range(2008, 2025)]
    rows_ic = {f: [] for f in factors}
    for y in range(2008, 2025):
        t = pd.Timestamp(f"{y}-{FORMATION_MONTH}")
        ann = m[m["ann_date"] <= t].sort_values(["ts_code", "end_date"]).drop_duplicates("ts_code", keep="last")
        ann["yr"] = y
        d = ann.merge(may[may["yr"] == y], on=["ts_code", "yr"], how="inner").dropna(subset=["fwd12"])
        if len(d) < 200:
            continue
        d["logamt"] = np.log(d["amt"].clip(lower=1.0))
        for f in factors:
            dd = d.dropna(subset=[f, "fwd12"])
            if len(dd) < 150:
                continue
            ic = np.corrcoef(dd[f].rank().values, dd["fwd12"].rank().values)[0, 1]
            # orthogonalized vs size
            x = np.column_stack([np.ones(len(dd)), dd["logamt"].rank(pct=True).values])
            yv = dd[f].rank(pct=True).values
            beta, *_ = np.linalg.lstsq(x, yv, rcond=None)
            res = yv - x @ beta
            ic_o = np.corrcoef(res, dd["fwd12"].rank().values)[0, 1]
            rows_ic[f].append((ic, ic_o, len(dd)))

    print(f"{'factor':14s} {'IC':>8s} {'IR':>7s} {'t':>6s} {'%pos':>5s} {'IC_orthSize':>12s} {'IR_o':>7s} {'nYr':>4s}")
    for f in factors:
        arr = np.array([(a, b, n) for a, b, n in rows_ic[f]])
        if len(arr) == 0:
            print(f"{f:14s} no data"); continue
        ic, ico = arr[:, 0], arr[:, 1]
        ir = ic.mean() / ic.std() if ic.std() else 0
        iro = ico.mean() / ico.std() if ico.std() else 0
        print(f"{f:14s} {ic.mean():+.4f} {ir:+.3f} {ir*np.sqrt(len(ic)):+6.2f} "
              f"{(ic>0).mean()*100:4.0f}% {ico.mean():+.4f}    {iro:+.3f} {len(arr):4d}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
