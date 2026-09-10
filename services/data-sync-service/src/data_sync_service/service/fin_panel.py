"""PiT financial panel builder — quarterly statements to TTM factors.

Shared by F1..F4 diagnostics (P0-11). Conventions:
- PiT key is ann_date: a row is knowable only on/after ann_date.
- tushare income/cashflow are YTD cumulative -> single-quarter split per
  fiscal year, then 4-quarter rolling TTM.
- Same (ts_code, end_date): keep the latest ann_date (corrections win).
- Financials (comp_type != '1') are flagged, not dropped — callers exclude.
"""

from __future__ import annotations

import pandas as pd

from data_sync_service.db import get_connection
INCOME_COLS = ("n_income_attr_p", "total_revenue", "operate_profit")
BALANCE_COLS = ("total_hldr_eqy_inc_min_int", "total_assets", "total_liab")
CASHFLOW_COLS = ("n_cashflow_act",)


def _load(table: str, cols: tuple[str, ...], end_from: str = "2018-01-01") -> pd.DataFrame:
    use = ", ".join(["ts_code", "ann_date", "end_date", "report_type", "comp_type", *cols])
    with get_connection() as conn:
        df = pd.read_sql(
            f"SELECT {use} FROM {table} "
            "WHERE end_date >= %s AND report_type = '1'",
            conn,
            params=(end_from,),
        )
    df["ann_date"] = pd.to_datetime(df["ann_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])
    df = df.sort_values(["ts_code", "end_date", "ann_date"])
    # corrections win: latest ann_date per (stock, period)
    df = df.drop_duplicates(["ts_code", "end_date"], keep="last")
    return df


def _single_quarter(df: pd.DataFrame, cols: tuple[str, ...]) -> pd.DataFrame:
    """YTD cumulative -> single-quarter values (Q4 = FY - Q3)."""
    df = df.copy()
    df["yr"] = df["end_date"].dt.year
    for c in cols:
        prev = df.groupby(["ts_code", "yr"])[c].shift(1)
        q1_mask = df["end_date"].dt.month == 3
        df[c + "_sq"] = (df[c] - prev).where(~q1_mask, df[c])
    return df


def _ttm(df: pd.DataFrame, sq_cols: tuple[str, ...]) -> pd.DataFrame:
    df = df.sort_values(["ts_code", "end_date"])
    for c in sq_cols:
        df[c + "_ttm"] = (
            df.groupby("ts_code")[c].transform(lambda s: s.rolling(4, min_periods=4).sum())
        )
    return df


def roe_ttm_panel(end_from: str = "2018-01-01") -> pd.DataFrame:
    """One row per (ts_code, end_date): roe_ttm + industry + median + flag."""
    inc = _ttm(_single_quarter(_load("cn_income_stmt", INCOME_COLS, end_from), INCOME_COLS),
               tuple(c + "_sq" for c in INCOME_COLS))
    bal = _load("cn_balance_sheet", BALANCE_COLS, end_from)
    eq = bal.sort_values(["ts_code", "end_date"]).copy()
    eq["eq_avg4"] = eq.groupby("ts_code")["total_hldr_eqy_inc_min_int"].transform(
        lambda s: s.rolling(4, min_periods=4).mean()
    )
    m = inc.merge(
        eq[["ts_code", "end_date", "eq_avg4"]],
        on=["ts_code", "end_date"],
        how="inner",
    )
    m["roe_ttm"] = m["n_income_attr_p_sq_ttm"] / m["eq_avg4"]
    m = m[(m["eq_avg4"] > 0)].copy()
    with get_connection() as conn:
        ind = pd.read_sql(
            "SELECT ts_code, industry FROM stock_basic", conn
        )
    m = m.merge(ind, on="ts_code", how="left")
    m["ind_med"] = m.groupby(["end_date", "industry"])["roe_ttm"].transform("median")
    m["above_ind"] = m["roe_ttm"] > m["ind_med"]
    m["is_fin"] = m["comp_type"] != "1"
    return m[[
        "ts_code", "end_date", "ann_date", "roe_ttm", "industry", "ind_med",
        "above_ind", "is_fin", "n_income_attr_p_sq_ttm", "eq_avg4",
    ]].sort_values(["ann_date", "ts_code"]).reset_index(drop=True)


def cashconv_panel(end_from: str = "2018-01-01") -> pd.DataFrame:
    """Cash conversion quality: CFO-TTM / NI-TTM + accruals.

    Primary metric ``ccr`` requires positive NI-TTM (ratio meaningless on
    losses — coverage loss is reported, not hidden). ``accruals`` =
    (NI_sq_ttm - CFO_sq_ttm) / avg assets works for all (Sloan-style).
    """
    inc = _ttm(_single_quarter(_load("cn_income_stmt", INCOME_COLS, end_from), INCOME_COLS),
               tuple(c + "_sq" for c in INCOME_COLS))
    cf = _ttm(_single_quarter(_load("cn_cashflow_stmt", CASHFLOW_COLS, end_from), CASHFLOW_COLS),
              tuple(c + "_sq" for c in CASHFLOW_COLS))
    bal = _load("cn_balance_sheet", BALANCE_COLS, end_from)
    assets = bal.sort_values(["ts_code", "end_date"]).copy()
    assets["assets_avg4"] = assets.groupby("ts_code")["total_assets"].transform(
        lambda s: s.rolling(4, min_periods=4).mean()
    )
    m = inc.merge(
        cf[["ts_code", "end_date", "n_cashflow_act_sq_ttm", "ann_date"]],
        on=["ts_code", "end_date"],
        how="inner",
        suffixes=("", "_cf"),
    )
    # knowable date = later of the two announcements (PiT)
    m["ann_date"] = pd.concat([m["ann_date"], m["ann_date_cf"]], axis=1).max(axis=1)
    m = m.merge(
        assets[["ts_code", "end_date", "assets_avg4"]],
        on=["ts_code", "end_date"],
        how="inner",
    )
    ni = m["n_income_attr_p_sq_ttm"]
    cfo = m["n_cashflow_act_sq_ttm"]
    m["ccr"] = (cfo / ni).where(ni > 0)
    m["accruals"] = (ni - cfo) / m["assets_avg4"]
    m = m[(m["assets_avg4"] > 0)].copy()
    with get_connection() as conn:
        ind = pd.read_sql("SELECT ts_code, industry FROM stock_basic", conn)
    m = m.merge(ind, on="ts_code", how="left")
    m["is_fin"] = m["comp_type"] != "1"
    return m[[
        "ts_code", "end_date", "ann_date", "ccr", "accruals", "industry",
        "is_fin", "n_income_attr_p_sq_ttm", "n_cashflow_act_sq_ttm",
    ]].sort_values(["ann_date", "ts_code"]).reset_index(drop=True)


def leverage_panel(end_from: str = "2018-01-01") -> pd.DataFrame:
    """Leverage safety: balance-sheet point-in-time (no TTM needed).

    Signal ``lev_safe`` = industry median debt ratio - own debt ratio
    (higher = safer). Debt ratio = total_liab / total_assets.
    """
    bal = _load("cn_balance_sheet", BALANCE_COLS, end_from)
    bal = bal[(bal["total_assets"] > 0)].copy()
    bal["debt_ratio"] = bal["total_liab"] / bal["total_assets"]
    with get_connection() as conn:
        ind = pd.read_sql("SELECT ts_code, industry FROM stock_basic", conn)
    bal = bal.merge(ind, on="ts_code", how="left")
    bal["ind_med"] = bal.groupby(["end_date", "industry"])["debt_ratio"].transform("median")
    bal["lev_safe"] = bal["ind_med"] - bal["debt_ratio"]
    bal["below_ind"] = bal["debt_ratio"] < bal["ind_med"]
    bal["is_fin"] = bal["comp_type"] != "1"
    return bal[[
        "ts_code", "end_date", "ann_date", "debt_ratio", "lev_safe", "industry",
        "ind_med", "below_ind", "is_fin",
    ]].sort_values(["ann_date", "ts_code"]).reset_index(drop=True)


def load_trade_calendar() -> tuple[list[str], dict[str, int]]:
    """All trade dates (daily table) + index map. A-share full since 2021."""
    with get_connection() as conn:
        cal = pd.read_sql(
            "SELECT DISTINCT trade_date FROM daily ORDER BY trade_date", conn,
            parse_dates=["trade_date"],
        )["trade_date"].dt.strftime("%Y-%m-%d").tolist()
    return cal, {d: i for i, d in enumerate(cal)}


def load_price_map() -> dict[str, dict[str, float]]:
    """{ts_code: {trade_date: close}} (qfq-adjusted closes)."""
    with get_connection() as conn:
        closes = pd.read_sql("SELECT ts_code, trade_date, close FROM daily", conn,
                             parse_dates=["trade_date"])
    closes["trade_date"] = closes["trade_date"].dt.strftime("%Y-%m-%d")
    px: dict[str, dict[str, float]] = {}
    for ts, d, c in zip(closes["ts_code"], closes["trade_date"], closes["close"]):
        if c and c > 0:
            px.setdefault(ts, {})[d] = float(c)
    return px


def shift_return(px: dict[str, dict[str, float]], cal: list[str],
                 idx_of: dict[str, int], ts: str, base: str,
                 horizon: int) -> float | None:
    """Forward (horizon>0) / backward (horizon<0) N-trading-day return %."""
    i = idx_of.get(base)
    if i is None or not 0 <= i + horizon < len(cal):
        return None
    days = px.get(ts)
    if not days:
        return None
    c0, c1 = days.get(cal[i]), days.get(cal[i + horizon])
    if not c0 or not c1:
        return None
    return (c1 / c0 - 1.0) * 100.0


def load_mv_map(col: str = "circ_mv") -> dict[str, dict[str, float]]:
    """{ts_code: {trade_date: market value}} from stock_dailybasic (2021+)."""
    assert col in ("circ_mv", "total_mv")
    with get_connection() as conn:
        df = pd.read_sql(
            f"SELECT ts_code, trade_date, {col} FROM stock_dailybasic", conn,
            parse_dates=["trade_date"],
        )
    df["trade_date"] = df["trade_date"].dt.strftime("%Y-%m-%d")
    mv: dict[str, dict[str, float]] = {}
    for ts, d, v in zip(df["ts_code"], df["trade_date"], df[col]):
        if v and v > 0:
            mv.setdefault(ts, {})[d] = float(v)
    return mv


def mv_asof(mv: dict[str, dict[str, float]], cal: list[str],
            idx_of: dict[str, int], ts: str, base: str) -> float | None:
    """Latest market value on/before base trading day."""
    i = idx_of.get(base)
    if i is None:
        return None
    days = mv.get(ts)
    if not days:
        return None
    for j in range(i, max(i - 10, -1), -1):
        v = days.get(cal[j])
        if v:
            return v
    return None
