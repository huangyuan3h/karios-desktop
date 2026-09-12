"""fin_panel (P0-11) — PiT statement -> TTM factor panels, DB-free.

``_load``/panel functions are exercised through a fake ``read_sql`` seam; the
arithmetic (YTD single-quarter split, 4q TTM, industry medians, PiT ann_date)
is asserted directly so research-script regressions are caught.
"""

from __future__ import annotations

import pandas as pd
import pytest

from data_sync_service.service import fin_panel as fp

_QUARTERS = ["2020-03-31", "2020-06-30", "2020-09-30", "2020-12-31", "2021-03-31"]
_ANNOUNCE = ["2020-04-30", "2020-08-30", "2020-10-30", "2021-04-30", "2021-04-30"]


def _income() -> pd.DataFrame:
    # YTD cumulative per fiscal year (Q4 = FY, next Q1 resets).
    series = {
        "A.SH": {
            "n_income_attr_p": [10.0, 25.0, 40.0, 60.0, 15.0],
            "total_revenue": [100.0, 220.0, 330.0, 460.0, 120.0],
            "operate_profit": [12.0, 28.0, 45.0, 70.0, 18.0],
        },
        "B.SH": {
            "n_income_attr_p": [5.0, 12.0, 20.0, 30.0, 8.0],
            "total_revenue": [50.0, 110.0, 165.0, 230.0, 60.0],
            "operate_profit": [6.0, 14.0, 22.0, 35.0, 9.0],
        },
    }
    rows = []
    for ts, cols in series.items():
        for i, (q, ann) in enumerate(zip(_QUARTERS, _ANNOUNCE, strict=True)):
            row = {
                "ts_code": ts,
                "ann_date": ann,
                "end_date": q,
                "report_type": "1",
                "comp_type": "2" if ts == "A.SH" else "1",
            }
            for col, values in cols.items():
                row[col] = values[i]
            rows.append(row)
    return pd.DataFrame(rows)


def _balance() -> pd.DataFrame:
    rows = []
    for ts in ("A.SH", "B.SH"):
        for q, ann in zip(_QUARTERS, _ANNOUNCE, strict=True):
            rows.append(
                {
                    "ts_code": ts,
                    "ann_date": ann,
                    "end_date": q,
                    "report_type": "1",
                    "comp_type": "2" if ts == "A.SH" else "1",
                    "total_hldr_eqy_inc_min_int": 100.0,
                    "total_assets": 200.0,
                    "total_liab": 80.0,
                }
            )
    return pd.DataFrame(rows)


def _cashflow() -> pd.DataFrame:
    rows = []
    for ts in ("A.SH", "B.SH"):
        for i, (q, ann) in enumerate(zip(_QUARTERS, _ANNOUNCE, strict=True)):
            rows.append(
                {
                    "ts_code": ts,
                    "ann_date": ann,
                    "end_date": q,
                    "report_type": "1",
                    "comp_type": "2" if ts == "A.SH" else "1",
                    "n_cashflow_act": 8.0 * (i + 1),
                    "free_cashflow": 4.0 * (i + 1),
                }
            )
    return pd.DataFrame(rows)


class _DummyConn:
    def __enter__(self) -> _DummyConn:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False


def _patch(monkeypatch: pytest.MonkeyPatch) -> None:
    income, balance, cashflow = _income(), _balance(), _cashflow()
    industry = pd.DataFrame({"ts_code": ["A.SH", "B.SH"], "industry": ["Bank", "Tech"]})
    cal = pd.DataFrame({"trade_date": pd.to_datetime(["2021-01-04", "2021-01-05"])})
    closes = pd.DataFrame(
        {
            "ts_code": ["A.SH", "A.SH"],
            "trade_date": pd.to_datetime(["2021-01-04", "2021-01-05"]),
            "close": [10.0, 11.0],
        }
    )
    mv = pd.DataFrame(
        {
            "ts_code": ["A.SH", "A.SH"],
            "trade_date": pd.to_datetime(["2021-01-04", "2021-01-05"]),
            "circ_mv": [1000.0, 1100.0],
        }
    )

    def fake_read_sql(sql, con, params=None, parse_dates=None):  # noqa: ANN001
        s = str(sql).lower()
        if "cn_income_stmt" in s:
            return income.copy()
        if "cn_balance_sheet" in s:
            return balance.copy()
        if "cn_cashflow_stmt" in s:
            return cashflow.copy()
        if "stock_basic" in s:
            return industry.copy()
        if "from daily" in s and "distinct" in s:
            return cal.copy()
        if "from daily" in s:
            return closes.copy()
        if "stock_dailybasic" in s:
            return mv.copy()
        raise AssertionError(f"unexpected SQL: {sql}")

    monkeypatch.setattr(fp, "get_connection", lambda: _DummyConn())
    monkeypatch.setattr(fp.pd, "read_sql", fake_read_sql)


def _single_quarter_frame() -> pd.DataFrame:
    df = _income()[_income()["ts_code"] == "A.SH"].copy()
    df["end_date"] = pd.to_datetime(df["end_date"])
    return df.sort_values(["ts_code", "end_date"]).reset_index(drop=True)


def test_single_quarter_splits_ytd() -> None:
    out = fp._single_quarter(_single_quarter_frame(), ("n_income_attr_p",))
    sq = out["n_income_attr_p_sq"].tolist()
    # YTD 10/25/40/60/15 -> single-quarter 10/15/15/20/15
    assert sq == pytest.approx([10.0, 15.0, 15.0, 20.0, 15.0])


def test_ttm_rolls_four_quarters() -> None:
    sq = fp._single_quarter(_single_quarter_frame(), ("n_income_attr_p",))
    out = fp._ttm(sq, ("n_income_attr_p_sq",))
    ttm = out["n_income_attr_p_sq_ttm"].tolist()
    assert pd.isna(ttm[0]) and pd.isna(ttm[2])
    assert ttm[3] == pytest.approx(10.0 + 15.0 + 15.0 + 20.0)
    assert ttm[4] == pytest.approx(15.0 + 15.0 + 20.0 + 15.0)


def test_roe_ttm_panel(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch)
    out = fp.roe_ttm_panel()
    assert {"roe_ttm", "industry", "ind_med", "above_ind", "is_fin"} <= set(out.columns)
    assert set(out["industry"]) == {"Bank", "Tech"}
    # comp_type '2' -> is_fin True (A), '1' -> is_fin False (B).
    assert set(out["is_fin"]) == {True, False}
    assert (out["end_date"] <= pd.Timestamp("2021-03-31")).all()


def test_cashconv_panel_computes_ccr_and_accruals(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch)
    out = fp.cashconv_panel()
    assert {"ccr", "accruals"} <= set(out.columns)
    assert out["accruals"].notna().any()
    assert (out["ccr"].dropna() >= 0).all()


def test_leverage_panel(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch)
    out = fp.leverage_panel()
    assert {"debt_ratio", "lev_safe", "ind_med", "below_ind"} <= set(out.columns)
    assert out["debt_ratio"].iloc[0] == pytest.approx(80.0 / 200.0)


def test_value_panel(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch)
    out = fp.value_panel()
    assert {"n_income_attr_p_sq_ttm", "free_cashflow_sq_ttm", "total_hldr_eqy_inc_min_int"} <= set(
        out.columns
    )


def test_ni_sq_panel(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch)
    out = fp.ni_sq_panel()
    assert "n_income_attr_p_sq" in out.columns
    assert out["comp_type"].isin(["1", "2"]).all()


def test_load_keeps_latest_announcement(monkeypatch: pytest.MonkeyPatch) -> None:
    """Corrections win: same (ts, end_date), later ann_date is kept."""
    dup = _income()
    extra = dup.iloc[[0]].copy()
    extra["ann_date"] = "2020-05-31"  # later correction
    extra["n_income_attr_p"] = 999.0
    frame = pd.concat([dup, extra], ignore_index=True)

    monkeypatch.setattr(fp, "get_connection", lambda: _DummyConn())
    monkeypatch.setattr(fp.pd, "read_sql", lambda *a, **k: frame.copy())
    out = fp._load("cn_income_stmt", ("n_income_attr_p",))
    row = out[(out["ts_code"] == "A.SH") & (out["end_date"] == pd.Timestamp("2020-03-31"))]
    assert len(row) == 1
    assert float(row["n_income_attr_p"].iloc[0]) == 999.0


def test_load_trade_calendar_and_maps(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch)
    cal, idx = fp.load_trade_calendar()
    assert cal == ["2021-01-04", "2021-01-05"]
    assert idx["2021-01-05"] == 1
    assert fp.load_price_map() == {"A.SH": {"2021-01-04": 10.0, "2021-01-05": 11.0}}
    assert fp.load_mv_map() == {"A.SH": {"2021-01-04": 1000.0, "2021-01-05": 1100.0}}


def test_load_mv_map_rejects_unknown_column() -> None:
    with pytest.raises(AssertionError):
        fp.load_mv_map("bad_col")


def test_shift_return_forward_and_guards() -> None:
    px = {"A.SH": {"2021-01-04": 100.0, "2021-01-08": 110.0}}
    cal = ["2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07", "2021-01-08"]
    idx = {d: i for i, d in enumerate(cal)}
    assert fp.shift_return(px, cal, idx, "A.SH", "2021-01-04", 4) == pytest.approx(10.0)
    assert fp.shift_return(px, cal, idx, "A.SH", "2021-01-04", -1) is None
    assert fp.shift_return(px, cal, idx, "A.SH", "2021-01-08", 1) is None
    assert fp.shift_return(px, cal, idx, "Z.SH", "2021-01-04", 1) is None
    assert fp.shift_return(px, cal, idx, "A.SH", "1999-01-01", 1) is None


def test_mv_asof_latest_on_or_before() -> None:
    mv = {"A.SH": {"2021-01-04": 10.0, "2021-01-08": 20.0}}
    cal = ["2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07", "2021-01-08"]
    idx = {d: i for i, d in enumerate(cal)}
    assert fp.mv_asof(mv, cal, idx, "A.SH", "2021-01-06", lookback=10) == 10.0
    assert fp.mv_asof(mv, cal, idx, "A.SH", "2021-01-08", lookback=10) == 20.0
    assert fp.mv_asof(mv, cal, idx, "A.SH", "1999-01-01") is None
    assert fp.mv_asof(mv, cal, idx, "Z.SH", "2021-01-04") is None
