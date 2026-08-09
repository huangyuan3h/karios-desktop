"""fund_basic service coverage (ETF list sync from tushare)."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from data_sync_service.service import fund_basic as fb


class _Settings:
    tu_share_api_key = "TEST_KEY"


class _Pro:
    def __init__(self, df=None) -> None:
        self.df = df
        self.calls = []

    def fund_basic(self, **kw):
        self.calls.append(kw)
        return self.df if self.df is not None else pd.DataFrame()


def _patch(monkeypatch, pro=None, last_ok=None):
    if pro is None:
        pro = _Pro()
    monkeypatch.setattr(fb, "get_settings", lambda: _Settings())
    monkeypatch.setattr(fb, "ts", type("ts", (), {"pro_api": staticmethod(lambda k: pro)}))
    monkeypatch.setattr(fb, "get_last_success", lambda jt: last_ok)
    monkeypatch.setattr(fb, "upsert_from_dataframe", lambda df: len(df))
    monkeypatch.setattr(fb, "insert_record", lambda **kw: None)
    return pro


def _etf_df(n=2):
    return pd.DataFrame({
        "ts_code": ["510300.SH", "159915.SZ"],
        "name": ["沪深300ETF", "创业板ETF"],
        "fund_type": ["股票型", "股票型"],
        "list_date": ["2012-05-28", None],
        "delist_date": [None, "20260101"],
    })[:n]


def test_parse_iso_datetime() -> None:
    d = datetime(2026, 8, 1, tzinfo=UTC)
    assert fb._parse_iso_datetime(d) is d
    assert fb._parse_iso_datetime("2026-08-01T00:00:00+00:00") == d
    assert fb._parse_iso_datetime("garbage") is None
    assert fb._parse_iso_datetime(None) is None
    assert fb._parse_iso_datetime("") is None


def test_is_same_utc_month() -> None:
    a = datetime(2026, 8, 1, tzinfo=UTC)
    b = datetime(2026, 8, 31, 23, 59, tzinfo=UTC)
    c = datetime(2026, 9, 1, tzinfo=UTC)
    assert fb._is_same_utc_month(a, b) is True
    assert fb._is_same_utc_month(a, c) is False
    d = datetime(2026, 8, 1, 8, 0, tzinfo=UTC)
    e = datetime(2026, 8, 1, 1, 0, tzinfo=UTC)
    assert fb._is_same_utc_month(d, e) is True


def test_map_etf_basic_to_stock_basic_df() -> None:
    out = fb.map_etf_basic_to_stock_basic_df(_etf_df())
    assert list(out.columns) == ["ts_code", "symbol", "name", "industry", "market", "list_date", "delist_date"]
    assert out["symbol"].tolist() == ["510300", "159915"]
    assert out["market"].tolist() == ["ETF", "ETF"]
    assert out["list_date"].tolist()[0] == "2012-05-28"
    assert pd.isna(out["list_date"].tolist()[1])
    assert pd.isna(out["delist_date"].tolist()[0])
    assert out["delist_date"].tolist()[1] == "2026-01-01"
    assert out["industry"].tolist() == ["股票型", "股票型"]


def test_map_etf_basic_empty() -> None:
    out = fb.map_etf_basic_to_stock_basic_df(pd.DataFrame())
    assert out.empty and "ts_code" in out.columns
    out2 = fb.map_etf_basic_to_stock_basic_df(None)
    assert out2.empty


def test_map_etf_basic_missing_columns() -> None:
    df = pd.DataFrame({"ts_code": ["510300.SH"]})
    out = fb.map_etf_basic_to_stock_basic_df(df)
    assert out["symbol"].tolist() == ["510300"]
    assert out["name"].tolist() == [None]


def test_map_etf_basic_bad_cells() -> None:
    df = pd.DataFrame({
        "ts_code": [None, " ", 12345],
        "name": ["x", "y", "z"],
        "fund_type": [None, " ", "股票型"],
        "list_date": ["20260101", "not-a-date", None],
    })
    out = fb.map_etf_basic_to_stock_basic_df(df)
    assert pd.isna(out["symbol"].tolist()[0]) and pd.isna(out["symbol"].tolist()[1])
    assert out["symbol"].tolist()[2] == "12345"
    assert out["list_date"].tolist()[0] == "2026-01-01"
    assert out["list_date"].tolist()[1] == "not-a-date"
    assert pd.isna(out["list_date"].tolist()[2])


def test_sync_skips_same_month(monkeypatch) -> None:
    _patch(monkeypatch, last_ok={"sync_at": "2026-08-01T00:00:00+00:00"})
    out = fb.sync_etf_fund_basic()
    assert out["skipped"] is True and "this month" in out["message"]


def test_sync_force_overrides_month_skip(monkeypatch) -> None:
    pro = _patch(monkeypatch, last_ok={"sync_at": "2026-08-01T00:00:00+00:00"})
    pro.df = _etf_df()
    out = fb.sync_etf_fund_basic(force=True)
    assert out["ok"] is True and out["updated"] == 2


def test_sync_bad_list_status(monkeypatch) -> None:
    _patch(monkeypatch)
    out = fb.sync_etf_fund_basic(list_status="X")
    assert out["ok"] is False and "list_status" in out["error"]


def test_sync_empty_last_ok(monkeypatch) -> None:
    pro = _patch(monkeypatch, last_ok=None)
    pro.df = _etf_df()
    out = fb.sync_etf_fund_basic()
    assert out["ok"] is True and out["updated"] == 2
    assert pro.calls[0]["status"] == "L" and pro.calls[0]["market"] == "E"


def test_sync_missing_api_key(monkeypatch) -> None:
    _patch(monkeypatch)
    monkeypatch.setattr(fb, "get_settings", lambda: type("S", (), {"tu_share_api_key": ""})())
    out = fb.sync_etf_fund_basic()
    assert out["ok"] is False and "API_KEY" in out["error"]


def test_sync_no_data_records_success(monkeypatch) -> None:
    pro = _patch(monkeypatch, last_ok=None)
    pro.df = pd.DataFrame()
    rec = {}
    monkeypatch.setattr(fb, "insert_record", lambda **kw: rec.update(kw))
    out = fb.sync_etf_fund_basic()
    assert out["ok"] is True and out["message"] == "no data from tushare"
    assert rec["success"] is True


def test_sync_success_records(monkeypatch) -> None:
    pro = _patch(monkeypatch, last_ok=None)
    pro.df = _etf_df()
    rec = {}
    monkeypatch.setattr(fb, "insert_record", lambda **kw: rec.update(kw))
    out = fb.sync_etf_fund_basic(list_status="d")
    assert out["ok"] is True and out["list_status"] == "D"
    assert rec["success"] is True and rec["job_type"] == fb.JOB_TYPE


def test_sync_failure_records(monkeypatch) -> None:
    class P:
        def fund_basic(self, **kw):
            raise ValueError("tushare down")

    rec = {}
    _patch(monkeypatch, pro=P(), last_ok=None)
    monkeypatch.setattr(fb, "insert_record", lambda **kw: rec.update(kw))
    out = fb.sync_etf_fund_basic()
    assert out["ok"] is False and out["error"] == "tushare down"
    assert rec["success"] is False


def test_get_etf_fund_basic_sync_status(monkeypatch) -> None:
    monkeypatch.setattr(fb, "get_last_success", lambda jt: None)
    assert fb.get_etf_fund_basic_sync_status()["last_success"] is None
    monkeypatch.setattr(fb, "get_last_success", lambda jt: {"sync_at": "x"})
    out = fb.get_etf_fund_basic_sync_status()
    assert out["last_success"] == {"sync_at": "x"} and out["job_type"] == fb.JOB_TYPE
