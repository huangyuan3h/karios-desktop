"""db/trade_calendar.py + service/trade_calendar.py coverage (mocked DB)."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd

from data_sync_service.db import trade_calendar as tc
from data_sync_service.service import trade_calendar as tc_svc


def _fake_conn(cur: Mock) -> Mock:
    conn = Mock()
    conn.cursor.return_value = cur
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    cur.__enter__ = Mock(return_value=cur)
    cur.__exit__ = Mock(return_value=False)
    return conn


class TestDateStr:
    def test_pd_na(self) -> None:
        assert tc._date_str(pd.NA) is None

    def test_short_non_digit(self) -> None:
        assert tc._date_str("12") == "12"


class TestUpsert:
    def test_skips_bad_dates(self, monkeypatch) -> None:
        cur = Mock()
        conn = _fake_conn(cur)
        monkeypatch.setattr(tc, "get_connection", lambda: conn)
        df = pd.DataFrame(
            [
                {"exchange": "SSE", "cal_date": "20240101", "pretrade_date": "20231229", "is_open": 1},
                {"exchange": None, "cal_date": pd.NA, "pretrade_date": pd.NA, "is_open": 0},
                {"exchange": "SSE", "cal_date": pd.NaT, "pretrade_date": None, "is_open": 0},
                {"exchange": " SZSE ", "cal_date": "2024-01-02", "pretrade_date": "2023-12-29", "is_open": 0},
            ]
        )
        assert tc.upsert_from_dataframe(df) == 2
        assert cur.executemany.call_count == 1
        args = cur.executemany.call_args[0][1]
        assert args[0] == ("SSE", "2024-01-01", 1, "2023-12-29")
        assert args[1][0] == "SZSE" and args[1][3] == "2023-12-29"

    def test_no_rows(self, monkeypatch) -> None:
        cur = Mock()
        conn = _fake_conn(cur)
        monkeypatch.setattr(tc, "get_connection", lambda: conn)
        assert tc.upsert_from_dataframe(pd.DataFrame([{"cal_date": pd.NA}])) == 0
        cur.executemany.assert_not_called()


class TestQueries:
    def test_is_trading_day_found(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = (1,)
        monkeypatch.setattr(tc, "get_connection", lambda: _fake_conn(cur))
        assert tc.is_trading_day("SSE", date(2024, 1, 2)) is True

    def test_is_trading_day_closed(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = (0,)
        monkeypatch.setattr(tc, "get_connection", lambda: _fake_conn(cur))
        assert tc.is_trading_day("SSE", date(2024, 1, 2)) is False

    def test_is_trading_day_missing(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = None
        monkeypatch.setattr(tc, "get_connection", lambda: _fake_conn(cur))
        assert tc.is_trading_day("SSE", date(2024, 1, 2)) is None

    def test_get_open_dates(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = [(date(2024, 1, 2),), (date(2024, 1, 3),), (None,)]
        monkeypatch.setattr(tc, "get_connection", lambda: _fake_conn(cur))
        out = tc.get_open_dates("SSE", date(2024, 1, 1), date(2024, 1, 31))
        assert out == [date(2024, 1, 2), date(2024, 1, 3)]

    def test_get_latest_date(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = (date(2024, 1, 2),)
        monkeypatch.setattr(tc, "get_connection", lambda: _fake_conn(cur))
        assert tc.get_latest_calendar_date("SSE") == date(2024, 1, 2)

    def test_get_latest_date_none(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = (None,)
        monkeypatch.setattr(tc, "get_connection", lambda: _fake_conn(cur))
        assert tc.get_latest_calendar_date("SSE") is None

    def test_summary(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = (10, 7)
        monkeypatch.setattr(tc, "get_connection", lambda: _fake_conn(cur))
        out = tc.summary("SSE", date(2024, 1, 1), date(2024, 1, 31))
        assert out == {"exchange": "SSE", "start_date": "2024-01-01", "end_date": "2024-01-31", "rows": 10, "open_days": 7}

    def test_summary_none(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = None
        monkeypatch.setattr(tc, "get_connection", lambda: _fake_conn(cur))
        out = tc.summary("SSE", date(2024, 1, 1), date(2024, 1, 31))
        assert out["rows"] == 0 and out["open_days"] == 0


class TestService:
    def test_missing_api_key(self, monkeypatch) -> None:
        monkeypatch.setattr(tc_svc, "get_settings", lambda: SimpleNamespace(tu_share_api_key=None))
        assert tc_svc.sync_trade_calendar()["ok"] is False

    def test_single_page(self, monkeypatch) -> None:
        monkeypatch.setattr(tc_svc, "get_settings", lambda: SimpleNamespace(tu_share_api_key="k"))
        pro = SimpleNamespace(trade_cal=lambda **kw: pd.DataFrame([{"cal_date": "20240101"}]))
        monkeypatch.setattr(tc_svc.ts, "pro_api", lambda key: pro)
        monkeypatch.setattr(tc_svc, "upsert_from_dataframe", lambda df: len(df))
        monkeypatch.setattr(tc_svc, "cal_summary", lambda **kw: {"rows": 1})
        out = tc_svc.sync_trade_calendar()
        assert out["ok"] is True and out["updated"] == 1 and out["summary"] == {"rows": 1}

    def test_pagination(self, monkeypatch) -> None:
        monkeypatch.setattr(tc_svc, "get_settings", lambda: SimpleNamespace(tu_share_api_key="k"))
        page = pd.DataFrame([{"cal_date": f"20240{i}01"} for i in range(1, 5000)])
        empty = pd.DataFrame()
        pro = SimpleNamespace(trade_cal=lambda **kw: page if kw["offset"] == 0 else empty)
        monkeypatch.setattr(tc_svc.ts, "pro_api", lambda key: pro)
        monkeypatch.setattr(tc_svc, "upsert_from_dataframe", lambda df: len(df))
        monkeypatch.setattr(tc_svc, "cal_summary", lambda **kw: {"rows": 4999})
        out = tc_svc.sync_trade_calendar()
        assert out["updated"] == 4999

    def test_empty_first_page(self, monkeypatch) -> None:
        monkeypatch.setattr(tc_svc, "get_settings", lambda: SimpleNamespace(tu_share_api_key="k"))
        pro = SimpleNamespace(trade_cal=lambda **kw: pd.DataFrame())
        monkeypatch.setattr(tc_svc.ts, "pro_api", lambda key: pro)
        monkeypatch.setattr(tc_svc, "upsert_from_dataframe", lambda df: len(df))
        monkeypatch.setattr(tc_svc, "cal_summary", lambda **kw: {"rows": 0})
        out = tc_svc.sync_trade_calendar()
        assert out["ok"] is True and out["updated"] == 0
