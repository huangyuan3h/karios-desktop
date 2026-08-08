"""db/industry_mainline_metrics.py remaining branches (mocked DB)."""

from __future__ import annotations

from unittest.mock import Mock

from data_sync_service.db import industry_mainline_metrics as imm


def _fake_conn(cur: Mock) -> Mock:
    conn = Mock()
    conn.cursor.return_value = cur
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    cur.__enter__ = Mock(return_value=cur)
    cur.__exit__ = Mock(return_value=False)
    return conn


class TestUpsert:
    def test_empty(self, monkeypatch) -> None:
        cur = Mock()
        monkeypatch.setattr(imm, "get_connection", lambda: _fake_conn(cur))
        assert imm.upsert_daily_rows([]) == 0
        cur.executemany.assert_not_called()

    def test_filters_falsy(self, monkeypatch) -> None:
        cur = Mock()
        monkeypatch.setattr(imm, "get_connection", lambda: _fake_conn(cur))
        assert imm.upsert_daily_rows([None, {}, {"date": "2026-08-07", "industry_name": "AI"}]) == 1
        args = cur.executemany.call_args_list[-1][0][1]
        assert len(args) == 1
        assert args[0][0] == "2026-08-07"
        assert args[0][1] == "AI"
        assert isinstance(args[0][10].obj, dict)

    def test_raw_not_dict_wrapped(self, monkeypatch) -> None:
        cur = Mock()
        monkeypatch.setattr(imm, "get_connection", lambda: _fake_conn(cur))
        imm.upsert_daily_rows([{"date": "d", "industry_name": "n", "raw": "plain"}])
        args = cur.executemany.call_args_list[-1][0][1]
        assert args[0][10].obj == {"raw": "plain"}


class TestListRowsByDate:
    def test_ok(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = [
            ("AI", 5, 2, 1, 3, 0.6, 12.5, 1.2, "2026-08-07T00:00:00+00:00", {"a": 1}),
        ]
        monkeypatch.setattr(imm, "get_connection", lambda: _fake_conn(cur))
        out = imm.list_rows_by_date("2026-08-07")
        assert out[0]["industry_name"] == "AI"
        assert out[0]["raw"] == {"a": 1}
        assert out[0]["surge_ratio"] == 0.6

    def test_raw_json_str(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = [
            ("AI", 0, None, None, None, None, None, None, None, '{"a": 2}'),
        ]
        monkeypatch.setattr(imm, "get_connection", lambda: _fake_conn(cur))
        out = imm.list_rows_by_date("2026-08-07")
        assert out[0]["raw"] == {"a": 2}
        assert out[0]["total_count"] == 0

    def test_empty(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = []
        monkeypatch.setattr(imm, "get_connection", lambda: _fake_conn(cur))
        assert imm.list_rows_by_date("2026-08-07") == []


class TestListRowsForDates:
    def test_empty_dates(self) -> None:
        assert imm.list_rows_for_dates([]) == []

    def test_ok(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = [
            ("2026-08-07", "AI", 5, 2, 1, 3, 0.6, 12.5, 1.2, "t", {"a": 1}),
        ]
        monkeypatch.setattr(imm, "get_connection", lambda: _fake_conn(cur))
        out = imm.list_rows_for_dates(["2026-08-07"])
        assert out[0]["date"] == "2026-08-07"
        assert out[0]["raw"] == {"a": 1}
        sql, params = cur.execute.call_args_list[-1][0]
        assert "ANY(%s)" in sql
        assert params == (["2026-08-07"],)

    def test_raw_str(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = [
            ("2026-08-07", "AI", 5, 2, 1, 3, 0.6, 12.5, 1.2, "t", '{"b": 2}'),
        ]
        monkeypatch.setattr(imm, "get_connection", lambda: _fake_conn(cur))
        out = imm.list_rows_for_dates(["2026-08-07"])
        assert out[0]["raw"] == {"b": 2}


class TestGetDatesUpto:
    def test_clamp_and_reverse(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = [("2026-08-07",), ("2026-08-06",), ("2026-08-05",), None]
        monkeypatch.setattr(imm, "get_connection", lambda: _fake_conn(cur))
        out = imm.get_dates_upto("2026-08-07", 99)
        assert out == ["2026-08-05", "2026-08-06", "2026-08-07"]
        sql, params = cur.execute.call_args_list[-1][0]
        assert params == ("2026-08-07", 60)

    def test_empty(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = []
        monkeypatch.setattr(imm, "get_connection", lambda: _fake_conn(cur))
        assert imm.get_dates_upto("2026-08-07", 5) == []
