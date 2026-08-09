"""db/sync_job_record.py list_recent_failures branches (mocked DB)."""

from __future__ import annotations

from unittest.mock import Mock

from data_sync_service.db import sync_job_record as sjr


def _fake_conn(cur: Mock) -> Mock:
    conn = Mock()
    conn.cursor.return_value = cur
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    cur.__enter__ = Mock(return_value=cur)
    cur.__exit__ = Mock(return_value=False)
    return conn


class TestListRecentFailures:
    def test_clamps_hours(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = []
        monkeypatch.setattr(sjr, "get_connection", lambda: _fake_conn(cur))
        sjr.list_recent_failures(hours=999)
        sql, params = cur.execute.call_args_list[-1][0]
        assert params == (168,)

    def test_hours_floor(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = []
        monkeypatch.setattr(sjr, "get_connection", lambda: _fake_conn(cur))
        sjr.list_recent_failures(hours=0)
        sql, params = cur.execute.call_args_list[-1][0]
        assert params == (1,)

    def test_empty(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = []
        monkeypatch.setattr(sjr, "get_connection", lambda: _fake_conn(cur))
        assert sjr.list_recent_failures() == []

    def test_iso_timestamps(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = [
            (1, "stock_close_sync", "2026-08-07T09:00:00+00:00", False, "CN:600519", "boom"),
        ]
        monkeypatch.setattr(sjr, "get_connection", lambda: _fake_conn(cur))
        out = sjr.list_recent_failures(hours=24)
        assert out[0]["job_type"] == "stock_close_sync"
        assert out[0]["sync_at"] == "2026-08-07T09:00:00+00:00"
        assert out[0]["last_ts_code"] == "CN:600519"
        assert out[0]["error_message"] == "boom"
        assert out[0]["success"] is False
