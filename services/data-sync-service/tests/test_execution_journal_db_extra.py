"""db/execution_journal.py remaining branches (mocked DB)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import Mock

from data_sync_service.db import execution_journal as ej


def _fake_conn(cur: Mock) -> Mock:
    conn = Mock()
    conn.cursor.return_value = cur
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    cur.__enter__ = Mock(return_value=cur)
    cur.__exit__ = Mock(return_value=False)
    return conn


class TestHelpers:
    def test_iso(self) -> None:
        assert ej._iso(None) is None
        assert ej._iso(datetime(2026, 8, 7, 10, 30, tzinfo=UTC)).startswith("2026-08-07T10:30")
        assert ej._iso(date(2026, 8, 7)) == "2026-08-07"
        assert ej._iso("raw") == "raw"

    def test_parse_jsonb(self) -> None:
        assert ej._parse_jsonb(None) is None
        assert ej._parse_jsonb({"a": 1}) == {"a": 1}
        assert ej._parse_jsonb([1, 2]) == [1, 2]
        assert ej._parse_jsonb('{"a": 1}') == {"a": 1}
        assert ej._parse_jsonb("not json") == "not json"
        assert ej._parse_jsonb(42) == 42

    def test_snapshot_row_none(self) -> None:
        assert ej._snapshot_row(None) == {}

    def test_change_row_none(self) -> None:
        assert ej._change_row(None) == {}


class TestInsertChanges:
    def test_empty_returns_empty(self, monkeypatch) -> None:
        cur = Mock()
        monkeypatch.setattr(ej, "get_connection", lambda: _fake_conn(cur))
        assert ej.insert_changes([]) == []
        sqls = [c.args[0] for c in cur.execute.call_args_list]
        assert not any("INSERT INTO" in s for s in sqls)

    def test_inserts_rows(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = ("c1", "2026-08-07", None, "s1", "s2", "scope", "symbol", "field", "o", "n", "src")
        monkeypatch.setattr(ej, "get_connection", lambda: _fake_conn(cur))
        rows = [{"id": "c1", "trade_date": "2026-08-07", "from_snapshot_id": "s1", "to_snapshot_id": "s2", "scope": "watchlist", "symbol": "CN:600519", "field": "positionPct", "old_value": "o", "new_value": "n", "source": "alpha"}]
        out = ej.insert_changes(rows)
        assert out[0]["id"] == "c1"
        sqls = [c.args[0] for c in cur.execute.call_args_list]
        assert any("INSERT INTO" in s for s in sqls)


class TestListSnapshots:
    def test_with_trade_date(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = [("sn1", "2026-08-07", None, "sync_all", '{"g":1}', '[{"c":1}]', "h", None)]
        monkeypatch.setattr(ej, "get_connection", lambda: _fake_conn(cur))
        out = ej.list_snapshots(trade_date="2026-08-07")
        assert out[0]["id"] == "sn1"
        assert out[0]["gate"] == {"g": 1}
        assert out[0]["cards"] == [{"c": 1}]
        sql = cur.execute.call_args_list[-1][0][0]
        assert "WHERE trade_date = %s::date" in sql

    def test_without_trade_date(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = []
        monkeypatch.setattr(ej, "get_connection", lambda: _fake_conn(cur))
        assert ej.list_snapshots() == []
        sql = cur.execute.call_args_list[-1][0][0]
        assert "WHERE trade_date" not in sql

    def test_limit_clamped(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = []
        monkeypatch.setattr(ej, "get_connection", lambda: _fake_conn(cur))
        ej.list_snapshots(limit=9999)
        assert cur.execute.call_args_list[-1][0][1] == (200,)


class TestHasSourceOnDate:
    def test_no_sources_false(self, monkeypatch) -> None:
        cur = Mock()
        monkeypatch.setattr(ej, "get_connection", lambda: _fake_conn(cur))
        assert ej.has_source_on_date("2026-08-07", []) is False
        cur.execute.assert_not_called()

    def test_found(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = (1,)
        monkeypatch.setattr(ej, "get_connection", lambda: _fake_conn(cur))
        assert ej.has_source_on_date("2026-08-07", ["sync_all"]) is True

    def test_not_found(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = None
        monkeypatch.setattr(ej, "get_connection", lambda: _fake_conn(cur))
        assert ej.has_source_on_date("2026-08-07", ["sync_all"]) is False


class TestListChanges:
    def test_with_since(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = [("c1", "2026-08-07", None, "s1", "s2", "scope", "symbol", "field", "o", "n", "src")]
        monkeypatch.setattr(ej, "get_connection", lambda: _fake_conn(cur))
        out = ej.list_changes(since="2026-08-07T00:00:00+00:00")
        assert out[0]["id"] == "c1"
        sql = cur.execute.call_args_list[-1][0][0]
        assert "changed_at >= %s::timestamptz" in sql

    def test_no_filters(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = []
        monkeypatch.setattr(ej, "get_connection", lambda: _fake_conn(cur))
        ej.list_changes()
        sql = cur.execute.call_args_list[-1][0][0]
        assert "WHERE" not in sql
