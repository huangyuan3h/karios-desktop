"""db/journal.py remaining branches (mocked DB)."""

from __future__ import annotations

from unittest.mock import Mock

from data_sync_service.db import journal


def _fake_conn(cur: Mock) -> Mock:
    conn = Mock()
    conn.cursor.return_value = cur
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    cur.__enter__ = Mock(return_value=cur)
    cur.__exit__ = Mock(return_value=False)
    return conn


ROW = ("j1", "T", "md", "2026-08-01T08:00:00+00:00", "2026-08-07T08:00:00+00:00")


def _ok(cur: Mock) -> None:
    cur.fetchone.return_value = ROW


class TestFetchAll:
    def test_clamps(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = (3,)
        cur.fetchall.return_value = [ROW]
        monkeypatch.setattr(journal, "get_connection", lambda: _fake_conn(cur))
        total, items = journal.fetch_all(limit=500, offset=-2)
        assert total == 3
        assert items[0]["id"] == "j1"
        assert items[0]["contentMd"] == "md"
        sql, params = cur.execute.call_args_list[-1][0]
        assert params == (200, 0)
        assert "LIMIT %s OFFSET %s" in sql

    def test_empty(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = (0,)
        cur.fetchall.return_value = []
        monkeypatch.setattr(journal, "get_connection", lambda: _fake_conn(cur))
        total, items = journal.fetch_all()
        assert (total, items) == (0, [])


class TestFetchById:
    def test_blank_id(self) -> None:
        assert journal.fetch_by_id("  ") is None

    def test_not_found(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = None
        monkeypatch.setattr(journal, "get_connection", lambda: _fake_conn(cur))
        assert journal.fetch_by_id("nope") is None

    def test_found(self, monkeypatch) -> None:
        cur = Mock()
        _ok(cur)
        monkeypatch.setattr(journal, "get_connection", lambda: _fake_conn(cur))
        out = journal.fetch_by_id("j1")
        assert out["title"] == "T"
        assert out["createdAt"] == "2026-08-01T08:00:00+00:00"


class TestCreate:
    def test_ok(self, monkeypatch) -> None:
        cur = Mock()
        _ok(cur)
        monkeypatch.setattr(journal, "get_connection", lambda: _fake_conn(cur))
        out = journal.create_journal(
            journal_id="j1", title="T", content_md="md",
            created_at="2026-08-01T08:00:00+00:00", updated_at="2026-08-07T08:00:00+00:00",
        )
        assert out["id"] == "j1"


class TestUpdate:
    def test_not_found(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = None
        monkeypatch.setattr(journal, "get_connection", lambda: _fake_conn(cur))
        assert journal.update_journal(journal_id="nope", updated_at="x") is None

    def test_title_only(self, monkeypatch) -> None:
        cur = Mock()
        _ok(cur)
        monkeypatch.setattr(journal, "get_connection", lambda: _fake_conn(cur))
        journal.update_journal(journal_id="j1", title="T2", updated_at="2026-08-08T08:00:00+00:00")
        update_call = next(c for c in cur.execute.call_args_list if "UPDATE trade_journals" in c.args[0])
        assert update_call.args[1] == ("T2", "md", "2026-08-08T08:00:00+00:00", "j1")

    def test_content_only(self, monkeypatch) -> None:
        cur = Mock()
        _ok(cur)
        monkeypatch.setattr(journal, "get_connection", lambda: _fake_conn(cur))
        journal.update_journal(journal_id="j1", content_md="md2", updated_at="2026-08-08T08:00:00+00:00")
        update_call = next(c for c in cur.execute.call_args_list if "UPDATE trade_journals" in c.args[0])
        assert update_call.args[1] == ("T", "md2", "2026-08-08T08:00:00+00:00", "j1")


class TestDelete:
    def test_blank_id(self) -> None:
        assert journal.delete_journal("") is False

    def test_not_deleted(self, monkeypatch) -> None:
        cur = Mock()
        cur.rowcount = 0
        monkeypatch.setattr(journal, "get_connection", lambda: _fake_conn(cur))
        assert journal.delete_journal("nope") is False

    def test_deleted(self, monkeypatch) -> None:
        cur = Mock()
        cur.rowcount = 1
        monkeypatch.setattr(journal, "get_connection", lambda: _fake_conn(cur))
        assert journal.delete_journal("j1") is True
