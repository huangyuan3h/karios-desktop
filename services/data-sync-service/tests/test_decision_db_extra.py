"""db/decision.py remaining branches (mocked DB)."""

from __future__ import annotations

from unittest.mock import Mock

from data_sync_service.db import decision as dcd


def _fake_conn(cur: Mock) -> Mock:
    conn = Mock()
    conn.cursor.return_value = cur
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    cur.__enter__ = Mock(return_value=cur)
    cur.__exit__ = Mock(return_value=False)
    return conn


class TestGetSession:
    def test_not_found(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = None
        monkeypatch.setattr(dcd, "get_connection", lambda: _fake_conn(cur))
        assert dcd.get_session(1) is None

    def test_found(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = (1, "t", "p", "sp", None, None)
        monkeypatch.setattr(dcd, "get_connection", lambda: _fake_conn(cur))
        out = dcd.get_session(1)
        assert out["id"] == 1 and out["title"] == "t" and out["model_profile"] == "p"


class TestUpdateSessionTitle:
    def test_not_found(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = None
        monkeypatch.setattr(dcd, "get_connection", lambda: _fake_conn(cur))
        assert dcd.update_session_title(1, "new") is None

    def test_found(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = (1, "new")
        monkeypatch.setattr(dcd, "get_connection", lambda: _fake_conn(cur))
        out = dcd.update_session_title(1, "new")
        assert out == {"id": 1, "title": "new"}


class TestUpdateSessionSettings:
    def test_both_fields(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = (1, "t")
        monkeypatch.setattr(dcd, "get_connection", lambda: _fake_conn(cur))
        out = dcd.update_session_settings(1, title="t2", system_prompt="sp2")
        assert out["title"] == "t"
        sql = cur.execute.call_args[0][0]
        assert "system_prompt = %s" in sql and "title = %s" in sql

    def test_not_found(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = None
        monkeypatch.setattr(dcd, "get_connection", lambda: _fake_conn(cur))
        assert dcd.update_session_settings(1, title="t2") is None


class TestTouchSession:
    def test_ok(self, monkeypatch) -> None:
        cur = Mock()
        monkeypatch.setattr(dcd, "get_connection", lambda: _fake_conn(cur))
        dcd.touch_session(1)
        assert cur.execute.call_args[0][1] == (1,)


class TestUpsertActions:
    def test_empty_returns_zero(self, monkeypatch) -> None:
        cur = Mock()
        monkeypatch.setattr(dcd, "get_connection", lambda: _fake_conn(cur))
        assert dcd.upsert_actions([]) == 0
        sqls = [c.args[0] for c in cur.execute.call_args_list]
        assert not any("DELETE FROM decision_actions" in s for s in sqls)

    def test_inserts_all(self, monkeypatch) -> None:
        cur = Mock()
        monkeypatch.setattr(dcd, "get_connection", lambda: _fake_conn(cur))
        actions = [
            {"session_id": 1, "message_id": 5, "symbol": "CN:600519", "action": "BUY", "rationale": "r", "confidence": 0.9, "snapshot_date": "2026-08-07"},
            {"session_id": 1, "message_id": 5, "symbol": "CN:000001", "action": "ADD", "rationale": None, "confidence": None, "snapshot_date": None},
        ]
        assert dcd.upsert_actions(actions) == 2
        assert cur.execute.call_count == 4


class TestListActions:
    def test_with_status_and_iso_conversion(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = [
            (1, 1, 5, "CN:600519", "BUY", "r", 0.9, "proposed", "alpha", None, None, None, None),
        ]
        monkeypatch.setattr(dcd, "get_connection", lambda: _fake_conn(cur))
        out = dcd.list_actions(status="proposed", days=30)
        assert out[0]["action"] == "BUY" and out[0]["status"] == "proposed"

    def test_without_status(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = []
        monkeypatch.setattr(dcd, "get_connection", lambda: _fake_conn(cur))
        assert dcd.list_actions() == []
