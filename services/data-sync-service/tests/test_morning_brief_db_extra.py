"""db/morning_brief.py remaining branches (mocked DB)."""

from __future__ import annotations

from unittest.mock import Mock

from data_sync_service.db import morning_brief as mb


def _fake_conn(cur: Mock) -> Mock:
    conn = Mock()
    conn.cursor.return_value = cur
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    cur.__enter__ = Mock(return_value=cur)
    cur.__exit__ = Mock(return_value=False)
    return conn


ROW = (
    "2026-08-07-morning", "2026-08-07", "morning",
    '[{"id": "n1", "title": "x"}]', "overview", "v1", ["n1", "n2"], "2026-08-07T08:30:00+00:00",
)


class TestUpsert:
    def test_ok(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = ROW
        monkeypatch.setattr(mb, "get_connection", lambda: _fake_conn(cur))
        out = mb.upsert_brief(
            brief_date="2026-08-07", brief_type="morning",
            items=[{"id": "n1"}], macro_overview="overview", model_version="v1",
            source_item_ids=["n1", "n2"],
        )
        assert out["briefDate"] == "2026-08-07"
        assert out["briefType"] == "morning"
        assert out["items"] == [{"id": "n1", "title": "x"}]
        assert out["macroOverview"] == "overview"
        assert out["sourceItemIds"] == ["n1", "n2"]
        assert out["modelVersion"] == "v1"


class TestFetch:
    def test_brief_found(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = ROW
        monkeypatch.setattr(mb, "get_connection", lambda: _fake_conn(cur))
        out = mb.fetch_brief("2026-08-07", "morning")
        assert out["id"] == "2026-08-07-morning"

    def test_brief_missing(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = None
        monkeypatch.setattr(mb, "get_connection", lambda: _fake_conn(cur))
        assert mb.fetch_brief("2026-08-07", "noon") is None

    def test_latest_with_type(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = ROW
        monkeypatch.setattr(mb, "get_connection", lambda: _fake_conn(cur))
        mb.fetch_latest_brief("morning")
        sql, params = cur.execute.call_args_list[-1][0]
        assert "WHERE brief_type = %s" in sql
        assert params == ("morning",)

    def test_latest_no_type(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchone.return_value = ROW
        monkeypatch.setattr(mb, "get_connection", lambda: _fake_conn(cur))
        mb.fetch_latest_brief()
        sql, params = cur.execute.call_args_list[-1][0]
        assert "WHERE" not in sql
        assert params == ()

    def test_recent_clamps(self, monkeypatch) -> None:
        cur = Mock()
        cur.fetchall.return_value = [ROW, ROW]
        monkeypatch.setattr(mb, "get_connection", lambda: _fake_conn(cur))
        out = mb.fetch_recent_briefs(limit=99)
        assert len(out) == 2
        sql, params = cur.execute.call_args_list[-1][0]
        assert params == (30,)


class TestRowToDict:
    def test_items_json_invalid(self) -> None:
        row = (ROW[0], ROW[1], ROW[2], "not-json{{", None, None, None, ROW[7])
        out = mb._row_to_dict(row)
        assert out["items"] == []
        assert out["macroOverview"] is None
        assert out["sourceItemIds"] is None

    def test_items_none(self) -> None:
        row = (ROW[0], ROW[1], ROW[2], None, "", "", [], ROW[7])
        out = mb._row_to_dict(row)
        assert out["items"] == []
        assert out["macroOverview"] is None
        assert out["sourceItemIds"] is None
