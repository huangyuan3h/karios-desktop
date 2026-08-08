"""api/journal_routes.py coverage."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from data_sync_service.api import journal_routes as jr
from data_sync_service.api.journal_routes import (
    TradeJournal,
    TradeJournalCreateRequest,
    TradeJournalUpdateRequest,
    create_journal,
    delete_journal,
    get_journal,
    list_journals,
    update_journal,
)
from data_sync_service.db import journal as journal_db

JOURNAL = {
    "id": "j1",
    "title": "T",
    "contentMd": "C",
    "createdAt": "2026-01-01T00:00:00+00:00",
    "updatedAt": "2026-01-01T00:00:00+00:00",
}


def test_list_journals(monkeypatch) -> None:
    monkeypatch.setattr(journal_db, "fetch_all", lambda limit, offset: (1, [JOURNAL]))
    out = list_journals(limit=20, offset=0)
    assert out.total == 1 and isinstance(out.items[0], TradeJournal) and out.items[0].id == "j1"


class TestGetJournal:
    def test_missing_id(self) -> None:
        with pytest.raises(HTTPException) as exc:
            get_journal("  ")
        assert exc.value.status_code == 400

    def test_not_found(self, monkeypatch) -> None:
        monkeypatch.setattr(journal_db, "fetch_by_id", lambda jid: None)
        with pytest.raises(HTTPException) as exc:
            get_journal("nope")
        assert exc.value.status_code == 404

    def test_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(journal_db, "fetch_by_id", lambda jid: JOURNAL)
        assert get_journal("j1").title == "T"


def test_create_journal(monkeypatch) -> None:
    def fake_create(journal_id, title, content_md, created_at, updated_at):  # noqa: ANN001
        return {**JOURNAL, "id": journal_id, "title": title, "contentMd": content_md}

    monkeypatch.setattr(journal_db, "create_journal", fake_create)
    out = create_journal(TradeJournalCreateRequest(title="  ", contentMd=""))
    assert out.title == "Trading Journal"


class TestUpdateJournal:
    def test_missing_id(self) -> None:
        with pytest.raises(HTTPException) as exc:
            update_journal("", TradeJournalUpdateRequest(title="T"))
        assert exc.value.status_code == 400

    def test_not_found(self, monkeypatch) -> None:
        monkeypatch.setattr(journal_db, "update_journal", lambda journal_id, title, content_md, updated_at: None)
        with pytest.raises(HTTPException) as exc:
            update_journal("j1", TradeJournalUpdateRequest(title="T"))
        assert exc.value.status_code == 404

    def test_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(journal_db, "update_journal", lambda journal_id, title, content_md, updated_at: {**JOURNAL, "title": title})
        assert update_journal("j1", TradeJournalUpdateRequest(title="New")).title == "New"


class TestDeleteJournal:
    def test_missing_id(self) -> None:
        with pytest.raises(HTTPException) as exc:
            delete_journal(None)
        assert exc.value.status_code == 400

    def test_not_found(self, monkeypatch) -> None:
        monkeypatch.setattr(journal_db, "delete_journal", lambda jid: None)
        with pytest.raises(HTTPException) as exc:
            delete_journal("j1")
        assert exc.value.status_code == 404

    def test_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(journal_db, "delete_journal", lambda jid: True)
        assert delete_journal("j1") == {"ok": True}


def test_router_paths() -> None:
    paths = {r.path for r in jr.router.routes}
    assert "/journals" in paths and "/journals/{journal_id}" in paths
