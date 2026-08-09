"""api/execution_journal_routes.py coverage."""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import HTTPException

from data_sync_service.api import execution_journal_routes as ejr
from data_sync_service.api.execution_journal_routes import (
    SnapshotIngestRequest,
    get_journal_md,
    get_snapshot,
    list_changes,
    list_snapshots,
    post_snapshot,
)
from data_sync_service.db import execution_journal as ej_db
from data_sync_service.service import execution_journal as ej_svc


class TestPostSnapshot:
    def test_bad_source(self) -> None:
        with pytest.raises(HTTPException) as exc:
            post_snapshot(SnapshotIngestRequest(source="hack", tradeDate="2026-08-07"))
        assert exc.value.status_code == 400

    def test_bad_trade_date(self) -> None:
        with pytest.raises(HTTPException) as exc:
            post_snapshot(SnapshotIngestRequest(source="manual", tradeDate="2026-8-7"))
        assert exc.value.status_code == 400

    def test_gate_not_dict(self) -> None:
        req = SnapshotIngestRequest(source="manual", tradeDate="2026-08-07")
        req.gate = []  # type: ignore[assignment]
        with pytest.raises(HTTPException) as exc:
            post_snapshot(req)
        assert exc.value.status_code == 400

    def test_ok(self, monkeypatch) -> None:
        result = {"snapshotId": "s1", "changed": True, "snapshot": {"id": "s1"}, "changes": []}
        monkeypatch.setattr(ej_svc, "ingest_snapshot", lambda trade_date, source, gate, cards, meta: result)
        out = post_snapshot(SnapshotIngestRequest(source="poll", tradeDate="2026-08-07T12:00:00", gate={"g": 1}, cards=[{"c": 1}], meta={"m": 1}))
        assert out.snapshotId == "s1" and out.changed is True

    def test_ok_without_meta(self, monkeypatch) -> None:
        result = {"snapshotId": "s2", "changed": False, "heartbeat": True, "snapshot": {"id": "s2"}, "changes": []}
        monkeypatch.setattr(ej_svc, "ingest_snapshot", lambda trade_date, source, gate, cards, meta: result)
        out = post_snapshot(SnapshotIngestRequest(source="eod", tradeDate="2026-08-07"))
        assert out.heartbeat is True


def test_list_snapshots(monkeypatch) -> None:
    monkeypatch.setattr(ej_db, "list_snapshots", lambda trade_date, limit: [{"id": "s1"}])
    assert list_snapshots(trade_date="2026-08-07", limit=50).items == [{"id": "s1"}]


class TestGetSnapshot:
    def test_missing_id(self) -> None:
        with pytest.raises(HTTPException) as exc:
            get_snapshot("  ")
        assert exc.value.status_code == 400

    def test_not_found(self, monkeypatch) -> None:
        monkeypatch.setattr(ej_db, "fetch_snapshot_by_id", lambda sid: None)
        with pytest.raises(HTTPException) as exc:
            get_snapshot("nope")
        assert exc.value.status_code == 404

    def test_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(ej_db, "fetch_snapshot_by_id", lambda sid: {"id": sid})
        assert get_snapshot("s1") == {"id": "s1"}


def test_list_changes(monkeypatch) -> None:
    monkeypatch.setattr(ej_db, "list_changes", lambda trade_date, since, limit: [{"id": "c1"}])
    assert list_changes(trade_date=None, since=None, limit=100).items == [{"id": "c1"}]


class TestJournalMd:
    def test_with_date(self, monkeypatch) -> None:
        monkeypatch.setattr(ej_svc, "build_journal_markdown", lambda trade_date, days: "# journal")
        resp = get_journal_md(trade_date="2026-08-07", days=5)
        assert resp.media_type.startswith("text/markdown") and resp.body == b"# journal"

    def test_default_date(self, monkeypatch) -> None:
        seen: dict[str, Any] = {}

        def fake(trade_date, days):  # noqa: ANN001
            seen["trade_date"] = trade_date
            return "body"

        monkeypatch.setattr(ej_svc, "build_journal_markdown", fake)
        get_journal_md(trade_date=None, days=3)
        assert len(seen["trade_date"]) == 10


def test_valid_sources() -> None:
    assert ejr.VALID_SOURCES == {"sync_all", "poll", "registry", "manual", "eod"}
