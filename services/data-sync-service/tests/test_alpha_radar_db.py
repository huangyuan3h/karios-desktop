"""db/alpha_radar coverage: timezone/iso helpers + connection drivers."""

from __future__ import annotations

import datetime
import uuid

from data_sync_service.db import alpha_radar as ard


def test_shanghai_today_and_day_start() -> None:
    assert len(ard.shanghai_today()) == 10
    iso = ard.shanghai_day_start_iso("2026-08-04")
    dt = datetime.datetime.fromisoformat(iso)
    assert dt.tzinfo is not None
    # +08:00 day start == 16:00 UTC previous day
    assert dt.hour == 16 and dt.day == 3


def test_meta_roundtrip_via_mock(monkeypatch) -> None:
    rows = {"k": "v"}

    class _Cur:
        def __init__(self):
            self.sql = None

        def execute(self, sql, params=None):
            self.sql = sql

        def fetchone(self):
            key = rows["__last_key"] if "__last_key" in rows else None
            return (rows.get(key),)

    cur = _Cur()
    calls = {"get": 0}

    def fake_execute(sql, params=None):
        cur.sql = sql
        if "SELECT" in sql:
            rows["__last_key"] = params[0]
            calls["get"] += 1

    class _Cur2:
        def execute(self, sql, params=None):
            fake_execute(sql, params)

        def fetchone(self):
            key = rows.get("__last_key")
            return (rows.get(key),)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

        def cursor(self):
            return _Cur2()

        def commit(self):
            return None

    monkeypatch.setattr(ard, "ensure_tables", lambda: None)
    monkeypatch.setattr(ard, "get_connection", lambda: _Conn())

    assert ard.get_meta("k") == "v"
    ard.set_meta("k2", "v2")  # insert path exercised
    assert calls["get"] == 1


def test_disable_sources_except_empty() -> None:
    assert ard.disable_sources_except(set()) == 0


def test_disable_sources_except_updates(monkeypatch) -> None:
    class _Cur:
        def execute(self, sql, params):
            pass

        rowcount = 3

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

        def cursor(self):
            return _Cur()

        def commit(self):
            return None

    monkeypatch.setattr(ard, "ensure_tables", lambda: None)
    monkeypatch.setattr(ard, "get_connection", lambda: _Conn())
    assert ard.disable_sources_except({"a", "b"}) == 3


def test_upsert_document_builds_query(monkeypatch) -> None:
    captured: list[tuple] = []

    class _Cur:
        def execute(self, sql, params):
            captured.append((sql, params))

        def fetchone(self):
            return ("doc-1", "src-1", "T", "u", "news", None, None, "2026-08-04", "2026-08-04T00:00:00+00:00", "raw")

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

        def cursor(self):
            return _Cur()

        def commit(self):
            return None

    monkeypatch.setattr(ard, "ensure_tables", lambda: None)
    monkeypatch.setattr(ard, "get_connection", lambda: _Conn())

    out = ard.upsert_document(
        doc_id="doc-1",
        source_id="src-1",
        title="T",
        url="u",
        category="news",
        summary=None,
        full_text_md=None,
        published_at="2026-08-04",
        fetched_at="2026-08-04T00:00:00+00:00",
        processing_status="raw",
    )
    assert out["id"] == "doc-1"
    assert out["processingStatus"] == "raw"
    assert out.get("_requeued") is True
    sql, params = captured[0]
    assert "ON CONFLICT" in sql
    assert params[0] == "doc-1"
