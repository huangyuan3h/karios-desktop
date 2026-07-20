"""Tests for alpha_radar document upsert (content-aware processing_status)."""

from __future__ import annotations

import uuid

import pytest

from data_sync_service.db.alpha_radar import (
    create_source,
    fetch_document_by_id,
    update_document_status,
    upsert_document,
)

pytestmark = pytest.mark.requires_postgres

def _doc_id() -> str:
    return uuid.uuid4().hex[:16]


def test_upsert_unchanged_preserves_mapped_status() -> None:
    doc_id = _doc_id()
    source_id = f"test-src-{doc_id[:8]}"
    create_source(
        source_id=source_id,
        name="Test Source",
        url=f"https://example.com/feed-{doc_id}",
        category="research",
    )
    fetched = "2026-06-01T10:00:00+00:00"
    upsert_document(
        doc_id=doc_id,
        source_id=source_id,
        title="Original title",
        url=f"https://example.com/article-{doc_id}",
        category="research",
        summary="Same summary body",
        full_text_md=None,
        published_at=fetched,
        fetched_at=fetched,
    )
    update_document_status(doc_id, "mapped")

    row = upsert_document(
        doc_id=doc_id,
        source_id=source_id,
        title="Original title",
        url=f"https://example.com/article-{doc_id}",
        category="research",
        summary="Same summary body",
        full_text_md=None,
        published_at=fetched,
        fetched_at="2026-06-02T10:00:00+00:00",
    )
    assert row.get("_inserted") is False
    assert row.get("processingStatus") == "mapped"
    assert row.get("_requeued") is False

    loaded = fetch_document_by_id(doc_id)
    assert loaded is not None
    assert loaded["processingStatus"] == "mapped"


def test_upsert_summary_change_resets_raw() -> None:
    doc_id = _doc_id()
    source_id = f"test-src-{doc_id[:8]}"
    create_source(
        source_id=source_id,
        name="Test Source 2",
        url=f"https://example.com/feed2-{doc_id}",
        category="research",
    )
    fetched = "2026-06-01T10:00:00+00:00"
    upsert_document(
        doc_id=doc_id,
        source_id=source_id,
        title="Title",
        url=f"https://example.com/article2-{doc_id}",
        category="research",
        summary="Version one",
        full_text_md=None,
        published_at=fetched,
        fetched_at=fetched,
    )
    update_document_status(doc_id, "mapped")

    row = upsert_document(
        doc_id=doc_id,
        source_id=source_id,
        title="Title",
        url=f"https://example.com/article2-{doc_id}",
        category="research",
        summary="Version two changed",
        full_text_md=None,
        published_at=fetched,
        fetched_at="2026-06-02T10:00:00+00:00",
    )
    assert row.get("processingStatus") == "raw"
    assert row.get("_requeued") is True


def test_upsert_force_reprocess() -> None:
    doc_id = _doc_id()
    source_id = f"test-src-{doc_id[:8]}"
    create_source(
        source_id=source_id,
        name="Test Source 3",
        url=f"https://example.com/feed3-{doc_id}",
        category="research",
    )
    fetched = "2026-06-01T10:00:00+00:00"
    upsert_document(
        doc_id=doc_id,
        source_id=source_id,
        title="Stable",
        url=f"https://example.com/article3-{doc_id}",
        category="research",
        summary="Stable summary",
        full_text_md=None,
        published_at=fetched,
        fetched_at=fetched,
    )
    update_document_status(doc_id, "mapped")

    row = upsert_document(
        doc_id=doc_id,
        source_id=source_id,
        title="Stable",
        url=f"https://example.com/article3-{doc_id}",
        category="research",
        summary="Stable summary",
        full_text_md=None,
        published_at=fetched,
        fetched_at="2026-06-02T10:00:00+00:00",
        force_reprocess=True,
    )
    assert row.get("processingStatus") == "raw"
    assert row.get("_requeued") is True
