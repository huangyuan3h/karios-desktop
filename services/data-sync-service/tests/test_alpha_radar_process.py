"""alpha_radar_process: storage fields, keywords, document/batch drivers."""

from __future__ import annotations

from data_sync_service.service import alpha_radar_process as ap


def test_resolve_trend_storage_fields_grade_b_rejected() -> None:
    assert ap._resolve_trend_storage_fields({"catalyst_grade": "B"}) is None


def test_resolve_trend_storage_fields_grade_s_defaults() -> None:
    out = ap._resolve_trend_storage_fields({"catalystGrade": "S"})
    assert out is not None
    assert out["catalyst_grade"] == "S"
    assert out["macro_theme"] == "Unknown"
    assert out["driver_type"] == "Global_Tech"
    assert out["event_focus"] == "Unknown"
    assert out["logic_summary"] == "Unknown"


def test_resolve_trend_storage_fields_full() -> None:
    out = ap._resolve_trend_storage_fields(
        {
            "trend_name": "AI Agent",
            "catalyst_grade": "a",
            "driver_type": "AI",
            "event_focus": "catalyst-x",
            "logic_summary": "summary-y",
        },
        category_hint="AI",
    )
    assert out["catalyst_grade"] == "A"
    assert out["macro_theme"] == "AI Agent"
    assert out["event_focus"] == "catalyst-x"
    assert out["logic_summary"] == "summary-y"


def test_keywords_from_trend() -> None:
    fields = {"macro_theme": "半导体"}
    assert ap._keywords_from_trend({"a_share_mapping": [" 中芯 ", ""]}, fields) == ["半导体", "中芯"]
    assert ap._keywords_from_trend({}, fields) == ["半导体"]
    assert ap._keywords_from_trend({}, {"macro_theme": ""}) == ["产业趋势"]


def test_process_document_short_text_raises(monkeypatch) -> None:
    monkeypatch.setattr(ap, "fetch_document_by_id", lambda doc_id: {"content": "short"})
    import pytest

    with pytest.raises(ValueError, match="text too short"):
        ap.process_document("doc-1")


def test_process_document_missing_raises(monkeypatch) -> None:
    monkeypatch.setattr(ap, "fetch_document_by_id", lambda doc_id: None)
    import pytest

    with pytest.raises(ValueError, match="not found"):
        ap.process_document("doc-1")


def test_process_document_saves_trends(monkeypatch) -> None:
    long_text = "x" * 100
    monkeypatch.setattr(
        ap,
        "fetch_document_by_id",
        lambda doc_id: {"fullTextMd": long_text, "title": "T", "category": "news", "url": "u"},
    )
    monkeypatch.setattr(
        ap,
        "_ai_extract_trends",
        lambda **kw: {"trends": [{"catalyst_grade": "S", "trend_name": "半导体"}]},
    )
    monkeypatch.setattr(ap, "delete_trends_for_document", lambda doc_id: None)
    monkeypatch.setattr(ap, "_load_risk_context", lambda: (["半导体"], {}))
    monkeypatch.setattr(ap, "update_document_status", lambda doc_id, status: None)

    saved_row = {"id": "tr1", "trend_name": "半导体", "mapping_confidence": 0.9}
    monkeypatch.setattr(ap, "_save_trend_row", lambda **kw: saved_row)

    out = ap.process_document("doc-1", map_cn=True)
    assert out["documentId"] == "doc-1"
    assert out["trends"] == [saved_row]
    assert out["processingStatus"] == "mapped"


def test_process_document_unmapped_status(monkeypatch) -> None:
    monkeypatch.setattr(
        ap, "fetch_document_by_id",
        lambda doc_id: {"fullTextMd": "y" * 100, "title": "T", "category": "news", "url": "u"},
    )
    monkeypatch.setattr(
        ap, "_ai_extract_trends", lambda **kw: {"trends": [{"catalyst_grade": "S"}]}
    )
    monkeypatch.setattr(ap, "delete_trends_for_document", lambda doc_id: None)
    monkeypatch.setattr(ap, "_load_risk_context", lambda: (["半导体"], {}))
    monkeypatch.setattr(ap, "update_document_status", lambda doc_id, status: None)
    monkeypatch.setattr(ap, "_save_trend_row", lambda **kw: None)  # mapping failed

    out = ap.process_document("doc-1", map_cn=True)
    assert out["trends"] == []
    assert out["processingStatus"] == "extracted"


def test_document_text_uses_fulltext_and_title_summary() -> None:
    assert ap._document_text({"fullTextMd": "FULL"}) == "FULL"
    out = ap._document_text({"title": "T", "summary": "S"})
    assert "T" in out and "S" in out
    assert ap._document_text({"title": "T"}) == "T"
