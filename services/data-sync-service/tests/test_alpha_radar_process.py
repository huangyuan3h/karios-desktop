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


def test_keywords_from_trend_empty_entries() -> None:
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
"""alpha_radar_process wave-2: extract/save/batch/pending drivers."""

import json  # noqa: E402
import urllib.error  # noqa: E402
import urllib.request  # noqa: E402

from data_sync_service.service import alpha_radar_process as arp  # noqa: E402


def test_ai_service_base_url(monkeypatch) -> None:
    monkeypatch.setenv("AI_SERVICE_BASE_URL", "http://x:1/")
    assert arp._ai_service_base_url() == "http://x:1"
    monkeypatch.delenv("AI_SERVICE_BASE_URL")
    monkeypatch.setattr(arp, "get_settings", lambda: type("S", (), {"ai_service_base_url": ""})())
    assert arp._ai_service_base_url() == "http://127.0.0.1:4310"
    monkeypatch.setattr(arp, "get_settings", lambda: type("S", (), {"ai_service_base_url": "http://cfg:2"})())
    assert arp._ai_service_base_url() == "http://cfg:2"


class _Resp:
    def __init__(self, body):
        self._b = body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return self._b


def test_document_text(monkeypatch) -> None:
    assert arp._document_text({"fullTextMd": "FULL", "title": "T", "summary": "S"}) == "FULL"
    assert arp._document_text({"title": "T", "summary": "S"}) == "T\n\nS"
    assert arp._document_text({}) == ""


def test_ai_extract_trends_ok(monkeypatch) -> None:
    monkeypatch.setattr(arp, "_ai_service_base_url", lambda: "http://ai")
    captured = {}

    def fake_urlopen(req, timeout=180):
        captured["url"] = req.full_url
        captured["body"] = req.data
        return _Resp(json.dumps({"trends": [{"trend_name": "X"}]}).encode())

    monkeypatch.setattr(arp.urllib.request, "urlopen", fake_urlopen)
    out = arp._ai_extract_trends(text="t", title="ti", category="news", source_url="http://s")
    assert out["trends"][0]["trend_name"] == "X"
    assert captured["url"] == "http://ai/alpha-radar/extract"


def test_ai_extract_trends_http_error(monkeypatch) -> None:
    def fake_urlopen(req, timeout=180):
        err = urllib.error.HTTPError("http://ai/alpha-radar/extract", 500, "boom", {}, None)
        raise err

    monkeypatch.setattr(arp, "_ai_service_base_url", lambda: "http://ai")
    monkeypatch.setattr(arp.urllib.request, "urlopen", fake_urlopen)
    try:
        arp._ai_extract_trends(text="t", title="ti", category="c", source_url="u")
        raise AssertionError()
    except RuntimeError as exc:
        assert "extract error" in str(exc)


def test_resolve_trend_storage_fields() -> None:
    out = arp._resolve_trend_storage_fields(
        {"trend_name": "A", "catalystGrade": "a", "driverType": "policy", "eventFocus": "e", "logicSummary": "l" * 50},
        category_hint="news",
    )
    assert out["catalyst_grade"] == "A"
    assert out["logic_summary"] == ("l" * 50)[:30]
    assert out["driver_type"] == "policy"

    low = arp._resolve_trend_storage_fields({"trendName": "B", "urgencyLevel": "B"})
    assert low is None

    dflt = arp._resolve_trend_storage_fields({"trendName": "C", "catalystGrade": "S"}, category_hint="unknown_cat")
    assert dflt["driver_type"] == "Global_Tech"


def test_keywords_from_trend() -> None:
    fields = {"macro_theme": "产业趋势"}
    out = arp._keywords_from_trend({"a_share_mapping": ["  kw1 ", "", "kw2"]}, fields)
    assert out[0] == "产业趋势" and "kw1" in out and "kw2" in out
    out2 = arp._keywords_from_trend({}, fields)
    assert out2 == ["产业趋势"]
    out3 = arp._keywords_from_trend({"keywordsForMapping": ["a", "b", "c", "d", "e", "f", "g", "h", "i"]}, fields)
    assert len(out3) == 8


def test_save_trend_row(monkeypatch) -> None:
    monkeypatch.setattr(arp, "_resolve_trend_storage_fields", lambda trend, category_hint=None: {
        "trend_name": "T", "macro_theme": "T", "catalyst_grade": "A", "urgency_level": "A",
        "driver_type": "policy", "event_focus": "E", "logic_summary": "L",
    })
    monkeypatch.setattr(arp, "_keywords_from_trend", lambda trend, fields: ["T"])
    monkeypatch.setattr(arp, "insert_trend", lambda **kw: {**kw, "trend_id": kw["trend_id"]})
    monkeypatch.setattr(arp, "map_trend_hybrid", lambda **kw: {"cnSymbols": ["600000.SH"], "mappingConfidence": 0.9, "riskStatus": "mapped"})
    monkeypatch.setattr(arp, "map_trend_hk", lambda **kw: {"hkSymbols": ["00700.HK"]})
    row = arp._save_trend_row(doc_id="d1", trend={"x": 1}, category_hint="news", map_cn=True, hot_names=[], mainline_map={})
    assert row["cnSymbols"] == ["600000.SH"]
    assert row["hkSymbols"] == ["00700.HK"]
    assert row["trend_json"]["x"] == 1

    monkeypatch.setattr(arp, "_resolve_trend_storage_fields", lambda trend, category_hint=None: None)
    assert arp._save_trend_row(doc_id="d1", trend={}, category_hint=None, map_cn=True, hot_names=[], mainline_map={}) is None

    monkeypatch.setattr(arp, "_resolve_trend_storage_fields", lambda trend, category_hint=None: {
        "trend_name": "T", "macro_theme": "T", "catalyst_grade": "A", "urgency_level": "A",
        "driver_type": "policy", "event_focus": "E", "logic_summary": "L",
    })
    monkeypatch.setattr(arp, "map_trend_hybrid", lambda **kw: (_ for _ in ()).throw(RuntimeError("map down")))
    row2 = arp._save_trend_row(doc_id="d1", trend={}, category_hint=None, map_cn=True, hot_names=[], mainline_map={})
    assert "cnSymbols" not in row2 or row2["cnSymbols"] == []  # mapping failed → field untouched
    assert row2["hkSymbols"] == ["00700.HK"]  # HK mapping independent


def test_load_risk_context(monkeypatch) -> None:
    monkeypatch.setattr(arp, "get_cn_industry_mainline", lambda: {
        "currentMainline": [{"industryName": " 银行 "}, {"industryName": ""}],
    })
    monkeypatch.setattr(arp, "build_mainline_score_map", lambda mainline: {"银行": 0.9})
    hot, m = arp._load_risk_context()
    assert hot == ["银行"] and m == {"银行": 0.9}

    monkeypatch.setattr(arp, "get_cn_industry_mainline", lambda: (_ for _ in ()).throw(RuntimeError("x")))
    hot2, m2 = arp._load_risk_context()
    assert hot2 == [] and m2 == {}


def test_process_document(monkeypatch) -> None:
    doc = {"id": "d1", "title": "T", "url": "http://u", "category": "news", "summary": "S" * 60}
    monkeypatch.setattr(arp, "fetch_document_by_id", lambda did: doc)
    monkeypatch.setattr(arp, "_document_text", lambda d: "x" * 100)
    monkeypatch.setattr(arp, "_ai_extract_trends", lambda **kw: {"trends": [{"trendName": "A", "catalystGrade": "S"}]})
    monkeypatch.setattr(arp, "delete_trends_for_document", lambda did: 1)
    monkeypatch.setattr(arp, "_load_risk_context", lambda: (["银行"], {"银行": 0.9}))
    monkeypatch.setattr(arp, "_save_trend_row", lambda **kw: {"trendName": "A"})
    monkeypatch.setattr(arp, "update_document_status", lambda did, status: None)
    out = arp.process_document("d1", map_cn=True)
    assert out["processingStatus"] == "mapped"
    assert len(out["trends"]) == 1

    monkeypatch.setattr(arp, "fetch_document_by_id", lambda did: None)
    try:
        arp.process_document("d1")
        raise AssertionError()
    except ValueError:
        pass

    monkeypatch.setattr(arp, "_document_text", lambda d: "short")
    monkeypatch.setattr(arp, "fetch_document_by_id", lambda did: {"id": "d1"})
    try:
        arp.process_document("d1")
        raise AssertionError()
    except ValueError as exc:
        assert "too short" in str(exc)


def test_batch_document_summary() -> None:
    assert arp._batch_document_summary({"summary": "  S  "}) == "S"
    assert arp._batch_document_summary({"fullTextMd": "F" * 4000}) == "F" * 3000
    assert arp._batch_document_summary({"title": "T"}) == "T"
    assert arp._batch_document_summary({}) is None


def test_ai_extract_batch_ok_and_error(monkeypatch) -> None:
    monkeypatch.setattr(arp, "_ai_service_base_url", lambda: "http://ai")
    captured = {}

    def fake_urlopen(req, timeout=240):
        captured["url"] = req.full_url
        return _Resp(json.dumps({"trends": [{"sourceIndex": 0}]}).encode())

    monkeypatch.setattr(arp.urllib.request, "urlopen", fake_urlopen)
    out = arp._ai_extract_batch(documents=[{"id": "d1", "title": "T", "summary": "S"}])
    assert captured["url"] == "http://ai/alpha-radar/extract-batch"
    assert out["trends"][0]["sourceIndex"] == 0

    def fail_urlopen(req, timeout=240):
        raise urllib.error.HTTPError("http://ai/x", 500, "e", {}, None)

    monkeypatch.setattr(arp.urllib.request, "urlopen", fail_urlopen)
    try:
        arp._ai_extract_batch(documents=[])
        raise AssertionError()
    except RuntimeError as exc:
        assert "extract-batch" in str(exc)


def test_process_document_batch(monkeypatch) -> None:
    docs = [{"id": f"d{i}", "category": "news"} for i in range(3)]
    monkeypatch.setattr(arp, "fetch_documents_by_status", lambda processing_status, limit, enabled_sources_only=None: docs)
    monkeypatch.setattr(arp, "_load_risk_context", lambda: (["银行"], {}))
    monkeypatch.setattr(arp, "_ai_extract_batch", lambda documents: {"trends": [
        {"sourceIndex": 1, "trendName": "B"},
        {"source_index": "x", "trendName": "C"},
    ]})
    monkeypatch.setattr(arp, "delete_trends_for_document", lambda did: 1)
    monkeypatch.setattr(arp, "_save_trend_row", lambda **kw: {"trendName": kw["trend"]["trendName"]})
    monkeypatch.setattr(arp, "update_document_status", lambda did, status: None)
    out = arp.process_document_batch(batch_size=3, map_cn=True)
    assert out["processed"] == 3
    assert len(out["trends"]) == 2

    monkeypatch.setattr(arp, "_ai_extract_batch", lambda documents: {"error": "ai down"})
    try:
        arp.process_document_batch(batch_size=3)
        raise AssertionError()
    except RuntimeError as exc:
        assert "ai down" in str(exc)


def test_process_document_batch_small_and_empty(monkeypatch) -> None:
    monkeypatch.setattr(arp, "fetch_documents_by_status", lambda processing_status, limit, enabled_sources_only=None: [])
    out = arp.process_document_batch(batch_size=10)
    assert out["processed"] == 0 and out["mode"] == "batch"

    monkeypatch.setattr(arp, "fetch_documents_by_status", lambda processing_status, limit, enabled_sources_only=None: [{"id": "d1"}])
    monkeypatch.setattr(arp, "process_document", lambda did, map_cn=True: {"trends": [{"t": 1}]})
    out2 = arp.process_document_batch(batch_size=10)
    assert out2["processed"] == 1


def test_process_pending_documents(monkeypatch) -> None:
    monkeypatch.setattr(arp, "fetch_documents_by_status", lambda processing_status, limit: [{"id": "d1"}])
    monkeypatch.setattr(arp, "_load_risk_context", lambda: ([], {}))
    monkeypatch.setattr(arp, "process_document", lambda did, map_cn=True, hot_industry_names=None, mainline_by_industry=None: {"documentId": did})
    out = arp.process_pending_documents(limit=3, mode="single")
    assert out["processed"] == 1

    def fail(did, map_cn=True, hot_industry_names=None, mainline_by_industry=None):
        raise RuntimeError("boom")

    monkeypatch.setattr(arp, "process_document", fail)
    out2 = arp.process_pending_documents(limit=3, mode="single")
    assert out2["processed"] == 0 and len(out2["errors"]) == 1

    monkeypatch.setattr(arp, "process_document_batch", lambda batch_size, map_cn=True: {"processed": 2, "batchSize": 2, "trends": [], "errors": [], "mode": "batch"})
    out3 = arp.process_pending_documents(limit=3, mode="batch")
    assert out3["processed"] == 2

    monkeypatch.setattr(arp, "process_document_batch", lambda batch_size, map_cn=True: (_ for _ in ()).throw(RuntimeError("batch down")))
    out4 = arp.process_pending_documents(limit=3, mode="batch")
    assert out4["processed"] == 0 and out4["errors"] == [{"error": "batch down"}]
