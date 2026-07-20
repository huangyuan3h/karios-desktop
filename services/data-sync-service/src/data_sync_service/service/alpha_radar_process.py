"""Alpha Radar LLM extraction pipeline."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
import uuid
from typing import Any

from data_sync_service.config import get_settings
from data_sync_service.db.alpha_radar import (
    delete_trends_for_document,
    fetch_document_by_id,
    fetch_documents_by_status,
    insert_trend,
    update_document_status,
)
from data_sync_service.service.alpha_radar_risk import build_mainline_score_map
from data_sync_service.service.alpha_radar_symbol_resolve import map_trend_hybrid
from data_sync_service.service.mainline import get_cn_industry_mainline

_CATEGORY_DRIVER_DEFAULT: dict[str, str] = {
    "academic": "Global_Tech",
    "earnings": "Global_Tech",
    "research": "Global_Tech",
    "policy": "Domestic_Policy",
    "cycle": "Cycle_Reversal",
    "consensus": "Cycle_Reversal",
}


def _ai_service_base_url() -> str:
    settings = get_settings()
    base = os.getenv("AI_SERVICE_BASE_URL") or settings.ai_service_base_url
    return (base or "http://127.0.0.1:4310").rstrip("/")


def _document_text(doc: dict[str, Any]) -> str:
    full = doc.get("fullTextMd") or doc.get("full_text_md")
    if full:
        return str(full)
    parts = [str(doc.get("title") or "")]
    summary = doc.get("summary")
    if summary:
        parts.append(str(summary))
    return "\n\n".join(p for p in parts if p.strip())


def _ai_extract_trends(*, text: str, title: str, category: str, source_url: str) -> dict[str, Any]:
    payload = json.dumps(
        {
            "text": text,
            "title": title,
            "category": category,
            "sourceUrl": source_url,
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        f"{_ai_service_base_url()}/alpha-radar/extract",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            return json.loads(resp.read().decode("utf-8") or "{}")
    except urllib.error.HTTPError as exc:
        msg = exc.read().decode("utf-8") if exc.fp else str(exc)
        raise RuntimeError(f"ai-service extract error: {msg}") from exc


def _resolve_trend_storage_fields(
    trend: dict[str, Any],
    *,
    category_hint: str | None = None,
) -> dict[str, str] | None:
    macro_theme = str(
        trend.get("macro_theme")
        or trend.get("macroTheme")
        or trend.get("trend_name")
        or trend.get("trendName")
        or "Unknown"
    )
    catalyst_grade = str(
        trend.get("catalyst_grade")
        or trend.get("catalystGrade")
        or trend.get("urgency_level")
        or trend.get("urgencyLevel")
        or "B"
    ).upper()
    if catalyst_grade not in ("S", "A"):
        return None

    driver_type = str(
        trend.get("driver_type")
        or trend.get("driverType")
        or _CATEGORY_DRIVER_DEFAULT.get(str(category_hint or "").lower(), "Global_Tech")
    )
    event_focus = str(
        trend.get("event_focus")
        or trend.get("eventFocus")
        or trend.get("catalyst")
        or macro_theme
    )
    logic_summary = str(
        trend.get("logic_summary") or trend.get("logicSummary") or event_focus
    )[:30]

    return {
        "trend_name": macro_theme,
        "macro_theme": macro_theme,
        "catalyst_grade": catalyst_grade,
        "urgency_level": catalyst_grade,
        "driver_type": driver_type,
        "event_focus": event_focus,
        "logic_summary": logic_summary,
    }


def _keywords_from_trend(trend: dict[str, Any], fields: dict[str, str]) -> list[str]:
    raw = list(
        trend.get("a_share_mapping")
        or trend.get("aShareMapping")
        or trend.get("keywords_for_mapping")
        or trend.get("keywordsForMapping")
        or []
    )
    keywords = [str(k).strip() for k in raw if str(k).strip()]
    macro = fields.get("macro_theme") or ""
    if macro and macro not in keywords:
        keywords.insert(0, macro)
    return keywords[:8] if keywords else [macro or "产业趋势"]


def _save_trend_row(
    *,
    doc_id: str,
    trend: dict[str, Any],
    category_hint: str | None,
    map_cn: bool,
    hot_names: list[str],
    mainline_map: dict[str, float],
    batch_meta: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    fields = _resolve_trend_storage_fields(trend, category_hint=category_hint)
    if fields is None:
        return None

    trend_id = str(uuid.uuid4())
    keywords = _keywords_from_trend(trend, fields)
    trend_payload = {**trend, **(batch_meta or {})}

    row = insert_trend(
        trend_id=trend_id,
        document_id=doc_id,
        trend_name=fields["trend_name"],
        macro_theme=fields["macro_theme"],
        catalyst_grade=fields["catalyst_grade"],
        catalyst=fields["event_focus"],
        global_target=None,
        urgency_level=fields["urgency_level"],
        driver_type=fields["driver_type"],
        event_focus=fields["event_focus"],
        logic_summary=fields["logic_summary"],
        keywords_for_mapping=keywords,
        cn_symbols=[],
        mapping_confidence=None,
        risk_status="waiting_v2_flow",
        trend_json=trend_payload,
    )

    if map_cn:
        try:
            mapped = map_trend_hybrid(
                trend_id=trend_id,
                trend=trend_payload,
                hot_industry_names=hot_names,
                mainline_by_industry=mainline_map,
            )
            row["cnSymbols"] = mapped.get("cnSymbols") or []
            row["mappingConfidence"] = mapped.get("mappingConfidence")
            row["riskStatus"] = mapped.get("riskStatus")
        except Exception as exc:
            print(f"[alpha_radar] mapping failed for {trend_id}: {exc}")
    return row


def _load_risk_context() -> tuple[list[str], dict[str, float]]:
    hot_names: list[str] = []
    mainline_map: dict[str, float] = {}
    try:
        mainline = get_cn_industry_mainline()
        mainline_map = build_mainline_score_map(mainline)
        for row in mainline.get("currentMainline") or []:
            name = str(row.get("industryName") or "").strip()
            if name:
                hot_names.append(name)
    except Exception as exc:
        print(f"[alpha_radar] risk context load failed: {exc}")
    return hot_names, mainline_map


def process_document(
    doc_id: str,
    *,
    map_cn: bool = True,
    hot_industry_names: list[str] | None = None,
    mainline_by_industry: dict[str, float] | None = None,
) -> dict[str, Any]:
    doc = fetch_document_by_id(doc_id)
    if not doc:
        raise ValueError(f"document not found: {doc_id}")
    text = _document_text(doc)
    if len(text.strip()) < 40:
        raise ValueError("document text too short for extraction")

    category = str(doc.get("category") or "academic")
    extracted = _ai_extract_trends(
        text=text,
        title=str(doc.get("title") or ""),
        category=category,
        source_url=str(doc.get("url") or ""),
    )
    trends = extracted.get("trends") or []
    delete_trends_for_document(doc_id)

    hot_names = hot_industry_names
    mainline_map = mainline_by_industry
    if hot_names is None or mainline_map is None:
        ctx_hot, ctx_mainline = _load_risk_context()
        hot_names = hot_names if hot_names is not None else ctx_hot
        mainline_map = mainline_map if mainline_map is not None else ctx_mainline

    saved: list[dict[str, Any]] = []
    for trend in trends[:3]:
        row = _save_trend_row(
            doc_id=doc_id,
            trend=trend,
            category_hint=category,
            map_cn=map_cn,
            hot_names=hot_names,
            mainline_map=mainline_map,
        )
        if row:
            saved.append(row)

    final_status = "mapped" if map_cn and saved else "extracted"
    update_document_status(doc_id, final_status)
    return {"documentId": doc_id, "trends": saved, "processingStatus": final_status}


def _batch_document_summary(doc: dict[str, Any]) -> str | None:
    summary = doc.get("summary")
    if summary and str(summary).strip():
        return str(summary).strip()
    for key in ("fullTextMd", "full_text_md"):
        full = doc.get(key)
        if full and str(full).strip():
            text = str(full).strip()
            return text[:3000]
    title = str(doc.get("title") or "").strip()
    return title or None


def _ai_extract_batch(*, documents: list[dict[str, Any]]) -> dict[str, Any]:
    payload_docs = []
    for doc in documents:
        payload_docs.append(
            {
                "documentId": str(doc.get("id") or ""),
                "title": str(doc.get("title") or ""),
                "url": str(doc.get("url") or ""),
                "category": str(doc.get("category") or "academic"),
                "summary": _batch_document_summary(doc),
            }
        )
    payload = json.dumps({"documents": payload_docs}).encode("utf-8")
    req = urllib.request.Request(
        f"{_ai_service_base_url()}/alpha-radar/extract-batch",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=240) as resp:
            return json.loads(resp.read().decode("utf-8") or "{}")
    except urllib.error.HTTPError as exc:
        msg = exc.read().decode("utf-8") if exc.fp else str(exc)
        raise RuntimeError(f"ai-service extract-batch error: {msg}") from exc


def process_document_batch(
    *,
    batch_size: int = 10,
    map_cn: bool = True,
    hot_industry_names: list[str] | None = None,
    mainline_by_industry: dict[str, float] | None = None,
) -> dict[str, Any]:
    docs = fetch_documents_by_status(
        processing_status="raw",
        limit=max(2, min(int(batch_size), 15)),
        enabled_sources_only=True,
    )
    if len(docs) < 2:
        if len(docs) == 1:
            out = process_document(str(docs[0]["id"]), map_cn=map_cn)
            return {
                "processed": 1,
                "batchSize": 1,
                "trends": out.get("trends") or [],
                "errors": [],
                "mode": "batch",
            }
        return {"processed": 0, "batchSize": 0, "trends": [], "errors": [], "mode": "batch"}

    hot_names = hot_industry_names
    mainline_map = mainline_by_industry
    if hot_names is None or mainline_map is None:
        ctx_hot, ctx_mainline = _load_risk_context()
        hot_names = hot_names if hot_names is not None else ctx_hot
        mainline_map = mainline_map if mainline_map is not None else ctx_mainline

    extracted = _ai_extract_batch(documents=docs)
    if extracted.get("error"):
        raise RuntimeError(f"ai-service extract-batch error: {extracted.get('error')}")
    trends = extracted.get("trends") or []
    saved: list[dict[str, Any]] = []
    doc_ids = [str(d.get("id") or "") for d in docs]
    doc_categories = {str(d.get("id") or ""): str(d.get("category") or "academic") for d in docs}

    for doc_id in doc_ids:
        if doc_id:
            delete_trends_for_document(doc_id)

    for trend in trends[:8]:
        idx_raw = trend.get("source_index", trend.get("sourceIndex", 0))
        try:
            idx = int(idx_raw)
        except (TypeError, ValueError):
            idx = 0
        idx = max(0, min(idx, len(docs) - 1))
        doc_id = doc_ids[idx]
        if not doc_id:
            continue

        row = _save_trend_row(
            doc_id=doc_id,
            trend=trend,
            category_hint=doc_categories.get(doc_id),
            map_cn=map_cn,
            hot_names=hot_names,
            mainline_map=mainline_map,
            batch_meta={"batch_mode": True, "batch_size": len(docs)},
        )
        if row:
            saved.append(row)

    for doc_id in doc_ids:
        if doc_id:
            status = "mapped" if map_cn and saved else "extracted"
            update_document_status(doc_id, status)

    if not saved:
        return {
            "processed": len(docs),
            "batchSize": len(docs),
            "trends": [],
            "errors": [],
            "mode": "batch",
        }

    return {
        "processed": len(docs),
        "batchSize": len(docs),
        "trends": saved,
        "errors": [],
        "mode": "batch",
    }


def process_pending_documents(*, limit: int = 3, map_cn: bool = True, mode: str = "single") -> dict[str, Any]:
    if mode == "batch":
        batch_size = max(2, min(int(limit), 15))
        try:
            return process_document_batch(batch_size=batch_size, map_cn=map_cn)
        except Exception as exc:
            return {"processed": 0, "batchSize": 0, "trends": [], "errors": [{"error": str(exc)}], "mode": "batch"}

    docs = fetch_documents_by_status(processing_status="raw", limit=limit)
    results: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    hot_names, mainline_map = _load_risk_context()
    for doc in docs:
        doc_id = str(doc.get("id") or "")
        if not doc_id:
            continue
        try:
            out = process_document(
                doc_id,
                map_cn=map_cn,
                hot_industry_names=hot_names,
                mainline_by_industry=mainline_map,
            )
            results.append(out)
        except Exception as exc:
            errors.append({"documentId": doc_id, "error": str(exc)})
    return {"processed": len(results), "results": results, "errors": errors}
