"""Alpha Radar A-share mapping via local search + optional Tavily + LLM."""

from __future__ import annotations

import json
import logging
import os
import re
import urllib.error
import urllib.request
from typing import Any

from data_sync_service.config import get_settings
from data_sync_service.db.alpha_radar import fetch_trend_by_id, update_trend_mapping
from data_sync_service.db.stock_basic import fetch_market_stocks
from data_sync_service.db.stock_eastmoney_industry import search_stocks_by_industry_keyword
from data_sync_service.service.alpha_radar_risk import compute_risk_status

logger = logging.getLogger(__name__)


def _ai_service_base_url() -> str:
    settings = get_settings()
    base = os.getenv("AI_SERVICE_BASE_URL") or settings.ai_service_base_url
    return (base or "http://127.0.0.1:4310").rstrip("/")


def tavily_api_key() -> str:
    return os.getenv("TAVILY_API_KEY", "").strip()


def _normalize_keyword(keyword: str) -> str:
    s = str(keyword or "").strip()
    s = re.sub(r"\s*A股\s*$", "", s, flags=re.IGNORECASE)
    return s.strip()


def search_cn_candidates(keywords: list[str], *, limit: int = 12) -> list[dict[str, Any]]:
    seen: set[str] = set()
    candidates: list[dict[str, Any]] = []

    def add_item(item: dict[str, Any]) -> None:
        sym = str(item.get("symbol") or "").strip()
        if not sym or sym in seen:
            return
        seen.add(sym)
        candidates.append(item)

    for kw in keywords[:8]:
        q = _normalize_keyword(kw)
        if not q or len(q) < 2:
            continue

        _total, items = fetch_market_stocks(market="CN", q=q, offset=0, limit=limit)
        for item in items:
            add_item(
                {
                    "symbol": str(item.get("symbol") or ""),
                    "ticker": str(item.get("ticker") or ""),
                    "name": str(item.get("name") or ""),
                    "market": str(item.get("market") or "CN"),
                    "source": "nameSearch",
                }
            )

        for em_row in search_stocks_by_industry_keyword(q, limit=limit):
            add_item(em_row)

        if re.search(r"[A-Za-z]", q):
            short = re.sub(r"[^A-Za-z0-9]", "", q)[:6]
            if len(short) >= 3:
                _total2, items2 = fetch_market_stocks(market="CN", q=short, offset=0, limit=6)
                for item in items2:
                    add_item(
                        {
                            "symbol": str(item.get("symbol") or ""),
                            "ticker": str(item.get("ticker") or ""),
                            "name": str(item.get("name") or ""),
                            "market": "CN",
                            "source": "tickerSearch",
                        }
                    )

    return candidates[:24]


def tavily_search_cn_context(keywords: list[str]) -> str | None:
    key = tavily_api_key()
    if not key:
        return None
    query = "A股 " + " ".join(_normalize_keyword(k) for k in keywords[:3] if k) + " 核心供应商 龙头"
    payload = json.dumps(
        {
            "api_key": key,
            "query": query,
            "search_depth": "basic",
            "max_results": 5,
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        "https://api.tavily.com/search",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8") or "{}")
        snippets = []
        for row in data.get("results") or []:
            title = str(row.get("title") or "")
            content = str(row.get("content") or "")
            if title or content:
                snippets.append(f"- {title}: {content[:400]}")
        return "\n".join(snippets) if snippets else None
    except Exception as exc:
        logger.warning(f"[alpha_radar] Tavily search failed: {exc}")
        return None


def _ai_map_cn_symbols(
    *,
    trend: dict[str, Any],
    candidates: list[dict[str, Any]],
    external_context: str | None,
    seed_symbols: list[str] | None = None,
) -> dict[str, Any]:
    payload = json.dumps(
        {
            "trend": trend,
            "candidates": candidates,
            "externalContext": external_context,
            "allowKnowledgeFallback": len(candidates) == 0,
            "seedSymbols": seed_symbols or [],
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        f"{_ai_service_base_url()}/alpha-radar/map-cn",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8") or "{}")
    except urllib.error.HTTPError as exc:
        msg = exc.read().decode("utf-8") if exc.fp else str(exc)
        raise RuntimeError(f"ai-service map-cn error: {msg}") from exc


def map_trend_to_cn(
    *,
    trend_id: str,
    trend: dict[str, Any] | None = None,
    hot_industry_names: list[str] | None = None,
    mainline_by_industry: dict[str, float] | None = None,
    seed_symbols: list[str] | None = None,
) -> dict[str, Any]:
    row = fetch_trend_by_id(trend_id) if trend is None else None
    if trend is None:
        if not row:
            raise ValueError(f"trend not found: {trend_id}")
        trend = dict(row.get("trendJson") or {})
        trend.setdefault("trend_name", row.get("trendName"))
        trend.setdefault("macro_theme", row.get("macroTheme"))
        trend.setdefault("catalyst_grade", row.get("catalystGrade"))
        trend.setdefault("keywords_for_mapping", row.get("keywordsForMapping") or [])

    keywords = list(trend.get("keywords_for_mapping") or trend.get("keywordsForMapping") or [])
    seeds = list(seed_symbols or trend.get("a_share_mapping") or trend.get("aShareMapping") or [])
    candidates = search_cn_candidates(keywords + seeds[:3])
    external = tavily_search_cn_context(keywords)
    ai_result = _ai_map_cn_symbols(
        trend=trend,
        candidates=candidates,
        external_context=external,
        seed_symbols=seeds[:3] or None,
    )
    cn_symbols = ai_result.get("cnSymbols") or ai_result.get("cn_symbols") or []
    confidence = ai_result.get("mappingConfidence") or ai_result.get("mapping_confidence")
    risk_status = compute_risk_status(
        keywords=keywords,
        hot_industry_names=hot_industry_names or [],
        mainline_by_industry=mainline_by_industry or {},
    )
    update_trend_mapping(
        trend_id=trend_id,
        cn_symbols=cn_symbols,
        mapping_confidence=float(confidence) if confidence is not None else None,
        risk_status=risk_status,
    )
    return {
        "trendId": trend_id,
        "cnSymbols": cn_symbols,
        "candidateCount": len(candidates),
        "mappingConfidence": confidence,
        "riskStatus": risk_status,
    }


def remap_trend_by_id(trend_id: str) -> dict[str, Any]:
    from data_sync_service.service.alpha_radar_process import _load_risk_context
    from data_sync_service.service.alpha_radar_symbol_resolve import map_trend_hybrid

    hot_names, mainline_map = _load_risk_context()
    row = fetch_trend_by_id(trend_id)
    if not row:
        raise ValueError(f"trend not found: {trend_id}")
    trend = dict(row.get("trendJson") or {})
    trend.setdefault("macro_theme", row.get("macroTheme"))
    trend.setdefault("catalyst_grade", row.get("catalystGrade"))
    trend.setdefault("a_share_mapping", row.get("keywordsForMapping") or [])
    return map_trend_hybrid(
        trend_id=trend_id,
        trend=trend,
        hot_industry_names=hot_names,
        mainline_by_industry=mainline_map,
    )
