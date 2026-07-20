"""Resolve LLM a_share_mapping strings to structured CN symbols."""

from __future__ import annotations

import re
from typing import Any

from data_sync_service.db.stock_basic import fetch_market_stocks
from data_sync_service.service.alpha_radar_mapping import map_trend_to_cn, search_cn_candidates

_TICKER_RE = re.compile(r"^\d{6}$")
_CN_PREFIX_RE = re.compile(r"^CN:(\d{6})$", re.IGNORECASE)


def _normalize_ticker(raw: str) -> str | None:
    text = str(raw or "").strip().upper()
    if not text:
        return None
    m = _CN_PREFIX_RE.match(text)
    if m:
        return m.group(1)
    digits = re.sub(r"\D", "", text)
    if _TICKER_RE.match(digits):
        return digits
    return None


def _lookup_by_ticker(ticker: str) -> dict[str, Any] | None:
    _total, items = fetch_market_stocks(market="CN", q=ticker, offset=0, limit=5)
    for item in items:
        item_ticker = str(item.get("ticker") or item.get("symbol") or "").replace("CN:", "")
        if item_ticker == ticker:
            sym = str(item.get("symbol") or f"CN:{ticker}")
            if not sym.startswith("CN:"):
                sym = f"CN:{sym}"
            return {
                "symbol": sym,
                "name": str(item.get("name") or ticker),
                "confidence": 0.85,
                "rationale": "Ticker match",
            }
    return None


def _lookup_by_name(name: str) -> dict[str, Any] | None:
    candidates = search_cn_candidates([name], limit=6)
    if not candidates:
        return None
    top = candidates[0]
    sym = str(top.get("symbol") or "")
    if sym and not sym.startswith("CN:"):
        ticker = str(top.get("ticker") or sym)
        sym = f"CN:{ticker}" if ticker else sym
    return {
        "symbol": sym,
        "name": str(top.get("name") or name),
        "confidence": 0.75,
        "rationale": "Name search match",
    }


def resolve_a_share_mapping(
    raw_symbols: list[str],
    *,
    logic_summary: str | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Resolve raw mapping strings. Returns (resolved symbols, unresolved raw strings)."""
    resolved: list[dict[str, Any]] = []
    unresolved: list[str] = []
    seen: set[str] = set()
    rationale = (logic_summary or "LLM mapping").strip()[:200]

    for raw in raw_symbols[:3]:
        text = str(raw or "").strip()
        if not text:
            continue
        ticker = _normalize_ticker(text)
        match: dict[str, Any] | None = None
        if ticker:
            match = _lookup_by_ticker(ticker)
        if match is None:
            match = _lookup_by_name(text)

        if match:
            sym = str(match.get("symbol") or "")
            if sym in seen:
                continue
            seen.add(sym)
            resolved.append(
                {
                    "symbol": sym,
                    "name": str(match.get("name") or text),
                    "confidence": float(match.get("confidence") or 0.75),
                    "rationale": str(match.get("rationale") or rationale),
                }
            )
        else:
            unresolved.append(text)

    return resolved, unresolved


def map_trend_hybrid(
    *,
    trend_id: str,
    trend: dict[str, Any],
    hot_industry_names: list[str] | None = None,
    mainline_by_industry: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Hybrid mapping: resolve a_share_mapping locally, fallback to map-cn LLM."""
    raw_mapping = list(
        trend.get("a_share_mapping")
        or trend.get("aShareMapping")
        or trend.get("keywords_for_mapping")
        or trend.get("keywordsForMapping")
        or []
    )
    logic_summary = str(
        trend.get("logic_summary") or trend.get("logicSummary") or ""
    ).strip() or None

    resolved, unresolved = resolve_a_share_mapping(raw_mapping, logic_summary=logic_summary)

    if len(resolved) >= 1:
        from data_sync_service.db.alpha_radar import update_trend_mapping
        from data_sync_service.service.alpha_radar_risk import compute_risk_status

        keywords = list(trend.get("keywords_for_mapping") or raw_mapping)
        macro = str(trend.get("macro_theme") or trend.get("macroTheme") or "")
        if macro and macro not in keywords:
            keywords.insert(0, macro)
        confidence = max(float(r.get("confidence") or 0) for r in resolved)
        risk_status = compute_risk_status(
            keywords=keywords,
            hot_industry_names=hot_industry_names or [],
            mainline_by_industry=mainline_by_industry or {},
        )
        update_trend_mapping(
            trend_id=trend_id,
            cn_symbols=resolved,
            mapping_confidence=confidence,
            risk_status=risk_status,
        )
        return {
            "trendId": trend_id,
            "cnSymbols": resolved,
            "mappingConfidence": confidence,
            "riskStatus": risk_status,
            "mappingMode": "local_resolve",
        }

    seed_keywords = unresolved + [
        str(trend.get("macro_theme") or trend.get("macroTheme") or ""),
    ]
    seed_keywords = [k for k in seed_keywords if k.strip()]
    trend_for_map = {**trend, "keywords_for_mapping": seed_keywords[:8]}

    mapped = map_trend_to_cn(
        trend_id=trend_id,
        trend=trend_for_map,
        hot_industry_names=hot_industry_names,
        mainline_by_industry=mainline_by_industry,
        seed_symbols=unresolved or raw_mapping[:3],
    )
    mapped["mappingMode"] = "map_cn_fallback"
    return mapped
