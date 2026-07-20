"""Aggregate Alpha Radar trends into stock-centric catalyst scores."""

from __future__ import annotations

import math
import os
from datetime import UTC, datetime
from typing import Any

from data_sync_service.db.alpha_radar import fetch_trends_for_catalyst

STALENESS_BASIS = "published_then_fetched"
DEFAULT_MAX_AGE_DAYS = 30
RECENCY_HALF_LIFE_DAYS = 14

URGENCY_WEIGHT: dict[str, float] = {
    "S": 1.0,
    "A": 0.85,
    "B": 0.65,
    "C": 0.45,
}


def default_max_age_days() -> int:
    raw = os.environ.get("ALPHA_RADAR_CATALYST_MAX_AGE_DAYS", str(DEFAULT_MAX_AGE_DAYS))
    try:
        return max(1, int(raw))
    except ValueError:
        return DEFAULT_MAX_AGE_DAYS


def _parse_iso_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def event_at_for_trend(trend: dict[str, Any]) -> datetime | None:
    published = _parse_iso_dt(trend.get("documentPublishedAt"))
    if published:
        return published
    return _parse_iso_dt(trend.get("documentFetchedAt"))


def age_days(event_at: datetime, now: datetime | None = None) -> float:
    ref = now or datetime.now(UTC)
    delta = ref - event_at
    return max(0.0, delta.total_seconds() / 86400.0)


def recency_decay(age_days_val: float, half_life_days: float = RECENCY_HALF_LIFE_DAYS) -> float:
    if half_life_days <= 0:
        return 1.0
    return math.exp(-math.log(2) * age_days_val / half_life_days)


def urgency_weight(level: str | None) -> float:
    key = str(level or "B").strip().upper()
    return URGENCY_WEIGHT.get(key, URGENCY_WEIGHT["B"])


def article_contribution(
    *,
    confidence: float,
    urgency_level: str | None,
    event_at: datetime,
    now: datetime | None = None,
) -> float:
    conf = max(0.0, min(float(confidence), 1.0))
    recency = recency_decay(age_days(event_at, now), RECENCY_HALF_LIFE_DAYS)
    return conf * urgency_weight(urgency_level) * recency


def compute_stock_catalyst_score(contributions: list[float]) -> float:
    """Combine per-article contributions into a 0-100 catalyst score."""
    if not contributions:
        return 0.0
    sorted_vals = sorted(contributions, reverse=True)
    primary = sorted_vals[0]
    secondary = min(sum(sorted_vals[1:]), 2.0 * primary)
    top3 = sorted_vals[:3]
    avg_top3 = sum(top3) / len(top3)
    breadth = (min(math.sqrt(len(sorted_vals)), 2.0) / 2.0) * avg_top3
    raw = 0.55 * primary + 0.30 * secondary + 0.15 * breadth
    return round(100.0 * min(raw, 1.0), 1)


def _normalize_symbol(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    if text.startswith("CN:"):
        return text[3:]
    return text


def _article_summary(trend: dict[str, Any]) -> str | None:
    event_focus = trend.get("eventFocus") or trend.get("event_focus")
    if event_focus and str(event_focus).strip():
        return str(event_focus).strip()
    doc_summary = trend.get("documentSummary")
    if doc_summary and str(doc_summary).strip():
        return str(doc_summary).strip()
    catalyst = trend.get("catalyst")
    if catalyst and str(catalyst).strip():
        return str(catalyst).strip()
    return None


def aggregate_catalyst_stocks(
    trends: list[dict[str, Any]],
    *,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    ref = now or datetime.now(UTC)
    by_symbol: dict[str, dict[str, Any]] = {}

    for trend in trends:
        event_at = event_at_for_trend(trend)
        if not event_at:
            continue
        document_id = str(trend.get("documentId") or "")
        for cn in trend.get("cnSymbols") or []:
            symbol_raw = str(cn.get("symbol") or "").strip()
            if not symbol_raw:
                continue
            symbol = _normalize_symbol(symbol_raw)
            name = str(cn.get("name") or symbol).strip()
            try:
                confidence = float(cn.get("confidence") or 0.0)
            except (TypeError, ValueError):
                confidence = 0.0
            contribution = article_contribution(
                confidence=confidence,
                urgency_level=str(
                    trend.get("catalystGrade") or trend.get("urgencyLevel") or "B"
                ),
                event_at=event_at,
                now=ref,
            )
            dedupe_key = f"{document_id}:{trend.get('id')}"
            article = {
                "trendId": str(trend.get("id") or ""),
                "trendName": str(trend.get("trendName") or ""),
                "macroTheme": str(trend.get("macroTheme") or trend.get("trendName") or ""),
                "catalystGrade": str(
                    trend.get("catalystGrade") or trend.get("urgencyLevel") or "B"
                ),
                "driverType": str(
                    trend.get("driverType") or trend.get("driver_type") or "Global_Tech"
                ),
                "eventFocus": str(trend.get("eventFocus") or trend.get("event_focus") or "").strip()
                or None,
                "logicSummary": str(trend.get("logicSummary") or trend.get("logic_summary") or "").strip()
                or None,
                "catalyst": str(trend.get("catalyst") or "").strip() or None,
                "globalTarget": str(trend.get("globalTarget") or "").strip() or None,
                "documentId": document_id,
                "relevance": round(confidence, 4),
                "contribution": round(contribution, 4),
                "documentTitle": trend.get("documentTitle"),
                "documentUrl": trend.get("documentUrl"),
                "summary": _article_summary(trend),
                "publishedAt": trend.get("documentPublishedAt") or trend.get("documentFetchedAt"),
                "urgencyLevel": str(trend.get("urgencyLevel") or "B"),
                "_dedupeKey": dedupe_key,
                "_eventAt": event_at,
            }

            bucket = by_symbol.setdefault(
                symbol,
                {
                    "symbol": symbol,
                    "name": name,
                    "articlesByDoc": {},
                },
            )
            if name and (not bucket.get("name") or bucket["name"] == symbol):
                bucket["name"] = name

            existing = bucket["articlesByDoc"].get(document_id)
            if existing is None or contribution > float(existing.get("contribution") or 0):
                bucket["articlesByDoc"][document_id] = article

    results: list[dict[str, Any]] = []
    for bucket in by_symbol.values():
        articles = list(bucket["articlesByDoc"].values())
        articles.sort(key=lambda a: float(a.get("contribution") or 0), reverse=True)
        contributions = [float(a.get("contribution") or 0) for a in articles]
        latest_dt = max((a["_eventAt"] for a in articles), default=None)
        clean_articles = []
        for a in articles:
            clean_articles.append(
                {
                    "trendId": a["trendId"],
                    "trendName": a["trendName"],
                    "macroTheme": a["macroTheme"],
                    "catalystGrade": a["catalystGrade"],
                    "driverType": a.get("driverType"),
                    "eventFocus": a.get("eventFocus"),
                    "logicSummary": a.get("logicSummary"),
                    "catalyst": a.get("catalyst"),
                    "globalTarget": a.get("globalTarget"),
                    "documentId": a["documentId"],
                    "relevance": a["relevance"],
                    "contribution": a["contribution"],
                    "documentTitle": a["documentTitle"],
                    "documentUrl": a["documentUrl"],
                    "summary": a["summary"],
                    "publishedAt": a["publishedAt"],
                    "urgencyLevel": a["urgencyLevel"],
                }
            )
        results.append(
            {
                "symbol": bucket["symbol"],
                "name": bucket["name"],
                "catalystScore": compute_stock_catalyst_score(contributions),
                "articleCount": len(clean_articles),
                "latestArticleAt": latest_dt.isoformat() if latest_dt else None,
                "articles": clean_articles,
            }
        )

    results.sort(
        key=lambda row: (float(row.get("catalystScore") or 0), str(row.get("latestArticleAt") or "")),
        reverse=True,
    )
    return results


def list_catalyst_stocks(
    *,
    limit: int = 50,
    max_age_days: int | None = None,
) -> dict[str, Any]:
    age_days_param = max_age_days if max_age_days is not None else default_max_age_days()
    trends = fetch_trends_for_catalyst(max_age_days=age_days_param)
    items = aggregate_catalyst_stocks(trends)
    lim = max(1, min(int(limit), 200))
    return {
        "stalenessBasis": STALENESS_BASIS,
        "maxAgeDays": age_days_param,
        "total": len(items),
        "items": items[:lim],
    }
