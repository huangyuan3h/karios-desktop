"""News Substrate 2.0 · Track 3 — Morning Brief generator.

Selects top 5–7 enriched news items for morning (08:30) and midday (12:30)
briefings. Scoring formula:

    score = importance × 0.3 + relevance_score × 0.3 + freshness × 0.2 + watchlist_boost × 0.2

freshness_bonus: items < 2h old = 100, < 6h = 70, < 12h = 40, else 10.
watchlist_boost: held ticker mention = 50, watched ticker = 30, sector match = 20.

Morning brief focuses on overnight + pre-market; midday focuses on
morning session.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from data_sync_service.db.morning_brief import upsert_brief
from data_sync_service.db.news import fetch_items

logger = logging.getLogger(__name__)

BRIEF_SIZE = 7  # top N items per brief
MORNING_MODEL_VERSION = "brief-v2"

# Exclude backward-looking patterns from brief
EXCLUDE_TITLE_PATTERNS = [
    "月度总结", "回顾", "月跌", "月涨", "上半年回顾", "年度回顾",
    "YTD", "Year-to-date", "月报", "半年报", "年报",
]

# Category assignment rules
CATEGORY_RULES: list[tuple[str, list[str]]] = [
    ("risk", ["制裁", "实体清单", "关税", "战争", "冲突", "制裁", "禁令", "限制"]),
    ("macro", ["央行", "货币政策", "GDP", "CPI", "PMI", "利率", "降准", "降息"]),
    ("sector", ["板块", "行业", "概念", "赛道"]),
]


def _freshness_bonus(published_at: str | None, fetched_at: str) -> int:
    """Compute freshness bonus 0–100 based on how recent the item is."""
    now = datetime.now(UTC)
    ref = published_at or fetched_at
    try:
        ts = datetime.fromisoformat(ref.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return 10

    age_hours = max(0, (now - ts).total_seconds() / 3600)
    if age_hours < 2:
        return 100
    if age_hours < 6:
        return 70
    if age_hours < 12:
        return 40
    return 10


def _load_watchlist_context() -> tuple[set[str], set[str]]:
    """Load held symbols and sectors from watchlist_registry + score_daily.

    Returns (held_symbols, held_sectors).
    held_symbols: symbols where positionPct > 0 (user actually owns these).
    held_sectors: industries from watchlist_score_daily for held symbols.

    Note: items in watchlist_registry WITHOUT broker positionPct are still
    treated as "watched" via `_load_watched_symbols()` — used by the
    watchlist boost and category assignment so an Alpha-Radar-registered
    stock with no broker integration still surfaces as "watchlist".
    """
    held_symbols: set[str] = set()
    held_sectors: set[str] = set()

    try:
        from data_sync_service.db import get_connection

        with get_connection() as conn:
            with conn.cursor() as cur:
                # Load registry with payload (contains positionPct)
                cur.execute(
                    "SELECT symbol, payload FROM watchlist_registry"
                )
                for row in cur.fetchall():
                    sym = str(row[0])
                    payload = row[1] if isinstance(row[1], dict) else {}
                    if not isinstance(payload, dict):
                        continue
                    pos_pct = payload.get("positionPct", 0)
                    if isinstance(pos_pct, (int, float)) and pos_pct > 0:
                        held_symbols.add(sym)
                        # Also add without suffix for matching
                        bare = sym.split(".")[0] if "." in sym else sym
                        held_symbols.add(bare)

                # Load sectors from score_daily for held symbols
                if held_symbols:
                    cur.execute(
                        "SELECT DISTINCT industry FROM watchlist_score_daily "
                        "WHERE symbol = ANY(%s) AND industry IS NOT NULL AND industry != ''",
                        (list(held_symbols),),
                    )
                    for row in cur.fetchall():
                        if row[0]:
                            held_sectors.add(str(row[0]))
    except Exception as exc:
        logger.debug("Could not load watchlist context: %s", exc)

    return held_symbols, held_sectors


def _load_watched_symbols() -> set[str]:
    """Symbols in watchlist_registry (any source).

    Distinct from `_load_watchlist_context`'s held_symbols: includes
    alpha_radar/screener/manual entries even without broker payload.
    """
    try:
        from data_sync_service.db import get_connection

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT symbol FROM watchlist_registry")
                rows = cur.fetchall()
        out: set[str] = set()
        for r in rows:
            sym = str(r[0])
            out.add(sym)
            bare = sym.split(":", 1)[-1] if ":" in sym else sym
            bare = bare.split(".")[0] if "." in bare else bare
            out.add(bare)
        return out
    except Exception:
        return set()


def _watchlist_boost(
    item: dict[str, Any],
    held_symbols: set[str],
    held_sectors: set[str],
    watched_symbols: set[str] | None = None,
) -> int:
    """Compute watchlist boost 0–50 based on ticker/sector overlap.

    Priority:
      - Held (broker positionPct > 0): +50
      - Watched (in watchlist_registry, no broker): +30
      - Sector match (held/watchlist sector): +20
      - No match: 0
    """
    tickers = set(item.get("tickers") or [])
    sectors = set(item.get("sectors") or [])

    # Check ticker match (both full code and bare code)
    if tickers & held_symbols:
        return 50
    # Check bare symbols (e.g., 600519 from 600519.SH)
    bare_tickers = {t.split(".")[0] for t in tickers if "." in t}
    if bare_tickers & held_symbols:
        return 50

    # Fall back to "watched" (in watchlist_registry but not held)
    if watched_symbols is not None:
        if tickers & watched_symbols:
            return 30
        bare_tickers2 = {t.split(".")[0] for t in tickers if "." in t}
        if bare_tickers2 & watched_symbols:
            return 30

    # Check sector match
    if sectors & held_sectors:
        return 20

    return 0


def _assign_category(item: dict[str, Any], watched_symbols: set[str]) -> str:
    """Assign a brief category: watchlist, risk, macro, sector.

    `watched_symbols` should include both held and registry entries so
    items mentioning an Alpha-Radar-registered ticker also get the
    "watchlist" bucket.
    """
    tickers = set(item.get("tickers") or [])
    bare_tickers = {t.split(".")[0] for t in tickers if "." in t}

    # If mentions any ticker in the user's watchlist (held or watched),
    # it's watchlist-related.
    if (tickers & watched_symbols) or (bare_tickers & watched_symbols):
        return "watchlist"

    title = (item.get("title") or "").lower()
    ai_summary = (item.get("aiSummary") or "").lower()
    text = f"{title} {ai_summary}"

    for category, keywords in CATEGORY_RULES:
        if any(kw in text for kw in keywords):
            return category

    return "macro"


def _score_item(
    item: dict[str, Any],
    held_symbols: set[str],
    held_sectors: set[str],
    watched_symbols: set[str] | None = None,
) -> float:
    """Score a news item for brief selection with watchlist awareness.

    Final score (0–100+):
      - importance × 0.3   (LLM-rated 0–5)
      - relevance × 0.3    (importance × 15 + watchlist boost, 0–100)
      - freshness × 0.2    (recency bonus 10/40/70/100)
      - watchlist_boost × 0.2  (held=50 / watched=30 / sector=20 / 0)

    Plus a +5 nudge for `actionability == "actionable"` so items the user
    can act on today surface above purely background context.
    """
    importance = item.get("importance") or 0
    relevance = item.get("relevanceScore") or 0
    freshness = _freshness_bonus(item.get("publishedAt"), item.get("fetchedAt", ""))
    boost = _watchlist_boost(item, held_symbols, held_sectors, watched_symbols)
    score = importance * 0.3 + relevance * 0.3 + freshness * 0.2 + boost * 0.2
    if item.get("actionability") == "actionable":
        score += 5
    return score


def _is_excluded(item: dict[str, Any]) -> bool:
    """Check if item should be excluded from brief (backward-looking patterns)."""
    title = item.get("title") or ""
    return any(p in title for p in EXCLUDE_TITLE_PATTERNS)


def select_brief_items(hours: int = 24, limit: int = 200) -> list[dict[str, Any]]:
    """Fetch enriched items and select top N for a brief."""
    _total, items = fetch_items(limit=limit, hours=hours)

    # Load watchlist context
    held_symbols, held_sectors = _load_watchlist_context()
    watched_symbols = _load_watched_symbols()

    # Only consider items that have been enriched
    enriched = [i for i in items if i.get("enrichmentStatus") == "done"]

    # Exclude backward-looking patterns
    enriched = [i for i in enriched if not _is_excluded(i)]

    # Exclude actionability == "historical" if field exists
    enriched = [i for i in enriched if i.get("actionability") != "historical"]

    # Skip noise (LLM scored importance=0): brief is for "worth knowing"
    # news, not noise pre-filter.
    enriched = [i for i in enriched if (i.get("importance") or 0) >= 1]

    # Score and sort
    scored = [
        (item, _score_item(item, held_symbols, held_sectors, watched_symbols))
        for item in enriched
    ]
    scored.sort(key=lambda x: x[1], reverse=True)

    # Assign categories and build result
    result = []
    for item, score in scored[:BRIEF_SIZE]:
        category = _assign_category(item, watched_symbols)
        result.append({
            "id": item["id"],
            "title": item["title"],
            "sourceId": item.get("sourceId"),
            "publishedAt": item.get("publishedAt"),
            "tickers": item.get("tickers") or [],
            "sectors": item.get("sectors") or [],
            "eventType": item.get("eventType"),
            "importance": item.get("importance"),
            "relevanceScore": item.get("relevanceScore"),
            "aiSummary": item.get("aiSummary"),
            "actionability": item.get("actionability"),
            "link": item.get("link"),
            "score": round(score, 1),
            "category": category,
        })

    return result


def generate_brief(brief_type: str = "morning") -> dict[str, Any]:
    """Generate and store a morning or midday brief.

    brief_type: 'morning' (08:30) or 'midday' (12:30)

    Returns the stored brief dict.
    """
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    items = select_brief_items(hours=24)
    source_ids = [it["id"] for it in items]

    # Build macro overview from categories and sectors
    category_counts: dict[str, int] = {}
    sector_counts: dict[str, int] = {}
    event_counts: dict[str, int] = {}
    for it in items:
        cat = it.get("category", "macro")
        category_counts[cat] = category_counts.get(cat, 0) + 1
        for s in it.get("sectors", []):
            sector_counts[s] = sector_counts.get(s, 0) + 1
        et = it.get("eventType")
        if et:
            event_counts[et] = event_counts.get(et, 0) + 1

    overview_parts = []
    if category_counts:
        cat_str = "、".join(f"{c}({n})" for c, n in category_counts.items())
        overview_parts.append(f"分类: {cat_str}")
    if sector_counts:
        top_sectors = sorted(sector_counts.items(), key=lambda x: x[1], reverse=True)[:3]
        overview_parts.append("热门板块: " + "、".join(f"{s}({c})" for s, c in top_sectors))
    if event_counts:
        top_events = sorted(event_counts.items(), key=lambda x: x[1], reverse=True)[:3]
        overview_parts.append("事件类型: " + "、".join(f"{e}({c})" for e, c in top_events))

    macro_overview = "；".join(overview_parts) if overview_parts else None

    brief = upsert_brief(
        brief_date=today,
        brief_type=brief_type,
        items=items,
        macro_overview=macro_overview,
        model_version=MORNING_MODEL_VERSION,
        source_item_ids=source_ids,
    )

    logger.info(
        "Generated %s brief for %s: %d items (top score=%.1f)",
        brief_type,
        today,
        len(items),
        items[0]["score"] if items else 0,
    )
    return brief
