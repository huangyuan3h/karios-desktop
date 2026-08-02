"""News Substrate 2.0 · Track 2 — LLM enrichment worker.

Batch-processes un-enriched news_items through the ai-service's
/v1/chat/completions endpoint (OpenAI-compatible).

Pipeline per item:
  1. Extract tickers (A-share 6-digit / HK 5-digit), sectors, event_type.
  2. Rate importance 0–5.
  3. Watchlist-aware relevance_score 0–100 (+30 for watchlist, +50 for held).
  4. One-paragraph ai_summary (≤ 120 chars).
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from datetime import UTC, datetime
from typing import Any

from data_sync_service.config import get_settings
from data_sync_service.db.news import (
    fetch_pending_enrichment,
    update_item_enrichment,
)

logger = logging.getLogger(__name__)

# Model used for enrichment (cheap + fast; upgrade to gpt-4o if quality insufficient)
ENRICHMENT_MODEL = os.getenv("NEWS_ENRICHMENT_MODEL", "gpt-4o-mini")

# Batch size per LLM call (token budget)
BATCH_SIZE = int(os.getenv("NEWS_ENRICHMENT_BATCH_SIZE", "10"))

# Max retries on transient failures
MAX_RETRIES = 3

# Watchlist symbols loaded once per process
_WATCHLIST_CACHE: list[str] | None = None


def _get_watchlist_symbols() -> list[str]:
    """Load user watchlist symbols for relevance scoring."""
    global _WATCHLIST_CACHE  # noqa: PLW0603
    if _WATCHLIST_CACHE is not None:
        return _WATCHLIST_CACHE
    try:
        from data_sync_service.db.watchlist import fetch_registry

        rows = fetch_registry()
        _WATCHLIST_CACHE = [str(r.get("symbol") or r.get("tsCode") or "") for r in rows]
        _WATCHLIST_CACHE = [s for s in _WATCHLIST_CACHE if s]
    except Exception:
        _WATCHLIST_CACHE = []
    return _WATCHLIST_CACHE


def _build_prompt(items: list[dict[str, Any]], watchlist: list[str]) -> str:
    """Build the LLM prompt for a batch of news items."""
    watchlist_str = ", ".join(watchlist[:50]) if watchlist else "(empty)"

    item_blocks = []
    for i, item in enumerate(items):
        block = f"[{i}] id={item['id']}\ntitle: {item['title']}"
        if item.get("summary"):
            block += f"\nsummary: {item['summary'][:300]}"
        if item.get("sourceId"):
            block += f"\nsource: {item['sourceId']}"
        item_blocks.append(block)

    items_text = "\n\n".join(item_blocks)

    return f"""You are an investment analyst for Chinese A-share / HK markets.

For each news item below, extract:
1. tickers: A-share codes (6-digit, e.g. 600519) or HK codes (5-digit, e.g. 00700).
   Only include tickers explicitly mentioned or clearly inferable from the title/summary.
2. sectors: Chinese sector names (e.g. 白酒, 新能源, 半导体).
3. event_type: One of: earnings, macro, policy, m&a, ipo, dividend, analyst, sector, other.
4. importance: 0–5 integer.
   0 = noise/ads, 1 = minor, 2 = noteworthy, 3 = market-relevant,
   4 = sector-moving, 5 = market-wide critical event.
5. relevance_score: 0–100 integer.
   Base score from importance × 15. Then:
   +30 if any ticker is in the user's watchlist.
   +50 if any ticker is in the user's held positions (subset of watchlist).
   Cap at 100.
6. ai_summary: One-sentence Chinese summary ≤ 30 characters.
7. actionability: One of: actionable, informational, historical.
   actionable = concrete catalyst needing today's decision (earnings, delivery numbers, policy change, price move).
   informational = background context (macro overview, meeting summary, geopolitical context).
   historical = backward-looking summary (monthly review, year-to-date, past performance recap).

User watchlist symbols: {watchlist_str}

Return a JSON array with one object per item (same order), each with keys:
id, tickers, sectors, eventType, importance, relevanceScore, aiSummary, actionability.

Items:
{items_text}"""


def _call_llm(prompt: str) -> str:
    """Call the ai-service /v1/chat/completions endpoint."""
    settings = get_settings()
    base = settings.ai_service_base_url

    payload = json.dumps(
        {
            "model": ENRICHMENT_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }
    ).encode("utf-8")

    headers: dict[str, str] = {"Content-Type": "application/json"}
    if settings.karios_api_keys:
        headers["Authorization"] = f"Bearer {settings.karios_api_keys[0]}"

    req = urllib.request.Request(
        f"{base}/v1/chat/completions",
        data=payload,
        headers=headers,
        method="POST",
    )

    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = json.loads(resp.read().decode("utf-8") or "{}")
                content = (
                    body.get("choices", [{}])[0]
                    .get("message", {})
                    .get("content", "")
                )
                return content
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError) as exc:
            if attempt == MAX_RETRIES - 1:
                raise RuntimeError(f"LLM call failed after {MAX_RETRIES} retries: {exc}") from exc
            logger.warning("LLM attempt %d failed: %s", attempt + 1, exc)

    return ""  # unreachable, but satisfies type checker


def _parse_llm_response(raw: str, item_ids: list[str]) -> list[dict[str, Any]]:
    """Parse LLM JSON response into a list of enrichment dicts, one per item_id."""
    if not raw:
        return []

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning("LLM returned invalid JSON: %s", exc)
        return []

    # LLM may return {"items": [...]} or just [...]
    if isinstance(parsed, dict):
        for key in ("items", "results", "data"):
            if key in parsed and isinstance(parsed[key], list):
                parsed = parsed[key]
                break

    if not isinstance(parsed, list):
        return []

    # Pad or truncate to match item_ids length
    results: list[dict[str, Any]] = []
    for i, item_id in enumerate(item_ids):
        if i < len(parsed) and isinstance(parsed[i], dict):
            entry = parsed[i]
            entry["id"] = item_id  # enforce correct id
            results.append(entry)
        else:
            results.append({"id": item_id})
    return results


def _validate_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize a single enrichment entry."""
    tickers = entry.get("tickers") or []
    if not isinstance(tickers, list):
        tickers = []
    tickers = [str(t).strip() for t in tickers if t]

    sectors = entry.get("sectors") or []
    if not isinstance(sectors, list):
        sectors = []
    sectors = [str(s).strip() for s in sectors if s]

    event_type = str(entry.get("eventType") or entry.get("event_type") or "other")
    valid_events = {"earnings", "macro", "policy", "m&a", "ipo", "dividend", "analyst", "sector", "other"}
    if event_type not in valid_events:
        event_type = "other"

    importance = entry.get("importance")
    if isinstance(importance, (int, float)) and 0 <= importance <= 5:
        importance = int(importance)
    else:
        importance = 0

    relevance = entry.get("relevanceScore") or entry.get("relevance_score")
    if isinstance(relevance, (int, float)) and 0 <= relevance <= 100:
        relevance = int(relevance)
    else:
        relevance = min(importance * 15, 100)

    ai_summary = str(entry.get("aiSummary") or entry.get("ai_summary") or "")[:300]

    actionability = str(entry.get("actionability") or "informational")
    valid_actionability = {"actionable", "informational", "historical"}
    if actionability not in valid_actionability:
        actionability = "informational"

    return {
        "tickers": tickers,
        "sectors": sectors,
        "event_type": event_type,
        "importance": importance,
        "relevance_score": relevance,
        "ai_summary": ai_summary,
        "actionability": actionability,
    }


def enrich_batch(items: list[dict[str, Any]]) -> int:
    """Enrich a batch of news items via LLM. Returns count of successfully enriched items."""
    if not items:
        return 0

    watchlist = _get_watchlist_symbols()
    item_ids = [item["id"] for item in items]

    prompt = _build_prompt(items, watchlist)
    raw = _call_llm(prompt)
    parsed = _parse_llm_response(raw, item_ids)

    enriched_count = 0
    for entry in parsed:
        item_id = entry.get("id", "")
        if not item_id:
            continue
        validated = _validate_entry(entry)
        ok = update_item_enrichment(
            item_id=item_id,
            tickers=validated["tickers"],
            sectors=validated["sectors"],
            event_type=validated["event_type"],
            importance=validated["importance"],
            relevance_score=validated["relevance_score"],
            ai_summary=validated["ai_summary"],
            actionability=validated["actionability"],
            enrichment_status="done",
            enrichment_model=ENRICHMENT_MODEL,
        )
        if ok:
            enriched_count += 1

    return enriched_count


def run_enrichment_cycle(max_batches: int = 10) -> dict[str, Any]:
    """Run enrichment on all pending items, up to max_batches rounds.

    Returns a summary dict with counts.
    """
    total_enriched = 0
    total_failed = 0
    batches_processed = 0

    for _batch_idx in range(max_batches):
        pending = fetch_pending_enrichment(limit=BATCH_SIZE)
        if not pending:
            break

        try:
            count = enrich_batch(pending)
            total_enriched += count
            total_failed += len(pending) - count
        except Exception as exc:
            logger.error("Enrichment batch failed: %s", exc)
            # Mark all items in this batch as failed so we don't retry forever
            for item in pending:
                update_item_enrichment(
                    item_id=item["id"],
                    enrichment_status="failed",
                    enrichment_model=ENRICHMENT_MODEL,
                )
            total_failed += len(pending)

        batches_processed += 1

    return {
        "batchesProcessed": batches_processed,
        "totalEnriched": total_enriched,
        "totalFailed": total_failed,
        "model": ENRICHMENT_MODEL,
    }
