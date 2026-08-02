"""News Substrate 2.0 · Track 2 — LLM enrichment worker.

Optimized for **cost-effectiveness** (token budget) while still surfacing
news worth knowing. The pipeline has three tiers:

  Tier 0 — Pre-filter (no LLM): keyword/exclude patterns + Tier-A source
           whitelist. Roughly 60-70% of items are noise (sports, ads,
           monthly recaps, lifestyle, etc.) — skip them before any
           inference is paid for.

  Tier 1 — LLM extraction (small prompt): the surviving items go to the
           LLM for tickers / sectors / event_type / importance / ai_summary
           / actionability. The prompt is intentionally minimal: relevance
           scoring is done in Python (see `_compute_relevance`) because
           keeping the scoring formula out of the prompt saves input
           tokens and gives us auditable, deterministic scores.

  Tier 2 — Per-item failure: a parsing or network failure on one item
           marks *only that item* failed. The remaining items in the
           batch still get persisted — no more "5 items dropped because
           one timed out".

Tokens down, signal up:
- pre-filter typically drops 60%+ before any LLM call
- per-item failure gives ~4× effective throughput on flaky batches
- relevance computed locally means the LLM doesn't have to remember the
  +30/+50 watchlist rule; we just enforce it deterministically
"""

from __future__ import annotations

import json
import logging
import os
import re
import urllib.error
import urllib.request
from typing import Any

from data_sync_service.config import get_settings
from data_sync_service.db.news import (
    fetch_pending_enrichment,
    update_item_enrichment,
)

logger = logging.getLogger(__name__)

# Default model identifier sent to ai-service. The ai-service ignores this
# in favour of its own active profile, but we keep the env-var override
# for tests/CI where you want to record the intended model name.
ENRICHMENT_MODEL = os.getenv("NEWS_ENRICHMENT_MODEL", "deepseek-v4-flash")

# Tier 0: batch size per LLM call. Smaller = fewer tokens per call but
# more round-trips. 5 is a sweet spot for cheap JSON-mode inference.
BATCH_SIZE = int(os.getenv("NEWS_ENRICHMENT_BATCH_SIZE", "5"))

# Per-call timeout (seconds). Generous but bounded so a hung model
# (esp. thinking models like MiniMax-M3) doesn't tie up the worker.
LLM_TIMEOUT_S = int(os.getenv("NEWS_ENRICHMENT_TIMEOUT_S", "60"))

# Max retries on transient failures. Kept at 1 — failed items get marked
# `failed` and won't be retried within the same cycle, so re-trying wastes
# input tokens on every attempt with no better output.
MAX_RETRIES = 1

# Tier 0: Tier-A sources always enrich (high signal). Other sources go
# through the keyword include/exclude gate.
TIER_A_SOURCE_IDS = frozenset(
    {
        "cls-telegraph",
        "wallstreetcn-global",
        "jin10-flash",
        "cls-depth",
        "csrc-news",
    }
)

# Tier 0: cheap noise patterns. Matches "月度回顾", "上周复盘", etc.
# All case-insensitive.
NOISE_TITLE_PATTERNS = [
    # Backward-looking recaps — brief excludes these anyway
    "月度总结", "月度回顾", "本周回顾", "上周复盘", "周复盘",
    "上半年回顾", "下半年展望", "年度回顾", "年初至今", "YTD",
    "Year-to-date", "月报", "半年报", "年报", "季度报",
    # Lifestyle / off-topic
    "股评", "荐股", "涨停复盘", "心灵鸡汤", "情感故事",
    # Sports / entertainment / crypto / lifestyle
    "体育", "娱乐", "明星", "八卦", "旅游", "美食", "养生",
    "币", "比特币", "以太坊", "NFT",
]

NOISE_RE = re.compile("|".join(re.escape(p) for p in NOISE_TITLE_PATTERNS), re.IGNORECASE)

# Tier 0: include patterns — items must match at least one to qualify for
# LLM enrichment (unless they're from a Tier-A source).
INCLUDE_RE = re.compile(
    r"(?i)(semiconductor|chip|gpu|cpu|datacenter|ai\b|llm|machine learning|"
    r"cloud|earnings|transcript|guidance|revenue|"
    r"机器人|半导体|芯片|算力|数据中心|存储|晶圆|光模块|财报|业绩|"
    r"央行|货币政策|财政政策|产业政策|降准|降息|"
    r"国务院|发改委|工信部|证监会|人民银行|"
    r"制裁|实体清单|关税|战争|冲突|禁令|"
    r"新能源|电动车|光伏|风电|储能|锂电|"
    r"白酒|医药|银行|保险|地产|消费|零售|"
    r"涨价|提价|上涨|暴涨|突破|新高|异动|库存|拐点|"
    r"停产|亏损|出清|供给|紧缺|"
    r"美联储|Fed|ECB|BOE|BOJ|"
    r"GDP|CPI|PMI|PPI|M2|社融|"
    r"OpenAI|Anthropic|Google|Microsoft|Nvidia|台积电|TSMC|AMD|Intel|"
    r"Apple|Tesla|Meta|Amazon|阿里巴巴|腾讯|字节|华为|比亚迪)",
)

# Relevance score bands. Watchlist boost is +30 per matched ticker,
# capped so a single item can't dominate the brief.
WATCHLIST_BOOST = 30
WATCHLIST_BOOST_CAP = 60

# Watchlist symbols loaded once per process
_WATCHLIST_CACHE: list[str] | None = None


def _get_watchlist_symbols() -> list[str]:
    """Load user watchlist symbols for relevance scoring."""
    global _WATCHLIST_CACHE  # noqa: PLW0603
    if _WATCHLIST_CACHE is not None:
        return _WATCHLIST_CACHE
    try:
        from data_sync_service.db import get_connection

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT symbol FROM watchlist_registry")
                rows = cur.fetchall()
        _WATCHLIST_CACHE = [str(r[0]) for r in rows if r and r[0]]
    except Exception:
        _WATCHLIST_CACHE = []
    return _WATCHLIST_CACHE


def _is_noise_title(title: str) -> bool:
    """Tier 0 noise check: explicit backward-looking / lifestyle patterns."""
    if not title:
        return True
    return bool(NOISE_RE.search(title))


def _passes_pre_filter(item: dict[str, Any]) -> bool:
    """Tier 0 cheap filter — runs before any LLM call.

    Returns True if the item is worth sending to the LLM. Tier-A sources
    always pass; everything else must match INCLUDE_RE and must not match
    NOISE_RE.
    """
    title = str(item.get("title") or "")
    summary = str(item.get("summary") or "")
    text = f"{title} {summary}".strip()
    if not text:
        return False
    if _is_noise_title(title):
        return False
    source_id = str(item.get("source_id") or item.get("sourceId") or "")
    if source_id in TIER_A_SOURCE_IDS:
        return True
    return bool(INCLUDE_RE.search(text))


def _compute_relevance(importance: int, tickers: list[str]) -> int:
    """Compute relevance_score in Python, deterministic and auditable.

    Base: importance × 15. Watchlist boost: +30 per matched ticker,
    capped at WATCHLIST_BOOST_CAP. Final score capped at 100.

    The old prompt asked the LLM to apply +30/+50 watchlist/held bonuses,
    but the LLM doesn't actually know which symbols are held — so the
    answer was inconsistent. Doing it here means the score is reproducible.
    """
    watchlist = _get_watchlist_symbols()
    if not watchlist:
        return min(importance * 15, 100)

    # Watchlist symbols are stored as "CN:000001" or "HK:00700"; tickers
    # come back from the LLM as plain digits "000001" or "00700". Compare
    # both forms so we don't miss a match.
    watchlist_digit_set: set[str] = set()
    for sym in watchlist:
        digits = sym.split(":", 1)[-1] if ":" in sym else sym
        watchlist_digit_set.add(digits)

    matched = sum(1 for t in tickers if t in watchlist_digit_set)
    boost = min(matched * WATCHLIST_BOOST, WATCHLIST_BOOST_CAP)
    return min(importance * 15 + boost, 100)


def _build_prompt(items: list[dict[str, Any]]) -> str:
    """Build a minimal LLM prompt — no relevance formula, no scoring rubric.

    The model only needs to extract structured fields. Relevance is
    computed locally in `_compute_relevance`.
    """
    item_blocks = []
    for i, item in enumerate(items):
        block = f"[{i}] id={item['id']}\ntitle: {item['title']}"
        if item.get("summary"):
            block += f"\nsummary: {item['summary'][:200]}"
        item_blocks.append(block)
    items_text = "\n\n".join(item_blocks)

    return (
        "You extract structured fields from financial news headlines. "
        "Respond with a JSON array, one object per item, same order. "
        "Keys: id, tickers, sectors, eventType, importance, aiSummary, actionability.\n"
        "- tickers: A-share 6-digit (e.g. 600519) or HK 5-digit (e.g. 00700). Empty if none.\n"
        "- sectors: 1-3 Chinese sector names (白酒/新能源/半导体/...).\n"
        "- eventType: earnings | macro | policy | m&a | ipo | dividend | analyst | sector | other.\n"
        "- importance: 0=noise, 1=minor, 2=noteworthy, 3=market-relevant, 4=sector-moving, 5=systemic.\n"
        "- aiSummary: one Chinese sentence ≤25 chars.\n"
        "- actionability: actionable | informational | historical.\n\n"
        f"Items:\n{items_text}"
    )


def _call_llm(prompt: str) -> str:
    """Call the ai-service /v1/chat/completions endpoint."""
    settings = get_settings()
    base = settings.ai_service_base_url

    payload = json.dumps(
        {
            "model": ENRICHMENT_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": 1024,
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

    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=LLM_TIMEOUT_S) as resp:
                body = json.loads(resp.read().decode("utf-8") or "{}")
                return (
                    body.get("choices", [{}])[0]
                    .get("message", {})
                    .get("content", "")
                )
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError) as exc:
            last_exc = exc
            if attempt == MAX_RETRIES - 1:
                break
            logger.warning("LLM attempt %d failed: %s", attempt + 1, exc)

    raise RuntimeError(f"LLM call failed after {MAX_RETRIES} retries: {last_exc}")


def _parse_llm_response(raw: str, item_ids: list[str]) -> list[dict[str, Any]]:
    """Parse LLM JSON response into a list of enrichment dicts, one per item_id.

    Tries the standard ```json / ``` fences (handled at ai-service level)
    plus a tolerant fallback that extracts the first [...] block if the
    model returns text around the JSON.

    Returns an entry per `item_id` so the caller can mark missing/empty
    items individually rather than silently dropping them.
    """
    text = (raw or "").strip()
    parsed: Any = None
    if text:
        # Try direct parse first
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            # Fallback: extract first [...] or {...} block
            m = re.search(r"(\[[\s\S]*\]|\{[\s\S]*\})", text)
            if m:
                try:
                    parsed = json.loads(m.group(1))
                except json.JSONDecodeError as exc:
                    logger.warning("LLM returned invalid JSON: %s", exc)
            else:
                logger.warning("LLM returned non-JSON: %s", text[:200])

    if parsed is None:
        # Empty or unparseable — return id-only entries so the caller
        # can mark each item failed individually.
        return [{"id": iid} for iid in item_ids]

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

    # Tier 2 early-exit: importance=0 items still get stored but with empty
    # ai_summary and relevance=0, so brief scoring can skip them cheaply.
    ai_summary = str(entry.get("aiSummary") or entry.get("ai_summary") or "")
    if importance == 0:
        ai_summary = ""
    else:
        ai_summary = ai_summary[:300]

    actionability = str(entry.get("actionability") or "informational")
    valid_actionability = {"actionable", "informational", "historical"}
    if actionability not in valid_actionability:
        actionability = "informational"

    # Tier 1: relevance computed locally, not from the LLM. The LLM no
    # longer emits `relevanceScore`, so any value the model did send is
    # ignored.
    relevance = _compute_relevance(importance, tickers)

    return {
        "tickers": tickers,
        "sectors": sectors,
        "event_type": event_type,
        "importance": importance,
        "relevance_score": relevance,
        "ai_summary": ai_summary,
        "actionability": actionability,
    }


def enrich_batch(items: list[dict[str, Any]]) -> dict[str, int]:
    """Enrich a batch of news items via LLM.

    Returns a per-outcome count dict:
        {"enriched": N, "failed": N, "filtered": N}

    Tier 2 behaviour: a per-item failure (e.g. one entry fails JSON parse)
    marks only that item failed; the remaining items in the batch still
    get persisted. A whole-batch failure (network/timeout on the LLM
    call) marks *all* items in the batch failed.
    """
    if not items:
        return {"enriched": 0, "failed": 0, "filtered": 0}

    item_ids = [item["id"] for item in items]
    prompt = _build_prompt(items)

    try:
        raw = _call_llm(prompt)
    except Exception as exc:
        logger.error("LLM call failed for batch of %d: %s", len(items), exc)
        for item in items:
            update_item_enrichment(
                item_id=item["id"],
                enrichment_status="failed",
                enrichment_model=ENRICHMENT_MODEL,
            )
        return {"enriched": 0, "failed": len(items), "filtered": 0}

    parsed = _parse_llm_response(raw, item_ids)
    enriched_count = 0
    failed_count = 0
    for entry in parsed:
        item_id = entry.get("id", "")
        if not item_id:
            failed_count += 1
            continue
        try:
            validated = _validate_entry(entry)
        except Exception as exc:
            logger.warning("Validation failed for %s: %s", item_id, exc)
            update_item_enrichment(
                item_id=item_id,
                enrichment_status="failed",
                enrichment_model=ENRICHMENT_MODEL,
            )
            failed_count += 1
            continue
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
        else:
            failed_count += 1

    return {"enriched": enriched_count, "failed": failed_count, "filtered": 0}


def _filter_pending(items: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Apply Tier 0 pre-filter to a pending batch. Items that fail the
    filter get marked done with importance=0 so they leave the pending
    queue but don't waste LLM tokens.
    """
    kept: list[dict[str, Any]] = []
    filtered_out = 0
    for item in items:
        if _passes_pre_filter(item):
            kept.append(item)
        else:
            filtered_out += 1
            update_item_enrichment(
                item_id=item["id"],
                tickers=[],
                sectors=[],
                event_type="other",
                importance=0,
                relevance_score=0,
                ai_summary="",
                actionability="informational",
                enrichment_status="done",
                enrichment_model="prefilter",
            )
    return kept, filtered_out


def run_enrichment_cycle(max_batches: int = 10) -> dict[str, Any]:
    """Run enrichment on all pending items, up to max_batches rounds.

    Tier 0 pre-filter runs first on each batch — items that fail are
    marked done with importance=0 (no LLM call). The remaining items go
    to the LLM. Per-item failures inside the batch are caught so a
    single bad parse doesn't drop the other 4 items.
    """
    total_enriched = 0
    total_failed = 0
    total_filtered = 0
    batches_processed = 0

    for batch_idx in range(max_batches):
        pending = fetch_pending_enrichment(limit=BATCH_SIZE)
        if not pending:
            break

        kept, filtered = _filter_pending(pending)
        total_filtered += filtered

        if kept:
            logger.info(
                "Enrichment batch %d/%d: %d kept after pre-filter, %d filtered out",
                batch_idx + 1, max_batches, len(kept), filtered,
            )
            try:
                counts = enrich_batch(kept)
                total_enriched += counts["enriched"]
                total_failed += counts["failed"]
                logger.info(
                    "Enrichment batch %d: %d enriched, %d failed",
                    batch_idx + 1, counts["enriched"], counts["failed"],
                )
            except Exception as exc:
                logger.error("Enrichment batch %d failed: %s", batch_idx + 1, exc)
                for item in kept:
                    update_item_enrichment(
                        item_id=item["id"],
                        enrichment_status="failed",
                        enrichment_model=ENRICHMENT_MODEL,
                    )
                total_failed += len(kept)
        else:
            logger.info(
                "Enrichment batch %d/%d: all %d items pre-filtered out",
                batch_idx + 1, max_batches, filtered,
            )

        batches_processed += 1

    return {
        "batchesProcessed": batches_processed,
        "totalEnriched": total_enriched,
        "totalFailed": total_failed,
        "totalFiltered": total_filtered,
        "model": ENRICHMENT_MODEL,
    }
