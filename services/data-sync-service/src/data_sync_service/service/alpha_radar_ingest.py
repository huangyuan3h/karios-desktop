"""Alpha Radar RSS ingestion — RSS-first; optional Jina fulltext for priority sources."""

from __future__ import annotations

import hashlib
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from typing import Any

from data_sync_service.db.alpha_radar import (
    create_source,
    disable_sources_except,
    ensure_tables,
    fetch_sources,
    update_source_last_fetch,
    upsert_document,
)
from data_sync_service.service.alpha_radar_filter import filter_feed_items

try:
    import feedparser  # type: ignore[import-not-found]
except ImportError:
    feedparser = None

RSS_USER_AGENT = (
    "Mozilla/5.0 (compatible; KairosAlphaRadar/1.1; +https://github.com/karios-desktop)"
)

DEFAULT_SOURCES: list[tuple[str, str, str, str]] = [
    ("stratechery", "Stratechery", "https://stratechery.com/feed/", "research"),
    (
        "mit-tech-review",
        "MIT Technology Review",
        "https://www.technologyreview.com/feed/",
        "academic",
    ),
    ("ieee-spectrum", "IEEE Spectrum", "https://spectrum.ieee.org/feeds/feed.rss", "academic"),
    (
        "seeking-alpha-tech",
        "Seeking Alpha Tech",
        "https://seekingalpha.com/sector/technology.xml",
        "earnings",
    ),
    ("semianalysis", "SemiAnalysis", "https://www.semianalysis.com/feed", "research"),
    ("next-platform", "The Next Platform", "https://www.nextplatform.com/feed/", "academic"),
    (
        "trendforce",
        "TrendForce News",
        "https://www.trendforce.com/news/feed_v2/",
        "research",
    ),
]

CHINESE_SOURCE_DEFAULTS: list[tuple[str, str, str, str, str]] = [
    (
        "cls-policy",
        "财联社·宏观产业电报",
        "/cls/telegraph",
        "policy",
        "ALPHA_RADAR_RSS_CLS_POLICY",
    ),
    (
        "gov-miit-policy",
        "工信部·政策解读",
        "/gov/miit/zcjd",
        "policy",
        "ALPHA_RADAR_RSS_GOV_MIIT",
    ),
    (
        "gov-ndrc-policy",
        "发改委·新闻发布",
        "/gov/ndrc/xwdt",
        "policy",
        "ALPHA_RADAR_RSS_GOV_NDRC",
    ),
    (
        "wallstreetcn-commodity",
        "华尔街见闻·大宗",
        "/wallstreetcn/live/global",
        "cycle",
        "ALPHA_RADAR_RSS_WALLSTREETCN",
    ),
    (
        "eastmoney-copper",
        "东方财富·铜产业",
        "/eastmoney/search/铜",
        "cycle",
        "ALPHA_RADAR_RSS_EASTMONEY_COPPER",
    ),
    (
        "cls-depth",
        "财联社·深度研判",
        "/cls/depth",
        "consensus",
        "ALPHA_RADAR_RSS_CLS_DEPTH",
    ),
]

DEFAULT_SOURCE_IDS = {sid for sid, _, _, _ in DEFAULT_SOURCES} | {
    sid for sid, _, _, _, _ in CHINESE_SOURCE_DEFAULTS
}
FULLTEXT_PRIORITY_SOURCE_IDS = frozenset({"stratechery"})
YOUTUBE_HOSTS = ("youtube.com", "youtu.be", "www.youtube.com", "m.youtube.com")


def jina_api_key() -> str:
    return os.getenv("JINA_API_KEY", "").strip()


def enrich_fulltext_enabled() -> bool:
    raw = os.getenv("ALPHA_RADAR_ENRICH_FULLTEXT", "0").strip().lower()
    return raw in ("1", "true", "yes", "on")


def max_items_per_source() -> int:
    raw = os.getenv("ALPHA_RADAR_MAX_ITEMS_PER_SOURCE", "5").strip()
    try:
        return max(1, min(int(raw), 50))
    except ValueError:
        return 5


def fulltext_max_per_priority_source() -> int:
    raw = os.getenv("ALPHA_RADAR_FULLTEXT_MAX_PER_SOURCE", "2").strip()
    try:
        return max(0, min(int(raw), 10))
    except ValueError:
        return 2


def rss_timeout_seconds() -> int:
    raw = os.getenv("ALPHA_RADAR_RSS_TIMEOUT", "60").strip()
    try:
        return max(15, min(int(raw), 120))
    except ValueError:
        return 60


def _proxy_url() -> str | None:
    for key in ("https_proxy", "HTTPS_PROXY", "http_proxy", "HTTP_PROXY"):
        val = os.getenv(key, "").strip()
        if val:
            return val
    return None


def _build_opener() -> urllib.request.OpenerDirector:
    handlers: list[Any] = []
    proxy = _proxy_url()
    if proxy:
        handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
    handlers.append(urllib.request.HTTPSHandler())
    return urllib.request.build_opener(*handlers)


_OPENER: urllib.request.OpenerDirector | None = None


def _opener() -> urllib.request.OpenerDirector:
    global _OPENER
    if _OPENER is None:
        _OPENER = _build_opener()
    return _OPENER


def _urlopen(req: urllib.request.Request, *, timeout: int) -> Any:
    return _opener().open(req, timeout=timeout)


def rsshub_base_url() -> str:
    return os.getenv("ALPHA_RADAR_RSSHUB_BASE_URL", "http://127.0.0.1:1200").rstrip("/")


def _chinese_source_url(route: str, env_key: str) -> str:
    override = os.getenv(env_key, "").strip()
    if override:
        return override
    base = rsshub_base_url()
    path = route if route.startswith("/") else f"/{route}"
    return f"{base}{path}"


def add_default_sources() -> None:
    ensure_tables()
    for sid, name, url, category in DEFAULT_SOURCES:
        try:
            create_source(source_id=sid, name=name, url=url, category=category, enabled=True)
        except Exception:
            pass
    for sid, name, route, category, env_key in CHINESE_SOURCE_DEFAULTS:
        try:
            create_source(
                source_id=sid,
                name=name,
                url=_chinese_source_url(route, env_key),
                category=category,
                enabled=True,
            )
        except Exception:
            pass
    try:
        disable_sources_except(DEFAULT_SOURCE_IDS)
    except Exception as exc:
        print(f"[alpha_radar] disable legacy sources failed: {exc}")


def _strip_html(text: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", cleaned).strip()


def _entry_summary(entry: Any, *, max_len: int = 3000) -> str | None:
    parts: list[str] = []
    for key in ("summary", "description"):
        raw = entry.get(key)
        if raw:
            parts.append(_strip_html(str(raw)))
    content_list = entry.get("content") or []
    if isinstance(content_list, list):
        for block in content_list:
            if isinstance(block, dict):
                val = block.get("value")
                if val:
                    parts.append(_strip_html(str(val)))
    encoded = entry.get("content_encoded") or entry.get("content:encoded")
    if encoded:
        parts.append(_strip_html(str(encoded)))
    merged = "\n\n".join(p for p in parts if p).strip()
    if not merged:
        return None
    return merged[:max_len]


def _quote_url(url: str) -> str:
    """Percent-encode non-ASCII chars in path/query (e.g. RSSHub routes
    with Chinese keywords like /eastmoney/search/铜). urllib fails with
    "'ascii' codec can't encode character" otherwise. Already-encoded
    sequences (%xx) are preserved."""
    parts = urllib.parse.urlsplit(url)
    if (parts.path.isascii() and parts.query.isascii()):
        return url
    path = urllib.parse.quote(parts.path, safe="/%:@")
    query = urllib.parse.quote(parts.query, safe="=&%?;,")
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, path, query, parts.fragment))


def fetch_rss_feed(url: str) -> list[dict[str, Any]]:
    if feedparser is None:
        raise RuntimeError("feedparser is not installed")
    req = urllib.request.Request(_quote_url(url), headers={"User-Agent": RSS_USER_AGENT}, method="GET")
    timeout = rss_timeout_seconds()
    try:
        with _urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        msg = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
        raise RuntimeError(f"HTTP {exc.code}: {msg[:200]}") from exc
    parsed = feedparser.parse(raw)
    items: list[dict[str, Any]] = []
    for entry in parsed.entries:
        title = str(entry.get("title", "")).strip()
        link = str(entry.get("link", "")).strip()
        if not title or not link:
            continue
        item_id = hashlib.md5(link.encode()).hexdigest()[:16]
        summary = _entry_summary(entry)
        published_at = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            try:
                ts = time.mktime(entry.published_parsed)
                published_at = datetime.fromtimestamp(ts, tz=UTC).isoformat()
            except Exception:
                pass
        items.append(
            {
                "id": item_id,
                "title": title,
                "link": link,
                "summary": summary,
                "published_at": published_at,
            }
        )
    return items


def _is_youtube_url(url: str) -> bool:
    try:
        host = urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return False
    return any(host == h or host.endswith("." + h) for h in YOUTUBE_HOSTS)


def fetch_jina_markdown(url: str, *, timeout: int = 45, retries: int = 2) -> str | None:
    if not jina_api_key() and not _proxy_url():
        # Public r.jina.ai is rate-limited; skip unless key or proxy is configured.
        pass
    encoded = urllib.parse.quote(url, safe="")
    req_url = f"https://r.jina.ai/{encoded}"
    headers = {
        "Accept": "text/plain",
        "X-Return-Format": "markdown",
        "User-Agent": RSS_USER_AGENT,
    }
    key = jina_api_key()
    if key:
        headers["Authorization"] = f"Bearer {key}"
    for attempt in range(max(1, retries)):
        req = urllib.request.Request(req_url, headers=headers, method="GET")
        try:
            with _urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8", errors="replace").strip()
                return body or None
        except urllib.error.HTTPError as exc:
            exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
            if exc.code in (429, 503) and attempt + 1 < retries:
                time.sleep(1.5 * (attempt + 1))
                continue
            break
        except Exception as exc:
            str(exc)
            if attempt + 1 < retries:
                time.sleep(1.0 * (attempt + 1))
                continue
            break
    return None


def _needs_jina_fallback(summary: str | None, *, min_chars: int = 280) -> bool:
    return len((summary or "").strip()) < min_chars


def _fetch_one_alpha_source(
    source: dict[str, Any],
    *,
    fetched_at: str,
    use_fulltext: bool,
    apply_filter: bool,
    force_reprocess: bool,
    jina_semaphore: Any | None = None,
) -> dict[str, Any]:
    source_id = source["id"]
    url = source["url"]
    category = source["category"]
    out: dict[str, Any] = {
        "source_id": source_id,
        "count": -1,
        "error": None,
        "fetched": 0,
        "filtered_out": 0,
        "stored": 0,
        "ingest_new": 0,
        "ingest_requeued": 0,
        "ingest_unchanged": 0,
        "fulltext_attempted": 0,
        "fulltext_ok": 0,
    }
    try:
        items = fetch_rss_feed(url)
        cap = max_items_per_source()
        capped = items[:cap]
        out["fetched"] = len(capped)

        if apply_filter:
            kept, filter_stats = filter_feed_items(capped, source_id=source_id)
            out["filtered_out"] = filter_stats["filteredOut"]
        else:
            kept = capped

        count = 0
        priority_fulltext_used = 0
        priority_cap = fulltext_max_per_priority_source()
        for item in kept:
            summary = item.get("summary")
            full_text_md = None

            should_try_jina = use_fulltext and (
                source_id in FULLTEXT_PRIORITY_SOURCE_IDS
                and priority_fulltext_used < priority_cap
                and _needs_jina_fallback(summary)
            )
            if should_try_jina:
                out["fulltext_attempted"] += 1
                if jina_semaphore is not None:
                    with jina_semaphore:
                        full_text_md = fetch_jina_markdown(str(item["link"]))
                else:
                    full_text_md = fetch_jina_markdown(str(item["link"]))
                if full_text_md:
                    out["fulltext_ok"] += 1
                    priority_fulltext_used += 1

            row = upsert_document(
                doc_id=item["id"],
                source_id=source_id,
                title=item["title"],
                url=item["link"],
                category=category,
                summary=summary,
                full_text_md=full_text_md,
                published_at=item["published_at"],
                fetched_at=fetched_at,
                processing_status="raw",
                force_reprocess=force_reprocess,
            )
            if row.get("_inserted"):
                out["ingest_new"] += 1
            elif row.get("_requeued"):
                out["ingest_requeued"] += 1
            else:
                out["ingest_unchanged"] += 1
            count += 1
        out["stored"] = count
        out["count"] = count
        update_source_last_fetch(source_id, fetched_at)
    except Exception as exc:
        out["error"] = str(exc)[:300]
        print(f"[alpha_radar] Failed to fetch RSS {url}: {exc}")
    return out


def fetch_all_sources(
    *,
    enrich_fulltext: bool | None = None,
    apply_filter: bool = True,
    force_reprocess: bool = False,
) -> dict[str, Any]:
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from threading import Semaphore

    ensure_tables()
    add_default_sources()
    use_fulltext = enrich_fulltext_enabled() if enrich_fulltext is None else enrich_fulltext
    sources = fetch_sources(enabled_only=True)
    fetched_at = datetime.now(UTC).isoformat()
    results: dict[str, int] = {}
    source_errors: dict[str, str] = {}
    total_fetched = 0
    total_filtered_out = 0
    total_stored = 0
    ingest_new = 0
    ingest_requeued = 0
    ingest_unchanged = 0
    fulltext_attempted = 0
    fulltext_ok = 0
    jina_semaphore = Semaphore(2) if use_fulltext else None

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {
            pool.submit(
                _fetch_one_alpha_source,
                source,
                fetched_at=fetched_at,
                use_fulltext=use_fulltext,
                apply_filter=apply_filter,
                force_reprocess=force_reprocess,
                jina_semaphore=jina_semaphore,
            ): source["id"]
            for source in sources
        }
        for future in as_completed(futures):
            row = future.result()
            source_id = row["source_id"]
            if row["error"]:
                results[source_id] = -1
                source_errors[source_id] = row["error"]
            else:
                results[source_id] = row["count"]
            total_fetched += row["fetched"]
            total_filtered_out += row["filtered_out"]
            total_stored += row["stored"]
            ingest_new += row["ingest_new"]
            ingest_requeued += row["ingest_requeued"]
            ingest_unchanged += row["ingest_unchanged"]
            fulltext_attempted += row["fulltext_attempted"]
            fulltext_ok += row["fulltext_ok"]

    if use_fulltext and fulltext_attempted and fulltext_ok == 0:
        print(
            "[alpha_radar] Jina fulltext unavailable "
            f"({fulltext_attempted} attempts failed). Using RSS summaries only. "
            "Set http_proxy/https_proxy or JINA_API_KEY, or ALPHA_RADAR_ENRICH_FULLTEXT=0."
        )

    return {
        "results": results,
        "sourceErrors": source_errors,
        "fullTextMode": "priority" if use_fulltext else "rss_only",
        "fullTextAttempted": fulltext_attempted,
        "fullTextFetched": fulltext_ok,
        "ingestStats": {
            "fetched": total_fetched,
            "filteredOut": total_filtered_out,
            "stored": total_stored,
            "new": ingest_new,
            "requeued": ingest_requeued,
            "unchanged": ingest_unchanged,
        },
    }
