"""Alpha Radar event storage (Postgres)."""

from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from data_sync_service.db import get_connection

SOURCES_TABLE = "alpha_radar_sources"
DOCUMENTS_TABLE = "alpha_radar_documents"
TRENDS_TABLE = "alpha_radar_trends"
META_TABLE = "alpha_radar_meta"
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
_SCHEMA_ADVISORY_LOCK_KEY = 58_239_001
_SCHEMA_LOCK = threading.Lock()
_SCHEMA_READY = False

TREND_COLUMN_MIGRATIONS: tuple[tuple[str, str], ...] = (
    ("macro_theme", "TEXT"),
    ("catalyst_grade", "TEXT"),
)

CREATE_SOURCES_SQL = f"""
CREATE TABLE IF NOT EXISTS {SOURCES_TABLE} (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    url         TEXT NOT NULL UNIQUE,
    category    TEXT NOT NULL,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    last_fetch  TEXT,
    created_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_alpha_radar_sources_enabled ON {SOURCES_TABLE}(enabled);
CREATE INDEX IF NOT EXISTS idx_alpha_radar_sources_category ON {SOURCES_TABLE}(category);
"""

CREATE_DOCUMENTS_SQL = f"""
CREATE TABLE IF NOT EXISTS {DOCUMENTS_TABLE} (
    id                  TEXT PRIMARY KEY,
    source_id           TEXT NOT NULL REFERENCES {SOURCES_TABLE}(id) ON DELETE CASCADE,
    title               TEXT NOT NULL,
    url                 TEXT NOT NULL,
    category            TEXT NOT NULL,
    summary             TEXT,
    full_text_md        TEXT,
    published_at        TEXT,
    fetched_at          TEXT NOT NULL,
    processing_status   TEXT NOT NULL DEFAULT 'raw'
);

CREATE INDEX IF NOT EXISTS idx_alpha_radar_docs_published ON {DOCUMENTS_TABLE}(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_alpha_radar_docs_status ON {DOCUMENTS_TABLE}(processing_status);
CREATE INDEX IF NOT EXISTS idx_alpha_radar_docs_category ON {DOCUMENTS_TABLE}(category);
CREATE UNIQUE INDEX IF NOT EXISTS idx_alpha_radar_docs_url ON {DOCUMENTS_TABLE}(url);
"""

CREATE_TRENDS_SQL = f"""
CREATE TABLE IF NOT EXISTS {TRENDS_TABLE} (
    id                  TEXT PRIMARY KEY,
    document_id         TEXT NOT NULL REFERENCES {DOCUMENTS_TABLE}(id) ON DELETE CASCADE,
    trend_name          TEXT NOT NULL,
    macro_theme         TEXT,
    catalyst_grade      TEXT,
    catalyst            TEXT,
    global_target       TEXT,
    urgency_level       TEXT NOT NULL DEFAULT 'B',
    keywords_for_mapping TEXT,
    cn_symbols          TEXT,
    mapping_confidence  DOUBLE PRECISION,
    risk_status         TEXT NOT NULL DEFAULT 'waiting_v2_flow',
    trend_json          TEXT NOT NULL,
    created_at          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_alpha_radar_trends_doc ON {TRENDS_TABLE}(document_id);
CREATE INDEX IF NOT EXISTS idx_alpha_radar_trends_risk ON {TRENDS_TABLE}(risk_status);
CREATE INDEX IF NOT EXISTS idx_alpha_radar_trends_created ON {TRENDS_TABLE}(created_at DESC);
"""

CREATE_META_SQL = f"""
CREATE TABLE IF NOT EXISTS {META_TABLE} (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
"""


def _trend_column_exists(cur: Any, column_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (TRENDS_TABLE, column_name),
    )
    return cur.fetchone() is not None


def _migrate_trend_columns(cur: Any) -> None:
    for column_name, column_type in TREND_COLUMN_MIGRATIONS:
        if _trend_column_exists(cur, column_name):
            continue
        cur.execute(
            f"ALTER TABLE {TRENDS_TABLE} ADD COLUMN {column_name} {column_type}"
        )


def _ensure_tables_once() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (_SCHEMA_ADVISORY_LOCK_KEY,))
            try:
                cur.execute(CREATE_SOURCES_SQL)
                cur.execute(CREATE_DOCUMENTS_SQL)
                cur.execute(CREATE_TRENDS_SQL)
                cur.execute(CREATE_META_SQL)
                _migrate_trend_columns(cur)
            finally:
                cur.execute("SELECT pg_advisory_unlock(%s)", (_SCHEMA_ADVISORY_LOCK_KEY,))
        conn.commit()


def ensure_tables() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return
        _ensure_tables_once()
        _SCHEMA_READY = True


def shanghai_today() -> str:
    return datetime.now(SHANGHAI_TZ).strftime("%Y-%m-%d")


def shanghai_day_start_iso(day: str | None = None) -> str:
    day_str = day or shanghai_today()
    dt = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=SHANGHAI_TZ)
    return dt.astimezone(timezone.utc).isoformat()


def get_meta(key: str) -> str | None:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT value FROM {META_TABLE} WHERE key = %s", (key,))
            row = cur.fetchone()
    return str(row[0]) if row and row[0] is not None else None


def set_meta(key: str, value: str) -> None:
    ensure_tables()
    updated_at = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {META_TABLE}(key, value, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = EXCLUDED.updated_at
                """,
                (key, value, updated_at),
            )
        conn.commit()


def disable_sources_except(source_ids: set[str]) -> int:
    ensure_tables()
    if not source_ids:
        return 0
    placeholders = ", ".join(["%s"] * len(source_ids))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {SOURCES_TABLE}
                SET enabled = FALSE
                WHERE id NOT IN ({placeholders})
                """,
                list(source_ids),
            )
            disabled = cur.rowcount or 0
        conn.commit()
    return disabled


def fetch_sources(*, enabled_only: bool = True, category: str | None = None) -> list[dict[str, Any]]:
    ensure_tables()
    conditions = []
    params: list[Any] = []
    if enabled_only:
        conditions.append("enabled = TRUE")
    if category:
        conditions.append("category = %s")
        params.append(category)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, name, url, category, enabled, last_fetch, created_at
                FROM {SOURCES_TABLE}
                {where}
                ORDER BY category, name
                """,
                params,
            )
            rows = cur.fetchall()
    return [
        {
            "id": str(r[0]),
            "name": str(r[1]),
            "url": str(r[2]),
            "category": str(r[3]),
            "enabled": bool(r[4]),
            "lastFetch": str(r[5]) if r[5] else None,
            "createdAt": str(r[6]),
        }
        for r in rows
    ]


def create_source(
    *,
    source_id: str,
    name: str,
    url: str,
    category: str,
    enabled: bool = True,
) -> dict[str, Any]:
    ensure_tables()
    created_at = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {SOURCES_TABLE}(id, name, url, category, enabled, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    url = EXCLUDED.url,
                    category = EXCLUDED.category,
                    enabled = EXCLUDED.enabled
                RETURNING id, name, url, category, enabled, last_fetch, created_at
                """,
                (source_id, name, url, category, enabled, created_at),
            )
            row = cur.fetchone()
        conn.commit()
    return _source_row(row)


def update_source_last_fetch(source_id: str, fetched_at: str) -> None:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {SOURCES_TABLE} SET last_fetch = %s WHERE id = %s",
                (fetched_at, source_id),
            )
        conn.commit()


def upsert_document(
    *,
    doc_id: str,
    source_id: str,
    title: str,
    url: str,
    category: str,
    summary: str | None,
    full_text_md: str | None,
    published_at: str | None,
    fetched_at: str,
    processing_status: str = "raw",
) -> dict[str, Any]:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {DOCUMENTS_TABLE}(
                    id, source_id, title, url, category, summary, full_text_md,
                    published_at, fetched_at, processing_status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    summary = COALESCE(EXCLUDED.summary, {DOCUMENTS_TABLE}.summary),
                    full_text_md = COALESCE(EXCLUDED.full_text_md, {DOCUMENTS_TABLE}.full_text_md),
                    fetched_at = EXCLUDED.fetched_at,
                    processing_status = 'raw'
                RETURNING id, source_id, title, url, category, summary, full_text_md,
                          published_at, fetched_at, processing_status
                """,
                (
                    doc_id,
                    source_id,
                    title,
                    url,
                    category,
                    summary,
                    full_text_md,
                    published_at,
                    fetched_at,
                    processing_status,
                ),
            )
            row = cur.fetchone()
        conn.commit()
    return _document_row(row)


def fetch_documents(
    *,
    limit: int = 50,
    offset: int = 0,
    category: str | None = None,
    processing_status: str | None = None,
    hours: int | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    ensure_tables()
    lim = max(1, min(int(limit), 200))
    off = max(0, int(offset))
    conditions: list[str] = []
    params: list[Any] = []
    if category:
        conditions.append("category = %s")
        params.append(category)
    if processing_status:
        conditions.append("processing_status = %s")
        params.append(processing_status)
    if hours is not None:
        from datetime import timedelta

        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        conditions.append("fetched_at >= %s")
        params.append(cutoff)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {DOCUMENTS_TABLE} {where}", params)
            total = int(cur.fetchone()[0] or 0)
            cur.execute(
                f"""
                SELECT id, source_id, title, url, category, summary, full_text_md,
                       published_at, fetched_at, processing_status
                FROM {DOCUMENTS_TABLE}
                {where}
                ORDER BY COALESCE(published_at, fetched_at) DESC
                LIMIT %s OFFSET %s
                """,
                params + [lim, off],
            )
            rows = cur.fetchall()
    return total, [_document_row(r) for r in rows]


def fetch_document_by_id(doc_id: str) -> dict[str, Any] | None:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, source_id, title, url, category, summary, full_text_md,
                       published_at, fetched_at, processing_status
                FROM {DOCUMENTS_TABLE}
                WHERE id = %s
                """,
                (doc_id,),
            )
            row = cur.fetchone()
    return _document_row(row) if row else None


def fetch_documents_by_status(
    *,
    processing_status: str,
    limit: int = 10,
    enabled_sources_only: bool = False,
) -> list[dict[str, Any]]:
    ensure_tables()
    lim = max(1, min(int(limit), 200))
    join = ""
    conditions = ["d.processing_status = %s"]
    params: list[Any] = [processing_status]
    if enabled_sources_only:
        join = f"JOIN {SOURCES_TABLE} s ON s.id = d.source_id"
        conditions.append("s.enabled = TRUE")
    where = f"WHERE {' AND '.join(conditions)}"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT d.id, d.source_id, d.title, d.url, d.category, d.summary, d.full_text_md,
                       d.published_at, d.fetched_at, d.processing_status
                FROM {DOCUMENTS_TABLE} d
                {join}
                {where}
                ORDER BY d.fetched_at DESC
                LIMIT %s
                """,
                params + [lim],
            )
            rows = cur.fetchall()
    return [_document_row(r) for r in rows]


def delete_trends_before(iso_timestamp: str) -> int:
    """Delete trends created strictly before iso_timestamp."""
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {TRENDS_TABLE} WHERE created_at < %s",
                (iso_timestamp,),
            )
            deleted = cur.rowcount or 0
        conn.commit()
    return deleted


def delete_trends_older_than_days(days: int) -> int:
    """Delete trends whose source document event time is older than days (optional ops prune)."""
    ensure_tables()
    age_days = max(1, int(days))
    cutoff = (datetime.now(timezone.utc) - timedelta(days=age_days)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                DELETE FROM {TRENDS_TABLE} t
                USING {DOCUMENTS_TABLE} d
                WHERE t.document_id = d.id
                  AND COALESCE(d.published_at, d.fetched_at) < %s
                """,
                (cutoff,),
            )
            deleted = cur.rowcount or 0
        conn.commit()
    return deleted


def count_trends_total() -> int:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TRENDS_TABLE}")
            row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def delete_trends_since(iso_timestamp: str) -> int:
    """Delete trends created at or after iso_timestamp (rollback failed batch)."""
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {TRENDS_TABLE} WHERE created_at >= %s",
                (iso_timestamp,),
            )
            deleted = cur.rowcount or 0
        conn.commit()
    return deleted


def delete_all_trends() -> int:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {TRENDS_TABLE}")
            deleted = cur.rowcount or 0
        conn.commit()
    return deleted


def delete_trends_for_day(day: str) -> int:
    ensure_tables()
    day_start = shanghai_day_start_iso(day)
    day_end = (
        datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=SHANGHAI_TZ) + timedelta(days=1)
    ).astimezone(timezone.utc).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                DELETE FROM {TRENDS_TABLE}
                WHERE created_at >= %s AND created_at < %s
                """,
                (day_start, day_end),
            )
            deleted = cur.rowcount or 0
        conn.commit()
    return deleted


def update_document_status(doc_id: str, processing_status: str) -> bool:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {DOCUMENTS_TABLE} SET processing_status = %s WHERE id = %s",
                (processing_status, doc_id),
            )
            ok = (cur.rowcount or 0) > 0
        conn.commit()
    return ok


_TREND_SELECT_COLS = 14


def insert_trend(
    *,
    trend_id: str,
    document_id: str,
    trend_name: str,
    macro_theme: str | None,
    catalyst_grade: str | None,
    catalyst: str | None,
    global_target: str | None,
    urgency_level: str,
    keywords_for_mapping: list[str],
    cn_symbols: list[dict[str, Any]] | None,
    mapping_confidence: float | None,
    risk_status: str,
    trend_json: dict[str, Any],
) -> dict[str, Any]:
    ensure_tables()
    created_at = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {TRENDS_TABLE}(
                    id, document_id, trend_name, macro_theme, catalyst_grade, catalyst,
                    global_target, urgency_level, keywords_for_mapping, cn_symbols,
                    mapping_confidence, risk_status, trend_json, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, document_id, trend_name, catalyst, global_target,
                          urgency_level, macro_theme, catalyst_grade,
                          keywords_for_mapping, cn_symbols, mapping_confidence,
                          risk_status, trend_json, created_at
                """,
                (
                    trend_id,
                    document_id,
                    trend_name,
                    macro_theme,
                    catalyst_grade,
                    catalyst,
                    global_target,
                    urgency_level,
                    json.dumps(keywords_for_mapping, ensure_ascii=False),
                    json.dumps(cn_symbols or [], ensure_ascii=False),
                    mapping_confidence,
                    risk_status,
                    json.dumps(trend_json, ensure_ascii=False),
                    created_at,
                ),
            )
            row = cur.fetchone()
        conn.commit()
    return _trend_row(row)


def delete_trend_by_id(trend_id: str) -> bool:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {TRENDS_TABLE} WHERE id = %s", (trend_id,))
            ok = (cur.rowcount or 0) > 0
        conn.commit()
    return ok


def delete_trends_for_document(document_id: str) -> int:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {TRENDS_TABLE} WHERE document_id = %s", (document_id,))
            deleted = cur.rowcount or 0
        conn.commit()
    return deleted


_TREND_DOC_SELECT = f"""
    SELECT t.id, t.document_id, t.trend_name, t.catalyst, t.global_target,
           t.urgency_level, t.macro_theme, t.catalyst_grade,
           t.keywords_for_mapping, t.cn_symbols,
           t.mapping_confidence, t.risk_status, t.trend_json, t.created_at,
           d.title, d.url, d.category, d.published_at, d.fetched_at, d.summary
    FROM {TRENDS_TABLE} t
    JOIN {DOCUMENTS_TABLE} d ON d.id = t.document_id
"""


def _attach_document_fields(item: dict[str, Any], row: tuple[Any, ...]) -> dict[str, Any]:
    item["documentTitle"] = str(row[14])
    item["documentUrl"] = str(row[15])
    item["documentCategory"] = str(row[16])
    item["documentPublishedAt"] = str(row[17]) if row[17] else None
    item["documentFetchedAt"] = str(row[18]) if row[18] else None
    item["documentSummary"] = str(row[19]) if row[19] else None
    return item


def fetch_trend_by_id(trend_id: str) -> dict[str, Any] | None:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                {_TREND_DOC_SELECT}
                WHERE t.id = %s
                """,
                (trend_id,),
            )
            row = cur.fetchone()
    if not row:
        return None
    item = _trend_row(row[:_TREND_SELECT_COLS])
    return _attach_document_fields(item, row)


def fetch_trends(
    *,
    limit: int = 50,
    offset: int = 0,
    document_id: str | None = None,
    risk_status: str | None = None,
    day: str | None = None,
    since: str | None = None,
    max_age_days: int | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    ensure_tables()
    lim = max(1, min(int(limit), 200))
    off = max(0, int(offset))
    conditions: list[str] = []
    params: list[Any] = []
    if document_id:
        conditions.append("t.document_id = %s")
        params.append(document_id)
    if risk_status:
        conditions.append("t.risk_status = %s")
        params.append(risk_status)
    if since:
        conditions.append("t.created_at >= %s")
        params.append(since)
    if day:
        day_start = shanghai_day_start_iso(day)
        day_end = (
            datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=SHANGHAI_TZ)
            + timedelta(days=1)
        ).astimezone(timezone.utc).isoformat()
        conditions.append("t.created_at >= %s AND t.created_at < %s")
        params.extend([day_start, day_end])
    if max_age_days is not None:
        days = max(1, int(max_age_days))
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        conditions.append("COALESCE(d.published_at, d.fetched_at) >= %s")
        params.append(cutoff)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    count_from = f"{TRENDS_TABLE} t JOIN {DOCUMENTS_TABLE} d ON d.id = t.document_id"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {count_from} {where}", params)
            total = int(cur.fetchone()[0] or 0)
            cur.execute(
                f"""
                {_TREND_DOC_SELECT}
                {where}
                ORDER BY t.created_at DESC
                LIMIT %s OFFSET %s
                """,
                params + [lim, off],
            )
            rows = cur.fetchall()
    items = []
    for r in rows:
        item = _trend_row(r[:_TREND_SELECT_COLS])
        _attach_document_fields(item, r)
        items.append(item)
    return total, items


def fetch_trends_for_catalyst(*, max_age_days: int = 30) -> list[dict[str, Any]]:
    """Trends with mapped CN symbols within the catalyst recency window."""
    ensure_tables()
    days = max(1, int(max_age_days))
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                {_TREND_DOC_SELECT}
                WHERE t.cn_symbols IS NOT NULL
                  AND t.cn_symbols != '[]'
                  AND t.cn_symbols != 'null'
                  AND COALESCE(d.published_at, d.fetched_at) >= %s
                ORDER BY COALESCE(d.published_at, d.fetched_at) DESC
                """,
                (cutoff,),
            )
            rows = cur.fetchall()
    items: list[dict[str, Any]] = []
    for r in rows:
        item = _trend_row(r[:_TREND_SELECT_COLS])
        _attach_document_fields(item, r)
        if item.get("cnSymbols"):
            items.append(item)
    return items


def update_trend_risk_status(trend_id: str, risk_status: str) -> bool:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {TRENDS_TABLE} SET risk_status = %s WHERE id = %s",
                (risk_status, trend_id),
            )
            ok = (cur.rowcount or 0) > 0
        conn.commit()
    return ok


def update_trend_mapping(
    *,
    trend_id: str,
    cn_symbols: list[dict[str, Any]],
    mapping_confidence: float | None,
    risk_status: str,
) -> bool:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {TRENDS_TABLE}
                SET cn_symbols = %s, mapping_confidence = %s, risk_status = %s
                WHERE id = %s
                """,
                (
                    json.dumps(cn_symbols, ensure_ascii=False),
                    mapping_confidence,
                    risk_status,
                    trend_id,
                ),
            )
            ok = (cur.rowcount or 0) > 0
        conn.commit()
    return ok


def _source_row(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "id": str(row[0]),
        "name": str(row[1]),
        "url": str(row[2]),
        "category": str(row[3]),
        "enabled": bool(row[4]),
        "lastFetch": str(row[5]) if row[5] else None,
        "createdAt": str(row[6]),
    }


def _document_row(row: tuple[Any, ...]) -> dict[str, Any]:
    full_text = row[6]
    return {
        "id": str(row[0]),
        "sourceId": str(row[1]),
        "title": str(row[2]),
        "url": str(row[3]),
        "category": str(row[4]),
        "summary": str(row[5]) if row[5] else None,
        "fullTextMd": str(full_text) if full_text else None,
        "publishedAt": str(row[7]) if row[7] else None,
        "fetchedAt": str(row[8]),
        "processingStatus": str(row[9]),
    }


def _trend_row(row: tuple[Any, ...]) -> dict[str, Any]:
    keywords_raw = row[8]
    cn_raw = row[9]
    trend_json_raw = row[12]
    keywords: list[str] = []
    cn_symbols: list[dict[str, Any]] = []
    trend_json: dict[str, Any] = {}
    try:
        if keywords_raw:
            keywords = json.loads(str(keywords_raw))
    except Exception:
        keywords = []
    try:
        if cn_raw:
            cn_symbols = json.loads(str(cn_raw))
    except Exception:
        cn_symbols = []
    try:
        if trend_json_raw:
            trend_json = json.loads(str(trend_json_raw))
    except Exception:
        trend_json = {}
    trend_name = str(row[2])
    urgency_level = str(row[5])
    macro_theme_raw = row[6]
    catalyst_grade_raw = row[7]
    macro_theme = str(macro_theme_raw) if macro_theme_raw else trend_name
    catalyst_grade = str(catalyst_grade_raw) if catalyst_grade_raw else urgency_level
    return {
        "id": str(row[0]),
        "documentId": str(row[1]),
        "trendName": trend_name,
        "macroTheme": macro_theme,
        "catalystGrade": catalyst_grade,
        "catalyst": str(row[3]) if row[3] else None,
        "globalTarget": str(row[4]) if row[4] else None,
        "urgencyLevel": urgency_level,
        "keywordsForMapping": keywords,
        "cnSymbols": cn_symbols,
        "mappingConfidence": float(row[10]) if row[10] is not None else None,
        "riskStatus": str(row[11]),
        "trendJson": trend_json,
        "createdAt": str(row[13]),
    }
