"""Research report storage (研报 → Alpha channel).

Stores sell-side research reports (rating / target price / EPS forecasts)
fetched from East Money's report center, plus the per-report alpha score
computed by service/research.py.

NOTE: keep CREATE_TABLE_SQL in sync with alembic/versions/0019_*.py.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from data_sync_service.db import get_connection

TABLE_NAME = "research_reports"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id            BIGSERIAL PRIMARY KEY,
    info_code     TEXT NOT NULL UNIQUE,
    stock_code    TEXT NOT NULL,
    stock_name    TEXT NOT NULL,
    title         TEXT NOT NULL,
    org_name      TEXT NOT NULL,
    rating        TEXT,
    target_price  DOUBLE PRECISION,
    eps_this_year DOUBLE PRECISION,
    pe_this_year  DOUBLE PRECISION,
    industry_name TEXT,
    market        TEXT NOT NULL DEFAULT 'CN',
    publish_date  DATE NOT NULL,
    encode_url    TEXT,
    source        TEXT NOT NULL DEFAULT 'eastmoney',
    alpha_score   DOUBLE PRECISION,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (stock_code, publish_date, title)
);
CREATE INDEX IF NOT EXISTS idx_research_reports_publish_date
    ON {TABLE_NAME} (publish_date DESC);
CREATE INDEX IF NOT EXISTS idx_research_reports_stock_code
    ON {TABLE_NAME} (stock_code);
"""


def _ensure_table_impl() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()


def ensure_table() -> None:
    _ensure_table_impl()


def _numeric(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def upsert_research_reports(rows: list[dict[str, Any]]) -> int:
    """Insert or update reports (keyed by info_code). Returns inserted count."""
    if not rows:
        return 0
    ensure_table()
    inserted = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    f"""
                    INSERT INTO {TABLE_NAME}
                        (info_code, stock_code, stock_name, title, org_name,
                         rating, target_price, eps_this_year, pe_this_year,
                         industry_name, market, publish_date, encode_url, source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (info_code) DO NOTHING
                    """,
                    (
                        str(row.get("infoCode") or "").strip(),
                        str(row.get("stockCode") or "").strip(),
                        str(row.get("stockName") or "").strip(),
                        str(row.get("title") or "").strip(),
                        str(row.get("orgName") or row.get("orgSName") or "").strip(),
                        str(row.get("rating") or "") or None,
                        _numeric(row.get("targetPrice")),
                        _numeric(row.get("epsThisYear")),
                        _numeric(row.get("peThisYear")),
                        str(row.get("industryName") or "") or None,
                        str(row.get("market") or "CN").strip() or "CN",
                        row.get("publishDate"),
                        str(row.get("encodeUrl") or "") or None,
                        str(row.get("source") or "eastmoney").strip(),
                    ),
                )
                if cur.rowcount and cur.rowcount > 0:
                    inserted += 1
        conn.commit()
    return inserted


def list_recent_reports(
    *,
    limit: int = 100,
    window_days: int | None = 7,
    min_score: float | None = None,
) -> list[dict[str, Any]]:
    """Recent reports, newest first. Optional score floor for signal lists."""
    ensure_table()
    conditions: list[str] = []
    params: list[object] = []
    if window_days is not None and window_days > 0:
        conditions.append("publish_date >= CURRENT_DATE - %s::int")
        params.append(int(window_days))
    if min_score is not None:
        conditions.append("alpha_score IS NOT NULL AND alpha_score >= %s")
        params.append(float(min_score))
    where_sql = " WHERE " + " AND ".join(conditions) if conditions else ""
    params.append(int(limit))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, info_code, stock_code, stock_name, title, org_name,
                       rating, target_price, eps_this_year, pe_this_year,
                       industry_name, market, publish_date, encode_url, source,
                       alpha_score, created_at
                FROM {TABLE_NAME}
                {where_sql}
                ORDER BY publish_date DESC, id DESC
                LIMIT %s
                """,
                params,
            )
            cols = [d.name for d in cur.description]
            rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        obj: dict[str, Any] = {}
        for col, val in zip(cols, r, strict=False):
            if val is None:
                obj[col] = None
            elif hasattr(val, "isoformat"):
                obj[col] = val.isoformat() if isinstance(val, date) else val.isoformat()
            else:
                obj[col] = val
        out.append(obj)
    return out


def fetch_reports_for_score_window(window_days: int = 14) -> list[dict[str, Any]]:
    """All reports in the scoring window, one row per report."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, info_code, stock_code, stock_name, title, org_name,
                       rating, target_price, industry_name, market, publish_date
                FROM {TABLE_NAME}
                WHERE publish_date >= CURRENT_DATE - %s::int
                ORDER BY publish_date DESC
                """,
                (int(window_days),),
            )
            cols = [d.name for d in cur.description]
            rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        obj: dict[str, Any] = {}
        for col, val in zip(cols, r, strict=False):
            if val is None:
                obj[col] = None
            elif hasattr(val, "isoformat"):
                obj[col] = val.isoformat() if isinstance(val, date) else val.isoformat()
            else:
                obj[col] = val
        out.append(obj)
    return out


def update_report_scores(rows: list[tuple[float, int]]) -> int:
    """Persist per-report alpha scores (id → score). Returns updated count."""
    if not rows:
        return 0
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            for score, rid in rows:
                cur.execute(
                    f"UPDATE {TABLE_NAME} SET alpha_score = %s WHERE id = %s",
                    (float(score), int(rid)),
                )
        conn.commit()
    return len(rows)


def research_stats() -> dict[str, int]:
    """Coarse ingestion stats for the API / dashboard."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
            total = int(cur.fetchone()[0] or 0)
            cur.execute(
                f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE publish_date >= CURRENT_DATE - 1"
            )
            last_24h = int(cur.fetchone()[0] or 0)
            cur.execute(
                f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE publish_date >= CURRENT_DATE - 7"
            )
            last_7d = int(cur.fetchone()[0] or 0)
            cur.execute(
                f"SELECT COUNT(DISTINCT stock_code) FROM {TABLE_NAME} "
                "WHERE publish_date >= CURRENT_DATE - 7"
            )
            stocks_7d = int(cur.fetchone()[0] or 0)
    return {"total": total, "last24h": last_24h, "last7d": last_7d, "stocks7d": stocks_7d}
