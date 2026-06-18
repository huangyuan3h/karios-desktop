"""East Money industry board mapping: ts_code -> industry board name."""

from __future__ import annotations

from typing import Any, Iterable

from data_sync_service.db import get_connection

TABLE_NAME = "stock_eastmoney_industry"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ts_code        TEXT PRIMARY KEY,
    industry_name  TEXT NOT NULL,
    industry_code  TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stock_em_industry_name ON {TABLE_NAME}(industry_name);
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def upsert_rows(rows: Iterable[dict[str, Any]]) -> int:
    ensure_table()
    values = []
    for r in rows:
        ts_code = str(r.get("ts_code") or "").strip()
        industry_name = str(r.get("industry_name") or "").strip()
        industry_code = str(r.get("industry_code") or "").strip()
        updated_at = str(r.get("updated_at") or "").strip()
        if not ts_code or not industry_name:
            continue
        values.append((ts_code, industry_name, industry_code, updated_at))
    if not values:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TABLE_NAME} (ts_code, industry_name, industry_code, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (ts_code) DO UPDATE SET
                    industry_name = excluded.industry_name,
                    industry_code = excluded.industry_code,
                    updated_at = excluded.updated_at
                """,
                values,
            )
        conn.commit()
    return len(values)


def lookup_by_ts_codes(ts_codes: list[str]) -> dict[str, str]:
    """Return ts_code -> East Money industry board name."""
    ensure_table()
    codes = [str(c or "").strip() for c in ts_codes if c and str(c).strip()]
    if not codes:
        return {}
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT ts_code, industry_name FROM {TABLE_NAME} WHERE ts_code = ANY(%s)",
                    (codes,),
                )
                rows = cur.fetchall()
        return {str(r[0]): str(r[1]) for r in rows if r and r[0] and r[1]}
    except Exception:
        return {}


def count_rows() -> int:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
            row = cur.fetchone()
    return int(row[0] or 0) if row else 0


_CN_STOCK_WHERE = """
    (sb.market IN ('主板', '中小板', '创业板', '科创板', 'CN') OR sb.ts_code ~ '^[0-9]{6}\\.(SH|SZ)$')
    AND (sb.ts_code LIKE '%.SH' OR sb.ts_code LIKE '%.SZ')
    AND (sb.delist_date IS NULL OR sb.delist_date > CURRENT_DATE)
"""


def coverage_stats() -> dict[str, int]:
    """Return CN A-share counts: total, em_mapped, missing."""
    ensure_table()
    from data_sync_service.db.stock_basic import ensure_table as ensure_sb

    ensure_sb()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*) AS total_cn
                FROM stock_basic sb
                WHERE {_CN_STOCK_WHERE}
                """
            )
            total_row = cur.fetchone()
            cur.execute(
                f"""
                SELECT COUNT(*) AS em_mapped
                FROM stock_basic sb
                INNER JOIN {TABLE_NAME} em ON sb.ts_code = em.ts_code
                WHERE {_CN_STOCK_WHERE}
                """
            )
            mapped_row = cur.fetchone()
    total_cn = int(total_row[0] or 0) if total_row else 0
    em_mapped = int(mapped_row[0] or 0) if mapped_row else 0
    missing = max(0, total_cn - em_mapped)
    return {"totalCnStocks": total_cn, "emMapped": em_mapped, "missingCount": missing}


def list_missing_cn_ts_codes(*, after_ts_code: str | None = None, limit: int = 500) -> list[str]:
    """CN A-shares in stock_basic without an East Money industry row."""
    ensure_table()
    from data_sync_service.db.stock_basic import ensure_table as ensure_sb

    ensure_sb()
    lim = max(1, min(int(limit), 5000))
    params: list[Any] = []
    after_clause = ""
    if after_ts_code:
        after_clause = "AND sb.ts_code > %s"
        params.append(after_ts_code)
    params.append(lim)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT sb.ts_code
                FROM stock_basic sb
                LEFT JOIN {TABLE_NAME} em ON sb.ts_code = em.ts_code
                WHERE {_CN_STOCK_WHERE}
                  AND em.ts_code IS NULL
                  {after_clause}
                ORDER BY sb.ts_code
                LIMIT %s
                """,
                tuple(params),
            )
            rows = cur.fetchall()
    return [str(r[0]) for r in rows if r and r[0]]


def list_stale_cn_ts_codes(
    *,
    after_ts_code: str | None = None,
    limit: int = 500,
    max_stale_days: int = 30,
) -> list[str]:
    """EM industry rows older than max_stale_days (CN A-shares only)."""
    ensure_table()
    lim = max(1, min(int(limit), 5000))
    days = max(1, int(max_stale_days))
    params: list[Any] = [days]
    after_clause = ""
    if after_ts_code:
        after_clause = "AND em.ts_code > %s"
        params.append(after_ts_code)
    params.append(lim)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT em.ts_code
                FROM {TABLE_NAME} em
                INNER JOIN stock_basic sb ON sb.ts_code = em.ts_code
                WHERE {_CN_STOCK_WHERE}
                  AND em.updated_at::timestamptz < NOW() - (%s || ' days')::interval
                  {after_clause}
                ORDER BY em.ts_code
                LIMIT %s
                """,
                tuple(params),
            )
            rows = cur.fetchall()
    return [str(r[0]) for r in rows if r and r[0]]


def search_stocks_by_industry_keyword(keyword: str, *, limit: int = 12) -> list[dict[str, Any]]:
    """Find CN stocks whose East Money industry board name matches keyword."""
    ensure_table()
    kw = str(keyword or "").strip()
    if not kw:
        return []
    lim = max(1, min(int(limit), 30))
    try:
        from data_sync_service.db.stock_basic import ensure_table as ensure_sb

        ensure_sb()
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT sb.ts_code, sb.symbol, sb.name
                    FROM {TABLE_NAME} em
                    JOIN stock_basic sb ON sb.ts_code = em.ts_code
                    WHERE em.industry_name LIKE %s
                      AND (sb.delist_date IS NULL OR sb.delist_date > CURRENT_DATE)
                    ORDER BY sb.symbol ASC
                    LIMIT %s
                    """,
                    (f"%{kw}%", lim),
                )
                rows = cur.fetchall()
    except Exception:
        return []

    out: list[dict[str, Any]] = []
    for r in rows:
        ticker = str(r[1] or "")
        if not ticker.isdigit() or len(ticker) != 6:
            continue
        suffix = "SH" if ticker.startswith("6") else "SZ"
        out.append(
            {
                "symbol": f"CN:{ticker}",
                "ticker": ticker,
                "name": str(r[2] or ""),
                "market": "CN",
                "source": "emIndustry",
            }
        )
    return out
