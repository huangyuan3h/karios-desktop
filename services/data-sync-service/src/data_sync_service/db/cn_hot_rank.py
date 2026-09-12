"""CN stock hot-rank (Eastmoney popularity) — daily attention panel.

Source: akshare stock_hot_rank_detail_em per stock (~366d history each).
Stored per (ts_code, trade_date). rank: smaller = hotter (full-market rank,
e.g. 1630/2213 — NOT top100-only). new_fans/iron_fans: follower mix.
Weekend rows exist (rank updates daily); consumers join A-share sessions.
"""

from __future__ import annotations

from data_sync_service.db import get_connection
from data_sync_service.db._ensure_guard import ensure_once

TABLE_NAME = "cn_hot_rank"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ts_code     TEXT NOT NULL,
    trade_date  DATE NOT NULL,
    rank        DOUBLE PRECISION,
    new_fans    DOUBLE PRECISION,
    iron_fans   DOUBLE PRECISION,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ts_code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_cn_hot_rank_date ON {TABLE_NAME}(trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_cn_hot_rank_ts_date ON {TABLE_NAME}(ts_code, trade_date DESC);
"""


def ensure_table() -> None:
    def _impl() -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_SQL)
            conn.commit()

    ensure_once(TABLE_NAME, _impl)


def _date(s):
    if not s:
        return None
    s = str(s).strip()[:10]
    return s or None


def _num(v):
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def upsert_rows(rows: list[dict]) -> int:
    ensure_table()
    if not rows:
        return 0
    vals = []
    for r in rows:
        ts = str(r.get("ts_code") or "").strip()
        d = _date(r.get("trade_date"))
        if not ts or not d:
            continue
        vals.append((ts, d, _num(r.get("rank")), _num(r.get("new_fans")), _num(r.get("iron_fans"))))
    if not vals:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TABLE_NAME}(ts_code, trade_date, rank, new_fans, iron_fans, updated_at)
                VALUES (%s,%s,%s,%s,%s, now())
                ON CONFLICT (ts_code, trade_date) DO UPDATE SET
                    rank=excluded.rank, new_fans=excluded.new_fans,
                    iron_fans=excluded.iron_fans, updated_at=now()
                """,
                vals,
            )
        conn.commit()
    return len(vals)


def fresh_codes(cutoff: str) -> set[str]:
    """ts_codes with rank data on/after cutoff (resume support)."""
    ensure_table()
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT ts_code FROM {TABLE_NAME} WHERE trade_date >= %s GROUP BY ts_code",
                    (cutoff,),
                )
                return {str(r[0]) for r in cur.fetchall() if r[0]}
    except Exception:
        return set()
