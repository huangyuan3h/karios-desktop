"""Shared helpers for the hot-money anatomy diagnostics (read-only).

Lesson (2026-09-15): `daily` stores qfq prices while `stk_limit` / `bar_5min`
are RAW. Limit-price comparisons MUST reconstruct raw: raw = qfq x adj_latest / adj.
"""

from __future__ import annotations

from typing import Any


def latest_adj(conn: Any, codes: list[str] | None = None) -> dict[str, float]:
    """{ts_code: adj_factor on its latest daily row} (the qfq anchor)."""
    sql = "SELECT DISTINCT ON (ts_code) ts_code, adj_factor FROM daily"
    if codes is None:
        with conn.cursor() as cur:
            cur.execute(sql + " ORDER BY ts_code, trade_date DESC")
            return {str(ts): float(a) for ts, a in cur.fetchall() if a}
    with conn.cursor() as cur:
        cur.execute(
            sql + " WHERE ts_code = ANY(%s) ORDER BY ts_code, trade_date DESC", (codes,)
        )
        return {str(ts): float(a) for ts, a in cur.fetchall() if a}


def raw_price(qfq: float | None, adj: float | None, adj_latest: float | None) -> float | None:
    if qfq is None or adj is None or adj <= 0 or adj_latest is None:
        return None
    return float(qfq) * float(adj_latest) / float(adj)
