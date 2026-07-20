"""Dragon-tiger board institutional flow: raw seats and daily summaries."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from data_sync_service.db import get_connection
from data_sync_service.db._ensure_guard import ensure_once

DAILY_TABLE = "market_top_inst_daily"
SUMMARY_TABLE = "market_top_inst_summary"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {DAILY_TABLE} (
    trade_date   DATE NOT NULL,
    ts_code      TEXT NOT NULL,
    exalter      TEXT NOT NULL,
    buy          DOUBLE PRECISION,
    sell         DOUBLE PRECISION,
    net_buy      DOUBLE PRECISION,
    side         TEXT,
    reason       TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, ts_code, exalter)
);

CREATE INDEX IF NOT EXISTS idx_top_inst_daily_ts_date
    ON {DAILY_TABLE}(ts_code, trade_date DESC);

CREATE TABLE IF NOT EXISTS {SUMMARY_TABLE} (
    trade_date       DATE NOT NULL,
    ts_code          TEXT NOT NULL,
    inst_net_buy     DOUBLE PRECISION,
    inst_net_buy_yi  DOUBLE PRECISION,
    seat_label       TEXT,
    lhasa_dominant   BOOLEAN NOT NULL DEFAULT FALSE,
    on_board         BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, ts_code)
);

CREATE INDEX IF NOT EXISTS idx_top_inst_summary_date
    ON {SUMMARY_TABLE}(trade_date DESC);
"""


def _ensure_table_impl() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def ensure_table() -> None:
    ensure_once("market_top_inst_daily", _ensure_table_impl)


def _date_str(val: object) -> str | None:
    if val is None:
        return None
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d")
    s = str(val).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s or None


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def upsert_daily_rows(rows: list[dict[str, Any]]) -> int:
    ensure_table()
    if not rows:
        return 0
    values = []
    for r in rows:
        td = _date_str(r.get("trade_date"))
        ts_code = str(r.get("ts_code") or "").strip()
        exalter = str(r.get("exalter") or "").strip()
        if not td or not ts_code or not exalter:
            continue
        values.append(
            (
                td,
                ts_code,
                exalter,
                r.get("buy"),
                r.get("sell"),
                r.get("net_buy"),
                str(r.get("side") or "") or None,
                str(r.get("reason") or "") or None,
            )
        )
    if not values:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {DAILY_TABLE}(
                    trade_date, ts_code, exalter, buy, sell, net_buy, side, reason, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (trade_date, ts_code, exalter) DO UPDATE SET
                    buy = excluded.buy,
                    sell = excluded.sell,
                    net_buy = excluded.net_buy,
                    side = excluded.side,
                    reason = excluded.reason,
                    updated_at = now()
                """,
                values,
            )
        conn.commit()
    return len(values)


def upsert_summary_rows(rows: list[dict[str, Any]]) -> int:
    ensure_table()
    if not rows:
        return 0
    values = []
    for r in rows:
        td = _date_str(r.get("trade_date"))
        ts_code = str(r.get("ts_code") or "").strip()
        if not td or not ts_code:
            continue
        values.append(
            (
                td,
                ts_code,
                r.get("inst_net_buy"),
                r.get("inst_net_buy_yi"),
                str(r.get("seat_label") or "") or None,
                bool(r.get("lhasa_dominant")),
                bool(r.get("on_board")),
            )
        )
    if not values:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {SUMMARY_TABLE}(
                    trade_date, ts_code, inst_net_buy, inst_net_buy_yi,
                    seat_label, lhasa_dominant, on_board, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (trade_date, ts_code) DO UPDATE SET
                    inst_net_buy = excluded.inst_net_buy,
                    inst_net_buy_yi = excluded.inst_net_buy_yi,
                    seat_label = excluded.seat_label,
                    lhasa_dominant = excluded.lhasa_dominant,
                    on_board = excluded.on_board,
                    updated_at = now()
                """,
                values,
            )
        conn.commit()
    return len(values)


def fetch_summaries_for_codes(
    ts_codes: list[str],
    *,
    trade_date: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Return latest summary per ts_code (optionally for a specific trade_date)."""
    ensure_table()
    codes = [c.strip() for c in ts_codes if c and c.strip()]
    if not codes:
        return {}
    if trade_date:
        td = _date_str(trade_date)
        if not td:
            return {}
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT trade_date, ts_code, inst_net_buy, inst_net_buy_yi,
                           seat_label, lhasa_dominant, on_board
                    FROM {SUMMARY_TABLE}
                    WHERE ts_code = ANY(%s) AND trade_date = %s
                    """,
                    (codes, td),
                )
                rows = cur.fetchall()
                columns = [d.name for d in cur.description]
    else:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT DISTINCT ON (ts_code)
                        trade_date, ts_code, inst_net_buy, inst_net_buy_yi,
                        seat_label, lhasa_dominant, on_board
                    FROM {SUMMARY_TABLE}
                    WHERE ts_code = ANY(%s)
                    ORDER BY ts_code, trade_date DESC
                    """,
                    (codes,),
                )
                rows = cur.fetchall()
                columns = [d.name for d in cur.description]
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        obj: dict[str, Any] = {}
        for col, val in zip(columns, row, strict=False):
            if col == "trade_date" and hasattr(val, "strftime"):
                obj[col] = val.strftime("%Y-%m-%d")
            elif col in ("lhasa_dominant", "on_board"):
                obj[col] = bool(val)
            elif col not in ("ts_code", "seat_label") and val is not None:
                try:
                    obj[col] = float(val)
                except (TypeError, ValueError):
                    obj[col] = val
            else:
                obj[col] = val
        ts = str(obj.get("ts_code") or "")
        if ts:
            out[ts] = obj
    return out


def fetch_daily_seats_batch(keys: list[tuple[str, str]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    ensure_table()
    normalized: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for ts_code_raw, trade_date_raw in keys:
        ts_code = str(ts_code_raw or "").strip()
        td = _date_str(trade_date_raw)
        if not ts_code or not td:
            continue
        key = (ts_code, td)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    if not normalized:
        return {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT trade_date, ts_code, exalter, buy, sell, net_buy, side, reason
                FROM {DAILY_TABLE}
                WHERE (ts_code, trade_date) IN (SELECT * FROM unnest(%s::text[], %s::date[]))
                ORDER BY ts_code, trade_date, net_buy DESC NULLS LAST
                """,
                ([ts_code for ts_code, _ in normalized], [td for _, td in normalized]),
            )
            rows = cur.fetchall()
            columns = [d.name for d in cur.description]
    out: dict[tuple[str, str], list[dict[str, Any]]] = {key: [] for key in normalized}
    for row in rows:
        obj: dict[str, Any] = {}
        for col, val in zip(columns, row, strict=False):
            if col == "trade_date" and hasattr(val, "strftime"):
                obj[col] = val.strftime("%Y-%m-%d")
            elif col not in ("ts_code", "exalter", "side", "reason") and val is not None:
                try:
                    obj[col] = float(val)
                except (TypeError, ValueError):
                    obj[col] = val
            else:
                obj[col] = val
        ts_code = str(obj.get("ts_code") or "")
        td = str(obj.get("trade_date") or "")
        if ts_code and td:
            out.setdefault((ts_code, td), []).append(obj)
    return out


def fetch_daily_seats(ts_code: str, trade_date: str) -> list[dict[str, Any]]:
    td = _date_str(trade_date)
    if not td:
        return []
    return fetch_daily_seats_batch([(ts_code, td)]).get((ts_code, td), [])
