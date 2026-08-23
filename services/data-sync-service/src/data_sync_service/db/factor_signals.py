"""Factor signals — daily morphology / microstructure signals (independent of S-3).

First signal: strong_scoop_exhaustion (exhaustion top in strong stocks).
Table is append-mostly per (trade_date, symbol, factor_name); re-scans replace same key.
"""

from __future__ import annotations

from data_sync_service.db import get_connection
from data_sync_service.db._ensure_guard import ensure_once

TABLE_NAME = "factor_signals"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    trade_date   DATE NOT NULL,
    symbol       TEXT NOT NULL,
    factor_name  TEXT NOT NULL,
    direction    TEXT NOT NULL,
    entry_price  DOUBLE PRECISION NOT NULL,
    target_price DOUBLE PRECISION NOT NULL,
    stop_price   DOUBLE PRECISION NOT NULL,
    probability  DOUBLE PRECISION NOT NULL,
    hold_days    INTEGER NOT NULL DEFAULT 20,
    status       TEXT NOT NULL DEFAULT 'pending',
    ret60        DOUBLE PRECISION,
    vol_ratio    DOUBLE PRECISION,
    industry     TEXT,
    board        TEXT,
    symbol_name  TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, symbol, factor_name)
);

CREATE INDEX IF NOT EXISTS idx_factor_signals_date ON {TABLE_NAME}(trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_factor_signals_factor ON {TABLE_NAME}(factor_name, trade_date DESC);
"""


def ensure_table() -> None:
    def _impl() -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_SQL)
            conn.commit()
    ensure_once(TABLE_NAME, _impl)


def upsert_rows(rows: list[dict]) -> int:
    ensure_table()
    if not rows:
        return 0
    vals = []
    for r in rows:
        td = str(r.get("trade_date") or "").strip()[:10]
        sym = str(r.get("symbol") or "").strip()
        fn = str(r.get("factor_name") or "").strip()
        if not td or not sym or not fn:
            continue
        vals.append((
            td, sym, fn,
            str(r.get("direction") or "short"),
            float(r.get("entry_price") or 0),
            float(r.get("target_price") or 0),
            float(r.get("stop_price") or 0),
            float(r.get("probability") or 0),
            int(r.get("hold_days") or 20),
            str(r.get("status") or "pending"),
            r.get("ret60"),
            r.get("vol_ratio"),
            r.get("industry"),
            r.get("board"),
            r.get("symbol_name"),
        ))
    if not vals:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TABLE_NAME} (
                    trade_date, symbol, factor_name, direction,
                    entry_price, target_price, stop_price, probability, hold_days, status,
                    ret60, vol_ratio, industry, board, symbol_name, updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
                ON CONFLICT (trade_date, symbol, factor_name) DO UPDATE SET
                    direction=excluded.direction, entry_price=excluded.entry_price,
                    target_price=excluded.target_price, stop_price=excluded.stop_price,
                    probability=excluded.probability, hold_days=excluded.hold_days,
                    status=excluded.status, ret60=excluded.ret60, vol_ratio=excluded.vol_ratio,
                    industry=excluded.industry, board=excluded.board,
                    symbol_name=excluded.symbol_name, updated_at=now()
                """,
                vals,
            )
        conn.commit()
    return len(vals)


def fetch_by_date(trade_date: str) -> list[dict]:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT trade_date, symbol, factor_name, direction, entry_price, target_price, stop_price, probability, hold_days, status, ret60, vol_ratio, industry, board, symbol_name FROM {TABLE_NAME} WHERE trade_date=%s ORDER BY probability DESC, symbol",
                (trade_date,),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r, strict=False)) for r in cur.fetchall()]
