"""Satellite push log — every 14:30 intraday screen, recorded in DB.

The intraday screen (twin_star_intraday) is cached per-day as JSON; this
table is the queryable twin: which names were pushed (candidates),
shown as alternates, blocked (limit-up) or skipped (C1), with the
gate/breadth context. Joins with user_trades on (ts_code, trade_date)
to audit push -> fill: did live buy what was pushed, and did pushed
names pay?

Write path: cache_intraday_sat() calls log_push() (best-effort, never
raises into the caching path). PK (trade_date, slot, ts_code) makes
re-logging idempotent.
"""

from __future__ import annotations

from datetime import datetime

from data_sync_service.db import get_connection
from data_sync_service.db._ensure_guard import ensure_once

# Canonical decision snapshot window (Asia/Shanghai): the 14:20 reminder /
# 14:30 buy. Later ticks (15:00 full-day tape) must not overwrite the list the
# user actually acted on, so only this window is persisted.
_DECISION_WINDOW_MIN = (14 * 60) + 20
_DECISION_WINDOW_MAX = (14 * 60) + 35


def _in_decision_window(snapshot_at) -> bool:
    try:
        t = datetime.fromisoformat(str(snapshot_at))
    except (TypeError, ValueError):
        return False
    mins = t.hour * 60 + t.minute
    return _DECISION_WINDOW_MIN <= mins <= _DECISION_WINDOW_MAX


TABLE_NAME = "sat_push_log"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    trade_date  DATE NOT NULL,
    slot        TEXT NOT NULL,
    ts_code     TEXT NOT NULL,
    amp         DOUBLE PRECISION,
    gap_pct     DOUBLE PRECISION,
    gate_open   BOOLEAN,
    breadth     DOUBLE PRECISION,
    snapshot_at TIMESTAMPTZ,
    stage       TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, slot, ts_code)
);
CREATE INDEX IF NOT EXISTS idx_sat_push_log_date ON {TABLE_NAME}(trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_sat_push_log_ts_date ON {TABLE_NAME}(ts_code, trade_date DESC);
"""


def ensure_table() -> None:
    def _impl() -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_SQL)
            conn.commit()

    ensure_once(TABLE_NAME, _impl)


def list_candidates(dates: list[str]) -> set[str]:
    """ts_codes pushed as ``candidates`` on the given trade dates.

    Operational truth for the behavior audit's satellite expected set: the
    names the 14:20-14:35 screen actually told the user to buy. Empty when the
    table has no rows for those dates (pre-2026-09-11 history).
    """
    if not dates:
        return set()
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT ts_code FROM {TABLE_NAME} "
                "WHERE trade_date = ANY(%s) AND slot = 'candidates'",
                (list(dates),),
            )
            return {str(r[0]) for r in cur.fetchall()}


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


def log_push(screen: dict) -> dict:
    """Persist one intraday screen. Returns {"ok": n} — never raises."""
    try:
        return {"ok": _log_push(screen)}
    except Exception as exc:  # noqa: BLE001 — caching path must not break
        return {"ok": 0, "error": str(exc)[:200]}


def _log_push(screen: dict) -> int:
    ensure_table()
    if not isinstance(screen, dict):
        return 0
    day = _date(screen.get("asOf"))
    if not day:
        return 0
    gate = screen.get("gateOpen")
    breadth = _num(screen.get("breadth"))
    snap = screen.get("snapshotAt")
    if not _in_decision_window(snap):
        # Only the 14:20-14:35 decision tape is canonical; skip overnight /
        # post-close rebuilds so push->fill auditing sees the acted-on list.
        return 0
    vals = []
    for slot in ("candidates", "alternates", "blocked", "skippedC1"):
        for row in screen.get(slot) or []:
            if not isinstance(row, dict):
                continue
            ts = str(row.get("ts") or "").strip()
            if not ts:
                continue
            stage = row.get("stage")
            vals.append(
                (
                    day,
                    slot,
                    ts,
                    _num(row.get("amp")),
                    _num(row.get("gapPct")),
                    None if gate is None else bool(gate),
                    breadth,
                    snap,
                    str(stage)[:40] if stage else None,
                )
            )
    if not vals:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TABLE_NAME}(trade_date, slot, ts_code, amp, gap_pct,
                    gate_open, breadth, snapshot_at, stage, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
                ON CONFLICT (trade_date, slot, ts_code) DO UPDATE SET
                    amp=excluded.amp, gap_pct=excluded.gap_pct,
                    gate_open=excluded.gate_open, breadth=excluded.breadth,
                    snapshot_at=excluded.snapshot_at, stage=excluded.stage,
                    updated_at=now()
                """,
                vals,
            )
        conn.commit()
    return len(vals)
