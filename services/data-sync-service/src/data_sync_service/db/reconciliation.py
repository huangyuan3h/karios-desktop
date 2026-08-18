"""backtest_paper_recon table — weekly reconciliation snapshots (2026-08-11)."""

from __future__ import annotations

import json
from typing import Any

from data_sync_service.db import get_connection

TABLE_NAME = "backtest_paper_recon"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id          SERIAL PRIMARY KEY,
    recon_date  TEXT NOT NULL,
    market      TEXT NOT NULL,
    "window"    TEXT NOT NULL,
    expected    INTEGER NOT NULL,
    actual      INTEGER NOT NULL,
    aligned     INTEGER NOT NULL,
    missing     INTEGER NOT NULL,
    extra       INTEGER NOT NULL,
    aligned_return_diff_pct   NUMERIC,
    bt_return_median_pct      NUMERIC,
    paper_return_median_pct   NUMERIC,
    detail      TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (recon_date, market)
);
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)


def insert_recon(*, recon_date: str, market: str, window: str, expected: int,
                 actual: int, aligned: int, missing: int, extra: int,
                 aligned_return_diff_pct: float | None = None,
                 bt_return_median_pct: float | None = None,
                 paper_return_median_pct: float | None = None,
                 detail: list[dict] | None = None) -> None:
    """Upsert the day's reconciliation per market (re-running the same day
    is idempotent — the cron re-runs Monday morning freely)."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {TABLE_NAME}
                    (recon_date, market, "window", expected, actual, aligned, missing, extra,
                     aligned_return_diff_pct, bt_return_median_pct, paper_return_median_pct, detail)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (recon_date, market) DO UPDATE SET
                    "window" = EXCLUDED."window",
                    expected = EXCLUDED.expected,
                    actual = EXCLUDED.actual,
                    aligned = EXCLUDED.aligned,
                    missing = EXCLUDED.missing,
                    extra = EXCLUDED.extra,
                    aligned_return_diff_pct = EXCLUDED.aligned_return_diff_pct,
                    bt_return_median_pct = EXCLUDED.bt_return_median_pct,
                    paper_return_median_pct = EXCLUDED.paper_return_median_pct,
                    detail = EXCLUDED.detail,
                    created_at = now()
                """,
                (recon_date, market, window, expected, actual, aligned, missing, extra,
                 aligned_return_diff_pct, bt_return_median_pct, paper_return_median_pct,
                 json.dumps(detail, ensure_ascii=False) if detail else None),
            )
            conn.commit()


def latest_recon(limit: int = 4) -> list[dict[str, Any]]:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT recon_date, market, "window", expected, actual, aligned, missing, extra,
                       aligned_return_diff_pct, bt_return_median_pct, paper_return_median_pct, detail
                FROM {TABLE_NAME}
                ORDER BY recon_date DESC, market
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
    out = []
    for r in rows:
        out.append({
            "reconDate": r[0],
            "market": r[1],
            "window": r[2],
            "expected": r[3],
            "actual": r[4],
            "aligned": r[5],
            "missing": r[6],
            "extra": r[7],
            "alignedReturnDiffPct": float(r[8]) if r[8] is not None else None,
            "btReturnMedianPct": float(r[9]) if r[9] is not None else None,
            "paperReturnMedianPct": float(r[10]) if r[10] is not None else None,
            "detail": json.loads(r[11]) if r[11] else None,
        })
    return out
