"""CN holder number — quarterly chip concentration."""

from __future__ import annotations

from data_sync_service.db import get_connection
from data_sync_service.db._ensure_guard import ensure_once

TABLE_NAME = "cn_holder_number"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ts_code     TEXT NOT NULL,
    ann_date    DATE NOT NULL,
    end_date    DATE NOT NULL,
    holder_num  DOUBLE PRECISION,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ts_code, ann_date, end_date)
);
CREATE INDEX IF NOT EXISTS idx_cn_holder_ann ON {TABLE_NAME}(ann_date DESC);
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
    s = str(s).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s or None


def upsert_rows(rows: list[dict]) -> int:
    ensure_table()
    if not rows:
        return 0
    vals = []
    for r in rows:
        ts = str(r.get("ts_code") or "").strip()
        ann = _date(r.get("ann_date"))
        end = _date(r.get("end_date"))
        if not ts or not ann or not end:
            continue
        vals.append((ts, ann, end, r.get("holder_num")))
    if not vals:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TABLE_NAME}(ts_code, ann_date, end_date, holder_num, updated_at)
                VALUES (%s,%s,%s,%s, now())
                ON CONFLICT (ts_code, ann_date, end_date) DO UPDATE SET holder_num=excluded.holder_num, updated_at=now()
                """,
                vals,
            )
        conn.commit()
    return len(vals)
