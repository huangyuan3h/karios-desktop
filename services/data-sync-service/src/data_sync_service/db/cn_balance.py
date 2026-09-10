"""CN balance sheet (tushare balancesheet) — PiT ann_date key.

Source: tushare balancesheet (per ts_code, all periods). Stored per
(ts_code, ann_date, end_date, report_type) so ML can forward-fill by ann_date
without lookahead. report_type is part of the key because tushare returns
both merged (1) and parent-company rows for the same period.
Hot quant subjects are typed columns; the full 150-field row is kept in
extra JSONB so no source data is lost.
"""

from __future__ import annotations

import json
import math

from data_sync_service.db import get_connection
from data_sync_service.db._ensure_guard import ensure_once

TABLE_NAME = "cn_balance_sheet"

#: Typed subjects (everything else lands in extra JSONB).
TYPED_FIELDS = (
    "total_assets",
    "total_cur_assets",
    "money_cap",
    "accounts_receiv",
    "oth_receiv",
    "inventories",
    "goodwill",
    "intan_assets",
    "total_liab",
    "total_cur_liab",
    "st_borr",
    "lt_borr",
    "bond_payable",
    "total_hldr_eqy_inc_min_int",
    "total_hldr_eqy_exc_min_int",
)

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ts_code            TEXT NOT NULL,
    ann_date           DATE NOT NULL,
    end_date           DATE NOT NULL,
    report_type        TEXT NOT NULL DEFAULT '1',
    f_ann_date         DATE,
    comp_type          TEXT,
    end_type           TEXT,
    total_assets       DOUBLE PRECISION,
    total_cur_assets   DOUBLE PRECISION,
    money_cap          DOUBLE PRECISION,
    accounts_receiv    DOUBLE PRECISION,
    oth_receiv         DOUBLE PRECISION,
    inventories        DOUBLE PRECISION,
    goodwill           DOUBLE PRECISION,
    intan_assets       DOUBLE PRECISION,
    total_liab         DOUBLE PRECISION,
    total_cur_liab     DOUBLE PRECISION,
    st_borr            DOUBLE PRECISION,
    lt_borr            DOUBLE PRECISION,
    bond_payable       DOUBLE PRECISION,
    total_hldr_eqy_inc_min_int DOUBLE PRECISION,
    total_hldr_eqy_exc_min_int DOUBLE PRECISION,
    update_flag        TEXT,
    extra              JSONB,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ts_code, ann_date, end_date, report_type)
);

CREATE INDEX IF NOT EXISTS idx_cn_balance_ann_date ON {TABLE_NAME}(ann_date DESC);
CREATE INDEX IF NOT EXISTS idx_cn_balance_ts_end ON {TABLE_NAME}(ts_code, end_date DESC);
"""


def ensure_table() -> None:
    def _impl() -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_SQL)
            conn.commit()

    ensure_once(TABLE_NAME, _impl)


def _date(s: str | None) -> str | None:
    if not s:
        return None
    s = str(s).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s or None


def _num(v) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def _clean_extra(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


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
        rt = str(r.get("report_type") or "1").strip() or "1"
        extra = {k: _clean_extra(v) for k, v in r.items() if k not in ("ts_code",)}
        vals.append(
            (
                ts,
                ann,
                end,
                rt,
                _date(r.get("f_ann_date")),
                (str(r.get("comp_type") or "").strip() or None),
                (str(r.get("end_type") or "").strip() or None),
                *(_num(r.get(f)) for f in TYPED_FIELDS),
                (str(r.get("update_flag") or "").strip() or None),
                json.dumps(extra, ensure_ascii=False, default=str),
            )
        )
    if not vals:
        return 0
    cols = ", ".join(TYPED_FIELDS)
    sets = ", ".join(f"{f}=excluded.{f}" for f in TYPED_FIELDS)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TABLE_NAME} (
                    ts_code, ann_date, end_date, report_type, f_ann_date,
                    comp_type, end_type, {cols},
                    update_flag, extra, updated_at
                ) VALUES ({",".join(["%s"] * (7 + len(TYPED_FIELDS)))},%s,%s::jsonb, now())
                ON CONFLICT (ts_code, ann_date, end_date, report_type) DO UPDATE SET
                    f_ann_date=excluded.f_ann_date, comp_type=excluded.comp_type,
                    end_type=excluded.end_type, {sets},
                    update_flag=excluded.update_flag, extra=excluded.extra, updated_at=now()
                """,
                vals,
            )
        conn.commit()
    return len(vals)
