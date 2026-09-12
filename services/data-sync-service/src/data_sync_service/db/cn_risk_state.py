"""TIP-017 risk-state data — ETF shares / margin total / north-bound / global index.

Four small daily series backing the risk-state sensor family (pre-registered
2026-09-09, docs/backtests/risk-state-sensors-2026-09-09.md):

- ``cn_etf_share``        broad-ETF share counts (国家队代理; tushare fund_share, 2018+)
- ``cn_margin_total``     market-wide margin balance (tushare margin, 2021+)
- ``cn_moneyflow_hsgt``   north/south-bound flow (tushare moneyflow_hsgt, 2021+)
- ``global_index_daily``  HSI/HSTECH daily bars (tushare index_global, 2018+)

global_index_daily is deliberately SEPARATE from macro_daily: backfilling HSI
into macro_daily would change the HK engine's long-window fail-closed regime
behavior and invalidate the frozen HK baseline (2026-09-09). Risk-state
modules read global_index_daily instead.
"""

from __future__ import annotations

from typing import LiteralString

from data_sync_service.db import get_connection
from data_sync_service.db._ensure_guard import ensure_once

ETF_SHARE_TABLE = "cn_etf_share"
MARGIN_TOTAL_TABLE = "cn_margin_total"
HSGT_TABLE = "cn_moneyflow_hsgt"
GLOBAL_INDEX_TABLE = "global_index_daily"
FLOW_DAILY_TABLE = "cn_flow_daily"

ETF_SHARE_SQL = f"""
CREATE TABLE IF NOT EXISTS {ETF_SHARE_TABLE} (
    trade_date   DATE NOT NULL,
    ts_code      TEXT NOT NULL,
    fd_share     DOUBLE PRECISION,
    fund_type    TEXT,
    market       TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, ts_code)
);
CREATE INDEX IF NOT EXISTS idx_cn_etf_share_date ON {ETF_SHARE_TABLE}(trade_date DESC);
"""

MARGIN_TOTAL_SQL = f"""
CREATE TABLE IF NOT EXISTS {MARGIN_TOTAL_TABLE} (
    trade_date   DATE NOT NULL,
    exchange_id  TEXT NOT NULL,
    rzye         DOUBLE PRECISION,
    rzmre        DOUBLE PRECISION,
    rzche        DOUBLE PRECISION,
    rqye         DOUBLE PRECISION,
    rqmcl        DOUBLE PRECISION,
    rzrqye       DOUBLE PRECISION,
    rqyl         DOUBLE PRECISION,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, exchange_id)
);
CREATE INDEX IF NOT EXISTS idx_cn_margin_total_date ON {MARGIN_TOTAL_TABLE}(trade_date DESC);
"""

HSGT_SQL = f"""
CREATE TABLE IF NOT EXISTS {HSGT_TABLE} (
    trade_date   DATE NOT NULL,
    ggt_ss       DOUBLE PRECISION,
    ggt_sz       DOUBLE PRECISION,
    hgt          DOUBLE PRECISION,
    sgt          DOUBLE PRECISION,
    north_money  DOUBLE PRECISION,
    south_money  DOUBLE PRECISION,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date)
);
CREATE INDEX IF NOT EXISTS idx_cn_moneyflow_hsgt_date ON {HSGT_TABLE}(trade_date DESC);
"""

GLOBAL_INDEX_SQL = f"""
CREATE TABLE IF NOT EXISTS {GLOBAL_INDEX_TABLE} (
    trade_date   DATE NOT NULL,
    ts_code      TEXT NOT NULL,
    open         DOUBLE PRECISION,
    high         DOUBLE PRECISION,
    low          DOUBLE PRECISION,
    close        DOUBLE PRECISION,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, ts_code)
);
CREATE INDEX IF NOT EXISTS idx_global_index_date ON {GLOBAL_INDEX_TABLE}(trade_date DESC);
"""

# Daily size-class flow rollup from cn_moneyflow (TIP-017 C 散户共振代理).
# turnover ≈ Σ(buy+sell, all classes)/2; 散户净买占比 = sm_net / turnover (frontend).
FLOW_DAILY_SQL = f"""
CREATE TABLE IF NOT EXISTS {FLOW_DAILY_TABLE} (
    trade_date   DATE NOT NULL,
    sm_net       DOUBLE PRECISION,
    md_net       DOUBLE PRECISION,
    lg_net       DOUBLE PRECISION,
    elg_net      DOUBLE PRECISION,
    turnover     DOUBLE PRECISION,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date)
);
CREATE INDEX IF NOT EXISTS idx_cn_flow_daily_date ON {FLOW_DAILY_TABLE}(trade_date DESC);
"""


def _ensure_sql(sql: LiteralString, name: str) -> None:
    def _impl() -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()

    ensure_once(name, _impl)


def ensure_etf_share() -> None:
    _ensure_sql(ETF_SHARE_SQL, ETF_SHARE_TABLE)


def ensure_margin_total() -> None:
    _ensure_sql(MARGIN_TOTAL_SQL, MARGIN_TOTAL_TABLE)


def ensure_hsgt() -> None:
    _ensure_sql(HSGT_SQL, HSGT_TABLE)


def ensure_global_index() -> None:
    _ensure_sql(GLOBAL_INDEX_SQL, GLOBAL_INDEX_TABLE)


def ensure_flow_daily() -> None:
    _ensure_sql(FLOW_DAILY_SQL, FLOW_DAILY_TABLE)


def aggregate_flow_daily(start_date: str, end_date: str) -> int:
    """Roll up cn_moneyflow per-stock rows into daily size-class nets."""
    ensure_flow_daily()
    sql = f"""INSERT INTO {FLOW_DAILY_TABLE}
        (trade_date, sm_net, md_net, lg_net, elg_net, turnover, updated_at)
        SELECT trade_date,
               SUM(COALESCE(buy_sm_amount, 0) - COALESCE(sell_sm_amount, 0)),
               SUM(COALESCE(buy_md_amount, 0) - COALESCE(sell_md_amount, 0)),
               SUM(COALESCE(buy_lg_amount, 0) - COALESCE(sell_lg_amount, 0)),
               SUM(COALESCE(buy_elg_amount, 0) - COALESCE(sell_elg_amount, 0)),
               SUM(COALESCE(buy_sm_amount, 0) + COALESCE(sell_sm_amount, 0)
                   + COALESCE(buy_md_amount, 0) + COALESCE(sell_md_amount, 0)
                   + COALESCE(buy_lg_amount, 0) + COALESCE(sell_lg_amount, 0)
                   + COALESCE(buy_elg_amount, 0) + COALESCE(sell_elg_amount, 0)) / 2.0,
               now()
        FROM cn_moneyflow
        WHERE trade_date BETWEEN %s AND %s
        GROUP BY trade_date
        ON CONFLICT (trade_date) DO UPDATE SET sm_net = excluded.sm_net,
            md_net = excluded.md_net, lg_net = excluded.lg_net,
            elg_net = excluded.elg_net, turnover = excluded.turnover,
            updated_at = now()"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (start_date, end_date))
            n = cur.rowcount
        conn.commit()
    return max(n or 0, 0)


def _date(s) -> str | None:
    if not s:
        return None
    s = str(s).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s or None


def _num(v) -> float | None:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _upsert(sql: str, vals: list[tuple]) -> int:
    if not vals:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, vals)
        conn.commit()
    return len(vals)


def upsert_etf_share(rows: list[dict]) -> int:
    ensure_etf_share()
    vals = [
        (
            _date(r.get("trade_date")),
            str(r.get("ts_code") or "").strip(),
            _num(r.get("fd_share")),
            r.get("fund_type"),
            r.get("market"),
        )
        for r in rows
        if _date(r.get("trade_date")) and r.get("ts_code")
    ]
    return _upsert(
        f"""INSERT INTO {ETF_SHARE_TABLE}(trade_date, ts_code, fd_share, fund_type, market, updated_at)
        VALUES (%s,%s,%s,%s,%s, now())
        ON CONFLICT (trade_date, ts_code) DO UPDATE SET fd_share=excluded.fd_share,
        fund_type=excluded.fund_type, market=excluded.market, updated_at=now()""",
        vals,
    )


def upsert_margin_total(rows: list[dict]) -> int:
    ensure_margin_total()
    vals = [
        (
            _date(r.get("trade_date")),
            str(r.get("exchange_id") or "").strip(),
            _num(r.get("rzye")),
            _num(r.get("rzmre")),
            _num(r.get("rzche")),
            _num(r.get("rqye")),
            _num(r.get("rqmcl")),
            _num(r.get("rzrqye")),
            _num(r.get("rqyl")),
        )
        for r in rows
        if _date(r.get("trade_date")) and r.get("exchange_id")
    ]
    return _upsert(
        f"""INSERT INTO {MARGIN_TOTAL_TABLE}(trade_date, exchange_id, rzye, rzmre, rzche, rqye, rqmcl, rzrqye, rqyl, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
        ON CONFLICT (trade_date, exchange_id) DO UPDATE SET rzye=excluded.rzye, rzmre=excluded.rzmre,
        rzche=excluded.rzche, rqye=excluded.rqye, rqmcl=excluded.rqmcl, rzrqye=excluded.rzrqye,
        rqyl=excluded.rqyl, updated_at=now()""",
        vals,
    )


def upsert_hsgt(rows: list[dict]) -> int:
    ensure_hsgt()
    vals = [
        (
            _date(r.get("trade_date")),
            _num(r.get("ggt_ss")),
            _num(r.get("ggt_sz")),
            _num(r.get("hgt")),
            _num(r.get("sgt")),
            _num(r.get("north_money")),
            _num(r.get("south_money")),
        )
        for r in rows
        if _date(r.get("trade_date"))
    ]
    return _upsert(
        f"""INSERT INTO {HSGT_TABLE}(trade_date, ggt_ss, ggt_sz, hgt, sgt, north_money, south_money, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s, now())
        ON CONFLICT (trade_date) DO UPDATE SET ggt_ss=excluded.ggt_ss, ggt_sz=excluded.ggt_sz,
        hgt=excluded.hgt, sgt=excluded.sgt, north_money=excluded.north_money,
        south_money=excluded.south_money, updated_at=now()""",
        vals,
    )


def upsert_global_index(rows: list[dict]) -> int:
    ensure_global_index()
    vals = [
        (
            _date(r.get("trade_date")),
            str(r.get("ts_code") or "").strip(),
            _num(r.get("open")),
            _num(r.get("high")),
            _num(r.get("low")),
            _num(r.get("close")),
        )
        for r in rows
        if _date(r.get("trade_date")) and r.get("ts_code")
    ]
    return _upsert(
        f"""INSERT INTO {GLOBAL_INDEX_TABLE}(trade_date, ts_code, open, high, low, close, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s, now())
        ON CONFLICT (trade_date, ts_code) DO UPDATE SET open=excluded.open, high=excluded.high,
        low=excluded.low, close=excluded.close, updated_at=now()""",
        vals,
    )
