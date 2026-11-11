"""Non-East-Money industry fund-flow fallback (2026-11-06).

Why
---
``industry_fund_flow`` used East Money's ``dataapi/bkzj`` (EOD) and
``push2his .../fflow/daykline`` (history). During the 2026-11-03+ egress
outage the history endpoint always failed, and the EOD endpoint intermittently
failed. The old code returned ``ok=True`` with 0 rows in that case — a silent
fake success.

What this module computes
-------------------------
Tushare's free ``moneyflow`` API gives per-stock buy/sell amounts by order size.
Main-flow (主力净流入) is the standard large + extra-large definition:

    main = (buy_lg + buy_elg) - (sell_lg + sell_elg)      [万元 → ×10,000 CNY]

Aggregated by 申万一级 industry (``index_member_all(is_new='Y')``) this
reproduces the East Money industry ranking closely — verified 2026-11-06 on
11-05/11-06: top-5 industry names identical, values within ~2-3%.

This is **not** a price/volume proxy: it is the same economic metric from a
different vendor. It is still tagged honestly (``source`` =
``tushare_moneyflow_sw_l1``) and never presented as East Money's own number.

Data source order
-----------------
1. ``cn_moneyflow`` (the canonical per-stock Tushare moneyflow table already
   populated by ``cn_extra_sync``) — no network, no extra API quota.
2. If the day is absent there, ingest it through
   :func:`cn_extra_sync.sync_moneyflow_for_dates` (the existing adapter), then
   re-read.
3. Industry membership is fetched once per Shanghai day and cached in-process.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

from data_sync_service.db import get_connection
from data_sync_service.service.industry_taxonomy import (
    SW_L1_LEVEL,
    SW_TAXONOMY,
)

logger = logging.getLogger(__name__)

SOURCE = "tushare_moneyflow_sw_l1"
METRIC = "main_net_inflow_lg_elg"
YI_WAN_TO_CNY = 10_000.0  # Tushare moneyflow amounts are 万元

_MEMBERSHIP_LOCK = threading.Lock()
_MEMBERSHIP_CACHE: dict[str, Any] = {"day": None, "map": {}}


def _shanghai_today_iso() -> str:
    from data_sync_service.service.trade_calendar_utils import shanghai_today_iso

    return shanghai_today_iso()


def reset_cache() -> None:
    """Clear the in-process membership cache (tests / explicit refresh)."""
    with _MEMBERSHIP_LOCK:
        _MEMBERSHIP_CACHE["day"] = None
        _MEMBERSHIP_CACHE["map"] = {}


def _fetch_sw_l1_membership() -> dict[str, tuple[str, str]]:
    """One Tushare call: current SW L1 members, ts_code -> (code, name)."""
    from data_sync_service.clients.tushare_pool import get_pool

    pro = get_pool().pro()
    df = pro.index_member_all(is_new="Y")
    if df is None or getattr(df, "empty", True):
        raise RuntimeError("tushare: index_member_all returned no rows")
    mapping: dict[str, tuple[str, str]] = {}
    for row in df.to_dict("records"):
        ts_code = str(row.get("ts_code") or "").strip()
        l1_code = str(row.get("l1_code") or "").strip()
        l1_name = str(row.get("l1_name") or "").strip()
        if ts_code and l1_code and l1_name:
            mapping[ts_code] = (l1_code, l1_name)
    if not mapping:
        raise RuntimeError("tushare: empty SW L1 membership")
    return mapping


def sw_l1_membership(*, force: bool = False) -> dict[str, tuple[str, str]]:
    """Cached SW L1 membership for the current Shanghai day."""
    today = _shanghai_today_iso()
    with _MEMBERSHIP_LOCK:
        cached = _MEMBERSHIP_CACHE.get("map") or {}
        if not force and _MEMBERSHIP_CACHE.get("day") == today and cached:
            return dict(cached)
    mapping = _fetch_sw_l1_membership()
    with _MEMBERSHIP_LOCK:
        _MEMBERSHIP_CACHE["day"] = today
        _MEMBERSHIP_CACHE["map"] = mapping
    return dict(mapping)


def _moneyflow_rows_from_db(day_iso: str) -> list[dict[str, Any]]:
    """Per-stock moneyflow rows for one date (empty when not ingested yet)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts_code, buy_lg_amount, sell_lg_amount,
                       buy_elg_amount, sell_elg_amount
                FROM cn_moneyflow
                WHERE trade_date = %s
                """,
                (day_iso,),
            )
            rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for r in rows or []:
        out.append(
            {
                "ts_code": str(r[0] or ""),
                "buy_lg_amount": r[1],
                "sell_lg_amount": r[2],
                "buy_elg_amount": r[3],
                "sell_elg_amount": r[4],
            }
        )
    return out


def _ensure_moneyflow_ingested(day_iso: str) -> list[dict[str, Any]]:
    """Return the day's rows, ingesting via the existing adapter if missing."""
    rows = _moneyflow_rows_from_db(day_iso)
    if rows:
        return rows
    try:
        from data_sync_service.service.cn_extra_sync import sync_moneyflow_for_dates

        sync_moneyflow_for_dates([day_iso])
    except Exception as exc:  # noqa: BLE001 - surfaced to the caller as a note
        logger.warning("tushare moneyflow ingest failed for %s: %s", day_iso, exc)
        raise RuntimeError(f"tushare: moneyflow ingest failed for {day_iso}: {exc}") from exc
    rows = _moneyflow_rows_from_db(day_iso)
    if not rows:
        raise RuntimeError(
            f"tushare: no moneyflow rows for {day_iso} (cn_moneyflow still empty after sync)"
        )
    return rows


def _to_float(value: Any) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return 0.0
    return f if f == f else 0.0


def aggregate_for_date(day_iso: str) -> list[dict[str, Any]]:
    """Aggregate one day's per-stock moneyflow into SW L1 industries.

    Returns rows shaped like ``industry_fund_flow``'s daily rows (CNY). Raises
    ``RuntimeError`` with a ``tushare:`` prefix when the source is unusable —
    the caller must treat that as a real provider failure, not a fake zero.
    """
    mapping = sw_l1_membership()
    rows = _ensure_moneyflow_ingested(day_iso)
    by_industry: dict[str, dict[str, Any]] = {}
    unmapped = 0
    for row in rows:
        ts_code = str(row.get("ts_code") or "")
        industry = mapping.get(ts_code)
        if not industry:
            unmapped += 1
            continue
        l1_code, l1_name = industry
        main_wan = (
            _to_float(row.get("buy_lg_amount"))
            + _to_float(row.get("buy_elg_amount"))
            - _to_float(row.get("sell_lg_amount"))
            - _to_float(row.get("sell_elg_amount"))
        )
        rec = by_industry.setdefault(
            l1_name,
            {
                "industry_code": l1_code,
                "industry_name": l1_name,
                "main_wan": 0.0,
                "members": 0,
            },
        )
        rec["main_wan"] += main_wan
        rec["members"] += 1

    if not by_industry:
        raise RuntimeError(f"tushare: no SW L1 industry matched for {day_iso}")

    out: list[dict[str, Any]] = []
    for rec in by_industry.values():
        out.append(
            {
                "date": day_iso,
                "industry_code": rec["industry_code"],
                "industry_name": rec["industry_name"],
                "net_inflow": float(rec["main_wan"]) * YI_WAN_TO_CNY,
                "raw": {
                    "provider": "tushare",
                    "api": "moneyflow",
                    "metric": METRIC,
                    "unit": "CNY",
                    "members": int(rec["members"]),
                    "unmappedRows": int(unmapped),
                },
                "taxonomy": SW_TAXONOMY,
                "industry_level": SW_L1_LEVEL,
                "source": SOURCE,
            }
        )
    out.sort(key=lambda r: float(r["net_inflow"]), reverse=True)
    return out


def aggregate_for_dates(days: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    """Aggregate several dates; returns (rows, per-date errors) without raising.

    Per-date isolation keeps a partially available history useful (e.g. one
    day not yet published) while still reporting which dates failed.
    """
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    for day in sorted({str(d) for d in days if d}):
        try:
            rows.extend(aggregate_for_date(day))
        except Exception as exc:  # noqa: BLE001 - keep other dates
            errors.append(f"{day}: {exc}")
    return rows, errors
