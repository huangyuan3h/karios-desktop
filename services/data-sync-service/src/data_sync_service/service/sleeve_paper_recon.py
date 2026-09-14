"""Core-leg paper recon (OPT-151) — expected-vs-actual for the multi-asset
sleeve paper automation, mirroring the satellite H5 recon contract
(``paper_twin_star_recon``).

The satellite leg has a daily expected-vs-actual gate; the core leg (50% NAV
when the satellite sleeve is live) only had a one-way mirror
(``sleeve_paper_auto.apply_sleeve_to_paper``) with no reconciliation. This
module answers, for any trading day, the three questions the operator asks:

1. What SHOULD the core leg have done (action from the same frozen state
   machine the 18:20 job ran)?
2. Did the paper book record it?
3. Did the user actually do it (informational — execution is manual)?

Decision reproduction: the state machine is re-evaluated against the paper
book state AS IT WAS BEFORE the 18:20 job — open CN:/ETF: rows created before
``day`` (identical to ``_build_multi_for_paper``) plus sleeve legs that the
job closed that day (``close_reason=sleeve_exit``, ``close_date=day``), minus
rows the job itself opened that day (multi-sleeve why-text / next_open fill
with ``signalDate == day``).

``ok`` is paper-level only (the automation must mirror the engine). The user
side never flips ``ok`` — execution is manual and may lag (BUY fills next
open).
"""

from __future__ import annotations

import logging
from typing import Any

from data_sync_service.db.paper_trading import (
    CLOSE_REASON_SLEEVE_EXIT,
    list_paper_trades,
)
from data_sync_service.service.multi_asset_sleeve import (
    CANDIDATES,
    build_multi_asset_sleeve,
)
from data_sync_service.service.portfolio_health import _health_block
from data_sync_service.service.sleeve_paper_auto import normalize_sleeve_pct

logger = logging.getLogger(__name__)

CANDIDATE_SYMBOLS = {c["symbol"] for c in CANDIDATES}

BUY_ACTIONS = ("BUY", "ROTATE")
SELL_ACTIONS = ("SELL_TO_REPO",)


def _day_of(value: Any) -> str:
    return str(value or "")[:10]


def _paper_snapshot() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return (
        list_paper_trades(status="open", limit=200),
        list_paper_trades(status="closed", limit=200),
    )


def _opened_by_sleeve_job_today(row: dict[str, Any], day: str) -> bool:
    """Rows the 18:20 job itself created on ``day`` (decision executions).

    These must not enter the pre-decision holdings — they ARE the decision.
    Gate on the decision day (signal snapshot, else row creation day), NOT on
    the why-text: a leg opened on an earlier day is still held today.
    """
    sym = str(row.get("symbol") or "").upper()
    if sym not in CANDIDATE_SYMBOLS:
        return False
    snap = row.get("signalSnapshot") or {}
    if isinstance(snap, dict) and snap.get("entryMode") == "next_open":
        sig = _day_of(snap.get("signalDate"))
        if sig:
            return sig == day
    return _day_of(row.get("createdAt")) == day


def _pre_decision_holdings(
    open_rows: list[dict[str, Any]],
    closed_rows: list[dict[str, Any]],
    day: str,
    exec_day: str | None = None,
) -> list[dict[str, Any]]:
    """Paper-book holdings as the 18:20 job saw them (shape-identical to
    ``paper_sleeve_holdings``: symbol / ts_code / entryDate / percent
    sleeve_pct), so the reproduced decision matches the job's.

    Harbor exits book at the next session (closeDate = exec_day); include those
    as held at ``day`` close so the decision is reproduced. Legs closed at
    ``day``'s own open (closeDate = day) are gone from the job's view → exclude.
    """
    sold_days = {exec_day} if exec_day else {day}
    holdings: list[dict[str, Any]] = []
    for r in open_rows:
        sym = str(r.get("symbol") or "").upper()
        if not sym.startswith(("CN:", "ETF:")):
            continue
        if _opened_by_sleeve_job_today(r, day):
            continue
        holdings.append(
            {
                "symbol": r.get("symbol"),
                "ts_code": r.get("tsCode") or r.get("ts_code"),
                "entryDate": r.get("entryDate") or r.get("entry_date"),
                "sleeve_pct": normalize_sleeve_pct(r.get("sleevePct") or r.get("sleeve_pct")),
            }
        )
    for r in closed_rows:
        if (
            str(r.get("symbol") or "").upper() in CANDIDATE_SYMBOLS
            and str(r.get("closeReason") or "") == CLOSE_REASON_SLEEVE_EXIT
            and _day_of(r.get("closeDate")) in sold_days
        ):
            holdings.append(
                {
                    "symbol": r.get("symbol"),
                    "ts_code": r.get("tsCode") or r.get("ts_code"),
                    "entryDate": r.get("entryDate") or r.get("entry_date"),
                    "sleeve_pct": normalize_sleeve_pct(r.get("sleevePct") or r.get("sleeve_pct")),
                }
            )
    return holdings


def _rebuild_decision(day: str, holdings: list[dict[str, Any]]) -> dict[str, Any]:
    cn_block = _health_block(market="CN", day=day)
    return build_multi_asset_sleeve(day=day, cn_block=cn_block, holdings_override=holdings)


def _expected_flows(
    multi: dict[str, Any], pre_legs: list[dict[str, Any]]
) -> tuple[list[str], list[str]]:
    """Expected ETF buys/sells from the reproduced action."""
    action = str(multi.get("action") or "NONE")
    pick = multi.get("pick") or {}
    pick_symbol = str(pick.get("symbol") or "")
    pick_symbol = pick_symbol.upper() if pick_symbol in CANDIDATE_SYMBOLS else ""
    buys: list[str] = []
    sells: list[str] = []
    if action in BUY_ACTIONS and pick_symbol:
        buys = [pick_symbol]
    if action in SELL_ACTIONS:
        sells = sorted({str(x.get("symbol") or "").upper() for x in pre_legs} - {""})
    elif action == "ROTATE":
        held = {str(x.get("symbol") or "").upper() for x in pre_legs}
        sells = sorted(held - {pick_symbol} if pick_symbol else held)
    return buys, sells


def _user_core_trades(day: str) -> list[dict[str, Any]]:
    from psycopg.rows import dict_row

    from data_sync_service.db import get_connection

    syms = sorted(CANDIDATE_SYMBOLS) + [
        s.replace("ETF:", "") + ".SH" for s in sorted(CANDIDATE_SYMBOLS)
    ]
    try:
        with get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    "SELECT symbol, side, trade_date, price, position_pct "
                    "FROM user_trades WHERE symbol = ANY(%s) "
                    "AND trade_date >= %s ORDER BY trade_date",
                    (syms, day),
                )
                return list(cur.fetchall())
    except Exception as exc:  # noqa: BLE001
        logger.warning("sleeve_paper_recon: user_trades load failed: %s", exc)
        return []


def _user_alignment(
    expected_buys: list[str],
    expected_sells: list[str],
    user_rows: list[dict[str, Any]],
    user_exec_date: str | None,
    day: str,
) -> str:
    """idle / aligned / pending / missing. Never flips ``ok``."""
    if not expected_buys and not expected_sells:
        return "idle"
    buy_days = {day, user_exec_date or ""}

    def _norm(sym: str) -> str:
        return str(sym or "").upper().replace("ETF:", "").replace(".SH", "")

    user_buys = {
        _norm(r.get("symbol"))
        for r in user_rows
        if str(r.get("side") or "") in ("BUY", "ADD") and _day_of(r.get("trade_date")) in buy_days
    }
    user_sells = {
        _norm(r.get("symbol"))
        for r in user_rows
        if str(r.get("side") or "") == "SELL" and _day_of(r.get("trade_date")) == day
    }
    normalized_buys = {_norm(s) for s in expected_buys}
    normalized_sells = {_norm(s) for s in expected_sells}
    if user_exec_date and user_exec_date > day and normalized_buys - user_buys:
        return "pending"  # BUY fills next open; the session has not happened
    if normalized_buys - user_buys or normalized_sells - user_sells:
        return "missing"
    return "aligned"


def sleeve_paper_recon(*, day: str | None = None) -> dict[str, Any]:
    """Core-leg expected-vs-actual for one day (defaults to today).

    Returns the three-question dict: expected action/flows, paper recorded
    flows, user actual flows. ``ok`` False ⇒ paper mismatch (or the decision
    itself could not be reproduced — ``error`` set).
    """
    from data_sync_service.service.trade_calendar_utils import shanghai_today_iso

    day = day or shanghai_today_iso()
    exit_exec_date: str | None = None
    try:
        from data_sync_service.service.paper_entry_fill import _next_session_after

        exit_exec_date = _next_session_after(day)
    except Exception:  # noqa: BLE001
        exit_exec_date = None
    try:
        open_rows, closed_rows = _paper_snapshot()
    except Exception as exc:  # noqa: BLE001
        return {"day": day, "ok": False, "error": f"list paper failed: {exc}"}

    held_closed_days = {exit_exec_date} if exit_exec_date else {day}
    pre_legs = [
        r
        for r in open_rows
        if str(r.get("symbol") or "").upper() in CANDIDATE_SYMBOLS
        and not _opened_by_sleeve_job_today(r, day)
    ] + [
        r
        for r in closed_rows
        if str(r.get("symbol") or "").upper() in CANDIDATE_SYMBOLS
        and str(r.get("closeReason") or "") == CLOSE_REASON_SLEEVE_EXIT
        and _day_of(r.get("closeDate")) in held_closed_days
    ]
    holdings = _pre_decision_holdings(open_rows, closed_rows, day, exit_exec_date)
    try:
        multi = _rebuild_decision(day, holdings)
    except Exception as exc:  # noqa: BLE001
        return {"day": day, "ok": False, "error": f"decision rebuild failed: {exc}"}

    pick = multi.get("pick") or {}
    expected_buys, expected_sells = _expected_flows(multi, pre_legs)

    created_today = sorted(
        {
            str(r.get("symbol") or "").upper()
            for r in open_rows
            if str(r.get("symbol") or "").upper() in CANDIDATE_SYMBOLS
            and _day_of(r.get("createdAt")) == day
        }
    )
    # Harbor exits fill at the next session (signal T close -> T+1 open), so a
    # sell decided today is booked with closeDate = next session (placeholder
    # when the open is not printed yet).
    # A sell decided today is booked with closeDate = exit_exec_date; a leg
    # closed at today's own open (closeDate = day) is NOT part of today's flow.
    sold_days = held_closed_days
    sold_today = sorted(
        {
            str(r.get("symbol") or "").upper()
            for r in closed_rows
            if str(r.get("symbol") or "").upper() in CANDIDATE_SYMBOLS
            and str(r.get("closeReason") or "") == CLOSE_REASON_SLEEVE_EXIT
            and _day_of(r.get("closeDate")) in sold_days
        }
    )
    missed_buys = sorted(set(expected_buys) - set(created_today))
    missed_sells = sorted(set(expected_sells) - set(sold_today))
    extra_opens = sorted(set(created_today) - set(expected_buys))

    user_rows = _user_core_trades(day)
    user_exec_date: str | None = None
    if expected_buys:
        try:
            from data_sync_service.service.paper_entry_fill import _next_session_after

            user_exec_date = _next_session_after(day)
        except Exception:  # noqa: BLE001
            user_exec_date = None

    ok = not missed_buys and not missed_sells and not extra_opens
    return {
        "day": day,
        "ok": ok,
        "decisionAvailable": bool(multi.get("pick")),
        "action": multi.get("action"),
        "label": multi.get("label"),
        "message": multi.get("message"),
        "pickKey": pick.get("key"),
        "pickSymbol": pick.get("symbol"),
        "idlePct": multi.get("idlePct"),
        "expectedBuys": expected_buys,
        "expectedSells": expected_sells,
        "paperBuysToday": created_today,
        "paperSellsToday": sold_today,
        "missedBuys": missed_buys,
        "missedSells": missed_sells,
        "extraOpens": extra_opens,
        "userBuys": sorted(
            str(r.get("symbol") or "")
            for r in user_rows
            if str(r.get("side") or "") in ("BUY", "ADD")
            and _day_of(r.get("trade_date")) in {day, user_exec_date or ""}
        ),
        "userSells": sorted(
            str(r.get("symbol") or "")
            for r in user_rows
            if str(r.get("side") or "") == "SELL" and _day_of(r.get("trade_date")) == day
        ),
        "userAlignment": _user_alignment(
            expected_buys, expected_sells, user_rows, user_exec_date, day
        ),
        "userExecDate": user_exec_date,
        "exitExecDate": exit_exec_date,
    }
