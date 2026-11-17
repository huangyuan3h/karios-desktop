"""Persist the 14:30 advice, Yuan's confirmation, and next-day fill deviations.

Yuan's maturity plan item ⑤: after the weekday 14:30 (Asia/Shanghai)
confirmation in chat, the system must keep three things in one place:

1. **建议** — that day's ``compose_daily_adjust`` table for the selected mode
   (loaded server-side; 乐乐 never pastes it).
2. **他的决定** — what Yuan confirmed / amended / deferred / rejected.
3. **次日成交与偏差** — the next session's actual fills / position changes vs
   that decision, with an explicit deviation list.

This reuses the TIP-015 ``decision_actions`` ledger (no fifth ledger): one row
per ``(tradeDate, mode, kind='daily_confirm')`` with ``source='daily_confirm'``.
A second confirmation the same day updates 他的决定; it never inserts a
duplicate. The reconcile pass only updates ``outcome.reconcile`` on that same
row — it never places an order, changes settings, or edits the watchlist.

Record-keeping / advice only. ``sleeve.action``, ``TRAIL_PCT`` (8%), the
parking cooldown (k=0) and every target weight stay untouched.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_SOURCE = "daily_confirm"
KIND = "daily_confirm"
DECISION_STATUSES = ("confirmed", "amended", "deferred", "rejected")
MAX_RECONCILE_ROWS = 40
SESSION_CLOSED_AFTER = time(15, 30)  # Asia/Shanghai, after the 15:00 close

_SYMBOL_RE = re.compile(r"\b(?:CN|HK|ETF):[A-Za-z0-9.]+\b", re.IGNORECASE)
_BUY_HINTS = ("买入", "加仓", "买")
_SELL_HINTS = ("卖出", "清仓", "减仓", "卖")


def _now() -> datetime:
    return datetime.now(tz=SHANGHAI_TZ)


def _now_iso(now: datetime | None = None) -> str:
    return (now or _now()).isoformat()


def _day(value: str | None) -> str:
    from data_sync_service.service.trade_calendar_utils import shanghai_today_iso

    return str(value or shanghai_today_iso())[:10]


def _outcome_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _resolve_mode(mode: str | None) -> str:
    if mode:
        return str(mode)
    try:
        from data_sync_service.api.settings_routes import (
            DEFAULT_STRATEGY_MODE,
            selected_strategy_mode,
        )

        return str(selected_strategy_mode() or DEFAULT_STRATEGY_MODE)
    except Exception as exc:  # noqa: BLE001 — settings is a default, not a gate
        logger.warning("daily confirm mode lookup failed: %s", exc)
        return "homeport"


def _normalize_lines(lines: list[Any] | None) -> list[Any]:
    out: list[Any] = []
    for line in lines or []:
        if isinstance(line, dict):
            row = {str(k): v for k, v in line.items() if v not in (None, "")}
            if row:
                out.append(row)
        elif line is not None and str(line).strip():
            out.append(str(line).strip())
    return out


def _compact_advice(advice: dict[str, Any]) -> dict[str, Any]:
    """Advice payload small enough for the decision log / compact brief."""
    adjustments = []
    for row in advice.get("adjustments") or []:
        if not isinstance(row, dict):
            continue
        adjustments.append(
            {
                k: row.get(k)
                for k in ("symbol", "side", "to", "reason", "conditional", "pendingOpenSell")
                if row.get(k) is not None
            }
        )
    warnings = [str(w) for w in (advice.get("warnings") or [])]
    return {
        "available": True,
        "tradeDate": advice.get("tradeDate"),
        "mode": advice.get("mode"),
        "strategyLabel": advice.get("strategyLabel"),
        "noChange": bool(advice.get("noChange")),
        "mustAct": bool(advice.get("mustAct")),
        "mustWatch": bool(advice.get("mustWatch")),
        "lines": [str(ln) for ln in (advice.get("lines") or [])][:20],
        "summary": advice.get("summary"),
        "adjustments": adjustments,
        "riskGates": {
            "warningCount": len(warnings),
            "warnings": warnings[:6],
        },
    }


def _load_advice(mode: str, day: str) -> tuple[dict[str, Any], str | None]:
    """Server-side ``compose_daily_adjust`` snapshot; fail-open on any error."""
    try:
        from data_sync_service.service.daily_adjust import compose_daily_adjust

        return _compact_advice(compose_daily_adjust(mode, trade_date=day)), None
    except Exception as exc:  # noqa: BLE001 — never 500 away his confirmation
        logger.warning("daily confirm advice unavailable: %s", exc)
        return {
            "available": False,
            "error": str(exc) or exc.__class__.__name__,
        }, str(exc) or exc.__class__.__name__


def _decision_lines_for(
    status: str, provided: list[Any] | None, advice: dict[str, Any]
) -> list[Any]:
    lines = _normalize_lines(provided)
    if lines:
        return lines
    if status == "confirmed" and advice.get("available"):
        return [str(ln) for ln in (advice.get("lines") or [])]
    return []


def record_daily_confirm(
    *,
    trade_date: str | None = None,
    mode: str | None = None,
    decision: str,
    note: str | None = None,
    decision_lines: list[Any] | None = None,
    contract_snapshot_id: str | None = None,
    source: str = DEFAULT_SOURCE,
) -> dict[str, Any]:
    """Persist (or update) one 14:30 confirmation row.

    ``advice`` is loaded here from ``compose_daily_adjust`` for the resolved
    mode — the caller only sends what Yuan decided. Re-confirming the same
    ``(tradeDate, mode)`` updates the existing row's decision / advice and does
    not insert a duplicate.
    """
    day = _day(trade_date)
    mode_value = _resolve_mode(mode)
    status = str(decision or "").strip().lower()
    if status not in DECISION_STATUSES:
        raise ValueError(f"decision must be one of {list(DECISION_STATUSES)}")

    advice, advice_error = _load_advice(mode_value, day)
    lines = _decision_lines_for(status, decision_lines, advice)
    now = _now()

    from data_sync_service.db.decision import get_daily_confirm_row, upsert_daily_confirm

    existing = get_daily_confirm_row(trade_date=day, source=source, mode=mode_value)
    previous = _outcome_dict((existing or {}).get("outcome"))
    prev_decision = previous.get("decision") or {}
    reconcile = previous.get("reconcile") or {"status": "pending"}
    decision_changed = (
        not previous
        or prev_decision.get("status") != status
        or prev_decision.get("decisionLines") != lines
        or (prev_decision.get("note") or None) != (note or None)
    )
    if decision_changed:
        reconcile = {"status": "pending"}

    decision_block: dict[str, Any] = {
        "status": status,
        "note": note,
        "decisionLines": lines,
        "decidedAt": _now_iso(now),
    }
    outcome: dict[str, Any] = {
        "kind": KIND,
        "mode": mode_value,
        "strategyLabel": advice.get("strategyLabel") or mode_value,
        "contractSnapshotId": contract_snapshot_id or previous.get("contractSnapshotId"),
        "advice": advice,
        "decision": decision_block,
        "reconcile": reconcile,
    }
    if advice_error:
        outcome["adviceError"] = advice_error

    summary = advice.get("summary") or "；".join(str(ln) for ln in lines[:4])
    rationale = note or (f"{status}：{summary}" if summary else status)
    row = upsert_daily_confirm(
        symbol="DAILY_CONFIRM",
        action=KIND,
        rationale=str(rationale)[:2000],
        snapshot_date=date.fromisoformat(day),
        source=source,
        outcome=outcome,
    )
    return {"ok": True, "item": row, "advice": advice, "mode": mode_value}


# ---------------------------------------------------------------------------
# Reconcile target: his amended lines win over the original advice
# ---------------------------------------------------------------------------


def _side_from_text(text: str) -> str | None:
    if any(hint in text for hint in _SELL_HINTS):
        return "SELL"
    if any(hint in text for hint in _BUY_HINTS):
        return "BUY"
    return None


def _parse_decision_lines(lines: list[Any]) -> tuple[list[dict[str, Any]], list[str]]:
    """Parse amended decision lines into ``{symbol, side, ...}`` targets."""
    parsed: list[dict[str, Any]] = []
    unparsed: list[str] = []
    for line in lines:
        if isinstance(line, dict):
            symbol = str(line.get("symbol") or line.get("ts") or "").upper()
            side = str(line.get("side") or line.get("action") or "").upper()
            if side in ("BUY", "ADD"):
                side = "BUY"
            if symbol and side in ("BUY", "SELL"):
                parsed.append({**line, "symbol": symbol, "side": side})
            else:
                unparsed.append(str(line))
            continue
        text = str(line)
        match = _SYMBOL_RE.search(text)
        side = _side_from_text(text)
        if match and side:
            parsed.append({"symbol": match.group(0).upper(), "side": side, "reason": text})
        else:
            unparsed.append(text)
    return parsed, unparsed


def resolve_target_adjustments(
    outcome: dict[str, Any],
) -> tuple[list[dict[str, Any]] | None, list[str]]:
    """What the book should have done, from his decision if he amended.

    Returns ``(targets, missing)``; ``targets is None`` means the target could
    not be established (reconcile must degrade rather than invent agreement).
    Conditional must-watch rows are never treated as mandatory fills.
    """
    decision = outcome.get("decision") or {}
    advice = outcome.get("advice") or {}
    status = str(decision.get("status") or "confirmed").lower()

    if status == "amended":
        lines = _normalize_lines(decision.get("decisionLines"))
        if not lines:
            return None, ["amended 决定缺少 decisionLines"]
        parsed, unparsed = _parse_decision_lines(lines)
        missing = [f"无法解析行：{ln}" for ln in unparsed]
        if not parsed:
            return None, missing or ["amended 决定未包含可识别的标的/方向"]
        return parsed, missing

    if status in ("deferred", "rejected"):
        return [], []

    if advice.get("available") is False:
        return None, ["当日建议不可用，无法确定目标"]

    targets = [
        row
        for row in (advice.get("adjustments") or [])
        if isinstance(row, dict) and not row.get("conditional")
    ]
    return targets, []


# ---------------------------------------------------------------------------
# Reconcile: pure comparison (unit-testable, no DB writes)
# ---------------------------------------------------------------------------


def _fill_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        k: row.get(k)
        for k in ("symbol", "side", "tradeDate", "price", "positionPct")
        if row.get(k) is not None
    }


def _held_positions(registry: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    held: dict[str, dict[str, Any]] = {}
    for row in registry:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").upper()
        try:
            pct = float(row.get("positionPct") or 0)
        except (TypeError, ValueError):
            pct = 0.0
        if symbol and pct > 0:
            held[symbol] = row
    return held


def reconcile_snapshot(
    outcome: dict[str, Any],
    *,
    next_session: str,
    registry: list[dict[str, Any]] | None,
    trades: list[dict[str, Any]] | None,
    errors: list[str] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Compare the stored decision with next-session evidence.

    ``registry`` is the current watchlist book (positions / cost / entryDate)
    and ``trades`` is ``user_trades`` for ``next_session``. Either being
    ``None`` (unreadable) degrades the result; it never fabricates agreement.
    """
    targets, target_missing = resolve_target_adjustments(outcome)
    missing = [*(errors or []), *target_missing]
    status = "done"
    if targets is None:
        status = "degraded"
        targets = []
    if registry is None:
        status = "degraded"
        missing.append("注册表不可用，无法判断持仓是否变化")
    if trades is None:
        status = "degraded"
        missing.append("成交流水不可用，无法核对次日成交")

    fills = [_fill_row(t) for t in (trades or []) if isinstance(t, dict)]
    held = _held_positions(registry or [])
    target_side = {
        str(t.get("symbol") or "").upper(): str(t.get("side") or "").upper() for t in targets
    }
    deviations: list[dict[str, Any]] = []

    def _dev(kind: str, symbol: str, side: str, copy: str) -> None:
        deviations.append({"type": kind, "symbol": symbol, "side": side, "copy": copy})

    for target in targets:
        symbol = str(target.get("symbol") or "").upper()
        side = str(target.get("side") or "").upper()
        if side == "BUY":
            side = "BUY"
        wanted_sides = ("BUY", "ADD") if side == "BUY" else (side,)
        matches = [
            f
            for f in fills
            if str(f.get("symbol") or "").upper() == symbol
            and str(f.get("side") or "").upper() in wanted_sides
        ]
        position = held.get(symbol)
        if side == "SELL":
            if matches:
                continue
            if position:
                pct = position.get("positionPct")
                _dev(
                    "not_filled",
                    symbol,
                    side,
                    f"建议卖出但未成交：注册表仍持有 {symbol}（{pct}%）",
                )
            else:
                missing.append(f"{symbol} 无 SELL 成交记录；按注册表已无持仓判定一致")
            continue

        if matches:
            continue
        entry = str((position or {}).get("entryDate") or "")[:10]
        if position and entry == str(next_session)[:10]:
            continue
        _dev("not_filled", symbol, side, f"建议买入但次日未见成交/建仓：{symbol}")

    if targets and trades is not None:
        for fill in fills:
            symbol = str(fill.get("symbol") or "").upper()
            side = str(fill.get("side") or "").upper()
            expected = target_side.get(symbol)
            if symbol and expected is None:
                _dev(
                    "unexpected_fill",
                    symbol,
                    side,
                    f"成交标的与决定不一致：{symbol} {side} 不在当日建议/决定内",
                )
            elif expected:
                ok_sides = ("BUY", "ADD") if expected == "BUY" else ("SELL",)
                if side not in ok_sides:
                    _dev(
                        "wrong_direction",
                        symbol,
                        side,
                        f"成交方向与决定不一致：{symbol} 决定 {expected}，实际 {side}",
                    )

    if not targets and fills:
        sides = "、".join(sorted({str(f.get("side") or "") for f in fills if f.get("side")}))
        symbols = "、".join(sorted({str(f.get("symbol") or "") for f in fills if f.get("symbol")}))
        _dev(
            "unexpected_fill",
            symbols or "—",
            sides or "—",
            f"决定为 noChange/延期，但次日出现成交：{symbols or '—'} {sides or ''}".strip(),
        )

    return {
        "status": status,
        "asOf": next_session,
        "fills": fills,
        "deviations": deviations,
        "missing": missing,
        "targetCount": len(targets),
        "checkedAt": _now_iso(now),
    }


# ---------------------------------------------------------------------------
# Reconcile orchestration (DB read/write, read-only against the live book)
# ---------------------------------------------------------------------------


def next_open_session(day_iso: str) -> str | None:
    """First SSE open session strictly after ``day_iso`` (None if unknown)."""
    from data_sync_service.service.trade_calendar_utils import open_sessions_between

    try:
        start = date.fromisoformat(str(day_iso)[:10])
    except (TypeError, ValueError):
        return None
    sessions = open_sessions_between(start.isoformat(), (start + timedelta(days=45)).isoformat())
    future = [s for s in sessions if s > str(day_iso)[:10]]
    return future[0] if future else None


def session_ended(session_iso: str, now: datetime | None = None) -> bool:
    """True once the A-share session has a judgeable close (15:30 CST)."""
    current = now or _now()
    day = str(session_iso)[:10]
    if day < current.date().isoformat():
        return True
    return day == current.date().isoformat() and current.time() >= SESSION_CLOSED_AFTER


def _load_evidence(
    next_session: str,
) -> tuple[
    list[dict[str, Any]] | None,
    list[dict[str, Any]] | None,
    list[str],
]:
    """Read the live book + next-session manual trades (never raises)."""
    registry: list[dict[str, Any]] | None
    trades: list[dict[str, Any]] | None
    errors: list[str] = []
    try:
        from data_sync_service.db.watchlist_automation import list_registry

        registry = [r for r in list_registry() if isinstance(r, dict)]
    except Exception as exc:  # noqa: BLE001
        registry = None
        errors.append(f"注册表读取失败：{exc}")
    try:
        from data_sync_service.db.user_trades import list_trades

        rows = list_trades(limit=500)
        trades = [
            r
            for r in rows
            if isinstance(r, dict) and str(r.get("tradeDate") or "")[:10] == next_session
        ]
    except Exception as exc:  # noqa: BLE001
        trades = None
        errors.append(f"成交流水读取失败：{exc}")
    return registry, trades, errors


def reconcile_pending(
    *,
    trade_date: str | None = None,
    mode: str | None = None,
    source: str = DEFAULT_SOURCE,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Fill ``outcome.reconcile`` for pending confirm rows whose next session is in.

    Idempotent: re-running updates the same reconcile block on the same row.
    A missing input leaves ``pending`` / ``degraded`` and never deletes the
    confirmation row.
    """
    day = _day(trade_date)
    current = now or _now()
    from data_sync_service.db.decision import list_daily_confirms, update_action_outcome

    rows = list_daily_confirms(
        before_date=day,
        source=source,
        mode=mode,
        limit=MAX_RECONCILE_ROWS,
    )
    evidence_cache: dict[
        str, tuple[list[dict[str, Any]] | None, list[dict[str, Any]] | None, list[str]]
    ] = {}
    items: list[dict[str, Any]] = []
    counts: dict[str, int] = {"done": 0, "pending": 0, "degraded": 0}

    for row in rows:
        outcome = _outcome_dict(row.get("outcome"))
        snapshot_day = str(row.get("snapshotDate") or "")[:10]
        next_session = next_open_session(snapshot_day)
        if not next_session:
            reconcile = {
                "status": "pending",
                "asOf": None,
                "deviations": [],
                "missing": ["无法确定下一交易日（交易日历不可用）"],
                "checkedAt": _now_iso(current),
            }
        elif not session_ended(next_session, current):
            reconcile = {
                "status": "pending",
                "asOf": next_session,
                "deviations": [],
                "missing": [f"下一交易日 {next_session} 尚未收盘，等待成交证据"],
                "checkedAt": _now_iso(current),
            }
        else:
            if next_session not in evidence_cache:
                evidence_cache[next_session] = _load_evidence(next_session)
            registry, trades, errors = evidence_cache[next_session]
            reconcile = reconcile_snapshot(
                outcome,
                next_session=next_session,
                registry=registry,
                trades=trades,
                errors=errors,
                now=current,
            )
        updated_outcome = {**outcome, "reconcile": reconcile}
        ok = update_action_outcome(int(row["id"]), updated_outcome)
        counts[str(reconcile.get("status") or "pending")] = (
            counts.get(str(reconcile.get("status") or "pending"), 0) + 1
        )
        items.append(
            {
                "id": row.get("id"),
                "tradeDate": snapshot_day,
                "mode": outcome.get("mode"),
                "updated": ok,
                "reconcile": reconcile,
            }
        )

    return {
        "ok": True,
        "tradeDate": day,
        "checked": len(rows),
        "updated": sum(1 for item in items if item.get("updated")),
        "counts": counts,
        "items": items,
    }
