"""Read side of the agent decision log (TIP-015 ``decision_actions``).

``POST /v1/agent/decision-log`` writes one explicit row (乐乐 / 14:30
confirmation). This module reads the same store back so the next day's
judgment brief can say what was actually decided, not just what the engine
suggests. Read-only and fail-open: a missing DB degrades to
``{"available": false, "error": ...}`` and never breaks the brief.

Nothing here writes rows on its own — auto-writing on every brief build would
spam the decision log.
"""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_LOOKBACK_SESSIONS = 3
MAX_LOOKBACK_SESSIONS = 20
LOG_NOTE = (
    "Rows are written only by POST /v1/agent/decision-log or "
    "POST /v1/agent/daily-confirm (explicit agent / 14:30 confirmation); "
    "a brief build never writes here."
)


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


def _compact_advice(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict) or not value:
        return None
    if not value.get("available", True):
        return {
            "available": False,
            "error": value.get("error"),
        }
    adjustments = []
    for row in value.get("adjustments") or []:
        if not isinstance(row, dict):
            continue
        adjustments.append(
            {
                k: row.get(k)
                for k in ("symbol", "side", "to", "conditional", "pendingOpenSell")
                if row.get(k) is not None
            }
        )
    return {
        "available": True,
        "tradeDate": value.get("tradeDate"),
        "mode": value.get("mode"),
        "strategyLabel": value.get("strategyLabel"),
        "noChange": value.get("noChange"),
        "mustAct": value.get("mustAct"),
        "mustWatch": value.get("mustWatch"),
        "lines": [str(ln) for ln in (value.get("lines") or [])][:12],
        "summary": value.get("summary"),
        "adjustments": adjustments,
    }


def _compact_reconcile(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict) or not value:
        return None
    deviations = []
    for row in value.get("deviations") or []:
        if isinstance(row, dict):
            deviations.append(
                {
                    k: row.get(k)
                    for k in ("type", "symbol", "side", "copy")
                    if row.get(k) is not None
                }
            )
        elif row:
            deviations.append({"copy": str(row)})
    return {
        "status": value.get("status"),
        "asOf": value.get("asOf"),
        "deviationCount": len(deviations),
        "deviations": deviations[:8],
        "fillCount": len(value.get("fills") or []),
        "missing": [str(m) for m in (value.get("missing") or [])][:6],
        "checkedAt": value.get("checkedAt"),
    }


def compact_decision_row(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize one DB row to the fields needed to re-read the decision."""
    outcome = _outcome_dict(row.get("outcome"))
    decision = outcome.get("decision")
    out = {
        "id": row.get("id"),
        "tradeDate": str(row.get("snapshotDate") or "")[:10] or None,
        "symbol": row.get("symbol"),
        "action": row.get("action"),
        "judgment": row.get("rationale"),
        "status": row.get("status"),
        "source": row.get("source"),
        "confidence": row.get("confidence"),
        "adoptedOverlay": outcome.get("adoptedOverlay"),
        "overlayId": outcome.get("overlayId"),
        "contractSnapshotId": outcome.get("contractSnapshotId"),
        "loggedAt": row.get("createdAt"),
        "kind": outcome.get("kind"),
        "mode": outcome.get("mode"),
        "strategyLabel": outcome.get("strategyLabel"),
        "advice": _compact_advice(outcome.get("advice")),
        "decision": decision if isinstance(decision, dict) else None,
        "reconcile": _compact_reconcile(outcome.get("reconcile")),
    }
    return {k: v for k, v in out.items() if v is not None}


def review_window(
    trade_date: str, lookback: int = DEFAULT_LOOKBACK_SESSIONS
) -> tuple[str, list[str]]:
    """``(from_date, sessions)`` for the last ``lookback`` open sessions ≤ day."""
    day = str(trade_date or "")[:10]
    try:
        end = date.fromisoformat(day)
    except ValueError:
        return day, [day]
    n = max(1, min(int(lookback or DEFAULT_LOOKBACK_SESSIONS), MAX_LOOKBACK_SESSIONS))
    probe = end - timedelta(days=max(n * 3, 10))
    sessions: list[str] = []
    try:
        from data_sync_service.service.trade_calendar_utils import open_sessions_between

        sessions = [s for s in open_sessions_between(probe.isoformat(), day) if s <= day]
    except Exception as exc:  # noqa: BLE001 — calendar is a nicety, not a gate
        logger.warning("decision log session calendar failed: %s", exc)
    if not sessions:
        return day, [day]
    selected = sessions[-n:]
    return selected[0], selected


def read_decision_log(
    *,
    trade_date: str,
    lookback: int = DEFAULT_LOOKBACK_SESSIONS,
    symbol: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    """Today's + the last few sessions' decision rows (fail-open)."""
    day = str(trade_date or "")[:10]
    start, sessions = review_window(day, lookback=lookback)
    base: dict[str, Any] = {
        "tradeDate": day,
        "fromDate": start,
        "sessions": sessions,
        "lookbackSessions": len(sessions),
    }
    try:
        from data_sync_service.db.decision import list_actions_between

        rows = list_actions_between(
            start_date=start,
            end_date=day,
            symbol=symbol,
            limit=limit,
        )
    except Exception as exc:  # noqa: BLE001 — DB down must not break the brief
        logger.warning("decision log read failed: %s", exc)
        return {
            **base,
            "available": False,
            "error": str(exc) or exc.__class__.__name__,
            "count": 0,
            "items": [],
        }
    items = [compact_decision_row(r) for r in rows]
    return {**base, "available": True, "count": len(items), "items": items, "note": LOG_NOTE}
