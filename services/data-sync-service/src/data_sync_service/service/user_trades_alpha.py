"""Alpha snapshot for the real trade journal (§19.3 forward data collection).

Every BUY / ADD / SELL leg in ``user_trades`` carries an as-of snapshot of the
alpha-radar state visible on the trade date (no-lookahead: only events whose
document time fell inside [trade_date-14d, trade_date] count). This turns the
journal into the dataset that can later answer:

- entry: does an alpha-endorsed BUY outperform a non-endorsed one?
- exit: does selling while alpha events have gone stale / risk-flipped beat
  holding to the S-3 price line? (the user's "alpha-as-exit" hypothesis)

Display / validation data only — never a gate (OPT-097 iron rule).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from data_sync_service.db.alpha_radar import fetch_trends_as_of

logger = logging.getLogger(__name__)

SNAPSHOT_WINDOW_DAYS = 14
TOP_EVENTS = 3
SA_GRADES = ("S", "A")


def _alpha_sym_key(symbol: str) -> str:
    """Normalize holding symbols to the alpha-radar format (HK:2099 -> HK:02099)."""
    if symbol.startswith("HK:") and len(symbol) == 7:  # HK: + 4 digits -> 5 digits
        return "HK:" + symbol[3:].zfill(5)
    return symbol


def _event_time(t: dict[str, Any]) -> str | None:
    pub = t.get("documentPublishedAt")
    return str(pub) if pub else t.get("createdAt")


def alpha_snapshot_for(symbol: str, trade_date: str) -> dict[str, Any] | None:
    """As-of alpha state for one symbol on ``trade_date``.

    Returns ``None`` when the symbol has no matching events in the window (or
    on error — snapshot capture must never block recording a trade).
    """
    try:
        items = fetch_trends_as_of(day=trade_date, window_days=SNAPSHOT_WINDOW_DAYS)
    except Exception as exc:  # noqa: BLE001
        logger.warning("alpha snapshot fetch failed for %s: %s", symbol, exc)
        return None
    if not items:
        return None
    want = _alpha_sym_key(symbol)
    today = datetime.strptime(trade_date, "%Y-%m-%d").date()
    matched: list[dict[str, Any]] = []
    for t in items:
        event_at = _event_time(t)
        days_ago = None
        if event_at:
            try:
                days_ago = (today - datetime.fromisoformat(str(event_at)[:10]).date()).days
            except (ValueError, TypeError):
                pass
        for entry in (t.get("cnSymbols") or []) + (t.get("hkSymbols") or []):
            s = str(entry.get("symbol") or "") if isinstance(entry, dict) else str(entry)
            if s and _alpha_sym_key(s) == want:
                grade = str(t.get("catalystGrade") or "")
                conf = t.get("mappingConfidence")
                try:
                    conf_f = round(float(conf), 2) if conf is not None else None
                except (TypeError, ValueError):
                    conf_f = None
                matched.append(
                    {
                        "trend": str(t.get("trendName") or ""),
                        "grade": grade,
                        "confidence": conf_f,
                        "daysAgo": days_ago,
                        "riskStatus": str(t.get("riskStatus") or ""),
                        "focus": str(t.get("eventFocus") or "")[:60],
                    }
                )
                break
    if not matched:
        return None
    matched.sort(key=lambda e: -(e["confidence"] or 0.0))
    return {
        "asOf": trade_date,
        "windowDays": SNAPSHOT_WINDOW_DAYS,
        "nEvents": len(matched),
        "hasSA": any(e["grade"] in SA_GRADES for e in matched),
        "maxConfidence": matched[0]["confidence"],
        "riskStatuses": sorted({e["riskStatus"] for e in matched if e["riskStatus"]}),
        "events": matched[:TOP_EVENTS],
    }
