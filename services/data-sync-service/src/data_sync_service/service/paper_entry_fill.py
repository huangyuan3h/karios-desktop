"""Paper entry fill = backtest ``entry_mode=next_open`` (audit P1b / 2026-08-29).

Signal is decided on day D (EOD intake). Fill price is the **next session open**.
When that bar is not in ``daily`` yet (same-evening intake), we still open the
paper row on the next calendar session date using day-D close as a placeholder
and set ``signal_snapshot.pendingOpenFill=true`` so ``run_update`` can patch
the real open once it lands.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from data_sync_service.db.daily import fetch_last_ohlcv_batch, fetch_ohlcv_batch_between
from data_sync_service.service.trade_calendar_utils import open_sessions_between

logger = logging.getLogger(__name__)


def _f(v: Any) -> float | None:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if x > 0 else None


def _next_session_after(signal_day: str, *, calendar_ts: str = "") -> str | None:
    """Return the next trading date strictly after ``signal_day``.

    Uses the real trading calendar (``trade_calendar_utils``, Mon–Fri
    fallback when unseeded). Previously this proxied ``000001.SH`` bars in
    ``daily`` — but index bars live in ``index_daily``, so the proxy was
    empty and CN intake silently opened nothing (OPT-138).
    ``calendar_ts`` is kept as an ignored kwarg for backward compatibility.
    """
    try:
        start = date.fromisoformat(signal_day[:10])
    except ValueError:
        return None
    end = (start + timedelta(days=20)).isoformat()
    sessions = open_sessions_between(
        (start + timedelta(days=1)).isoformat(), end
    )
    return sessions[0] if sessions else None


def resolve_next_open_fill(
    ts_code: str,
    signal_day: str,
    *,
    signal_close: float | None = None,
) -> dict[str, Any] | None:
    """Resolve paper fill for a signal decided on ``signal_day``.

    Returns ``None`` when neither next open nor a usable placeholder exists.
    """
    ts = str(ts_code or "").strip().upper()
    day = str(signal_day or "")[:10]
    if not ts or not day:
        return None

    entry_date = _next_session_after(day)
    if not entry_date:
        return None

    # Prefer real next-session open when the bar already exists.
    bars = fetch_ohlcv_batch_between([ts], day, entry_date).get(ts) or []
    open_px: float | None = None
    for b in bars:
        if str(b[0])[:10] == entry_date:
            open_px = _f(b[1])
            break

    snap_base = {
        "entryMode": "next_open",
        "signalDate": day,
    }

    if open_px is not None:
        return {
            "entry_date": entry_date,
            "entry_price": open_px,
            "pending_open_fill": False,
            "signal_snapshot": {**snap_base, "pendingOpenFill": False},
        }

    # Same-evening / pre-open: placeholder = signal close (or caller-supplied).
    close_px = signal_close
    if close_px is None or close_px <= 0:
        sig_bars = fetch_ohlcv_batch_between([ts], day, day).get(ts) or []
        if sig_bars:
            close_px = _f(sig_bars[-1][4])
        if close_px is None:
            recent = fetch_last_ohlcv_batch([ts], days=5).get(ts) or []
            for b in reversed(recent):
                if str(b[0])[:10] <= day:
                    close_px = _f(b[4])
                    break
    if close_px is None or close_px <= 0:
        return None

    return {
        "entry_date": entry_date,
        "entry_price": float(close_px),
        "pending_open_fill": True,
        "signal_snapshot": {
            **snap_base,
            "pendingOpenFill": True,
            "placeholderClose": float(close_px),
        },
    }


def merge_entry_snapshot(
    existing: dict[str, Any] | None,
    fill: dict[str, Any],
) -> dict[str, Any]:
    out = dict(existing or {})
    out.update(fill.get("signal_snapshot") or {})
    return out


def try_resolve_pending_open(
    *,
    ts_code: str,
    entry_date: str,
    signal_snapshot: dict[str, Any] | None,
) -> float | None:
    """If snapshot is pending next_open fill and open is available, return it."""
    snap = signal_snapshot or {}
    if not snap.get("pendingOpenFill"):
        return None
    if str(snap.get("entryMode") or "") != "next_open":
        return None
    bars = fetch_ohlcv_batch_between([ts_code], entry_date[:10], entry_date[:10]).get(
        str(ts_code).upper()
    ) or []
    if not bars:
        # last-N may include entry_date once close_sync wrote the bar
        recent = fetch_last_ohlcv_batch([ts_code], days=5).get(str(ts_code).upper()) or []
        bars = [b for b in recent if str(b[0])[:10] == entry_date[:10]]
    if not bars:
        return None
    return _f(bars[0][1])
