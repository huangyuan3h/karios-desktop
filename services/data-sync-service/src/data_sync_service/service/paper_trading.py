"""Paper-trading service layer (OPT-049).

Three entry points, all run by the scheduler:

- :func:`run_intake` — at 17:40 Asia/Shanghai, weekdays. Reads the decision
  journal for the trading day, finds BUY/ADD actions whose position is still
  0% (i.e. the user did not actually follow the signal), and writes a paper
  trade per signal. **Idempotent on (symbol, entry_date, side)**.

- :func:`run_update` — at 17:45 Asia/Shanghai, weekdays. For every open
  trade, looks up the latest daily close and updates ``close_price``,
  ``pnl_pct``, ``holding_days``. If the close triggers a v0.1 close condition
  (stop loss / target hit / score floor / pool exit / max hold), closes the
  trade with the appropriate reason.

- :func:`compute_stats` — exposed via the API. Returns a small summary for
  the last N days: total closed, win count, win rate, mean pnl_pct.

Why v0.1 is CN-only: HK paper-trading needs FX, T+0/T+2 settlement differences,
and a separate update cadence. Punted to OPT-050+.

This file deliberately does NOT import ``data_sync_service.db.paper_trading``
functions in a way that hides them — every DB call is a thin CRUD function in
``db.paper_trading`` and the service is where the business policy lives.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from data_sync_service.db import execution_journal as ej_db
from data_sync_service.db import paper_trading as pt_db
from data_sync_service.db import watchlist_automation as wa_db
from data_sync_service.db.daily import fetch_last_ohlcv_batch
from data_sync_service.service.trendok import _symbol_to_ts_code  # for CN resolution

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Intake
# ---------------------------------------------------------------------------


def _resolve_cn_ts_code(symbol: str) -> str | None:
    """Return ``ts_code`` for a CN symbol, or None if the symbol isn't CN.

    HK / ETF / unknown symbols are out of v0 scope; the intake log records
    them as a ``skip`` so the operator can see them.
    """
    parsed = _symbol_to_ts_code(symbol)
    if parsed is None:
        return None
    market, ticker, ts_code = parsed
    if market != "CN":
        return None
    return ts_code


def run_intake(*, trade_date: str | None = None) -> dict[str, Any]:
    """One pass of the intake cron.

    Algorithm:
    1. Read all journal changes for ``trade_date`` (defaults to today).
    2. Filter to actions in (BUY, ADD) where the live position is 0%
       (decision journal cards with the matching symbol + the watchlist
       registry's positionPct is null/0).
    3. For each, look up the close price from the daily table.
    4. Insert a paper_trade row. Idempotent — re-runs are no-ops.

    Returns a summary dict for the cron recorder.
    """
    trade_date = trade_date or pt_db.today_iso()
    summary: dict[str, Any] = {
        "tradeDate": trade_date,
        "candidates": 0,
        "inserted": 0,
        "skipped": 0,
        "skippedReasons": {},
    }

    try:
        changes = list(ej_db.list_changes(trade_date=trade_date, limit=500))
    except Exception as exc:  # noqa: BLE001
        summary["error"] = f"ej_db.list_changes failed: {exc}"
        logger.warning("paper_trade intake list_changes failed: %s", exc)
        return summary

    # Build a map of symbol -> positionPct from the watchlist registry to know
    # which signals were *not* followed (we paper-trade only the unfollowed
    # ones, otherwise we'd be double-counting the user's actual position).
    try:
        from data_sync_service.db.watchlist_automation import list_registry  # noqa: PLC0415

        registry = list_registry()
        pos_by_symbol: dict[str, float | None] = {
            str(r.get("symbol") or ""): r.get("positionPct") for r in registry if r
        }
    except Exception as exc:  # noqa: BLE001
        pos_by_symbol = {}
        logger.warning("paper_trade intake list_registry failed: %s", exc)

    # Keep only BUY / ADD with no live position, AND only CN symbols (v0
    # scope). Non-CN symbols are counted as skipped here so the operator can
    # see they were seen but rejected for scope reasons, not silently dropped.
    candidates: list[tuple[dict[str, Any], str]] = []  # (ch, ts_code)
    for ch in changes:
        if not isinstance(ch, dict):
            continue
        action = (ch.get("action") or "").upper()
        if action not in ("BUY", "ADD"):
            continue
        symbol = str(ch.get("symbol") or "").strip()
        if not symbol:
            continue
        ts = _resolve_cn_ts_code(symbol)
        if ts is None:
            # v0 only does CN; HK / ETF / unknown are explicitly skipped.
            summary["skipped"] += 1
            summary["skippedReasons"]["non-cn"] = (
                summary["skippedReasons"].get("non-cn", 0) + 1
            )
            continue
        pos = pos_by_symbol.get(symbol)
        if isinstance(pos, (int, float)) and pos > 0:
            # User already has a position — do not paper-trade.
            continue
        candidates.append((ch, ts))
    summary["candidates"] = len(candidates)

    if not candidates:
        return summary

    # Look up close prices in one batch.
    ts_codes: list[str] = [ts for _, ts in candidates]

    closes_by_ts: dict[str, float] = {}
    if ts_codes:
        try:
            # fetch_last_ohlcv_batch returns {ts_code: [(date, o, h, l, c, v), ...]}
            # The list is in ASC date order per docs in db/daily.py; the last
            # entry is the most recent bar. We trust the daily table to be at
            # least one row deep for symbols already in CN universe.
            bars_by_ts = fetch_last_ohlcv_batch(ts_codes, days=2)
            for ts, bars in bars_by_ts.items():
                if not bars:
                    continue
                last = bars[-1]
                # last = (date, open, high, low, close, volume)
                close = last[4] if len(last) >= 5 else None
                if close is not None:
                    try:
                        closes_by_ts[str(ts)] = float(close)
                    except (TypeError, ValueError):
                        continue
        except Exception as exc:  # noqa: BLE001
            logger.warning("paper_trade intake fetch_last_ohlcv_batch failed: %s", exc)

    for ch, ts in candidates:
        if ts not in closes_by_ts:
            reason = "no-close-price"
            summary["skipped"] += 1
            summary["skippedReasons"][reason] = summary["skippedReasons"].get(reason, 0) + 1
            continue
        sym = str(ch.get("symbol") or "")
        raw_source = ch.get("source")
        # TIP-011: only accept the closed enum; everything else → None.
        source = raw_source if raw_source in pt_db.SOURCES else None
        try:
            row = pt_db.insert_paper_trade(
                symbol=sym,
                entry_date=trade_date,
                side=str(ch.get("action") or "").upper(),
                entry_price=closes_by_ts[ts],
                score_at_entry=ch.get("score"),
                why_at_entry=ch.get("why"),
                sleeve_pct=ch.get("sleevePct"),
                source=source,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("paper_trade insert failed for %s: %s", sym, exc)
            reason = "insert-error"
            summary["skipped"] += 1
            summary["skippedReasons"][reason] = summary["skippedReasons"].get(reason, 0) + 1
            continue
        if row is None:
            # Idempotent skip: a row already exists for (symbol, date, side).
            reason = "duplicate"
            summary["skipped"] += 1
            summary["skippedReasons"][reason] = summary["skippedReasons"].get(reason, 0) + 1
            continue
        summary["inserted"] += 1

    return summary


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------


def _holding_days_for(entry_date_iso: str, today_iso: str) -> int:
    """Trading-day count between entry_date and today. v0 counts calendar days
    (correct enough for a 5-day max hold; refine later with the trade calendar)."""
    try:
        e = date.fromisoformat(entry_date_iso)
        t = date.fromisoformat(today_iso)
    except ValueError:
        return 0
    return max(0, (t - e).days)


def run_update(*, today_iso: str | None = None) -> dict[str, Any]:
    """One pass of the update cron.

    For every open trade:
    1. Look up the latest close.
    2. Update pnl_pct / holding_days.
    3. Apply v0.1 close conditions, in priority order (first match wins):
       - ``stop_hit``: pnl_pct <= STOP_LOSS_PCT
       - ``target_hit``: pnl_pct >= TARGET_PNL_PCT
       - ``score_floor``: latest TrendOK score < SCORE_FLOOR (fail-open: a
         missing score never closes)
       - ``pool_exit``: symbol no longer in the watchlist registry
         (fail-open: registry read failure never closes)
       - ``max_hold``: holding_days >= MAX_HOLD_DAYS

    Returns a summary dict for the cron recorder.
    """
    today_iso = today_iso or pt_db.today_iso()
    summary: dict[str, Any] = {
        "today": today_iso,
        "scanned": 0,
        "updated": 0,
        "closed": 0,
        "closeReasons": {},
    }

    try:
        open_trades = pt_db.get_open_paper_trades()
    except Exception as exc:  # noqa: BLE001
        summary["error"] = f"get_open_paper_trades failed: {exc}"
        return summary
    summary["scanned"] = len(open_trades)

    if not open_trades:
        return summary

    # Batch-fetch latest closes for the open symbols' CN ts_codes.
    ts_codes: list[str] = []
    for t in open_trades:
        ts = _resolve_cn_ts_code(str(t.get("symbol") or ""))
        if ts:
            ts_codes.append(ts)

    closes_by_ts: dict[str, float] = {}
    if ts_codes:
        try:
            bars_by_ts = fetch_last_ohlcv_batch(ts_codes, days=2)
            for ts, bars in bars_by_ts.items():
                if not bars:
                    continue
                last = bars[-1]
                close = last[4] if len(last) >= 5 else None
                if close is not None:
                    try:
                        closes_by_ts[str(ts)] = float(close)
                    except (TypeError, ValueError):
                        continue
        except Exception as exc:  # noqa: BLE001
            logger.warning("paper_trade update fetch_last_ohlcv_batch failed: %s", exc)

    # Watchlist registry snapshot for the `pool_exit` condition. None means
    # the read failed → fail open (never close on pool_exit without data).
    registry_symbols: set[str] | None = None
    try:
        registry_symbols = {
            str(r.get("symbol") or "") for r in wa_db.list_registry() if r
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("paper_trade update list_registry failed: %s", exc)

    for t in open_trades:
        sym = str(t.get("symbol") or "")
        ts = _resolve_cn_ts_code(sym)
        if ts is None or ts not in closes_by_ts:
            # No fresh price; skip without erroring (the next day will retry).
            continue
        close_price = closes_by_ts[ts]
        entry_price = float(t.get("entry_price") or 0.0)
        if entry_price <= 0:
            continue
        pnl_pct = (close_price - entry_price) / entry_price * 100.0
        holding_days = _holding_days_for(str(t.get("entry_date") or ""), today_iso)

        reason = _pick_close_reason(
            t=t,
            pnl_pct=pnl_pct,
            holding_days=holding_days,
            registry_symbols=registry_symbols,
        )
        if reason is not None:
            try:
                pt_db.close_paper_trade(
                    trade_id=str(t.get("id") or ""),
                    close_date=today_iso,
                    close_price=close_price,
                    pnl_pct=pnl_pct,
                    holding_days=holding_days,
                    close_reason=reason,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("close_paper_trade (%s) failed for %s: %s", reason, sym, exc)
                continue
            summary["closed"] += 1
            summary["closeReasons"][reason] = summary["closeReasons"].get(reason, 0) + 1
            continue

        # Otherwise just touch the live state.
        try:
            pt_db.update_paper_trade_price(
                trade_id=str(t.get("id") or ""),
                close_price=close_price,
                pnl_pct=pnl_pct,
                holding_days=holding_days,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("update_paper_trade_price failed for %s: %s", sym, exc)
            continue
        summary["updated"] += 1

    return summary


def _pick_close_reason(
    *,
    t: dict[str, Any],
    pnl_pct: float,
    holding_days: int,
    registry_symbols: set[str] | None,
) -> str | None:
    """Choose the close reason for an open trade, or None to keep it open.

    Priority: stop_hit > target_hit > score_floor > pool_exit > max_hold.
    ``score_floor`` and ``pool_exit`` are fail-open — when their input data is
    unavailable they are skipped instead of force-closing.
    """
    if pnl_pct <= pt_db.STOP_LOSS_PCT:
        return pt_db.CLOSE_REASON_STOP_HIT
    if pnl_pct >= pt_db.TARGET_PNL_PCT:
        return pt_db.CLOSE_REASON_TARGET_HIT

    score: float | None = None
    try:
        score = wa_db.fetch_latest_score_since(
            str(t.get("symbol") or ""),
            str(t.get("entry_date") or ""),
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("paper_trade fetch_latest_score_since failed: %s", exc)
    if score is not None and score < pt_db.SCORE_FLOOR:
        return pt_db.CLOSE_REASON_SCORE_FLOOR

    if registry_symbols is not None and str(t.get("symbol") or "") not in registry_symbols:
        return pt_db.CLOSE_REASON_POOL_EXIT

    if holding_days >= pt_db.MAX_HOLD_DAYS:
        return pt_db.CLOSE_REASON_MAX_HOLD

    return None


# ---------------------------------------------------------------------------
# Stats (exposed via the API)
# ---------------------------------------------------------------------------


def compute_stats(*, since_iso: str) -> dict[str, Any]:
    """Return a small summary for trades closed since ``since_iso``.

    The shape is intentionally small and stable; richer analytics (drawdown,
    per-industry break-down, etc.) live in OPT-050.
    """
    try:
        total, wins = pt_db.count_since(since_iso)
        avg = pt_db.avg_pnl_pct_since(since_iso)
    except Exception as exc:  # noqa: BLE001
        return {
            "since": since_iso,
            "error": f"stats query failed: {exc}",
        }
    win_rate = (wins / total) if total > 0 else None
    return {
        "since": since_iso,
        "closedCount": total,
        "winningCount": wins,
        "winRate": win_rate,
        "avgPnlPct": avg,
    }
