"""Paper-trading service layer (OPT-049, v0.2 / OPT-062).

Three entry points, all run by the scheduler:

- :func:`run_intake` — at 17:40 Asia/Shanghai, weekdays. Reads the decision
  journal for the trading day, finds BUY/ADD actions whose position is still
  0% (i.e. the user did not actually follow the signal), and writes a paper
  trade per signal. **Idempotent on (symbol, entry_date, side)**.

- :func:`run_update` — at 17:45 Asia/Shanghai, weekdays. For every open
  trade, looks up the latest daily close and updates ``close_price``,
  ``pnl_pct``, ``holding_days``. If the close triggers a close condition
  (stop loss / target hit / score floor / pool exit / max hold), closes the
  trade with the appropriate reason.

- :func:`compute_stats` — exposed via the API. Returns a small summary for
  the last N days: total closed, win count, win rate, mean pnl_pct (NET).

v0.2 (OPT-062 / L3-P1) scope:

- **CN + HK**. HK bars share the ``daily`` table (ts_code like ``00700.HK``)
  and are priced in HKD (no FX conversion — L3-P3 refinement). ETF stays out
  of scope (score/close semantics undefined); intake records it as a skip.
- **Cost model**: closed trades carry NET pnl — ``pnl_pct = gross - costs``,
  with the split in ``gross_pnl_pct`` / ``costs_pct`` (see
  ``paper_cost_model``). Stop/target conditions trigger on NET pnl
  (conservative, close to what a real account would see). Open rows keep
  showing the current GROSS pnl until the trade closes.
- **HK close conditions**: score_floor fails open for HK (TrendOK is CN
  daily only); pool_exit and max_hold apply as for CN.

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
from data_sync_service.service.execution_gate import REGIME_STRONG
from data_sync_service.service.paper_cost_model import (
    MARKETS,
    round_trip_cost_pct,
)
from data_sync_service.service.trendok import _symbol_to_ts_code  # CN/HK resolution

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Intake
# ---------------------------------------------------------------------------


def _resolve_ts_code(symbol: str) -> tuple[str, str] | None:
    """Return ``(market, ts_code)`` for a CN or HK symbol.

    ETF / unknown symbols are out of v0.2 scope; the intake log records them
    as a ``skip`` so the operator can see them.
    """
    parsed = _symbol_to_ts_code(symbol)
    if parsed is None:
        return None
    market, _ticker, ts_code = parsed
    if market not in MARKETS:
        return None
    return market, ts_code


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

    # Keep only BUY / ADD with no live position, AND only CN/HK symbols
    # (v0.2 scope). ETF / unknown symbols are counted as skipped here so the
    # operator can see they were seen but rejected for scope reasons, not
    # silently dropped.
    candidates: list[tuple[dict[str, Any], str, str]] = []  # (ch, market, ts_code)
    for ch in changes:
        if not isinstance(ch, dict):
            continue
        # execution_decision_changes rows come back as
        # {field: 'action', newValue: 'BUY'|'ADD', symbol, source, ...}
        # (db/execution_journal._change_row). Anything else is not an action.
        if str(ch.get("field") or "") != "action":
            continue
        action = str(ch.get("newValue") or "").upper()
        if action not in ("BUY", "ADD"):
            continue
        symbol = str(ch.get("symbol") or "").strip()
        if not symbol:
            continue
        resolved = _resolve_ts_code(symbol)
        if resolved is None:
            # v0.2 does CN + HK; ETF / unknown are explicitly skipped.
            summary["skipped"] += 1
            summary["skippedReasons"]["out-of-scope"] = (
                summary["skippedReasons"].get("out-of-scope", 0) + 1
            )
            continue
        market, ts = resolved
        pos = pos_by_symbol.get(symbol)
        if isinstance(pos, (int, float)) and pos > 0:
            # User already has a position — do not paper-trade.
            continue
        candidates.append((ch, market, ts))
    summary["candidates"] = len(candidates)

    if not candidates:
        return summary

    # Look up close prices in one batch (CN + HK share the `daily` table).
    ts_codes: list[str] = [ts for _, _, ts in candidates]

    closes_by_ts: dict[str, float] = {}
    if ts_codes:
        try:
            # fetch_last_ohlcv_batch returns {ts_code: [(date, o, h, l, c, v), ...]}
            # The list is in ASC date order per docs in db/daily.py; the last
            # entry is the most recent bar. We trust the daily table to be at
            # least one row deep for symbols already in the universe.
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

    for ch, market, ts in candidates:
        if ts not in closes_by_ts:
            reason = "no-close-price"
            summary["skipped"] += 1
            summary["skippedReasons"][reason] = summary["skippedReasons"].get(reason, 0) + 1
            continue
        sym = str(ch.get("symbol") or "")
        raw_source = ch.get("source")
        # action is function-scoped in the filter loop above; re-read it per
        # candidate (previously the LAST action change seen leaked into every
        # insert — a WATCH/TRIM/EXIT tail row made all inserts fail or mislabel).
        action = str(ch.get("newValue") or "").upper()
        # TIP-011: only accept the closed enum; everything else → None.
        source = raw_source if raw_source in pt_db.SOURCES else None
        try:
            row = pt_db.insert_paper_trade(
                symbol=sym,
                entry_date=trade_date,
                side=action,
                entry_price=closes_by_ts[ts],
                # The journal does not store score/why/sleeve (field/newValue
                # only) — these stay None and close conditions read live data.
                source=source,
                market=market,
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
        e = date.fromisoformat(str(entry_date_iso or ""))
        t = date.fromisoformat(str(today_iso or ""))
    except (ValueError, TypeError):
        return 0
    return max(0, (t - e).days)


def run_update(*, today_iso: str | None = None) -> dict[str, Any]:
    """One pass of the update cron.

    For every open trade:
    1. Look up the latest close.
    2. Update pnl_pct / holding_days.
    3. Apply close conditions, in priority order (first match wins):
       - ``stop_hit``: NET pnl_pct <= STOP_LOSS_PCT
       - ``target_hit``: NET pnl_pct >= TARGET_PNL_PCT
       - ``score_floor``: latest TrendOK score < SCORE_FLOOR (CN only;
         fails open for HK where TrendOK has no definition)
       - ``pool_exit``: symbol no longer in the watchlist registry
         (fail-open: registry read failure never closes)
       - ``max_hold``: holding_days >= MAX_HOLD_DAYS

    v0.2 (OPT-062): stop/target trigger on the NET pnl (gross minus the
    market's round-trip cost). The net/gross/costs split is written once,
    at close time; open rows keep showing the current gross pnl.

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

    # Batch-fetch latest closes for the open symbols' CN/HK ts_codes.
    ts_codes: list[str] = []
    for t in open_trades:
        resolved = _resolve_ts_code(str(t.get("symbol") or ""))
        if resolved:
            ts_codes.append(resolved[1])

    closes_by_ts: dict[str, float] = {}
    bars_by_ts_all: dict[str, list] = {}  # raw bars; peak computed per-trade below
    if ts_codes:
        try:
            bars_by_ts_all = fetch_last_ohlcv_batch(ts_codes, days=max(pt_db.MAX_HOLD_DAYS, 5) + 2)
            for ts, bars in bars_by_ts_all.items():
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
        resolved = _resolve_ts_code(sym)
        if resolved is None or resolved[1] not in closes_by_ts:
            # No fresh price; skip without erroring (the next day will retry).
            continue
        market, ts = resolved
        close_price = closes_by_ts[ts]
        # db rows come back camelCase (db/paper_trading._row_to_dict); accept
        # both shapes so the backtest engine's snake_case position dicts work
        # through the same code path.
        entry_price = _row_number(t, "entryPrice", "entry_price")
        if entry_price is None or entry_price <= 0:
            continue
        gross_pnl_pct = (close_price - entry_price) / entry_price * 100.0
        # v0.2: round-trip cost applies once, at close time. For open trades
        # we keep showing the gross pnl (costs not yet incurred); the NET
        # value is what close conditions and the final pnl_pct use.
        costs_pct = round_trip_cost_pct(market) * 100.0
        net_pnl = gross_pnl_pct - costs_pct
        holding_days = _holding_days_for(
            str(_row_str(t, "entryDate", "entry_date") or ""), today_iso
        )

        # OPT-105 (2026-08-13 固化): CN S-3 paper exits switch to the
        # entry-locked ATR% x S3_ATR_STOP_MULT line while today's regime is
        # Strong (let winners run); Diverging/Weak fall back to the fixed
        # pt_db constants (cut fast). Mirror of the backtest S3_CONFIG —
        # same rule set, same code path (_pick_close_reason).
        stop_pct = pt_db.STOP_LOSS_PCT
        trail_pct = pt_db.TRAILING_STOP_PCT
        if market == "CN" and str(t.get("source") or "") == "S3":
            entry = _row_str(t, "entryDate", "entry_date") or ""
            atr_pct = _atr_pct_at_entry(bars_by_ts_all.get(ts, []), entry, entry_price)
            if atr_pct > 0 and _cn_regime_today() == REGIME_STRONG:
                stop_pct = trail_pct = -(pt_db.S3_ATR_STOP_MULT * atr_pct)

        reason = _pick_close_reason(
            t=t,
            pnl_pct=net_pnl,
            holding_days=holding_days,
            registry_symbols=registry_symbols,
            stop_loss_pct=stop_pct,
            # 2026-08-11: S-3 paper lines (CN + HK) are managed by the S-3
            # rule set (same code as the backtest); pool_exit (registry
            # membership) is a v0-manual-book rule and must NOT apply — the
            # S-3 HK universe (vol top 500) is by design outside the user's
            # watchlist registry.
            exclude_pool_exit=str(t.get("source") or "") in ("S3", "S3HK"),
        )
        # S-3 trailing stop: close when price pulls back >= 8% from the
        # post-entry CLOSE peak (2026-08-12 C4 alignment: the live line must
        # peak on CLOSES exactly like the backtest engine and the health
        # card — high-based peaking made the paper book exit earlier than
        # the backtest (HK:00622 case: intraday spike peak triggered a
        # trailing the close-based engine never fires)).
        if reason is None and trail_pct != 0:
            peak = 0.0
            entry = _row_str(t, "entryDate", "entry_date") or ""
            for b in bars_by_ts_all.get(ts, []):
                if str(b[0]) < entry:
                    continue
                try:
                    c = float(b[4])
                except (TypeError, ValueError):
                    continue
                if c > peak:
                    peak = c
            if peak > 0 and (close_price - peak) / peak * 100.0 <= trail_pct:
                reason = pt_db.CLOSE_REASON_TRAILING
        # A6 profit-trail (same rule as the backtest engine): once the leg is
        # past the profit trigger, tighten the allowed peak pullback to
        # protect realized gains. Disabled until the walk-forward audit
        # passes (live constants are the ship gate).
        if (
            reason is None
            and pt_db.PROFIT_TRAIL_TRIGGER_PCT > 0
            and pt_db.PROFIT_TRAIL_PCT < 0
        ):
            entry_px = _row_number(t, "entryPrice", "entry_price") or 0.0
            peak = 0.0
            entry = _row_str(t, "entryDate", "entry_date") or ""
            for b in bars_by_ts_all.get(ts, []):
                if str(b[0]) < entry:
                    continue
                try:
                    h = float(b[2])
                except (TypeError, ValueError):
                    continue
                if h > peak:
                    peak = h
            if (
                entry_px > 0
                and peak > 0
                and (peak - entry_px) / entry_px * 100.0 >= pt_db.PROFIT_TRAIL_TRIGGER_PCT
                and (close_price - peak) / peak * 100.0 <= pt_db.PROFIT_TRAIL_PCT
            ):
                reason = pt_db.CLOSE_REASON_TRAILING
        if reason is not None:
            try:
                pt_db.close_paper_trade(
                    trade_id=str(t.get("id") or ""),
                    close_date=today_iso,
                    close_price=close_price,
                    pnl_pct=net_pnl,
                    holding_days=holding_days,
                    close_reason=reason,
                    gross_pnl_pct=gross_pnl_pct,
                    costs_pct=costs_pct,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("close_paper_trade (%s) failed for %s: %s", reason, sym, exc)
                continue
            summary["closed"] += 1
            summary["closeReasons"][reason] = summary["closeReasons"].get(reason, 0) + 1
            continue

        # Otherwise just touch the live state (still GROSS pnl for open rows).
        try:
            pt_db.update_paper_trade_price(
                trade_id=str(t.get("id") or ""),
                close_price=close_price,
                pnl_pct=gross_pnl_pct,
                holding_days=holding_days,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("update_paper_trade_price failed for %s: %s", sym, exc)
            continue
        summary["updated"] += 1

    return summary


def _row_number(t: dict[str, Any], camel: str, snake: str) -> float | None:
    """Read a numeric row field tolerating db camelCase + engine snake_case."""
    raw = t.get(camel)
    if raw is None:
        raw = t.get(snake)
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _row_str(t: dict[str, Any], camel: str, snake: str) -> str | None:
    raw = t.get(camel)
    if raw is None:
        raw = t.get(snake)
    return str(raw) if raw is not None else None


def _atr_pct_at_entry(
    bars: list, entry_date: str, entry_price: float
) -> float:
    """ATR14 / entry_price x 100 computed from the sessions BEFORE entry.

    OPT-105: the S-3 Strong-regime stop uses the entry-locked ATR% (same as
    the backtest engine's ``atr14_pct_for``). 0.0 when bars are insufficient
    → the fixed constants apply (safe fallback)."""
    before = sorted(
        [b for b in bars if str(b[0]) < entry_date], key=lambda b: str(b[0])
    )[-15:]
    if len(before) < 8 or entry_price is None or entry_price <= 0:
        return 0.0
    trs: list[float] = []
    prev: float | None = None
    for b in before:
        try:
            hi, lo = float(b[2]), float(b[3])
        except (TypeError, ValueError):
            continue
        if prev is None:
            prev = hi
            continue
        trs.append(max(hi - lo, abs(hi - prev), abs(lo - prev)))
        prev = hi
    if not trs:
        return 0.0
    return sum(trs) / len(trs) / float(entry_price) * 100.0


def _cn_regime_today() -> str | None:
    """Today's CN market regime (backtest engine's market-aware loader —
    same source the S-3 candidate build uses). None on failure → fixed
    stops apply (safe fallback)."""
    from data_sync_service.service.backtest_engine import (
        BacktestConfig,
        _load_regime_by_day,
    )

    try:
        today = pt_db.today_iso()
        cfg = BacktestConfig(start_date=today, end_date=today, market="CN", gates="full")
        return _load_regime_by_day(cfg, [today]).get(today)
    except Exception as exc:  # noqa: BLE001
        logger.warning("paper regime lookup failed: %s", exc)
        return None


def _pick_close_reason(
    *,
    t: dict[str, Any],
    pnl_pct: float,
    holding_days: int,
    registry_symbols: set[str] | None,
    score: float | None = None,
    stop_loss_pct: float | None = None,
    target_pnl_pct: float | None = None,
    max_hold_days: int | None = None,
    score_floor: float | None = None,
    exclude_pool_exit: bool = False,
) -> str | None:
    """Choose the close reason for an open trade, or None to keep it open.

    Priority: stop_hit > target_hit > score_floor > pool_exit > max_hold.
    ``pnl_pct`` is the NET pnl (v0.2). ``score_floor`` and ``pool_exit`` are
    fail-open — when their input data is unavailable they are skipped instead
    of force-closing. score_floor only ever fires for CN trades: TrendOK is
    CN-daily-only, so HK symbols have no score and fail open by design.

    Threshold overrides (OPT-063): the backtest engine passes its config
    values here so the same code path can sweep parameters. Live callers
    pass None and the module-level live constants (pt_db.STOP_LOSS_PCT ...)
    apply — behaviour is byte-identical for the paper cron.

    ``score`` (OPT-063): when given, use it as the AS-OF TrendOK score for
    the score_floor check, skipping the live DB lookup. The backtest engine
    injects the historical score recorded on that day — reading live data
    would be a look-ahead bias. Live callers pass None (default) and keep
    the existing ``fetch_latest_score_since`` behaviour.
    """
    stop = stop_loss_pct if stop_loss_pct is not None else pt_db.STOP_LOSS_PCT
    target = target_pnl_pct if target_pnl_pct is not None else pt_db.TARGET_PNL_PCT
    hold = max_hold_days if max_hold_days is not None else pt_db.MAX_HOLD_DAYS
    floor = score_floor if score_floor is not None else pt_db.SCORE_FLOOR

    if pnl_pct <= stop:
        return pt_db.CLOSE_REASON_STOP_HIT
    if pnl_pct >= target:
        return pt_db.CLOSE_REASON_TARGET_HIT

    if score is None:
        try:
            score = wa_db.fetch_latest_score_since(
                str(t.get("symbol") or ""),
                str(_row_str(t, "entryDate", "entry_date") or ""),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("paper_trade fetch_latest_score_since failed: %s", exc)
    if score is not None and score < floor:
        return pt_db.CLOSE_REASON_SCORE_FLOOR

    if not exclude_pool_exit and registry_symbols is not None and str(t.get("symbol") or "") not in registry_symbols:
        return pt_db.CLOSE_REASON_POOL_EXIT

    if holding_days >= hold:
        return pt_db.CLOSE_REASON_MAX_HOLD

    return None


# ---------------------------------------------------------------------------
# Stats (exposed via the API)
# ---------------------------------------------------------------------------


def compute_stats(*, since_iso: str, market: str | None = None) -> dict[str, Any]:
    """Return a small summary for trades closed since ``since_iso``.

    All numbers are NET-of-costs (v0.2 / OPT-062). ``market`` narrows the
    window to 'CN' | 'HK' when given; ``byMarket`` always carries the
    per-market split for the window.

    The shape is intentionally small and stable; richer analytics (drawdown,
    per-industry break-down, etc.) live in L3-P3.
    """
    try:
        total, wins = pt_db.count_since(since_iso)
        avg = pt_db.avg_pnl_pct_since(since_iso)
        by_market = pt_db.count_by_market_since(since_iso)
    except Exception as exc:  # noqa: BLE001
        return {
            "since": since_iso,
            "error": f"stats query failed: {exc}",
        }
    win_rate = (wins / total) if total > 0 else None

    if market is not None:
        bucket = by_market.get(market)
        if bucket is not None:
            total = int(bucket.get("closedCount") or 0)
            wins = int(bucket.get("winningCount") or 0)
            avg = bucket.get("avgPnlPct")
            win_rate = bucket.get("winRate")
        else:
            total = wins = 0
            avg = None
            win_rate = None
    return {
        "since": since_iso,
        "closedCount": total,
        "winningCount": wins,
        "winRate": win_rate,
        "avgPnlPct": avg,
        "byMarket": by_market,
    }
