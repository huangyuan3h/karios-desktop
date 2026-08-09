"""S-3 paper intake (G4) — paper trades that follow the S-3 backtest entry rules.

The S-3 strategy was fixed by backtesting (docs/modules/backtest-strategy.md,
双年验证 2026-08-09). The regular ``run_intake`` paper-trades the live
execution decisions (BUY_SCORE_MIN=80 user spec) — that is NOT the S-3
universe. This module re-runs the S-3 entry rules on today's data using the
**same backtest-engine code paths** (``_load_regime_by_day`` /
``_load_flow_mainline_data`` / ``_load_rs_ranks``) so the paper record is a
faithful out-of-sample probe of the strategy.

Entry rules (S-3, identical to BacktestConfig S-3):
  - score >= 65 (watchlist_score_daily)
  - regime != Weak (Strong full size / Diverging full size, scale 1.0)
  - sector flow not all-negative AND industry on the mainline whitelist
  - RS percentile >= 0.5 (whole market, 20d)
  - not in panic cooldown (panic day + 3 trade days)
  - not already held (live registry) and no open paper row
  - sorted by score desc, capped at max_positions, 5% sleeve each
"""

from __future__ import annotations

import logging
from typing import Any

from data_sync_service.db.daily import fetch_last_ohlcv_batch
from data_sync_service.db.paper_trading import SOURCE_S3, insert_paper_trade
from data_sync_service.service.backtest_engine import (
    BacktestConfig,
    _load_flow_mainline_data,
    _load_industries,
    _load_regime_by_day,
    _load_rs_ranks,
)
from data_sync_service.service.market_sentiment import get_cn_sentiment, get_panic_cooldown
from data_sync_service.service.paper_trading import _resolve_ts_code
from data_sync_service.service.trendok import _lookup_stock_basic

logger = logging.getLogger(__name__)

S3_SCORE_THRESHOLD = 65.0
S3_RS_MIN = 0.5
S3_MAX_POSITIONS = 20
S3_POSITION_PCT = 0.05  # per-sleeve size (paper is 5%; backtest 10%x20 is the upper bound)

SENTIMENT_BLOCK_MODES = ("no_new_positions", "extreme_caution")


def _load_today_scores(trade_date: str, market: str = "CN") -> dict[str, float]:
    """{symbol: score} from watchlist_score_daily for one market."""
    prefix = f"{market}:"
    from data_sync_service.db import get_connection

    out: dict[str, float] = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, score FROM watchlist_score_daily
                WHERE trade_date = %s AND score IS NOT NULL
                """,
                (trade_date,),
            )
            for sym, score in cur.fetchall():
                s = str(sym or "").upper()
                if s.startswith(prefix):
                    try:
                        out[s] = float(score)
                    except (TypeError, ValueError):
                        continue
    return out


def _open_paper_symbols() -> set[str]:
    """Symbols with an open paper trade (any source) — never double-book."""
    from data_sync_service.db.paper_trading import list_paper_trades

    out: set[str] = set()
    for row in list_paper_trades(status="open"):
        sym = str(row.get("symbol") or "")
        if sym:
            out.add(sym)
    return out


def _live_held_symbols() -> set[str]:
    """Symbols with a real (non-zero) registry position."""
    from data_sync_service.db.watchlist_automation import list_registry

    out: set[str] = set()
    try:
        for r in list_registry():
            pos = r.get("positionPct")
            if isinstance(pos, (int, float)) and pos > 0:
                sym = str(r.get("symbol") or "")
                if sym:
                    out.add(sym)
    except Exception as exc:  # noqa: BLE001
        logger.warning("paper_s3 live positions lookup failed: %s", exc)
    return out


def build_s3_candidates(
    *,
    trade_date: str | None = None,
    max_positions: int = S3_MAX_POSITIONS,
) -> list[dict[str, Any]]:
    """S-3-qualified candidates on ``trade_date`` (same gates as the backtest)."""
    from data_sync_service.db.paper_trading import today_iso

    day = trade_date or today_iso()
    cfg = BacktestConfig(
        start_date=day,
        end_date=day,
        score_threshold=S3_SCORE_THRESHOLD,
        gates="full",
        rs_rank_min=S3_RS_MIN,
        diverging_scale=1.0,
    )
    regime_by_day = _load_regime_by_day(cfg, [day])
    flow_any_positive_by_day, mainline_allow_by_day = _load_flow_mainline_data(cfg, [day])
    panic = get_panic_cooldown(days=10, cooldown_days=3, as_of_date=day)

    scores = _load_today_scores(day)
    if not scores:
        return []
    universe = sorted(scores)
    resolved: dict[str, str] = {}
    for sym in universe:
        parsed = _resolve_ts_code(sym)
        if parsed and parsed[0] == "CN":
            resolved[sym] = parsed[1]
    if not resolved:
        return []
    rs_by_sym = _load_rs_ranks(cfg, [day], set(resolved.values()))
    rs_by_day = rs_by_sym.get(day, {})

    industry_by_ts = _load_industries(list(resolved.values()))
    held = _live_held_symbols()
    open_paper = _open_paper_symbols()

    regime = regime_by_day.get(day)
    if regime in ("Weak", None):
        return []
    flow_ok = flow_any_positive_by_day.get(day, False)
    if not flow_ok:
        return []
    mainline = mainline_allow_by_day.get(day) or set()
    sentiment_items = get_cn_sentiment(days=1, as_of_date=day)["items"]
    today_mode = sentiment_items[-1].get("riskMode") if sentiment_items else ""
    if today_mode in SENTIMENT_BLOCK_MODES or panic.get("active"):
        return []

    out: list[dict[str, Any]] = []
    for sym in sorted(scores, key=lambda s: -scores[s]):
        score = scores[sym]
        if score < S3_SCORE_THRESHOLD:
            continue
        if sym in held or sym in open_paper:
            continue
        ts = resolved.get(sym)
        if not ts:
            continue
        rs = rs_by_day.get(ts)
        if rs is None or rs < S3_RS_MIN:
            continue
        industry = industry_by_ts.get(ts) or ""
        if not industry or industry not in mainline:
            continue
        out.append(
            {
                "symbol": sym,
                "name": None,  # filled in run_intake_s3 via stock basic
                "ts_code": ts,
                "industry": industry,
                "score": score,
                "rs": rs,
                "regime": regime,
            }
        )
        if len(out) >= max_positions:
            break
    return out


def run_intake_s3(*, trade_date: str | None = None, max_positions: int = S3_MAX_POSITIONS) -> dict[str, Any]:
    """Insert paper trades for S-3 candidates (idempotent per day).

    Returns a summary for the cron recorder. Sleeve = 5% per trade, same as
    the manual discipline; backtest 10% is the upper bound.
    """
    from data_sync_service.db.paper_trading import today_iso

    day = trade_date or today_iso()
    summary: dict[str, Any] = {
        "tradeDate": day,
        "candidates": 0,
        "inserted": 0,
        "skipped": 0,
        "skippedReasons": {},
    }
    try:
        candidates = build_s3_candidates(trade_date=day, max_positions=max_positions)
    except Exception as exc:  # noqa: BLE001
        summary["error"] = f"build_s3_candidates failed: {exc}"
        logger.warning("paper_s3 build failed: %s", exc)
        return summary
    summary["candidates"] = len(candidates)
    if not candidates:
        return summary

    ts_codes = [c["ts_code"] for c in candidates]
    by_name = {}
    try:
        by_name = _lookup_stock_basic(ts_codes)[0]
    except Exception as exc:  # noqa: BLE001
        logger.warning("paper_s3 stock basic lookup failed: %s", exc)

    closes: dict[str, float] = {}
    try:
        bars_by_ts = fetch_last_ohlcv_batch(ts_codes, days=2)
        for ts, bars in bars_by_ts.items():
            if not bars:
                continue
            last = bars[-1]
            try:
                closes[str(ts)] = float(last[4])
            except (TypeError, ValueError, IndexError):
                continue
    except Exception as exc:  # noqa: BLE001
        logger.warning("paper_s3 close fetch failed: %s", exc)

    for cand in candidates:
        ts = cand["ts_code"]
        px = closes.get(ts)
        if px is None or px <= 0:
            summary["skipped"] += 1
            summary["skippedReasons"]["no-close-price"] = summary["skippedReasons"].get("no-close-price", 0) + 1
            continue
        name = by_name.get(ts)
        why = (
            f"S-3 {cand['regime']} score={cand['score']:.0f} rs={cand['rs']:.0%} "
            f"industry={cand['industry']}"
        )
        try:
            row = insert_paper_trade(
                symbol=cand["symbol"],
                entry_date=day,
                side="BUY",
                entry_price=px,
                score_at_entry=round(cand["score"], 2),
                why_at_entry=why,
                sleeve_pct=S3_POSITION_PCT,
                source=SOURCE_S3,
                market="CN",
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("paper_s3 insert failed for %s: %s", cand["symbol"], exc)
            summary["skipped"] += 1
            summary["skippedReasons"]["insert-error"] = summary["skippedReasons"].get("insert-error", 0) + 1
            continue
        if row is None:
            summary["skipped"] += 1
            summary["skippedReasons"]["duplicate"] = summary["skippedReasons"].get("duplicate", 0) + 1
            continue
        summary["inserted"] += 1
        summary.setdefault("symbols", []).append(
            {"symbol": cand["symbol"], "name": name, "score": cand["score"], "sleevePct": S3_POSITION_PCT}
        )
    return summary
