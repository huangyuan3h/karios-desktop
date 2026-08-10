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
from data_sync_service.db.paper_trading import (
    CLOSE_REASON_SWAPPED,
    SOURCE_S3,
    SOURCE_S3_HK,
    close_paper_trade,
    insert_paper_trade,
)
from data_sync_service.service.backtest_engine import (
    BacktestConfig,
    _load_flow_mainline_data,
    _load_industries,
    _load_regime_by_day,
    _load_rs_ranks,
)
from data_sync_service.service.market_sentiment import get_cn_sentiment, get_panic_cooldown
from data_sync_service.service.paper_cost_model import round_trip_cost_pct
from data_sync_service.service.paper_trading import _holding_days_for, _resolve_ts_code
from data_sync_service.service.trendok import _lookup_stock_basic

logger = logging.getLogger(__name__)

S3_SCORE_THRESHOLD = 65.0
S3_RS_MIN = 0.5
S3_MAX_POSITIONS = 20
S3_POSITION_PCT = 0.05  # per-sleeve size (paper is 5%; backtest 10%x20 is the upper bound)

# ChiNext (300xxx) excluded from S-3 candidates — user-approved 2026-08-09
# (A4 focus-pool analysis + triple-window validation):
#   - A4: ChiNext has never contributed alpha in any window (OOS2 25 trades
#     -3.2pt at 28% win, valid 5 trades all lost); main board = stable base,
#     STAR (688) = alpha engine in the recent window (+59.6pt / 77%).
#   - walk-forward: exclude_boards=300 → OOS2 +124.3 vs 106.9 (+17.4pt, DD
#     21.0→18.7), train +151.1 vs 147.5 (+3.6pt), valid +77.2 flat (DD 5.8→5.0).
#     Excluding STAR as well destroys valid (43.8, -30.1pt) — rejected.
# Empty tuple = no board filter.
S3_EXCLUDE_BOARDS: tuple[str, ...] = ("300",)

# RS-rotation swap params (validated on backtest double windows 2026-08-09):
# a held S-3 trade whose RS falls into the weakest 30% after >= 10 days is
# swapped for the strongest RS>=0.8 candidate, at most SWAP_MAX_PER_DAY pairs.
#
# 2026-08-09 决策：机制保留、默认关闭（用户拍板）。正确基线（含 trailing -8）
# 下 swap 增量仅 +0~3pt 且邻域双窗不一致（trailing 已承担走弱淘汰职能），
# 但机制有价值保留为灵活性（未来信号层变化时可直接启用）。详见
# docs/todo.md §19.3 / docs/modules/backtest-strategy.md 6.8。
SWAP_WEAK_RS_BELOW = 0.30
SWAP_STRONG_RS_AT_LEAST = 0.80
SWAP_MIN_HOLD_DAYS = 10
SWAP_MAX_PER_DAY = 2
# Master switch: swap only runs when this is > 0 (0 = disabled, the
# system's default — flexibility kept, not exercised).
SWAP_ENABLED = False

# Pyramiding (user-approved 2026-08-09, §19.2 step 8): add a half sleeve on
# the same-day close when the S-3 main leg is +2.5%; at most 1 add per
# position; the add leg is a separate paper trade (source='S3', why
# 'S-3 pyramid-add'). Triple-window monotone: +1%>+2.5%>+5%>+10%>+30%
# (train 158.5/155.5/148.1/137.1/122.0) — the S-3 entry gates already filter
# a high-win-rate pool, so confirming EARLIER captures more of the move.
# 2.5% chosen as the explainable floor ("held above cost for ~2 sessions")
# instead of 1% (noise-level, +3pt only) or 0 (no business meaning).
PYRAMID_TRIGGER_PCT = 2.5
PYRAMID_ADD_SCALE = 0.5
PYRAMID_MAX_ADDS = 1
PYRAMID_ENABLED = True

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
    market: str = "CN",
) -> list[dict[str, Any]]:
    """S-3-qualified candidates on ``trade_date`` (same gates as the backtest).

    2026-08-10 (HK parallel line): ``market="HK"`` runs the HK regime line —
    HSI/HSTECH regime gate (via the engine's market-aware ``_load_regime_by_day``),
    no sector flow/mainline whitelist (HK has none), RS ranked inside the HK
    universe, panic cooldown from the CN sentiment feed (matches the backtest
    engine's HK path exactly). CN keeps the full gates.
    """
    from data_sync_service.db.paper_trading import today_iso

    day = trade_date or today_iso()
    is_hk = market == "HK"
    cfg = BacktestConfig(
        start_date=day,
        end_date=day,
        market=market,
        score_threshold=S3_SCORE_THRESHOLD,
        gates="regime" if is_hk else "full",
        rs_rank_min=S3_RS_MIN,
        diverging_scale=1.0,
    )
    regime_by_day = _load_regime_by_day(cfg, [day])
    panic = get_panic_cooldown(days=10, cooldown_days=3, as_of_date=day)

    scores = _load_today_scores(day, market=market)
    if not scores:
        return []
    universe = sorted(scores)
    resolved: dict[str, str] = {}
    for sym in universe:
        parsed = _resolve_ts_code(sym)
        if parsed and parsed[0] == market:
            resolved[sym] = parsed[1]
    if not resolved:
        return []
    rs_by_sym = _load_rs_ranks(cfg, [day], set(resolved.values()))
    rs_by_day = rs_by_sym.get(day, {})

    held = _live_held_symbols()
    open_paper = _open_paper_symbols()

    regime = regime_by_day.get(day)
    if regime in ("Weak", None):
        return []
    if is_hk:
        # HK line: gates=regime only (mirrors the HK backtest) — panic
        # cooldown applies, direct sentiment-mode blocking does not.
        if panic.get("active"):
            return []
        flow_ok = True
        mainline: set[str] = set()
    else:
        flow_any_positive_by_day, mainline_allow_by_day = _load_flow_mainline_data(cfg, [day])
        flow_ok = flow_any_positive_by_day.get(day, False)
        if not flow_ok:
            return []
        mainline = mainline_allow_by_day.get(day) or set()
        sentiment_items = get_cn_sentiment(days=1, as_of_date=day)["items"]
        today_mode = sentiment_items[-1].get("riskMode") if sentiment_items else ""
        if today_mode in SENTIMENT_BLOCK_MODES or panic.get("active"):
            return []
        industry_by_ts = _load_industries(list(resolved.values()))

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
        code = str(sym).split(":")[-1]
        if S3_EXCLUDE_BOARDS and code[:3] in S3_EXCLUDE_BOARDS:
            continue
        if not is_hk:
            industry = industry_by_ts.get(ts) or ""
            if not industry or industry not in mainline:
                continue
        out.append(
            {
                "symbol": sym,
                "name": None,  # filled in run_intake_s3 via stock basic
                "ts_code": ts,
                "industry": None,
                "score": score,
                "rs": rs,
                "regime": regime,
            }
        )
        if len(out) >= max_positions:
            break
    return out


def _s3_open_holds(source: str = SOURCE_S3) -> list[dict[str, Any]]:
    """Open paper trades that follow the S-3 strategy (source='S3' / 'S3HK')."""
    from data_sync_service.db.paper_trading import list_paper_trades

    return [r for r in list_paper_trades(status="open") if r.get("source") == source]


def _fetch_closes(ts_codes: list[str]) -> dict[str, float]:
    """{ts_code: latest close} — best-effort, empty dict on failure."""
    if not ts_codes:
        return {}
    closes: dict[str, float] = {}
    try:
        bars_by_ts = fetch_last_ohlcv_batch(sorted(set(ts_codes)), days=2)
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
    return closes


def _pyramid_adds(
    *, day: str, holds: list[dict[str, Any]], closes: dict[str, float], source: str = SOURCE_S3,
) -> int:
    """S-3 pyramiding: add a half sleeve at same-day close when the main leg
    is up >= PYRAMID_TRIGGER_PCT. Idempotent per (symbol, entry_date, side);
    at most PYRAMID_MAX_ADDS adds per symbol (counted via the 'pyramid-add'
    marker in existing open S-3 rows)."""
    if not PYRAMID_ENABLED or PYRAMID_MAX_ADDS <= 0:
        return 0
    n = 0
    for h in holds:
        sym = str(h.get("symbol") or "")
        if not sym:
            continue
        entry = float(h.get("entryPrice") or 0.0)
        if entry <= 0:
            continue
        ts = str(h.get("tsCode") or "")
        px = closes.get(ts)
        if px is None or px <= 0:
            continue
        if "pyramid-add" in str(h.get("whyAtEntry") or ""):
            continue  # this row already is an add leg
        existing = sum(
            1
            for x in holds
            if x.get("symbol") == sym and "pyramid-add" in str(x.get("whyAtEntry") or "")
        )
        if existing >= PYRAMID_MAX_ADDS:
            continue
        gross = (px - entry) / entry * 100.0
        if gross < PYRAMID_TRIGGER_PCT:
            continue
        try:
            row = insert_paper_trade(
                symbol=sym,
                entry_date=day,
                side="BUY",
                entry_price=px,
                why_at_entry=f"S-3 pyramid-add (main leg +{gross:.0f}%)",
                sleeve_pct=S3_POSITION_PCT * PYRAMID_ADD_SCALE,
                source=source,
                market=("HK" if source == SOURCE_S3_HK else "CN"),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("paper_s3 pyramid-add failed for %s: %s", sym, exc)
            continue
        if row is not None:
            n += 1
    return n


def _swap_holds_for_candidates(
    *,
    day: str,
    holds: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    rs_by_ts: dict[str, float],
    closes: dict[str, float],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """RS rotation: swap RS-weakened S-3 holds for clearly stronger candidates.

    Returns (swapped_candidates_to_insert, remaining_candidates). Mirrors the
    backtest engine's swap gate (backtest_engine.py simulate step 1.5) so the
    paper record stays a faithful probe. Costs: round-trip applied on the
    closed leg (same as paper run_update).
    """
    weak: list[tuple[float, dict[str, Any]]] = []
    for h in holds:
        ts = h.get("tsCode") or ""
        rsv = rs_by_ts.get(ts)
        if rsv is None or rsv >= SWAP_WEAK_RS_BELOW:
            continue
        entry_date = str(h.get("entryDate") or "")
        if _holding_days_for(entry_date, day) < SWAP_MIN_HOLD_DAYS:
            continue
        weak.append((rsv, h))
    weak.sort(key=lambda kv: kv[0])  # weakest RS first

    strong: list[tuple[float, dict[str, Any]]] = []
    for c in candidates:
        rsv = float(c.get("rs") or 0.0)
        if rsv >= SWAP_STRONG_RS_AT_LEAST:
            strong.append((rsv, c))
    strong.sort(key=lambda kv: -kv[0])  # strongest RS first

    swapped: list[dict[str, Any]] = []
    rest: list[dict[str, Any]] = list(candidates)
    for (_rsv_w, hold), (_rsv_c, cand) in zip(weak, strong, strict=False):
        if len(swapped) >= SWAP_MAX_PER_DAY:
            break
        ts_w = hold.get("tsCode") or ""
        close_px = closes.get(ts_w)
        if close_px is None or close_px <= 0:
            continue
        ts_c = cand.get("ts_code") or ""
        px_c = closes.get(ts_c)
        if px_c is None or px_c <= 0:
            continue
        entry_px = float(hold.get("entryPrice") or 0.0)
        if entry_px <= 0:
            continue
        gross = (close_px - entry_px) / entry_px * 100.0
        costs = round_trip_cost_pct("CN") * 100.0
        close_paper_trade(
            trade_id=str(hold.get("id") or ""),
            close_date=day,
            close_price=close_px,
            pnl_pct=gross - costs,
            holding_days=_holding_days_for(str(hold.get("entryDate") or ""), day),
            close_reason=CLOSE_REASON_SWAPPED,
            gross_pnl_pct=gross,
            costs_pct=costs,
        )
        cand["entry_price"] = px_c
        cand["sleeve_pct"] = S3_POSITION_PCT
        swapped.append(cand)
        rest = [c for c in rest if c["symbol"] != cand["symbol"]]
    return swapped, rest


def run_intake_s3(
    *,
    trade_date: str | None = None,
    max_positions: int = S3_MAX_POSITIONS,
    market: str = "CN",
) -> dict[str, Any]:
    """Insert paper trades for S-3 candidates (idempotent per day).

    Returns a summary for the cron recorder. Sleeve = 5% per trade, same as
    the manual discipline; backtest 10% is the upper bound.

    2026-08-10 (HK parallel line): ``market="HK"`` uses the HK regime line
    (HSI/HSTECH gates) and attributes rows to source='S3HK' — fully
    independent from the CN S-3 book.
    """
    from data_sync_service.db.paper_trading import today_iso

    source = SOURCE_S3_HK if market == "HK" else SOURCE_S3
    day = trade_date or today_iso()
    summary: dict[str, Any] = {
        "tradeDate": day,
        "market": market,
        "candidates": 0,
        "inserted": 0,
        "pyramidAdded": 0,
        "swappedIn": 0,
        "swappedOut": 0,
        "skipped": 0,
        "skippedReasons": {},
    }

    # S-3 pyramiding first: add legs on held winners are regime-independent
    # (like the backtest's mark-to-market step) and free up no sleeve.
    try:
        holds0 = _s3_open_holds(source=source)
        if holds0:
            closes0 = _fetch_closes([str(h.get("tsCode") or "") for h in holds0 if h.get("tsCode")])
            summary["pyramidAdded"] = _pyramid_adds(day=day, holds=holds0, closes=closes0, source=source)
    except Exception as exc:  # noqa: BLE001
        logger.warning("paper_s3 pyramiding failed: %s", exc)

    try:
        candidates = build_s3_candidates(trade_date=day, max_positions=max_positions, market=market)
    except Exception as exc:  # noqa: BLE001
        summary["error"] = f"build_s3_candidates failed: {exc}"
        logger.warning("paper_s3 build failed: %s", exc)
        return summary
    summary["candidates"] = len(candidates)

    # RS rotation: swap RS-weakened S-3 holds for the strongest candidates
    # before any fresh entries (same gate order as the backtest engine).
    swapped_cands: list[dict[str, Any]] = []
    try:
        holds = _s3_open_holds(source=source)  # fresh: includes today's add legs
        if holds and candidates and SWAP_ENABLED:
            cfg = BacktestConfig(
                start_date=day,
                end_date=day,
                score_threshold=S3_SCORE_THRESHOLD,
                gates="full",
                rs_rank_min=S3_RS_MIN,
                diverging_scale=1.0,
            )
            hold_ts = {str(h.get("tsCode") or "") for h in holds if h.get("tsCode")}
            rs_by_ts = _load_rs_ranks(cfg, [day], hold_ts).get(day, {})
            all_ts = sorted(hold_ts | {c["ts_code"] for c in candidates})
            closes = _fetch_closes(all_ts)
            swapped_cands, candidates = _swap_holds_for_candidates(
                day=day, holds=holds, candidates=candidates,
                rs_by_ts=rs_by_ts, closes=closes,
            )
            summary["swappedOut"] = len(swapped_cands)
    except Exception as exc:  # noqa: BLE001
        # Rotation is best-effort: a failure must not block the fresh intake.
        logger.warning("paper_s3 rotation failed: %s", exc)

    if not candidates and not swapped_cands:
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

    for cand in swapped_cands:
        ts = cand["ts_code"]
        px = cand.get("entry_price") or closes.get(ts)
        if px is None or px <= 0:
            summary["skipped"] += 1
            summary["skippedReasons"]["no-close-price"] = summary["skippedReasons"].get("no-close-price", 0) + 1
            continue
        name = by_name.get(ts)
        why = (
            f"S-3 swap-in {cand['regime']} score={cand['score']:.0f} rs={cand['rs']:.0%} "
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
                source=source,
                market=("HK" if source == SOURCE_S3_HK else "CN"),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("paper_s3 swap-in insert failed for %s: %s", cand["symbol"], exc)
            summary["skipped"] += 1
            summary["skippedReasons"]["insert-error"] = summary["skippedReasons"].get("insert-error", 0) + 1
            continue
        if row is None:
            summary["skipped"] += 1
            summary["skippedReasons"]["duplicate"] = summary["skippedReasons"].get("duplicate", 0) + 1
            continue
        summary["swappedIn"] += 1
        summary.setdefault("symbols", []).append(
            {"symbol": cand["symbol"], "name": name, "score": cand["score"], "sleevePct": S3_POSITION_PCT,
             "swappedIn": True}
        )

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
                source=source,
                market=("HK" if source == SOURCE_S3_HK else "CN"),
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
