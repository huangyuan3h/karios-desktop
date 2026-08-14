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
    list_paper_trades,
)
from data_sync_service.service.backtest_engine import (
    BacktestConfig,
    _load_flow_mainline_data,
    _load_industries,
    _load_regime_by_day,
    _load_rs_ranks,
)
from data_sync_service.service.market_sentiment import get_cn_sentiment, get_panic_cooldown
from data_sync_service.service.env_label import (
    ENV_FAN,
    ENV_UPTREND,
    ENV_WEAK,
    ENV_UNKNOWN,
)
from data_sync_service.service.paper_cost_model import round_trip_cost_pct
from data_sync_service.service.paper_trading import _holding_days_for, _resolve_ts_code
from data_sync_service.service.trendok import _lookup_stock_basic

logger = logging.getLogger(__name__)

S3_SCORE_THRESHOLD = 65.0
S3_RS_MIN = 0.5
# HK RS floor matches the HK backtest baseline (HK_S3_CONFIG.rs_rank_min=0.6,
# strategy-params.md §HK) — the shared 0.5 is the CN S-3 floor (2026-08-11).
S3_RS_MIN_HK = 0.6
S3_MAX_POSITIONS = 20
# 2026-08-12 (long-window defence): drawdown circuit breaker — live mirror
# of backtest drawdown_circuit_pct=-25 (30d realized window, CN line only).
# Realized net pnl over trailing 30 days <= -25% → block new S-3 entries
# (2022/2023 showed the entry edge turns negative in losing streaks).
# 2026-08-12 (OPT-094): red-light block — walk-forward verified (OOS2 win
# 48→51%, valid win 61→79% & total +10.7pt, no window worse). CN line only;
# HK index lights show no separation (OPT-093) so HK stays unblocked.
S3_LIGHT_RED_BLOCK = True
S3_CIRCUIT_PCT = -25.0
S3_CIRCUIT_WINDOW_DAYS = 30
S3_CIRCUIT_MIN_TRADES = 3
S3_POSITION_PCT = 0.10  # per-sleeve size — SAME as the backtest (10%x20)
# 2026-08-11: paper was 5% (conservative); user decision: paper must mirror
# the backtest exactly, so paper book results are directly comparable to the
# backtest numbers. position_pct is a pure leverage knob (sharpe-invariant,
# strategy-params §1) — no re-validation needed, backtest untouched.

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

# TIP-014 (2026-08-14): implicit-weak breadth ratio (up/down < 0.5 blocks
# new CN entries). Mirrors execution_gate.WEAK_RATIO_MAX + env_label.
WEAK_RATIO_MAX = 0.5

# TIP-014 (2026-08-14): environment-aware entry style — mirrors the S-3
# backtest config (entry_style=auto RS0.7 dip3%). uptrend → momentum
# (RS>=ENTRY_STYLE_RS_MIN, NOT in pullback); fan → dip (RS>=rs_min AND in
# pullback <= -ENTRY_STYLE_DIP_MIN); weak/neutral → blocked (handled by
# the sentiment block above); unknown → no style filter.
ENTRY_STYLE_RS_MIN = 0.7
ENTRY_STYLE_DIP_MIN = 3.0


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


def _env_for_day(sentiment_items: list[dict[str, Any]]) -> str:
    """Environment bucket for today's CN sentiment items (TIP-014).

    Mirrors service/env_label.load_env_by_day's per-day logic WITHOUT the
    mainline-churn signal (that needs historical mainline rows; the live
    intraday path only has today's snapshot). Fallback when mainline data is
    absent: ratio-only labels (fan 0.5..1.5 / weak < 0.5 / else unknown),
    matching env_label's pre-mainline behaviour.
    """
    if not sentiment_items:
        return ENV_UNKNOWN
    item = sentiment_items[-1]
    mode = str(item.get("riskMode") or "")
    if mode in SENTIMENT_BLOCK_MODES:
        return ENV_WEAK
    up = int(item.get("upCount") or 0)
    down = int(item.get("downCount") or 0)
    if up > 0 and down > 0 and (up / down) < WEAK_RATIO_MAX:
        return ENV_WEAK
    if up > 0 and down > 0:
        ratio = up / down
        if ratio >= 2.0 and float(item.get("yesterdayLimitupPremium") or 0.0) >= 0.0:
            return ENV_UPTREND
        if 0.5 <= ratio <= 1.5:
            return ENV_FAN
    return ENV_UNKNOWN


def _env_style_for(sentiment_items: list[dict[str, Any]]) -> str | None:
    """Entry style for today's CN environment (TIP-014 auto mode).

    Returns 'momentum' (uptrend), 'dip' (fan), or None (unknown/no filter).
    weak/neutral days never reach here — they are blocked upstream.
    """
    env = _env_for_day(sentiment_items)
    if env == ENV_UPTREND:
        return "momentum"
    if env == ENV_FAN:
        return "dip"
    return None


def _ret5_for(ts_code: str, as_of: str) -> float | None:
    """5-session return % for ``ts_code`` at ``as_of`` (None when bars are
    insufficient). Same window as the backtest engine's ret5_for — used by
    the TIP-014 style filter in live paper."""
    try:
        bars = fetch_last_ohlcv_batch([ts_code], days=10).get(ts_code) or []
        closes = [(str(b[0]), float(b[4])) for b in bars if len(b) > 4 and b[4] is not None]
    except Exception as exc:  # noqa: BLE001
        logger.warning("paper_s3 ret5 fetch failed for %s: %s", ts_code, exc)
        return None
    closes.sort(key=lambda kv: kv[0])
    idx = None
    for i, (d, _c) in enumerate(closes):
        if d == as_of:
            idx = i
            break
    if idx is None or idx < 5:
        return None
    c_prev = closes[idx - 5][1]
    c_now = closes[idx][1]
    if c_prev <= 0:
        return None
    return (c_now / c_prev - 1.0) * 100.0


def _latest_daily_date_before(day: str) -> str | None:
    """Latest trade_date in ``daily`` strictly before ``day``.

    Intraday (before 17:10 close_sync) today's bars do not exist yet, so
    scores/RS computed from the DB are as-of the previous session. Used as
    the RS fallback so the intraday candidate surface is not empty.
    """
    from data_sync_service.db import get_connection

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(trade_date) FROM daily WHERE trade_date < %s",
                    (day,),
                )
                row = cur.fetchone()
        return str(row[0]) if row and row[0] else None
    except Exception as exc:  # noqa: BLE001
        logger.warning("paper_s3 latest daily date lookup failed: %s", exc)
        return None


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


_LIGHT_NAMES_CN = {"沪深300", "中证500", "创业板指"}
_LIGHT_RANK = {"deep_green": 4, "green": 3, "yellow": 2, "red": 1, "unknown": 0}


def _index_light_red(*, as_of: str) -> bool:
    """CN tighter index light is red (OPT-094) — as-of replay, same as the
    backtest loader. Returns False on missing data (fail-open at the index
    level; the regime gate still applies)."""
    try:
        from data_sync_service.service.market_regime import get_index_signals

        signals = get_index_signals(as_of_date=as_of, include_breadth=False)
        lights = [
            str(s.get("signal") or "unknown")
            for s in signals
            if str(s.get("name") or "") in _LIGHT_NAMES_CN
        ]
        if not lights:
            return False
        return min(lights, key=lambda x: _LIGHT_RANK.get(x, 0)) == "red"
    except Exception:  # noqa: BLE001
        return False


def _circuit_blocked(*, as_of: str) -> bool:
    """True when the trailing realized pnl window is in a losing streak.

    Same rule as the backtest's drawdown_circuit_pct=-25: at least
    S3_CIRCUIT_MIN_TRADES closed trades whose NET pnl sums to <= -25% over
    the trailing 30 calendar days → halt new CN S-3 entries. Mirrors the
    engine exactly (as-of close_date comparison, live rows only).
    """
    from datetime import date, timedelta

    cutoff = (date.fromisoformat(as_of) - timedelta(days=S3_CIRCUIT_WINDOW_DAYS)).isoformat()
    closed = list_paper_trades(status="closed", market="CN", limit=1000)
    recent = []
    for r in closed:
        cd = str(r.get("closeDate") or r.get("close_date") or "")
        if not cd or cd < cutoff:
            continue
        p = r.get("pnlPct")
        if p is None:
            p = r.get("pnl_pct")
        try:
            recent.append(float(p))
        except (TypeError, ValueError):
            continue
    return len(recent) >= S3_CIRCUIT_MIN_TRADES and sum(recent) <= S3_CIRCUIT_PCT


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

    2026-08-11: HK RS floor is 0.6 (HK_S3_CONFIG.rs_rank_min, backtest
    baseline) — the shared S3_RS_MIN (0.5) is the CN S-3 floor.
    """
    from data_sync_service.db.paper_trading import today_iso

    day = trade_date or today_iso()
    is_hk = market == "HK"
    rs_min = S3_RS_MIN_HK if is_hk else S3_RS_MIN
    cfg = BacktestConfig(
        start_date=day,
        end_date=day,
        market=market,
        score_threshold=S3_SCORE_THRESHOLD,
        gates="regime" if is_hk else "full",
        rs_rank_min=rs_min,
        diverging_scale=1.0,
    )
    regime_by_day = _load_regime_by_day(cfg, [day])
    panic = get_panic_cooldown(days=10, cooldown_days=3, as_of_date=day)

    # 2026-08-12: drawdown circuit breaker (CN line). Block new entries when
    # the trailing realized pnl is in a losing streak — mirrors the backtest
    # drawdown_circuit_pct; paper and backtest stay same-code.
    if market == "CN" and _circuit_blocked(as_of=day):
        return []

    # 2026-08-12 (OPT-094): CN red-light days produce no candidates (no
    # recommendations) — same replay as the backtest light_red_block.
    if market == "CN" and S3_LIGHT_RED_BLOCK and _index_light_red(as_of=day):
        return []

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
    rs_by_day = rs_by_sym.get(day)
    if not rs_by_day:
        # Intraday: today's daily bars are not synced yet (close_sync 17:10),
        # so the RS percentile for today is absent. Fall back to the latest
        # available RS day (previous session's close) so the intraday S-3
        # surface works during trading hours. The EOD chain (17:30 scores +
        # 17:42 paper intake) re-evaluates with today's close; this fallback
        # only affects the intraday decision surface.
        prev = _latest_daily_date_before(day)
        if prev:
            cfg_prev = BacktestConfig(
                start_date=prev,
                end_date=prev,
                market=market,
                score_threshold=S3_SCORE_THRESHOLD,
                gates="regime" if is_hk else "full",
                rs_rank_min=rs_min,
                diverging_scale=1.0,
            )
            prev_rs = _load_rs_ranks(cfg_prev, [prev], set(resolved.values())).get(prev)
            if prev_rs:
                rs_by_day = prev_rs
        rs_by_day = rs_by_day or {}

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
        flow_any_positive_by_day, mainline_allow_by_day, _flow5d = _load_flow_mainline_data(cfg, [day])
        flow_ok = flow_any_positive_by_day.get(day, False)
        if not flow_ok:
            return []
        mainline = mainline_allow_by_day.get(day) or set()
        sentiment_items = get_cn_sentiment(days=1, as_of_date=day)["items"]
        today_mode = sentiment_items[-1].get("riskMode") if sentiment_items else ""
        # TIP-014 (2026-08-14): implicit-weak day — breadth ratio < 0.5
        # (跌家数 > 2× 涨家数) even when risk_mode is only normal/caution.
        # Same definition as service/execution_gate.WEAK_RATIO_MAX and the
        # backtest engine (env_label.WEAK_RATIO_MAX): 16/16 losing trades in
        # the valid window.
        s_up = int(sentiment_items[-1].get("upCount") or 0) if sentiment_items else 0
        s_down = int(sentiment_items[-1].get("downCount") or 0) if sentiment_items else 0
        breadth_weak = (
            s_up > 0 and s_down > 0 and (s_up / s_down) < WEAK_RATIO_MAX
        )
        if (
            today_mode in SENTIMENT_BLOCK_MODES
            or breadth_weak
            or panic.get("active")
        ):
            return []
        industry_by_ts = _load_industries(list(resolved.values()))

    out: list[dict[str, Any]] = []
    style = None
    if not is_hk:
        style = _env_style_for(sentiment_items)
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
        if rs is None or rs < rs_min:
            continue
        if style is not None and not is_hk:
            if rs < ENTRY_STYLE_RS_MIN:
                continue
            # ret5 as-of `day`; intraday (before 17:10 close_sync) today's
            # bar is absent → fall back to the previous session (same as the
            # RS fallback above) so the intraday surface is not emptied.
            ret5 = _ret5_for(ts, day) or _ret5_for(ts, _latest_daily_date_before(day) or "")
            if ret5 is None:
                continue
            if style == "momentum" and ret5 < -ENTRY_STYLE_DIP_MIN:
                continue
            if style == "dip" and ret5 > -ENTRY_STYLE_DIP_MIN:
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
    sleeve_scale: float = 1.0,
) -> int:
    """S-3 pyramiding: add a half sleeve at same-day close when the main leg
    is up >= PYRAMID_TRIGGER_PCT. Idempotent per (symbol, entry_date, side);
    at most PYRAMID_MAX_ADDS adds per symbol (counted via the 'pyramid-add'
    marker in existing open S-3 rows). 2026-08-11 (T4): sleeve scales with
    the week's market allocation; a 0-weight market adds no capital."""
    if not PYRAMID_ENABLED or PYRAMID_MAX_ADDS <= 0 or sleeve_scale <= 0:
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
                sleeve_pct=S3_POSITION_PCT * PYRAMID_ADD_SCALE * sleeve_scale,
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


def _signal_snapshot_for(
    *,
    symbol: str,
    industry: str | None,
    trade_date: str,
) -> dict[str, Any] | None:
    """Info-layer snapshot at entry (C4 validation data — never a gate).

    SW L1 industry 5-day net-inflow rank/total/amount (CN only; HK has no
    industry flow feed) + alpha-event count for the symbol (14-day window).
    """
    snap: dict[str, Any] = {}
    if industry:
        try:
            from data_sync_service.service.portfolio_health import _industry_flow_map

            f = _industry_flow_map(trade_date).get(industry)
            if f:
                snap["industryRank5d"] = f["rank5d"]
                snap["industryTotal"] = f["total"]
                snap["industryNetInflow5d"] = f["netInflow5d"]
        except Exception as exc:  # noqa: BLE001
            logger.warning("paper_s3 signal snapshot flow failed: %s", exc)
    try:
        from data_sync_service.service.portfolio_health import _alpha_events_for_symbols

        events = _alpha_events_for_symbols([symbol])
        n_events = len(events.get(_alpha_key(symbol)) or [])
        if n_events > 0:
            snap["alphaEvents"] = n_events
    except Exception as exc:  # noqa: BLE001
        logger.warning("paper_s3 signal snapshot alpha failed: %s", exc)
    return snap if snap else None


def _alpha_key(symbol: str) -> str:
    if symbol.startswith("HK:") and len(symbol) == 7:
        return "HK:" + symbol[3:].zfill(5)
    return symbol

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
    # T4 (2026-08-11): shared capital pool — the week's R5c weights scale the
    # sleeve; a 0-weight market opens no NEW positions (existing holdings keep
    # normal exit management via paper_trading_update).
    try:
        from data_sync_service.service.allocation import week_weights

        w_row = week_weights(day)["decision"]
        w_market = float(w_row.get("w_hk") if market == "HK" else w_row.get("w_cn") or 0.0)
        sleeve_scale = w_market
    except Exception as exc:  # noqa: BLE001
        logger.warning("paper_s3 allocation weights failed (fallback 100%%): %s", exc)
        sleeve_scale = 1.0
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
        "allocation": sleeve_scale,
    }

    # S-3 pyramiding first: add legs on held winners are regime-independent
    # (like the backtest's mark-to-market step) and free up no sleeve.
    try:
        holds0 = _s3_open_holds(source=source)
        if holds0:
            closes0 = _fetch_closes([str(h.get("tsCode") or "") for h in holds0 if h.get("tsCode")])
            summary["pyramidAdded"] = _pyramid_adds(day=day, holds=holds0, closes=closes0, source=source, sleeve_scale=sleeve_scale)
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

    if sleeve_scale <= 0:
        # T4: this market has no capital this week (R5c) — no NEW positions.
        summary["skipped"] += len(candidates) + len(swapped_cands)
        summary["skippedReasons"]["allocation-zero"] = summary["skippedReasons"].get("allocation-zero", 0) + len(candidates) + len(swapped_cands)
        return summary

    sleeve = S3_POSITION_PCT * sleeve_scale

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
                sleeve_pct=sleeve,
                source=source,
                market=("HK" if source == SOURCE_S3_HK else "CN"),
                signal_snapshot=_signal_snapshot_for(
                    symbol=cand["symbol"], industry=cand.get("industry"), trade_date=day,
                ),
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
            {"symbol": cand["symbol"], "name": name, "score": cand["score"], "sleevePct": sleeve,
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
                sleeve_pct=sleeve,
                source=source,
                market=("HK" if source == SOURCE_S3_HK else "CN"),
                signal_snapshot=_signal_snapshot_for(
                    symbol=cand["symbol"], industry=cand.get("industry"), trade_date=day,
                ),
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
            {"symbol": cand["symbol"], "name": name, "score": cand["score"], "sleevePct": sleeve}
        )
    return summary
