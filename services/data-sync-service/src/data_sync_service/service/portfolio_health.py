"""Portfolio health check aligned to the S-3 backtest exit rules.

For every real holding in the watchlist registry (positionPct > 0) compute the
S-3 exit conditions from the SAME constants the paper system uses
(db/paper_trading.py): fixed stop -5% · trailing -8% from peak · 60-day cap.
Market state (regime / sentiment / panic cooldown / S-3 candidates) is
attached so a decision agent can answer "should I cut?" exactly the way the
backtest would.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any

from data_sync_service.db.paper_trading import (
    MAX_HOLD_DAYS,
    MAX_HOLD_DAYS_ENV_SHORTEN,
    STOP_LOSS_PCT,
    TRAILING_STOP_PCT,
)
from data_sync_service.db.watchlist_automation import list_registry
from data_sync_service.service.paper_s3 import (
    PANIC_COOLDOWN_DAYS,
    _env_position_scale_for,
)
from data_sync_service.service.realtime_quote import fetch_realtime_quotes

logger = logging.getLogger(__name__)

# 2026-08-10 (no-choice UX): the manual trade size that matches the
# backtested edge. Backtest = 10%/sleeve × 20 = 200% nominal (no-leverage
# impossible), paper = 5% × 20 = 100%. For a manual book (<=10 positions)
# the no-leverage max is 10%/position × 10 = 100% — same per-position
# exposure as the backtest.
SUGGESTED_SIZE_PCT = 10.0

# Top-N buy list shown on the health card (score desc after dedupe).
TOP_CANDIDATES = 5


def _held_company_names(*, market: str, day: str) -> set[str]:
    """Company names of live holdings on ``market`` (best-effort)."""
    try:
        holdings = _build_holdings_block(market=market, day=day)
    except Exception as exc:  # noqa: BLE001
        logger.warning("portfolio health held names failed: %s", exc)
        return set()
    return {str(h.get("name") or "").strip() for h in holdings or [] if h.get("name")}


def _as_float(v: Any) -> float | None:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _resolve_holding_ts(symbol: str) -> str | None:
    """CN/HK resolve via the paper engine; ETF by exchange code prefix."""
    from data_sync_service.service.paper_trading import _resolve_ts_code

    parsed = _resolve_ts_code(symbol)
    if parsed:
        return parsed[1]
    if symbol.startswith("ETF:"):
        code = symbol.removeprefix("ETF:")
        if len(code) == 6 and code.isdigit():
            return f"{code}.SH" if code.startswith(("5", "6")) else f"{code}.SZ"
    return None


def _lookup_stock_basic(ts_codes: list[str]) -> tuple[dict[str, str], dict[str, str]]:
    """{ts_code: name}, {ts_code: industry} — best-effort, empty on failure."""
    from data_sync_service.db.stock_basic import fetch_all

    names: dict[str, str] = {}
    industries: dict[str, str] = {}
    try:
        for r in fetch_all():
            ts = r.get("ts_code")
            if not ts:
                continue
            if ts in ts_codes:
                if r.get("name"):
                    names[ts] = str(r["name"])
                if r.get("industry"):
                    industries[ts] = str(r["industry"])
    except Exception as exc:  # noqa: BLE001
        logger.warning("portfolio health stock basic failed: %s", exc)
    return names, industries


def _holding_check(
    *,
    name: str,
    cost: float,
    entry_date: str,
    ts: str,
    trade_date: str,
    stop_pct: float | None = None,
    trailing_pct: float | None = None,
    max_hold: int | None = None,
    realtime_price: float | None = None,
    regime: str | None = None,
) -> dict[str, Any]:
    """S-3 exit-condition check for one holding (same constants as paper).

    2026-08-10 (HK parallel line): per-market rule overrides — HK uses the
    HK line's trailing -12% (stop -5% / hold 60 unchanged).
    OPT-105 (2026-08-13): CN Strong sessions use the entry-locked ATR% x
    S3_ATR_STOP_MULT line; Diverging/Weak keep the fixed constants. The
    chosen rule is exposed via ``stopRule`` so the UI can show it.
    """
    from data_sync_service.db import get_connection
    from data_sync_service.db.paper_trading import S3_ATR_STOP_MULT

    stop = stop_pct if stop_pct is not None else STOP_LOSS_PCT
    trail = trailing_pct if trailing_pct is not None else TRAILING_STOP_PCT
    hold = max_hold if max_hold is not None else MAX_HOLD_DAYS
    out: dict[str, Any] = {
        "symbol": name,
        "costPrice": cost,
        "entryDate": entry_date,
        "tsCode": ts,
        "checkDate": trade_date,
    }
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Pull ~35 sessions before entry too: the OPT-105 ATR line is
            # locked at entry time (sessions BEFORE entry).
            lookback = (
                (date.fromisoformat(entry_date) - __import__("datetime").timedelta(days=45)).isoformat()
                if regime == "Strong"
                else entry_date
            )
            cur.execute(
                """
                SELECT trade_date, high, low, close FROM daily
                WHERE ts_code = %s AND trade_date >= %s AND trade_date <= %s
                ORDER BY trade_date
                """,
                (ts, lookback, trade_date),
            )
            bars = cur.fetchall()
    if not bars:
        out["status"] = "no-price-data"
        out["action"] = "HOLD"
        out["note"] = "无价格数据，继续持有观察"
        return out

    # OPT-105: regime-adaptive stop rule. Strong → entry-locked ATR% x mult
    # (same shape as the backtest engine); otherwise fixed constants.
    atr_pct = 0.0
    if regime == "Strong":
        pre = [b for b in bars if str(b[0]) < entry_date][-15:]
        if len(pre) >= 8 and cost > 0:
            trs: list[float] = []
            prev: float | None = None
            for _d, hi, lo, _c in pre:
                hi_f, lo_f = float(hi), float(lo)
                if prev is None:
                    prev = hi_f
                    continue
                trs.append(max(hi_f - lo_f, abs(hi_f - prev), abs(lo_f - prev)))
                prev = hi_f
            if trs:
                atr_pct = (sum(trs) / len(trs)) / cost * 100.0
        if atr_pct > 0:
            stop = trail = -(S3_ATR_STOP_MULT * atr_pct)
            out["stopRule"] = "atr"
            out["stopRuleDetail"] = f"Strong · ATR×{S3_ATR_STOP_MULT}（入场锁定 {atr_pct:.1f}%）"
        else:
            out["stopRule"] = "fixed"
            out["stopRuleDetail"] = "Strong · ATR 数据不足，回退固定"
    else:
        out["stopRule"] = "fixed"
        if str(ts).endswith(".HK"):
            out["stopRuleDetail"] = "固定 -5%/-12%（HK 线）"
        else:
            out["stopRuleDetail"] = "固定 -5%/-8%"

    last_date, _h, _l, last_close = bars[-1]
    last_close = float(last_close)
    # Trailing stop is evaluated on CLOSE prices (same as backtest engine).
    # OPT-101 (2026-08-13): the ACTION is ALWAYS close-caliber (backtest
    # caliber — the user follows the exact backtest behaviour). An optional
    # realtime price (HK line) only produces a WARNING when it has breached a
    # line the close has not — "盘中已破线 · 待收盘确认". The trailing peak
    # stays close-based; the close-based pnl/drawdown drive the action.
    peak = max(float(b[3]) for b in bars if b[3] is not None)
    peak_date = max(bars, key=lambda b: float(b[3]))[0]
    rt_price = realtime_price if realtime_price is not None and realtime_price > 0 else None
    pnl = (last_close - cost) / cost * 100.0
    drawdown = (last_close - peak) / peak * 100.0
    rt_pnl = (rt_price - cost) / cost * 100.0 if rt_price is not None else None
    rt_drawdown = (rt_price - peak) / peak * 100.0 if rt_price is not None else None
    try:
        days = (date.fromisoformat(trade_date) - date.fromisoformat(entry_date)).days
    except ValueError:
        days = 0

    out["lastClose"] = last_close
    out["lastDate"] = str(last_date)
    out["peakPrice"] = peak
    out["peakDate"] = str(peak_date)
    out["evaluatedPrice"] = round(rt_price if rt_price is not None else last_close, 3)
    out["realtime"] = rt_price is not None
    out["pnlPct"] = round(pnl, 2)
    out["drawdownFromPeakPct"] = round(drawdown, 2)
    out["holdingDays"] = days
    out["stopLossLine"] = round(cost * (1 + stop / 100.0), 3)
    out["trailingLine"] = round(peak * (1 + trail / 100.0), 3)
    expire = date.fromisoformat(entry_date) + __import__("datetime").timedelta(days=hold)
    out["maxHoldDate"] = expire.isoformat()
    out["expireDate"] = expire.isoformat()

    reasons: list[str] = []
    if pnl <= stop:
        reasons.append(f"stop_loss（净亏{abs(pnl):.1f}% >= {abs(stop):.0f}% 阈值）")
    if drawdown <= trail:
        reasons.append(f"trailing_stop（峰值回撤{abs(drawdown):.1f}% >= {abs(trail):.0f}% 阈值）")
    if days >= hold:
        reasons.append(f"max_hold（已持{days}天 >= {hold} 天）")
    if reasons:
        out["action"] = "EXIT"
        out["reason"] = "；".join(reasons)
    else:
        out["action"] = "HOLD"
    # OPT-101: realtime line breach that the close has NOT confirmed → warn
    # only (action stays close-caliber). The user's intraday conditional
    # orders may already have fired; this flags "回测尚未确认".
    if rt_price is not None and out["action"] == "HOLD":
        rt_reasons: list[str] = []
        if rt_pnl is not None and rt_pnl <= stop:
            rt_reasons.append(f"实时已破止损线（{rt_pnl:.1f}% <= {stop:.1f}%）")
        if rt_drawdown is not None and rt_drawdown <= trail:
            rt_reasons.append(f"实时已破吊灯线（回撤{abs(rt_drawdown):.1f}% >= {abs(trail):.0f}%）")
        if rt_reasons:
            out["realtimeWarning"] = True
            out["realtimeAlert"] = "；".join(rt_reasons) + " · 待收盘确认（回测口径）"
    return out


def _detect_line_ops(
    *,
    prev_trail: float | None,
    cur_trail: float | None,
    prev_stop: float | None,
    cur_stop: float | None,
    max_hold_days: int | None,
    holding_days: int | None,
    expire_date: str | None = None,
) -> dict[str, Any]:
    """Conditional-order lines that moved since the last notification.

    A broker fixed-price conditional order goes stale when the trailing
    line climbs with the close-peak (or the stop line climbs on a pyramid
    add) — flag it so the user can re-arm the order. Also flags expiry
    inside the last 5 days (broker orders have no auto-expire sell).
    """
    ops: dict[str, Any] = {}
    if prev_trail is not None and cur_trail is not None and cur_trail > prev_trail:
        ops["trail_up"] = [round(prev_trail, 3), round(cur_trail, 3)]
    if prev_stop is not None and cur_stop is not None and cur_stop > prev_stop:
        ops["stop_up"] = [round(prev_stop, 3), round(cur_stop, 3)]
    if max_hold_days and holding_days is not None:
        days_left = max_hold_days - holding_days
        if 0 <= days_left <= 5:
            ops["expire_soon"] = days_left
    if expire_date is not None:
        ops["expireDate"] = expire_date
    return ops


def _alpha_sym_key(symbol: str) -> str:
    """Normalize holding symbols to the alpha-radar format (HK:2099 -> HK:02099)."""
    if symbol.startswith("HK:") and len(symbol) == 7:  # HK: + 4 digits -> 5 digits
        return "HK:" + symbol[3:].zfill(5)
    return symbol


def _alpha_events_for_symbols(
    symbols: list[str], max_age_days: int = 14
) -> dict[str, list[dict[str, Any]]]:
    """{symbol: [alpha trends]} from alpha_radar_trends (best-effort, top-3)."""
    from data_sync_service.db.alpha_radar import fetch_trends

    out: dict[str, list[dict[str, Any]]] = {}
    if not symbols:
        return out
    try:
        _total, items = fetch_trends(limit=200, max_age_days=max_age_days)
    except Exception as exc:  # noqa: BLE001
        logger.warning("portfolio health alpha events failed: %s", exc)
        return out
    want = {_alpha_sym_key(s) for s in symbols}
    today = datetime.now(UTC).date()
    buckets: dict[str, list[dict[str, Any]]] = {}
    for t in items:
        conf = t.get("mappingConfidence")
        try:
            conf_f = float(conf) if conf is not None else None
        except (TypeError, ValueError):
            conf_f = None
        matched: list[str] = []
        for entry in (t.get("cnSymbols") or []) + (t.get("hkSymbols") or []):
            s = str(entry.get("symbol") or "")
            if s and _alpha_sym_key(s) in want:
                matched.append(_alpha_sym_key(s))
        if not matched:
            continue
        published = t.get("documentPublishedAt") or t.get("createdAt") or ""
        days_ago = None
        if published:
            try:
                days_ago = (today - datetime.fromisoformat(str(published)[:10]).date()).days
            except (ValueError, TypeError):
                pass
        event = {
            "trend": str(t.get("trendName") or ""),
            "grade": str(t.get("catalystGrade") or ""),
            "confidence": round(conf_f, 2) if conf_f is not None else None,
            "daysAgo": days_ago,
            "riskStatus": str(t.get("riskStatus") or ""),
            "focus": str(t.get("eventFocus") or "")[:60],
        }
        for s in matched:
            buckets.setdefault(s, []).append(event)
    for sym, events in buckets.items():
        events.sort(key=lambda e: -(e["confidence"] or 0.0))
        out[sym] = events[:3]
    return out


def _l1_industry_for_symbols(symbols: list[str]) -> dict[str, str]:
    """{CN:xxxx: SW L1 industry} from the S-3 score table's latest row per symbol.

    Same source as the S-3 candidates' industry (SW L1), so holdings carry
    the identical industry dialect as the score gate that admitted them.
    """
    out: dict[str, str] = {}
    if not symbols:
        return out
    from data_sync_service.db import get_connection

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (symbol) symbol, industry
                    FROM watchlist_score_daily
                    WHERE symbol = ANY(%s)
                      AND industry IS NOT NULL AND industry <> ''
                    ORDER BY symbol, trade_date DESC
                    """,
                    (symbols,),
                )
                for sym, ind in cur.fetchall():
                    if ind:
                        out[str(sym)] = str(ind)
    except Exception as exc:  # noqa: BLE001
        logger.warning("portfolio health L1 industry lookup failed: %s", exc)
    return out


def _industry_flow_map(trade_date: str | None) -> dict[str, dict[str, Any]]:
    """{SW L1 industry: {netInflow5d(亿元), rank5d, total}} last 5 sessions."""
    from data_sync_service.db.industry_fund_flow import (
        get_dates_upto,
        get_rows_for_dates,
    )
    from data_sync_service.service.industry_fund_flow_read import flow_items_from_rows

    out: dict[str, dict[str, Any]] = {}
    if trade_date is None:
        from data_sync_service.db.paper_trading import today_iso

        day = today_iso()
    else:
        day = trade_date
    try:
        dates = get_dates_upto(day, 5)
        rows = get_rows_for_dates(dates)
        items = flow_items_from_rows(rows, dates)
    except Exception as exc:  # noqa: BLE001
        logger.warning("portfolio health industry flow failed: %s", exc)
        return out
    ranked = sorted(items, key=lambda x: -(float(x.get("sum5d") or 0.0)))
    total = len(ranked)
    for idx, it in enumerate(ranked):
        name = str(it.get("industryName") or "")
        if not name:
            continue
        out[name] = {
            "industry": name,
            "netInflow5d": round(float(it.get("sum5d") or 0.0) / 1e8, 2),
            "rank5d": idx + 1,
            "total": total,
        }
    return out


def _build_holdings_block(
    market: str,
    day: str,
    alpha_map: dict[str, list[dict[str, Any]]] | None = None,
    flow_map: dict[str, dict[str, Any]] | None = None,
    l1_map: dict[str, str] | None = None,
    regime: str | None = None,
) -> list[dict[str, Any]]:
    """Holdings for one market vs its S-3 exit rules.

    2026-08-10 (HK parallel line): CN uses the CN rules (trail -8); HK uses
    the HK line rules (trail -12). Holdings are split by symbol prefix; the
    CN line also covers A-share ETFs (ETF:XXXXXX — e.g. 513180 Hang Seng
    Tech, a large sleeve) under the same A-share rules.
    OPT-105: CN passes today's regime so Strong sessions use the ATR line.
    """
    from data_sync_service.service.paper_s3 import PYRAMID_TRIGGER_PCT

    trail = -12.0 if market == "HK" else None
    if market == "HK":
        def in_market(sym: str) -> bool:
            return sym.startswith("HK:")
    else:
        def in_market(sym: str) -> bool:
            return sym.startswith(("CN:", "ETF:"))

    holdings: list[dict[str, Any]] = []
    try:
        pyramid_syms = _pyramided_symbols()
        # Prefetch the orthogonal info layers once per block: alpha events
        # (news/catalyst radar) and SW L1 industry fund flow (5-day).
        registry_rows = list_registry()
        hold_syms = [
            str(r.get("symbol") or "").upper()
            for r in registry_rows
            if in_market(str(r.get("symbol") or "").upper())
        ]
        # OPT-100: HK holdings evaluate against the REALTIME price so the
        # health card and the watchlist/copy realtime trigger agree. Peak
        # stays close-based; CN line keeps the close-caliber evaluation.
        realtime_by_ts: dict[str, float] = {}
        if market == "HK" and hold_syms:
            try:
                ts_list = [_resolve_holding_ts(s) for s in hold_syms]
                ts_list = [t for t in ts_list if t]
                if ts_list:
                    quotes = fetch_realtime_quotes(ts_list)
                    for it in quotes.get("items") or []:
                        px = _as_float(it.get("price"))
                        code = str(it.get("ts_code") or "").upper()
                        if px and px > 0 and code:
                            realtime_by_ts[code] = px
            except Exception as exc:  # noqa: BLE001
                logger.warning("portfolio health HK realtime quotes failed: %s", exc)
        if alpha_map is None:
            alpha_map = _alpha_events_for_symbols(hold_syms)
        if l1_map is None:
            l1_map = _l1_industry_for_symbols(hold_syms)
        if flow_map is None:
            flow_map = _industry_flow_map(day)
        for r in registry_rows:
            sym = str(r.get("symbol") or "").upper()
            if not in_market(sym):
                continue
            payload = r.get("payload") or {}
            pct = payload.get("positionPct", r.get("positionPct"))
            cost = payload.get("costPrice", r.get("costPrice"))
            entry = payload.get("entryDate", r.get("entryDate"))
            name = payload.get("name", r.get("name"))
            if not (isinstance(pct, (int, float)) and pct > 0 and cost and entry):
                continue
            ts = _resolve_holding_ts(sym)
            check = _holding_check(
                name=str(name or sym),
                cost=float(cost),
                entry_date=str(entry),
                ts=ts or "",
                trade_date=day,
                trailing_pct=trail,
                realtime_price=realtime_by_ts.get(str(ts or "").upper()),
                regime=regime,
            )
            check["symbol"] = sym
            check["name"] = str(name or "")
            check["positionPct"] = pct
            check["pyramidTriggerLine"] = round(
                float(cost) * (1 + PYRAMID_TRIGGER_PCT / 100.0), 3
            )
            check["pyramidAdded"] = sym in pyramid_syms
            # 2026-08-12: merge the main table's trend-structure exit signal
            # so the health card and the watchlist table never disagree on
            # whether to exit (S-3 price/time rules stay as-is).
            # 2026-08-12 (OPT-097): S-3-only surface — trendok structure
            # signals are fully removed here (backtested: they truncate the
            # trend leg everywhere). Exits come only from S-3 stop/trail/hold.
            if ts is None:
                check["action"] = "HOLD"
                check["note"] = "无法解析标的代码，人工核对"
            # Orthogonal info layers (display only — never gates or exits):
            # alpha events for the symbol; CN SW L1 industry 5-day fund flow.
            check["alphaEvents"] = alpha_map.get(_alpha_sym_key(sym)) or []
            if market == "CN":
                l1 = l1_map.get(sym)
                if l1 and l1 in flow_map:
                    check["industryFlow"] = flow_map[l1]
            # Conditional-order lines that moved vs the last notified baseline
            # (peak climbs -> fixed-price trailing order goes stale; pyramid
            # add -> stop line climbs). Baseline persisted per-symbol so each
            # move alerts once; first sighting only stores the baseline.
            prev_ops = (r.get("conditionalOps") or payload.get("conditionalOps") or {})
            prev_trail = _as_float(prev_ops.get("trail"))
            prev_stop = _as_float(prev_ops.get("stop"))
            ops = _detect_line_ops(
                prev_trail=prev_trail,
                cur_trail=check.get("trailingLine"),
                prev_stop=prev_stop,
                cur_stop=check.get("stopLossLine"),
                max_hold_days=MAX_HOLD_DAYS,
                holding_days=check.get("holdingDays"),
                expire_date=check.get("expireDate"),
            )
            check["lineOps"] = ops
            if prev_trail is None or prev_stop is None or ops:
                try:
                    from data_sync_service.db.watchlist_automation import update_registry_payload

                    update_registry_payload(sym, {
                        "conditionalOps": {
                            "trail": check.get("trailingLine"),
                            "stop": check.get("stopLossLine"),
                        }
                    })
                except Exception as exc:  # noqa: BLE001
                    logger.warning("portfolio health persist line baseline %s failed: %s", sym, exc)
            holdings.append(check)
    except Exception as exc:  # noqa: BLE001
        logger.warning("portfolio health holdings failed: %s", exc)
    return holdings


def _score_data_as_of(*, market: str, day: str) -> str | None:
    """Latest ``watchlist_score_daily`` trade_date for ``market`` (<= ``day``).

    Scores are written by the EOD watchlist automation (17:30) — and since
    2026-08-11 also by the intraday realtime pass (10:30 / 14:00). During
    trading hours before the first intraday run, the latest score date is the
    previous session, which the frontend must distinguish from "no candidates".
    """
    from data_sync_service.db import get_connection

    prefix = f"{market}:"
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT MAX(trade_date) FROM watchlist_score_daily
                    WHERE trade_date <= %s AND symbol LIKE %s
                    """,
                    (day, f"{prefix}%"),
                )
                row = cur.fetchone()
        return str(row[0]) if row and row[0] else None
    except Exception as exc:  # noqa: BLE001
        logger.warning("portfolio health score as-of lookup failed: %s", exc)
        return None


def _health_block(*, market: str, day: str) -> dict[str, Any]:
    """One market's S-3 health block (CN = current live system, HK = parallel line).

    2026-08-10 (no-choice UX): the card only lists the TOP 5 candidates
    (score-desc, deduped) instead of the full list — the caller gets an
    unambiguous "buy these tomorrow" list. ``s3CandidateTotal`` keeps the
    headline count; ``s3Rules.suggestedSizePct`` mirrors the backtest's
    10%-per-sleeve so the manual trade size matches the backtested edge.
    """
    from data_sync_service.service.backtest_engine import BacktestConfig, _load_regime_by_day
    from data_sync_service.service.market_sentiment import get_cn_sentiment, get_panic_cooldown
    from data_sync_service.service.paper_s3 import (
        PYRAMID_ADD_SCALE,
        PYRAMID_TRIGGER_PCT,
        S3_MAX_POSITIONS,
        build_s3_candidates,
    )

    if market == "HK":
        from data_sync_service.service.market_regime import get_hk_regime

        regime = None
        try:
            regime = str(get_hk_regime(as_of_date=day).get("regime") or "")
        except Exception:  # noqa: BLE001
            pass
        sentiment = None
        panic = get_panic_cooldown(days=10, cooldown_days=PANIC_COOLDOWN_DAYS, as_of_date=day)
        candidates: list[dict[str, Any]] = []
        try:
            candidates = build_s3_candidates(trade_date=day, market="HK", max_positions=S3_MAX_POSITIONS)
        except Exception as exc:  # noqa: BLE001
            logger.warning("portfolio health HK candidates failed: %s", exc)
        rules: dict[str, Any] = {
            "entryScore": 65,
            "rsMin": 0.6,
            "stopLossPct": -5.0,
            "trailingStopPct": -12.0,
            "maxHoldDays": 60,
            "pyramidTriggerPct": PYRAMID_TRIGGER_PCT,
            "pyramidAddScale": PYRAMID_ADD_SCALE,
            "gates": "regime",
            "suggestedSizePct": SUGGESTED_SIZE_PCT,
        }
    else:
        cfg = BacktestConfig(
            start_date=day, end_date=day,
            score_threshold=65.0, gates="full", rs_rank_min=0.5,
        )
        regime = None
        try:
            regime = _load_regime_by_day(cfg, [day]).get(day)
        except Exception:  # noqa: BLE001
            pass
        sentiment = None
        panic = None
        candidates: list[dict[str, Any]] = []
        env_scale = 1.0
        try:
            items = get_cn_sentiment(days=1, as_of_date=day)["items"]
            sentiment = items[-1].get("riskMode") if items else None
            panic = get_panic_cooldown(days=10, cooldown_days=PANIC_COOLDOWN_DAYS, as_of_date=day)
            candidates = build_s3_candidates(trade_date=day, max_positions=S3_MAX_POSITIONS)
            # D3 (2026-08-15): today's env sleeve multiplier (uptrend 1.25 /
            # fan 0.75 / else 1.0) — same helper as the live paper intake.
            env_scale = _env_position_scale_for(items)
        except Exception as exc:  # noqa: BLE001
            logger.warning("portfolio health s3 candidates failed: %s", exc)
        rules: dict[str, Any] = {
            "entryScore": 65,
            "rsMin": 0.5,
            "stopLossPct": STOP_LOSS_PCT,
            "trailingStopPct": TRAILING_STOP_PCT,
            "maxHoldDays": MAX_HOLD_DAYS,
            "pyramidTriggerPct": PYRAMID_TRIGGER_PCT,
            "pyramidAddScale": PYRAMID_ADD_SCALE,
            # D3 (2026-08-15): env-aware position sizing (uptrend 1.25x /
            # fan 0.75x) — suggestedSizePct is TODAY's actual sleeve (10% *
            # env scale), so the buy list / buy dialog use the env-scaled
            # number directly; envScaleToday explains the multiplier.
            "suggestedSizePct": round(SUGGESTED_SIZE_PCT * env_scale, 2),
            "envScaleToday": env_scale,
            # TIP-014 (2026-08-14): env-aware rules surfaced for the UI —
            # mirrors the S-3 backtest config.
            "maxHoldEnvShorten": MAX_HOLD_DAYS_ENV_SHORTEN,
            "entryStyle": "auto",
            "neutralBlock": True,
            "panicCooldownDays": PANIC_COOLDOWN_DAYS,
            # D3 (2026-08-15): env-aware position sizing (uptrend 1.25x /
            # fan 0.75x) — mirrors the S-3 backtest env_position_scale (v4).
            "envPositionScale": "uptrend:1.25,fan:0.75",
        }

    try:
        if candidates:
            ts_codes = [c["ts_code"] for c in candidates]
            names = _lookup_stock_basic(ts_codes)[0]
            for c in candidates:
                c["name"] = names.get(c["ts_code"])
    except Exception as exc:  # noqa: BLE001
        logger.warning("portfolio health candidate names failed: %s", exc)

    # Info layers (P2): alpha events + industry fund flow for candidates.
    # Built once here, shared with the holdings block below.
    try:
        cand_syms = [str(c.get("symbol") or "") for c in candidates]
        market_syms = _market_holdings_symbols(market)
        alpha_map = _alpha_events_for_symbols(market_syms + cand_syms)
        flow_map = _industry_flow_map(day)
        l1_map = _l1_industry_for_symbols(market_syms + cand_syms)
        for c in candidates:
            sym = str(c.get("symbol") or "")
            c["alphaEvents"] = alpha_map.get(_alpha_sym_key(sym)) or []
            l1 = l1_map.get(sym)
            if l1 and l1 in flow_map:
                c["industryFlow"] = flow_map[l1]
    except Exception as exc:  # noqa: BLE001
        logger.warning("portfolio health candidate info layers failed: %s", exc)
        alpha_map, flow_map, l1_map = {}, {}, {}

    # 2026-08-10 (no-choice UX): collapse to the top 5 by score — one
    # unambiguous "buy these" list. HK additionally drops candidates whose
    # company is already held on the CN side (e.g. 紫金矿业 601899 held vs
    # 02899 candidate) — one company, one exposure across the pair.
    if market == "HK":
        try:
            cn_held_names = _held_company_names(market="CN", day=day)
            if cn_held_names:
                candidates = [c for c in candidates if str(c.get("name") or "") not in cn_held_names]
        except Exception as exc:  # noqa: BLE001
            logger.warning("portfolio health HK dedupe failed: %s", exc)
    candidate_total = len(candidates)
    candidates = candidates[:TOP_CANDIDATES]

    strength = 0.0
    try:
        from data_sync_service.service.market_regime import regime_strength_score

        strength = float(regime_strength_score(as_of_date=day, market=market)["strength"])
    except Exception as exc:  # noqa: BLE001
        logger.warning("portfolio health strength failed: %s", exc)

    score_as_of = _score_data_as_of(market=market, day=day)
    circuit = False
    if market == "CN":
        try:
            from data_sync_service.service.paper_s3 import _circuit_blocked

            circuit = _circuit_blocked(as_of=day)
        except Exception as exc:  # noqa: BLE001
            logger.warning("portfolio health circuit check failed: %s", exc)

    holdings = _build_holdings_block(
        market=market, day=day, alpha_map=alpha_map, flow_map=flow_map, l1_map=l1_map,
        regime=regime if market == "CN" else None,
    )
    info_summary = {
        "holdingsCount": len(holdings),
        "eventHoldings": sum(1 for h in holdings if h.get("alphaEvents")),
        "industryOutflow": sum(
            1 for h in holdings if h.get("industryFlow", {}).get("netInflow5d", 0) < 0
        ),
        "industryInflow": sum(
            1 for h in holdings if h.get("industryFlow", {}).get("netInflow5d", 0) > 0
        ),
    }
    return {
        "regime": regime,
        "strength": strength,
        "sentiment": sentiment,
        "panicCooldown": panic,
        "circuitBlocked": circuit,
        "scoreDataAsOfDate": score_as_of,
        "scoreFresh": score_as_of == day,
        "s3Candidates": candidates,
        "s3CandidateTotal": candidate_total,
        "s3Rules": rules,
        "holdings": holdings,
        "infoSummary": info_summary,
    }


def build_portfolio_health(
    *,
    trade_date: str | None = None,
    markets: tuple[str, ...] = ("CN",),
) -> dict[str, Any]:
    """Full S-3-aligned health report for the real holdings.

    2026-08-10 (HK parallel line): ``markets`` selects which strategy lines to
    include. Top-level fields stay CN (backward compatible for the decision
    agent); ``hkHealth`` carries the HK line block (null when not requested).
    """
    from data_sync_service.db.paper_trading import today_iso

    day = trade_date or today_iso()
    blocks: dict[str, dict[str, Any]] = {}
    for m in markets:
        if m in ("CN", "HK"):
            blocks[m] = _health_block(market=m, day=day)

    cn = blocks.get("CN") or _health_block(market="CN", day=day)
    return {
        "tradeDate": day,
        **cn,
        "hkHealth": blocks.get("HK"),
    }


def _market_holdings_symbols(market: str) -> list[str]:
    """Registry symbols belonging to one market line (holdings + watchlist)."""
    if market == "HK":
        return [
            str(r.get("symbol") or "").upper()
            for r in list_registry()
            if str(r.get("symbol") or "").upper().startswith("HK:")
        ]
    return [
        str(r.get("symbol") or "").upper()
        for r in list_registry()
        if str(r.get("symbol") or "").upper().startswith(("CN:", "ETF:"))
    ]


def _pyramided_symbols() -> set[str]:
    """Symbols that already have an open S-3 pyramid-add leg."""
    from data_sync_service.db.paper_trading import list_paper_trades

    out: set[str] = set()
    try:
        for r in list_paper_trades(status="open"):
            if r.get("source") == "S3" and "pyramid-add" in str(r.get("whyAtEntry") or ""):
                out.add(str(r.get("symbol") or ""))
    except Exception as exc:  # noqa: BLE001
        logger.warning("portfolio health pyramided lookup failed: %s", exc)
    return out
