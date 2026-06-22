"""TrendOK/Score computation for Watchlist (CN daily only)."""

from __future__ import annotations

import copy
import math
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from data_sync_service.db.daily import fetch_last_ohlcv_batch
from data_sync_service.db.industry_fund_flow import (
    get_dates_upto,
    get_latest_date as get_latest_industry_date,
    get_rows_for_dates,
)
from data_sync_service.service.industry_fund_flow_read import build_trendok_flow_context_from_rows
from data_sync_service.service.trade_calendar_utils import trade_dates_upto
from data_sync_service.db.stoploss import compute_effective_stoploss
from data_sync_service.db.stock_basic import ensure_table as ensure_stock_basic
from data_sync_service.db.stock_eastmoney_industry import lookup_by_ts_codes as lookup_em_industries
from data_sync_service.db.top_inst import fetch_daily_seats, fetch_summaries_for_codes
from data_sync_service.service.market_regime import get_market_regime
from data_sync_service.service.realtime_quote import fetch_realtime_quotes
from data_sync_service.service.top_inst_flow import build_inst_flow_payload

TRENDOK_CACHE_TTL_SECONDS = 60
_trendok_cache: dict[tuple[frozenset[str], bool, str], tuple[list[dict[str, Any]], float]] = {}


def clear_trendok_cache() -> None:
    """Clear in-process TrendOK TTL cache (after bars force sync or tests)."""
    _trendok_cache.clear()


def _trendok_cache_key(symbols: list[str], realtime: bool, latest_bar_date: str | None) -> tuple[frozenset[str], bool, str]:
    return (frozenset(symbols), bool(realtime), str(latest_bar_date or ""))


def _trendok_from_cache(key: tuple[frozenset[str], bool, str]) -> list[dict[str, Any]] | None:
    entry = _trendok_cache.get(key)
    if not entry:
        return None
    rows, expire_at = entry
    if time.time() >= expire_at:
        _trendok_cache.pop(key, None)
        return None
    return copy.deepcopy(rows)


def _ema(values: list[float], period: int) -> list[float]:
    if period <= 0 or not values:
        return []
    alpha = 2.0 / (float(period) + 1.0)
    out: list[float] = []
    prev = values[0]
    out.append(prev)
    for v in values[1:]:
        prev = alpha * v + (1.0 - alpha) * prev
        out.append(prev)
    return out


def _rsi(values: list[float], period: int = 14) -> list[float]:
    if period <= 0 or len(values) < 2:
        return []
    gains: list[float] = [0.0]
    losses: list[float] = [0.0]
    for i in range(1, len(values)):
        chg = values[i] - values[i - 1]
        gains.append(max(0.0, chg))
        losses.append(max(0.0, -chg))
    avg_gain = 0.0
    avg_loss = 0.0
    out: list[float] = [0.0] * len(values)
    for i in range(1, len(values)):
        if i <= period:
            avg_gain = sum(gains[1 : i + 1]) / max(1.0, float(i))
            avg_loss = sum(losses[1 : i + 1]) / max(1.0, float(i))
        else:
            avg_gain = (avg_gain * (period - 1) + gains[i]) / float(period)
            avg_loss = (avg_loss * (period - 1) + losses[i]) / float(period)
        if avg_loss <= 0.0:
            out[i] = 100.0 if avg_gain > 0.0 else 50.0
        else:
            rs = avg_gain / avg_loss
            out[i] = 100.0 - (100.0 / (1.0 + rs))
    return out


def _macd(values: list[float], fast: int = 12, slow: int = 26, signal: int = 9) -> tuple[list[float], list[float], list[float]]:
    if not values:
        return ([], [], [])
    ema_fast = _ema(values, fast)
    ema_slow = _ema(values, slow)
    macd_line = [a - b for a, b in zip(ema_fast, ema_slow, strict=True)]
    signal_line = _ema(macd_line, signal)
    hist = [m - s for m, s in zip(macd_line, signal_line, strict=True)]
    return (macd_line, signal_line, hist)


def _atr14(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float | None:
    if period <= 0:
        return None
    n = min(len(highs), len(lows), len(closes))
    if n < period + 1:
        return None
    tr: list[float] = []
    for i in range(1, n):
        h = highs[i]
        low = lows[i]
        pc = closes[i - 1]
        tr_i = max(h - low, abs(h - pc), abs(low - pc))
        tr.append(tr_i)
    if len(tr) < period:
        return None
    atr = sum(tr[:period]) / float(period)
    for x in tr[period:]:
        atr = (atr * (period - 1) + x) / float(period)
    return atr if math.isfinite(atr) else None


def _parse_float_safe(v: Any) -> float | None:
    try:
        if v is None:
            return None
        n = float(v)
        return n if math.isfinite(n) else None
    except Exception:
        return None


def _clip01(x: float) -> float:
    return 0.0 if x <= 0.0 else 1.0 if x >= 1.0 else x


# V4.0 Watchlist Score weights
_W_EMA = 0.40
_W_MACD = 0.20
_W_BREAK = 0.10
_W_RSI = 0.10
_W_VOL = 0.20


def _score_sub_ema(ema5: float, ema20: float, ema60: float, ema20_prev: float) -> tuple[float, float]:
    s_ema = 0.0
    if ema5 > ema20:
        s_ema += 0.4
    if ema20 > ema60:
        s_ema += 0.4
    if ema20_prev > 0 and (ema20 - ema20_prev) / ema20_prev > 0.001:
        s_ema += 0.2
    return s_ema, 100.0 * _W_EMA * s_ema


def _score_sub_macd(macd_last: float, hist: list[float]) -> tuple[float, float]:
    s_macd = 0.0
    if macd_last >= 0 and len(hist) >= 2:
        h0, h1 = hist[-2], hist[-1]
        if h0 > 0 and h1 > 0 and h1 > h0:
            s_macd = 1.0
    return s_macd, 100.0 * _W_MACD * s_macd


def _score_sub_breakout(close: float, high20_high: float) -> tuple[float, float]:
    ratio_hi = close / high20_high if high20_high > 0 else 0.0
    s_break = _clip01((ratio_hi - 0.85) / 0.10)
    return s_break, 100.0 * _W_BREAK * s_break


def _score_sub_rsi(rsi14: float) -> tuple[float, float]:
    s_rsi = _clip01(1.0 - abs(rsi14 - 65.0) / 15.0)
    if rsi14 > 80.0:
        s_rsi *= _clip01(1.0 - (rsi14 - 80.0) / 10.0)
    return s_rsi, 100.0 * _W_RSI * s_rsi


def _score_sub_volume(ratio_vol: float) -> tuple[float, float]:
    if ratio_vol < 1.0:
        s_vol = ratio_vol
    elif ratio_vol < 1.2:
        s_vol = 0.5 + 0.5 * (ratio_vol - 1.0) / 0.2
    elif ratio_vol <= 2.0:
        s_vol = 1.0
    elif ratio_vol <= 3.0:
        s_vol = 1.0 - (ratio_vol - 2.0) / 1.0
    else:
        s_vol = 0.0
    s_vol = _clip01(s_vol)
    return s_vol, 100.0 * _W_VOL * s_vol


def _score_bonus_ema20_slope_5d(ema20s: list[float]) -> float:
    if len(ema20s) < 6:
        return 0.0
    for i in range(-5, 0):
        if ema20s[i] <= ema20s[i - 1]:
            return 0.0
    return 5.0


def _score_anti_spike_penalties(
    *,
    close: float,
    ema20: float,
    intraday_chg_pct: float | None,
    atr14: float | None,
    vol_today: float,
    avg_vol30: float,
) -> tuple[float, dict[str, float]]:
    penalty = 0.0
    parts: dict[str, float] = {}

    if intraday_chg_pct is not None and intraday_chg_pct > INTRADAY_SURGE_THRESHOLD_PCT:
        p = 20.0
        penalty += p
        parts["penalty_intraday_spike"] = -round(p, 3)

    if atr14 is not None and close > 0:
        atr_ratio = float(atr14) / float(close)
        if atr_ratio > 0.05:
            p = (atr_ratio - 0.05) * 1000.0
            penalty += p
            parts["penalty_volatility_atr"] = -round(p, 3)

    if avg_vol30 > 0 and vol_today / avg_vol30 > 3.0:
        p = 15.0
        penalty += p
        parts["penalty_volume_climax"] = -round(p, 3)

    if close < ema20:
        p = 30.0
        penalty += p
        parts["penalty_below_ema20"] = -round(p, 3)

    return penalty, parts


def _compute_watchlist_score_v4(
    *,
    close: float,
    ema5: float,
    ema20: float,
    ema60: float,
    ema20s: list[float],
    rsi14: float,
    avg5: float,
    avg30: float,
    macd_last: float,
    hist: list[float],
    high20_high: float,
    highs: list[float],
    lows: list[float],
    closes: list[float],
    vols: list[float],
    intraday_chg_pct: float | None,
) -> tuple[float, dict[str, float]]:
    ema20_prev = ema20s[-2] if len(ema20s) >= 2 else 0.0
    _, pts_ema = _score_sub_ema(ema5, ema20, ema60, ema20_prev)
    _, pts_macd = _score_sub_macd(macd_last, hist)
    _, pts_break = _score_sub_breakout(close, high20_high)
    _, pts_rsi = _score_sub_rsi(rsi14)
    ratio_vol = (avg5 / avg30) if avg30 > 0 else (1.0 if avg5 > 0 else 0.0)
    _, pts_vol = _score_sub_volume(ratio_vol)

    parts: dict[str, float] = {
        "ema": round(pts_ema, 3),
        "macd": round(pts_macd, 3),
        "breakout": round(pts_break, 3),
        "rsi": round(pts_rsi, 3),
        "volume": round(pts_vol, 3),
    }

    total = pts_ema + pts_macd + pts_break + pts_rsi + pts_vol

    bonus = _score_bonus_ema20_slope_5d(ema20s)
    if bonus > 0:
        parts["bonus_ema20_slope_5d"] = round(bonus, 3)
        total += bonus

    atr14 = _atr14(highs, lows, closes, 14)
    vol_today = vols[-1] if vols else 0.0
    penalty, pen_parts = _score_anti_spike_penalties(
        close=close,
        ema20=ema20,
        intraday_chg_pct=intraday_chg_pct,
        atr14=atr14,
        vol_today=vol_today,
        avg_vol30=avg30,
    )
    parts.update(pen_parts)
    total -= penalty

    score = max(0.0, min(100.0, total))
    return round(score, 3), parts


def _shanghai_today_iso() -> str:
    return datetime.now(tz=ZoneInfo("Asia/Shanghai")).date().isoformat()


INTRADAY_SURGE_THRESHOLD_PCT = 6.0
_GAP_UP_WEAK_REGIMES = frozenset({"Weak", "Diverging"})


def _compute_day_risk_metrics(
    dates: list[str],
    highs: list[float],
    lows: list[float],
    closes: list[float],
    *,
    today: str,
) -> dict[str, Any]:
    """
    Session change % and gap-up on the latest daily bar.
    Gap-up: latest low > previous high.
    riskMetricsLive is True only when the latest bar is for calendar today (Shanghai).
    """
    if len(closes) < 2 or not dates:
        return {"intradayChgPct": None, "gapUp": None, "riskMetricsLive": False}

    pre_close = closes[-2]
    current = closes[-1]
    intraday: float | None = None
    if pre_close > 0:
        intraday = ((current - pre_close) / pre_close) * 100.0
        if not math.isfinite(intraday):
            intraday = None

    gap_up = bool(lows[-1] > highs[-2])
    live = str(dates[-1]) == str(today)
    return {
        "intradayChgPct": round(intraday, 3) if intraday is not None else None,
        "gapUp": gap_up,
        "riskMetricsLive": live,
    }


def _build_server_risk_alerts(
    *,
    intraday_chg_pct: float | None,
    gap_up: bool | None,
    market_regime: str | None,
    buy_checks: dict[str, Any] | None,
    buy_action: str | None = None,
    risk_metrics_live: bool = False,
    inst_flow: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    alerts: list[dict[str, str]] = []
    checks = buy_checks if isinstance(buy_checks, dict) else {}

    if (
        risk_metrics_live
        and intraday_chg_pct is not None
        and intraday_chg_pct > INTRADAY_SURGE_THRESHOLD_PCT
    ):
        alerts.append(
            {
                "code": "intraday_surge",
                "severity": "block",
                "message": f"Intraday change {intraday_chg_pct:.1f}% exceeds 6.0%; no new positions",
            }
        )
    if gap_up is True and str(market_regime or "").strip() in _GAP_UP_WEAK_REGIMES:
        regime = str(market_regime or "").strip()
        gap_blocked = bool(checks.get("blocked_gap_up_weak_market"))
        severity = "block" if gap_blocked and str(buy_action or "") == "avoid" else "warn"
        alerts.append(
            {
                "code": "gap_up_weak_market",
                "severity": severity,
                "message": f"Gap-up with {regime} market; do not chase highs",
            }
        )
    if (
        risk_metrics_live
        and isinstance(inst_flow, dict)
        and inst_flow.get("onBoard") is True
        and inst_flow.get("lhasaDominant") is True
        and isinstance(inst_flow.get("instNetBuyYi"), (int, float))
        and float(inst_flow["instNetBuyYi"]) < 0
        and intraday_chg_pct is not None
        and intraday_chg_pct > INTRADAY_SURGE_THRESHOLD_PCT
    ):
        yi = float(inst_flow["instNetBuyYi"])
        alerts.append(
            {
                "code": "inst_retail_chase",
                "severity": "block",
                "message": (
                    f"Institutional net sell {yi:.1f}亿 with Lhasa-dominated buying "
                    f"and intraday +{intraday_chg_pct:.1f}%; veto"
                ),
            }
        )
    return alerts


def _apply_inst_flow_risk_buy_blocks(
    res: dict[str, Any],
    *,
    inst_flow: dict[str, Any] | None,
) -> None:
    """Block buy when institutions net-sell while Lhasa retail dominates on a surge day."""
    if bool((res.get("stopLossParts") or {}).get("exit_now")):
        return
    if not bool(res.get("riskMetricsLive")):
        return
    if not isinstance(inst_flow, dict) or inst_flow.get("onBoard") is not True:
        return
    if not inst_flow.get("lhasaDominant"):
        return
    yi = inst_flow.get("instNetBuyYi")
    if not isinstance(yi, (int, float)) or float(yi) >= 0:
        return
    intraday = res.get("intradayChgPct")
    if not isinstance(intraday, (int, float)) or float(intraday) <= INTRADAY_SURGE_THRESHOLD_PCT:
        return
    buy_checks = res.get("buyChecks")
    if not isinstance(buy_checks, dict):
        buy_checks = {}
        res["buyChecks"] = buy_checks
    res["buyAction"] = "avoid"
    res["buyWhy"] = "风险：机构净卖且拉萨主买，禁止追高"
    buy_checks["blocked_inst_retail_chase"] = True


def _apply_intraday_risk_buy_blocks(
    res: dict[str, Any],
    *,
    market_regime: str | None,
) -> None:
    """Override buy recommendation when intraday surge or gap-up in weak/diverging market."""
    if bool((res.get("stopLossParts") or {}).get("exit_now")):
        return
    if not bool(res.get("riskMetricsLive")):
        return

    intraday = res.get("intradayChgPct")
    gap_up = res.get("gapUp")
    buy_checks = res.get("buyChecks")
    if not isinstance(buy_checks, dict):
        buy_checks = {}
        res["buyChecks"] = buy_checks

    if isinstance(intraday, (int, float)) and float(intraday) > INTRADAY_SURGE_THRESHOLD_PCT:
        res["buyAction"] = "avoid"
        res["buyWhy"] = "风险：日内涨幅超过6%，禁止建仓"
        buy_checks["blocked_intraday_surge"] = True
        return

    regime = str(market_regime or "").strip()
    if gap_up is True and regime in _GAP_UP_WEAK_REGIMES:
        buy_mode = str(res.get("buyMode") or "")
        buy_action = str(res.get("buyAction") or "")
        if buy_mode == "B_momentum" or buy_action == "buy":
            res["buyAction"] = "avoid"
            res["buyWhy"] = "风险：跳空缺口+震荡/弱势大盘，禁止追高"
            buy_checks["blocked_gap_up_weak_market"] = True
        else:
            buy_checks["blocked_gap_up_weak_market"] = True
            if buy_action != "avoid":
                prev_why = str(res.get("buyWhy") or "").strip()
                gap_msg = "风险：跳空缺口+震荡/弱势大盘，禁止追高"
                res["buyWhy"] = gap_msg if not prev_why else f"{prev_why}；{gap_msg}"


def _quote_trade_date(q: dict[str, Any]) -> str | None:
    tt = str(q.get("trade_time") or "").strip()
    if not tt:
        return None
    if len(tt) >= 10 and tt[4] == "-" and tt[7] == "-":
        return tt[:10]
    if len(tt) >= 8 and tt[:8].isdigit():
        return f"{tt[:4]}-{tt[4:6]}-{tt[6:8]}"
    return None


def _pick_str(value: Any, fallback: str) -> str:
    if value is None:
        return fallback
    s = str(value).strip()
    return s if s else fallback


def _merge_realtime_bar(
    bars: list[tuple[str, str, str, str, str, str]],
    quote: dict[str, Any],
) -> list[tuple[str, str, str, str, str, str]]:
    if not bars:
        return bars
    price = _parse_float_safe(quote.get("price"))
    if price is None:
        return bars
    date = _quote_trade_date(quote) or _shanghai_today_iso()
    last = bars[-1]
    last_date = str(last[0])
    if date < last_date:
        return bars

    close_s = _pick_str(quote.get("price"), str(last[4]) if date == last_date else str(price))
    open_s = _pick_str(quote.get("open"), str(last[1]) if date == last_date else close_s)
    high_s = _pick_str(quote.get("high"), str(last[2]) if date == last_date else close_s)
    low_s = _pick_str(quote.get("low"), str(last[3]) if date == last_date else close_s)
    vol_s = _pick_str(quote.get("volume"), str(last[5]) if date == last_date else "0")
    next_bar = (date, open_s, high_s, low_s, close_s, vol_s)

    if date == last_date:
        return [*bars[:-1], next_bar]
    return [*bars, next_bar]


def _symbol_to_ts_code(symbol: str) -> tuple[str, str, str] | None:
    """
    Map UI symbol to (market, ticker, ts_code).
    Currently only supports CN.
    """
    s = (symbol or "").strip().upper()
    if not s:
        return None
    if s.startswith("CN:"):
        ticker = s.split(":", 1)[1].strip()
        if len(ticker) == 6 and ticker.isdigit():
            suffix = "SH" if ticker.startswith("6") else "SZ"
            return "CN", ticker, f"{ticker}.{suffix}"
        return None
    return None


def _lookup_stock_basic(ts_codes: list[str]) -> tuple[dict[str, str], dict[str, str]]:
    """
    Best-effort name + Tushare industry lookup from stock_basic (single query).
    """
    ensure_stock_basic()
    if not ts_codes:
        return {}, {}
    try:
        from data_sync_service.db import get_connection

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ts_code, name, industry FROM stock_basic WHERE ts_code = ANY(%s)",
                    (ts_codes,),
                )
                rows = cur.fetchall()
        by_name: dict[str, str] = {}
        by_industry: dict[str, str] = {}
        for r in rows:
            if not r or not r[0]:
                continue
            code = str(r[0])
            if r[1]:
                by_name[code] = str(r[1])
            if len(r) > 2 and r[2]:
                by_industry[code] = str(r[2])
        return by_name, by_industry
    except Exception:
        return {}, {}


def _lookup_em_industry_boards(ts_codes: list[str]) -> dict[str, str]:
    """East Money industry board name (ts_code -> industry_name)."""
    if not ts_codes:
        return {}
    try:
        return lookup_em_industries(ts_codes)
    except Exception:
        return {}


def _pick_flow_as_of_date(as_of_date: str | None) -> str | None:
    latest = get_latest_industry_date()
    if latest and as_of_date:
        return latest if latest <= as_of_date else as_of_date
    return latest or as_of_date


def _build_industry_flow_context(as_of_date: str | None) -> dict[str, Any]:
    """
    Build industry flow context for scoring adjustments.
    """
    flow_date = _pick_flow_as_of_date(as_of_date)
    if not flow_date:
        return {"asOfDate": None, "ok": False}

    dates_5 = trade_dates_upto(flow_date, 5, fallback_dates_fn=get_dates_upto)
    if not dates_5:
        return {"asOfDate": flow_date, "ok": False}

    rows = get_rows_for_dates(dates_5)
    return build_trendok_flow_context_from_rows(flow_date=flow_date, dates_5=dates_5, rows=rows)


def _industry_flow_score_adjustment(industry: str, ctx: dict[str, Any]) -> tuple[float, dict[str, float], list[str]]:
    """
    Compute industry-flow-based score adjustments.
    """
    if not industry or not ctx.get("ok"):
        return 0.0, {}, []

    large_outflow = -1.0e8
    delta = 0.0
    parts: dict[str, float] = {}
    reasons: list[str] = []

    top_today_3 = ctx.get("top_today_3") or set()
    top_today_5 = ctx.get("top_today_5") or set()
    top_yesterday_3 = ctx.get("top_yesterday_3") or set()
    top_5d_3 = ctx.get("top_5d_3") or set()
    bottom_5d_5 = ctx.get("bottom_5d_5") or set()
    net_today = ctx.get("net_today") or {}
    net_yesterday = ctx.get("net_yesterday") or {}

    # 5D flow ranking
    if industry in top_5d_3:
        delta += 10.0
        parts["industry_flow_5d_top3"] = 10.0
        reasons.append("industry_flow_5d_top3")
    if industry in bottom_5d_5:
        delta -= 20.0
        parts["industry_flow_5d_bottom5"] = -20.0
        reasons.append("industry_flow_5d_bottom5")

    # Today's hotspots (top inflow)
    if industry in top_today_3:
        delta += 5.0
        parts["hotspots_today_top3"] = 5.0
        reasons.append("hotspots_today_top3")
    elif industry in top_today_5:
        delta += 3.0
        parts["hotspots_today_top4_5"] = 3.0
        reasons.append("hotspots_today_top4_5")

    today_inflow = float(net_today.get(industry) or 0.0)
    yesterday_inflow = float(net_yesterday.get(industry) or 0.0)
    in_hot_today = industry in top_today_5

    # Yesterday top3, today falls out of top5 and has large negative inflow
    if industry in top_yesterday_3 and not in_hot_today and today_inflow <= large_outflow:
        delta -= 15.0
        parts["hotspot_falloff_big_outflow"] = -15.0
        reasons.append("hotspot_falloff_big_outflow")

    # Not in hotspots and 2-day large outflow
    if not in_hot_today and today_inflow <= large_outflow and yesterday_inflow <= large_outflow:
        delta -= 10.0
        parts["hotspot_absent_2d_big_outflow"] = -10.0
        reasons.append("hotspot_absent_2d_big_outflow")

    return delta, parts, reasons


def compute_trendok_for_symbols(
    symbols: list[str],
    realtime: bool = False,
) -> list[dict[str, Any]]:
    """
    Compute TrendOK for up to 200 symbols using DB-cached daily bars.
    Data freshness depends on scheduled close sync or manual `/bars?force=true` refresh.
    `realtime` enables best-effort quote merge for the latest bar during trading hours.
    """
    syms0 = [str(s or "").strip().upper() for s in (symbols or [])]
    syms = [s for s in syms0 if s]
    if not syms:
        return []
    if len(syms) > 200:
        syms = syms[:200]

    parsed: dict[str, tuple[str, str, str]] = {}
    ts_codes: list[str] = []
    for s in syms:
        m = _symbol_to_ts_code(s)
        if m:
            parsed[s] = m
            ts_codes.append(m[2])

    bars_by_code = fetch_last_ohlcv_batch(ts_codes, days=120)
    if realtime and ts_codes:
        q = fetch_realtime_quotes(ts_codes)
        items = q.get("items") if isinstance(q, dict) else None
        if q.get("ok") and isinstance(items, list):
            by_code = {str(x.get("ts_code")): x for x in items if x and x.get("ts_code")}
            for code, bars in list(bars_by_code.items()):
                qt = by_code.get(code)
                if qt:
                    bars_by_code[code] = _merge_realtime_bar(bars, qt)

    latest_bar_date: str | None = None
    for bars in bars_by_code.values():
        if not bars:
            continue
        d = str(bars[-1][0])
        if not latest_bar_date or d > latest_bar_date:
            latest_bar_date = d

    cache_key = _trendok_cache_key(syms, realtime, latest_bar_date)
    cached = _trendok_from_cache(cache_key)
    if cached is not None:
        return cached

    by_name, by_tushare_industry = _lookup_stock_basic(ts_codes)
    by_em_industry = _lookup_em_industry_boards(ts_codes)
    inst_by_code = fetch_summaries_for_codes(
        ts_codes,
        trade_date=latest_bar_date if latest_bar_date else None,
    )
    out: list[dict[str, Any]] = []
    flow_ctx = _build_industry_flow_context(latest_bar_date)
    market_regime: str | None = None
    try:
        regime_info = get_market_regime(as_of_date=latest_bar_date, include_breadth=False)
        market_regime = str(regime_info.get("regime") or "Unknown")
    except Exception:
        market_regime = "Unknown"
    for sym in syms:
        market_ticker_ts = parsed.get(sym)
        if not market_ticker_ts:
            out.append({"symbol": sym, "missingData": ["unsupported_market"]})
            continue
        _, ticker, ts_code = market_ticker_ts
        name = by_name.get(ts_code)
        tushare_industry = by_tushare_industry.get(ts_code)
        em_industry = by_em_industry.get(ts_code)
        industry_for_flow = em_industry or tushare_industry
        bars = bars_by_code.get(ts_code, [])
        out.append(
            _trendok_one(
                symbol=sym,
                name=name,
                industry=industry_for_flow,
                tushare_industry=tushare_industry,
                em_industry=em_industry,
                bars=bars,
                flow_ctx=flow_ctx,
                market_regime=market_regime,
                inst_summary=inst_by_code.get(ts_code),
            )
        )
    _trendok_cache[cache_key] = (copy.deepcopy(out), time.time() + TRENDOK_CACHE_TTL_SECONDS)
    return out


def _trendok_one(
    *,
    symbol: str,
    name: str | None,
    industry: str | None,
    tushare_industry: str | None = None,
    em_industry: str | None = None,
    bars: list[tuple[str, str, str, str, str, str]],
    flow_ctx: dict[str, Any] | None = None,
    market_regime: str | None = None,
    inst_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Ported from quant-service `_market_stock_trendok_one` with the same checks/score behavior.
    bars: list of (date, open, high, low, close, volume) ordered by date ASC.
    """
    res: dict[str, Any] = {
        "symbol": symbol,
        "name": name,
        "asOfDate": None,
        "trendOk": None,
        "score": None,
        "scoreParts": {},
        "stopLossPrice": None,
        "stopLossParts": {},
        "buyMode": None,
        "buyAction": None,
        "buyZoneLow": None,
        "buyZoneHigh": None,
        "buyRefPrice": None,
        "buyWhy": None,
        "buyChecks": {},
        "marketRegime": market_regime,
        "intradayChgPct": None,
        "gapUp": None,
        "riskMetricsLive": False,
        "riskAlerts": [],
        "instFlow": None,
        "checks": {},
        "values": {},
        "missingData": [],
    }

    if not symbol.startswith("CN:"):
        res["missingData"].append("unsupported_market")
        return res

    closes: list[float] = []
    vols: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    opens: list[float] = []
    dates: list[str] = []

    # NOTE: Use explicit variable names to avoid mypy confusion with later locals.
    for d, open_s, high_s, low_s, close_s, vol_s in bars:
        c2 = _parse_float_safe(close_s)
        v2 = _parse_float_safe(vol_s)
        h2 = _parse_float_safe(high_s)
        l2 = _parse_float_safe(low_s)
        o2 = _parse_float_safe(open_s)
        if c2 is None:
            continue
        closes.append(c2)
        vols.append(v2 if v2 is not None else 0.0)
        highs.append(h2 if h2 is not None else c2)
        lows.append(l2 if l2 is not None else c2)
        opens.append(o2 if o2 is not None else c2)
        dates.append(str(d))

    if not closes:
        res["missingData"].append("no_bars")
        return res

    res["asOfDate"] = dates[-1]
    res["values"]["close"] = closes[-1]
    if tushare_industry:
        res["values"]["industry"] = tushare_industry
    if em_industry:
        res["values"]["emIndustry"] = em_industry
    elif industry and not tushare_industry:
        res["values"]["industry"] = industry

    if len(closes) < 60:
        res["missingData"].append("bars_lt_60")

    day_risk = _compute_day_risk_metrics(dates, highs, lows, closes, today=_shanghai_today_iso())
    res["intradayChgPct"] = day_risk.get("intradayChgPct")
    res["gapUp"] = day_risk.get("gapUp")
    res["riskMetricsLive"] = bool(day_risk.get("riskMetricsLive"))

    # Checks + values
    ema5s = _ema(closes, 5)
    ema20s = _ema(closes, 20)
    ema60s = _ema(closes, 60)
    if ema5s and ema20s and ema60s:
        res["values"]["ema5"] = ema5s[-1]
        res["values"]["ema20"] = ema20s[-1]
        res["values"]["ema60"] = ema60s[-1]
        # Rule 1 (optimized): allow EMA5 short-term noise.
        # TrendOK requires close above EMA20 and EMA20 above EMA60.
        res["checks"]["emaOrder"] = bool(closes[-1] > ema20s[-1] and ema20s[-1] > ema60s[-1])

    macd_line, sig_line, hist = _macd(closes, 12, 26, 9)
    if macd_line and sig_line and hist:
        res["values"]["macd"] = macd_line[-1]
        res["values"]["macdSignal"] = sig_line[-1]
        res["values"]["macdHist"] = hist[-1]
        res["checks"]["macdPositive"] = bool(macd_line[-1] > 0.0)
        # Rule 3 (optimized): only require histogram above zero axis.
        # "Expanding" is handled by the Score system as a soft signal.
        res["checks"]["macdHistExpanding"] = bool(hist[-1] > 0.0)
        if len(hist) >= 4:
            h4 = hist[-4:]
            res["values"]["macdHist4"] = [float(x) for x in h4]

    rsi14s = _rsi(closes, 14)
    if rsi14s:
        res["values"]["rsi14"] = rsi14s[-1]
        # Rule 5 (optimized): allow strong trend RSI up to 90.
        res["checks"]["rsiInRange"] = bool(50.0 <= rsi14s[-1] <= 90.0)

    if len(closes) >= 20:
        high20 = max(closes[-20:])
        res["values"]["high20"] = high20
        res["checks"]["closeNear20dHigh"] = bool(closes[-1] >= 0.90 * high20)

    if len(vols) >= 30:
        avg5 = sum(vols[-5:]) / 5.0
        avg30 = sum(vols[-30:]) / 30.0
        res["values"]["avgVol5"] = avg5
        res["values"]["avgVol30"] = avg30
        # Rule 6 (optimized): avoid filtering strong "tight volume" trends.
        # Volume "surge" is moved to the Score system; TrendOK only blocks volume cliffs.
        res["checks"]["volumeSurge"] = bool(avg5 > 0.9 * avg30) if avg30 > 0 else bool(avg5 > 0)

    # Score V4.0: trend continuity + anti-spike penalties
    try:
        v = res["values"]
        if (
            v.get("close") is None
            or v.get("ema5") is None
            or v.get("ema20") is None
            or v.get("ema60") is None
            or v.get("high20") is None
            or v.get("rsi14") is None
            or v.get("avgVol5") is None
            or v.get("avgVol30") is None
            or v.get("macd") is None
            or v.get("macdHist") is None
            or not ema20s
            or len(hist) < 2
        ):
            res["score"] = None
        else:
            close = float(v["close"])
            high20_high = max(highs[-20:]) if len(highs) >= 20 else float(v["high20"])
            intraday_raw = res.get("intradayChgPct")
            intraday_chg_pct = float(intraday_raw) if isinstance(intraday_raw, (int, float)) else None

            score, parts = _compute_watchlist_score_v4(
                close=close,
                ema5=float(v["ema5"]),
                ema20=float(v["ema20"]),
                ema60=float(v["ema60"]),
                ema20s=ema20s,
                rsi14=float(v["rsi14"]),
                avg5=float(v["avgVol5"]),
                avg30=float(v["avgVol30"]),
                macd_last=float(v["macd"]),
                hist=hist,
                high20_high=high20_high,
                highs=highs,
                lows=lows,
                closes=closes,
                vols=vols,
                intraday_chg_pct=intraday_chg_pct,
            )
            res["score"] = score
            res["scoreParts"] = parts
            if industry and flow_ctx:
                delta, flow_parts, flow_reasons = _industry_flow_score_adjustment(industry, flow_ctx)
                checks = res.get("checks") if isinstance(res.get("checks"), dict) else {}
                positive_bonus_allowed = all(
                    bool(checks.get(k))
                    for k in (
                        "emaOrder",
                        "macdPositive",
                        "macdHistExpanding",
                        "closeNear20dHigh",
                        "rsiInRange",
                        "volumeSurge",
                    )
                )
                effective_flow_parts = {
                    k: v
                    for k, v in flow_parts.items()
                    if float(v) < 0.0 or positive_bonus_allowed
                }
                if flow_parts:
                    if effective_flow_parts:
                        res["scoreParts"].update(effective_flow_parts)
                    res["values"]["industryFlowAsOfDate"] = flow_ctx.get("asOfDate")
                    res["values"]["industryFlowReasons"] = flow_reasons
                if delta != 0.0 and res.get("score") is not None:
                    negative_delta = min(delta, 0.0)
                    positive_delta = max(delta, 0.0)
                    allowed_positive_delta = positive_delta if positive_bonus_allowed else 0.0
                    effective_delta = negative_delta + allowed_positive_delta
                    if effective_delta != 0.0:
                        res["score"] = round(max(0.0, min(100.0, float(res["score"]) + effective_delta)), 3)
            buy_seats: list[dict[str, Any]] | None = None
            if inst_summary and inst_summary.get("on_board"):
                td = str(inst_summary.get("trade_date") or "")
                ts_tuple = _symbol_to_ts_code(symbol)
                if td and ts_tuple:
                    buy_seats = fetch_daily_seats(ts_tuple[2], td)
            inst_flow = build_inst_flow_payload(inst_summary, buy_seats=buy_seats)
            res["instFlow"] = inst_flow
            if inst_flow.get("synced") is False:
                res.setdefault("missingData", []).append("instFlow")
            if (
                inst_flow
                and inst_flow.get("lhasaDominant")
                and isinstance(inst_flow.get("instNetBuyYi"), (int, float))
                and float(inst_flow["instNetBuyYi"]) < 0
                and isinstance(intraday_chg_pct, (int, float))
                and float(intraday_chg_pct) > INTRADAY_SURGE_THRESHOLD_PCT
                and res.get("score") is not None
            ):
                res["score"] = round(min(float(res["score"]), 60.0), 3)
                parts2 = res.get("scoreParts")
                if isinstance(parts2, dict):
                    parts2["inst_retail_chase_cap"] = 60.0
    except Exception:
        res["score"] = None

    # ---------- StopLoss (CN daily), formula-based (ported; chips support omitted) ----------
    # stop_loss = max(final_support - atr_k*ATR14, current*(1-max_loss_pct))
    try:
        stop_parts: dict[str, Any] = {}
        current = float(closes[-1])
        stop_parts["current_price"] = round(current, 6)

        if not lows or res["values"].get("ema20") is None:
            res["stopLossPrice"] = None
            res["missingData"].append("stoploss_missing_inputs")
        else:
            swing_low = min(lows[-10:]) if len(lows) >= 10 else min(lows)
            if len(lows) >= 20:
                # Exclude last 5 days if possible
                platform_slice = lows[-20:-5] if len(lows) >= 25 else lows[: max(0, len(lows) - 5)]
                platform_low = min(platform_slice) if platform_slice else swing_low
            else:
                platform_low = min(lows[: max(0, len(lows) - 5)]) if len(lows) > 5 else swing_low

            ema20 = float(res["values"]["ema20"])
            structural_support = max(swing_low, platform_low, ema20)
            stop_parts["swing_low_10d"] = round(swing_low, 6)
            stop_parts["platform_low_20d_excl_5d"] = round(platform_low, 6)
            stop_parts["ema20"] = round(ema20, 6)
            stop_parts["structural_support"] = round(structural_support, 6)

            final_support = structural_support
            stop_parts["final_support"] = round(final_support, 6)

            # Exit-now overrides:
            # 1) Trend structure break: EMA5 < EMA20 OR close < EMA20 => exit immediately (stop = current)
            exit_now = False
            exit_reasons: list[str] = []
            exit_check_ema5_lt_ema20 = False
            exit_check_close_lt_ema20 = False
            exit_check_mom_exhaust = False
            exit_check_vol_dry = False
            if res["values"].get("ema5") is not None and res["values"].get("ema20") is not None:
                if float(res["values"]["ema5"]) < float(res["values"]["ema20"]):
                    exit_now = True
                    exit_check_ema5_lt_ema20 = True
                    exit_reasons.append("trend_structure_break:ema5_below_ema20")
            if res["values"].get("ema20") is not None and current < float(res["values"]["ema20"]):
                exit_now = True
                exit_check_close_lt_ema20 = True
                exit_reasons.append("trend_structure_break:close_below_ema20")

            # 2) Momentum exhaustion: MACD hist shrinks 3 days then turns negative + volume dries up
            # Warning case: hist shrinks but stays positive => suggest reducing half.
            warn_reduce_half = False
            warn_reasons: list[str] = []
            if res["values"].get("avgVol5") is not None and res["values"].get("avgVol30") is not None:
                avg5v = float(res["values"]["avgVol5"])
                avg30v = float(res["values"]["avgVol30"])
                if len(hist) >= 4:
                    hist4 = [float(x) for x in hist[-4:]]
                    shrink_then_flip = (hist4[0] > hist4[1] > hist4[2] > 0.0) and (hist4[3] < 0.0)
                    vol_dry = avg30v > 0.0 and (avg5v < avg30v)
                    exit_check_vol_dry = bool(vol_dry)
                    if shrink_then_flip and vol_dry:
                        exit_now = True
                        exit_check_mom_exhaust = True
                        exit_reasons.append("momentum_exhaustion:hist_shrink3_flip_negative_and_volume_dry")

                    if not shrink_then_flip:
                        shrink_cnt = 0
                        if hist4[1] < hist4[0]:
                            shrink_cnt += 1
                        if hist4[2] < hist4[1]:
                            shrink_cnt += 1
                        if hist4[3] < hist4[2]:
                            shrink_cnt += 1
                        stop_parts["warn_hist4"] = [round(x, 6) for x in hist4]
                        stop_parts["warn_hist_shrink_cnt_3"] = shrink_cnt
                        if avg30v > 0:
                            stop_parts["warn_vol_ratio_5_30"] = round(avg5v / avg30v, 6)
                        if hist4[3] > 0.0 and shrink_cnt >= 2:
                            warn_reduce_half = True
                            warn_reasons.append(
                                "momentum_warning:hist_shrinking_and_volume_dry" if vol_dry else "momentum_warning:hist_shrinking"
                            )
            else:
                # If volume averages are unavailable, still warn based on MACD histogram shrinking (best-effort).
                if len(hist) >= 4:
                    hist4 = [float(x) for x in hist[-4:]]
                    shrink_cnt = 0
                    if hist4[1] < hist4[0]:
                        shrink_cnt += 1
                    if hist4[2] < hist4[1]:
                        shrink_cnt += 1
                    if hist4[3] < hist4[2]:
                        shrink_cnt += 1
                    stop_parts["warn_hist4"] = [round(x, 6) for x in hist4]
                    stop_parts["warn_hist_shrink_cnt_3"] = shrink_cnt
                    stop_parts["warn_vol_ratio_5_30"] = None
                    if hist4[3] > 0.0 and shrink_cnt >= 2:
                        warn_reduce_half = True
                        warn_reasons.append("momentum_warning:hist_shrinking_volume_unknown")

            stop_parts["exit_now"] = bool(exit_now)
            stop_parts["exit_reasons"] = exit_reasons
            stop_parts["exit_check_ema5_lt_ema20"] = bool(exit_check_ema5_lt_ema20)
            stop_parts["exit_check_close_lt_ema20"] = bool(exit_check_close_lt_ema20)
            stop_parts["exit_check_momentum_exhaustion"] = bool(exit_check_mom_exhaust)
            stop_parts["exit_check_volume_dry"] = bool(exit_check_vol_dry)
            stop_parts["warn_reduce_half"] = bool(warn_reduce_half)
            stop_parts["warn_reasons"] = warn_reasons
            if warn_reduce_half:
                stop_parts["warn_display"] = "警告：MACD柱缩小但未转负，建议至少卖出一半"

            if exit_now:
                # Immediate exit: stop at current price.
                computed_stop = round(current, 6)
                ts_code = _symbol_to_ts_code(symbol)
                if ts_code:
                    effective_stop, used_stored = compute_effective_stoploss(
                        ts_code[2], computed_stop, res.get("asOfDate")
                    )
                    res["stopLossPrice"] = effective_stop
                    stop_parts["final_stop_loss"] = effective_stop
                    stop_parts["computed_stop_loss"] = computed_stop
                    stop_parts["used_stored_higher"] = used_stored
                else:
                    res["stopLossPrice"] = computed_stop
                    stop_parts["final_stop_loss"] = computed_stop
                stop_parts["exit_display"] = "立刻离场"
                res["stopLossParts"] = stop_parts
            else:
                # Volatility bin: std(returns[-20:])
                vol_std20: float | None = None
                if len(closes) >= 21:
                    rets_sl: list[float] = []
                    for i in range(-20, 0):
                        c0 = closes[i - 1]
                        c1 = closes[i]
                        if c0 > 0:
                            rets_sl.append((c1 / c0) - 1.0)
                    if len(rets_sl) >= 10:
                        mu = sum(rets_sl) / float(len(rets_sl))
                        var = sum((r - mu) ** 2 for r in rets_sl) / float(len(rets_sl))
                        vol_std20 = math.sqrt(max(0.0, var))
                stop_parts["vol_std20"] = round(vol_std20, 6) if vol_std20 is not None else None

                if vol_std20 is None:
                    atr_k = 1.2
                    max_loss_pct = 0.08
                    vol_bin = "unknown"
                elif vol_std20 <= 0.02:
                    atr_k = 1.1
                    max_loss_pct = 0.06
                    vol_bin = "low"
                elif vol_std20 <= 0.04:
                    atr_k = 1.2
                    max_loss_pct = 0.08
                    vol_bin = "mid"
                else:
                    atr_k = 1.4
                    max_loss_pct = 0.10
                    vol_bin = "high"
                stop_parts["vol_bin"] = vol_bin
                stop_parts["atr_k"] = atr_k
                stop_parts["max_loss_pct"] = max_loss_pct

                atr14 = _atr14(highs, lows, closes, 14)
                if atr14 is None:
                    res["stopLossPrice"] = None
                    res["missingData"].append("atr14_unavailable")
                else:
                    buffer = atr_k * atr14
                    hard_stop = current * (1.0 - max_loss_pct)
                    stop_loss_support = final_support - buffer
                    final_stop = max(stop_loss_support, hard_stop)
                    final_stop = min(final_stop, current)  # never above current
                    computed_stop = round(final_stop, 6)
                    stop_parts["atr14"] = round(atr14, 6)
                    stop_parts["buffer"] = round(buffer, 6)
                    stop_parts["hard_stop"] = round(hard_stop, 6)
                    stop_parts["stop_loss_support_minus_buffer"] = round(stop_loss_support, 6)
                    stop_parts["computed_stop_loss"] = computed_stop
                    ts_code_tuple = _symbol_to_ts_code(symbol)
                    if ts_code_tuple:
                        effective_stop, used_stored = compute_effective_stoploss(
                            ts_code_tuple[2], computed_stop, res.get("asOfDate")
                        )
                        stop_parts["final_stop_loss"] = effective_stop
                        stop_parts["used_stored_higher"] = used_stored
                        res["stopLossPrice"] = effective_stop
                    else:
                        stop_parts["final_stop_loss"] = computed_stop
                        res["stopLossPrice"] = computed_stop
                    res["stopLossParts"] = stop_parts
    except Exception:
        res["stopLossPrice"] = None

    # ---------- Buy (CN daily), deterministic (ported) ----------
    # Unified two-mode right-side system:
    # - Mode A: breakout + pullback
    # - Mode B: momentum new-high
    try:
        buy_checks: dict[str, Any] = {}
        buy_mode: str = "none"
        buy_action: str = "wait"
        buy_zone_low: float | None = None
        buy_zone_high: float | None = None
        buy_why: str | None = None

        if bool((res.get("stopLossParts") or {}).get("exit_now")):
            buy_mode = "none"
            buy_action = "avoid"
            buy_why = "风险：立刻离场信号触发，禁止买入"
        else:
            n = len(closes)
            if n >= 26 and len(opens) == n and len(highs) == n and len(lows) == n and len(vols) == n:
                close = closes[-1]
                vol = vols[-1]
                vol_prev = vols[-2] if n >= 2 else vol

                vol_sma20 = (sum(vols[-21:-1]) / 20.0) if n >= 21 else None
                buy_checks["vol_sma20"] = round(vol_sma20, 6) if vol_sma20 is not None else None

                ema20_rising = False
                if ema20s and len(ema20s) >= 2:
                    ema20_rising = bool(ema20s[-1] > ema20s[-2])
                macd_hist_now = float(hist[-1]) if hist else 0.0
                in_trend = bool(
                    res["values"].get("ema20") is not None
                    and close > float(res["values"]["ema20"])
                    and ema20_rising
                    and macd_hist_now > 0.0
                )
                allow_mode_b = str(market_regime or "").strip() == "Strong"
                buy_checks["mode_b_allowed"] = allow_mode_b
                if in_trend and not allow_mode_b:
                    buy_checks["mode_b_blocked"] = True
                    in_trend = False
                buy_checks["in_trend"] = in_trend
                buy_checks["ema20_rising"] = ema20_rising
                buy_checks["macd_hist_now"] = round(macd_hist_now, 6)

                if in_trend:
                    buy_mode = "B_momentum"
                    prev10_high = max(highs[-11:-1]) if n >= 11 else max(highs[:-1])
                    new_high = bool(close > prev10_high)
                    vol_ok = bool(vol_sma20 is not None and vol > vol_sma20 * 1.2)
                    macd_inc = bool(len(hist) >= 2 and float(hist[-1]) > float(hist[-2]))
                    rsi_ok = bool(res["values"].get("rsi14") is not None and float(res["values"]["rsi14"]) < 80.0)
                    buy_checks["b_prev10_high"] = round(prev10_high, 6)
                    buy_checks["b_new_high"] = new_high
                    buy_checks["b_vol_ok"] = vol_ok
                    buy_checks["b_macd_inc"] = macd_inc
                    buy_checks["b_rsi_ok"] = rsi_ok

                    buy_zone_low = float(prev10_high)
                    buy_zone_high = float(prev10_high) * 1.02
                    if new_high and vol_ok and macd_inc and rsi_ok:
                        buy_action = "buy"
                        buy_why = "模式B：趋势中创10日新高，放量且动能增强"
                    else:
                        buy_action = "wait"
                        buy_why = "模式B：趋势中，等待新高+放量/动能确认"
                else:
                    buy_mode = "A_pullback"
                    breakout_idx: int | None = None
                    breakout_level: float | None = None
                    # Search last 1..5 days for breakout day (exclude today)
                    for k in range(1, min(6, n)):
                        di = n - 1 - k
                        if di < 21:
                            continue
                        level = max(highs[di - 20 : di])
                        vol_ma = sum(vols[di - 20 : di]) / 20.0
                        is_breakout = bool(closes[di] > level and vols[di] > vol_ma * 1.2)
                        if is_breakout:
                            breakout_idx = di
                            breakout_level = level
                            break
                    in_pullback_window = breakout_idx is not None
                    buy_checks["a_in_pullback_window"] = in_pullback_window
                    buy_checks["a_breakout_idx"] = breakout_idx
                    buy_checks["a_breakout_level"] = round(breakout_level, 6) if breakout_level is not None else None

                    ema20_now = float(res["values"]["ema20"]) if res["values"].get("ema20") is not None else None
                    low10 = min(lows[-10:]) if n >= 10 else min(lows)
                    support = max(low10, ema20_now) if ema20_now is not None else low10
                    buy_checks["a_support"] = round(support, 6)

                    if breakout_level is not None and ema20_now is not None:
                        pullback_signal = (
                            (lows[-1] <= breakout_level * 1.01)
                            and (close >= support * 0.99)
                            and (vol < vol_prev)
                            and (closes[-1] > opens[-1])
                        )
                        buy_checks["a_pullback_signal"] = bool(pullback_signal)
                        buy_zone_low = max(support * 0.99, breakout_level * 0.99)
                        buy_zone_high = breakout_level * 1.01
                        if in_pullback_window and pullback_signal:
                            buy_action = "buy"
                            buy_why = "模式A：突破后回踩到支撑区，缩量止跌"
                        elif in_pullback_window:
                            buy_action = "wait"
                            buy_why = "模式A：回踩窗口内，等待缩量止跌"
                        else:
                            buy_action = "wait"
                            buy_why = "模式A：未在回踩窗口"
                    else:
                        if breakout_level is None:
                            buy_checks["a_breakout_missing"] = True
                            buy_action = "wait"
                            buy_why = "模式A：未找到近5日突破日"
                        elif ema20_now is None:
                            buy_checks["a_ema20_missing"] = True
                            buy_action = "wait"
                            buy_why = "模式A：EMA20 数据不足"
                        else:
                            buy_action = "wait"
                            buy_why = "模式A：数据不足（需要≥20日平台/EMA）"
            else:
                buy_mode = "none"
                buy_action = "wait"
                buy_why = "数据不足（需要至少26日K线）"

        res["buyMode"] = buy_mode
        res["buyAction"] = buy_action
        res["buyZoneLow"] = round(buy_zone_low, 6) if buy_zone_low is not None else None
        res["buyZoneHigh"] = round(buy_zone_high, 6) if buy_zone_high is not None else None
        res["buyRefPrice"] = round(float(closes[-1]), 6) if closes else None
        res["buyWhy"] = buy_why
        res["buyChecks"] = buy_checks
    except Exception:
        res["buyMode"] = None
        res["buyAction"] = None

    try:
        inst_flow_payload = res.get("instFlow") if isinstance(res.get("instFlow"), dict) else None
        _apply_intraday_risk_buy_blocks(res, market_regime=market_regime)
        _apply_inst_flow_risk_buy_blocks(res, inst_flow=inst_flow_payload)
        res["riskAlerts"] = _build_server_risk_alerts(
            intraday_chg_pct=res.get("intradayChgPct"),
            gap_up=res.get("gapUp"),
            market_regime=market_regime,
            buy_checks=res.get("buyChecks"),
            buy_action=str(res.get("buyAction") or ""),
            risk_metrics_live=bool(res.get("riskMetricsLive")),
            inst_flow=inst_flow_payload,
        )
    except Exception:
        res["riskAlerts"] = []

    # Decide final TrendOK
    required = [
        res["checks"].get("emaOrder"),
        res["checks"].get("macdPositive"),
        res["checks"].get("macdHistExpanding"),
        res["checks"].get("closeNear20dHigh"),
        res["checks"].get("rsiInRange"),
        res["checks"].get("volumeSurge"),
    ]
    if any(x is None for x in required):
        res["trendOk"] = None
        res["missingData"].append("insufficient_indicators")
    else:
        res["trendOk"] = bool(all(bool(x) for x in required))
    if res.get("score") is not None and res.get("trendOk") is not True:
        res["score"] = round(min(float(res["score"]), 79.9), 3)
    return res

