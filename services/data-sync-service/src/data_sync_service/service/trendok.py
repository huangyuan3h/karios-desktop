"""TrendOK/Score computation for Watchlist (CN daily only)."""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from data_sync_service.db.daily import fetch_last_ohlcv_batch
from data_sync_service.db.industry_fund_flow import (
    get_dates_upto,
    get_latest_date as get_latest_industry_date,
    get_rows_by_date,
    get_sum_by_industry_for_dates,
)
from data_sync_service.db.stock_basic import ensure_table as ensure_stock_basic
from data_sync_service.service.market_regime import get_market_regime
from data_sync_service.service.realtime_quote import fetch_realtime_quotes


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


def _shanghai_today_iso() -> str:
    return datetime.now(tz=ZoneInfo("Asia/Shanghai")).date().isoformat()


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


def _lookup_names(ts_codes: list[str]) -> dict[str, str]:
    """
    Best-effort name lookup from stock_basic (ts_code -> name).
    """
    ensure_stock_basic()
    if not ts_codes:
        return {}
    try:
        from data_sync_service.db import get_connection

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ts_code, name FROM stock_basic WHERE ts_code = ANY(%s)",
                    (ts_codes,),
                )
                rows = cur.fetchall()
        return {str(r[0]): str(r[1]) for r in rows if r and r[0] and r[1]}
    except Exception:
        return {}


def _lookup_industries(ts_codes: list[str]) -> dict[str, str]:
    """
    Best-effort industry lookup from stock_basic (ts_code -> industry).
    """
    ensure_stock_basic()
    if not ts_codes:
        return {}
    try:
        from data_sync_service.db import get_connection

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ts_code, industry FROM stock_basic WHERE ts_code = ANY(%s)",
                    (ts_codes,),
                )
                rows = cur.fetchall()
        return {str(r[0]): str(r[1]) for r in rows if r and r[0] and r[1]}
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

    dates_2 = get_dates_upto(flow_date, 2)
    dates_5 = get_dates_upto(flow_date, 5)
    today = dates_2[-1] if dates_2 else flow_date
    yesterday = dates_2[-2] if len(dates_2) >= 2 else None

    rows_today = get_rows_by_date(today)
    rows_yesterday = get_rows_by_date(yesterday) if yesterday else []

    top_today = sorted(rows_today, key=lambda x: float(x.get("net_inflow") or 0.0), reverse=True)
    top_today_5 = [str(x.get("industry_name") or "") for x in top_today[:5] if x.get("industry_name")]
    top_today_3 = top_today_5[:3]

    top_yesterday = sorted(rows_yesterday, key=lambda x: float(x.get("net_inflow") or 0.0), reverse=True)
    top_yesterday_3 = [str(x.get("industry_name") or "") for x in top_yesterday[:3] if x.get("industry_name")]

    net_today = {str(x.get("industry_name") or ""): float(x.get("net_inflow") or 0.0) for x in rows_today}
    net_yesterday = {str(x.get("industry_name") or ""): float(x.get("net_inflow") or 0.0) for x in rows_yesterday}

    sums_5d = get_sum_by_industry_for_dates(dates_5) if dates_5 else []
    top_5d_3 = [str(x.get("industry_name") or "") for x in sums_5d[:3] if x.get("industry_name")]
    bottom_5d_5 = [
        str(x.get("industry_name") or "") for x in reversed(sums_5d[-5:]) if x.get("industry_name")
    ]

    return {
        "ok": True,
        "asOfDate": flow_date,
        "today": today,
        "yesterday": yesterday,
        "top_today_3": set(top_today_3),
        "top_today_5": set(top_today_5),
        "top_yesterday_3": set(top_yesterday_3),
        "net_today": net_today,
        "net_yesterday": net_yesterday,
        "top_5d_3": set(top_5d_3),
        "bottom_5d_5": set(bottom_5d_5),
    }


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
    refresh: bool = False,
    realtime: bool = False,
) -> list[dict[str, Any]]:
    """
    Compute TrendOK for up to 200 symbols using DB-cached daily bars.
    `refresh` is accepted for compatibility but ignored (data-sync-service does not trigger network fetch here).
    `realtime` enables best-effort quote merge for the latest bar during trading hours.
    """
    _ = refresh
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

    by_name = _lookup_names(ts_codes)
    by_industry = _lookup_industries(ts_codes)
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

    out: list[dict[str, Any]] = []
    latest_bar_date: str | None = None
    for bars in bars_by_code.values():
        if not bars:
            continue
        d = str(bars[-1][0])
        if not latest_bar_date or d > latest_bar_date:
            latest_bar_date = d
    flow_ctx = _build_industry_flow_context(latest_bar_date)
    market_regime: str | None = None
    try:
        regime_info = get_market_regime(as_of_date=latest_bar_date)
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
        industry = by_industry.get(ts_code)
        bars = bars_by_code.get(ts_code, [])
        out.append(
            _trendok_one(
                symbol=sym,
                name=name,
                industry=industry,
                bars=bars,
                flow_ctx=flow_ctx,
                market_regime=market_regime,
            )
        )
    return out


def _trendok_one(
    *,
    symbol: str,
    name: str | None,
    industry: str | None,
    bars: list[tuple[str, str, str, str, str, str]],
    flow_ctx: dict[str, Any] | None = None,
    market_regime: str | None = None,
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
    if industry:
        res["values"]["industry"] = industry

    if len(closes) < 60:
        res["missingData"].append("bars_lt_60")

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
        # Rule 5 (optimized): allow strong trend RSI up to 82.
        res["checks"]["rsiInRange"] = bool(50.0 <= rsi14s[-1] <= 82.0)

    if len(closes) >= 20:
        high20 = max(closes[-20:])
        res["values"]["high20"] = high20
        res["checks"]["closeNear20dHigh"] = bool(closes[-1] >= 0.95 * high20)

    if len(vols) >= 30:
        avg5 = sum(vols[-5:]) / 5.0
        avg30 = sum(vols[-30:]) / 30.0
        res["values"]["avgVol5"] = avg5
        res["values"]["avgVol30"] = avg30
        # Rule 6 (optimized): avoid filtering strong "tight volume" trends.
        # Volume "surge" is moved to the Score system; TrendOK only blocks volume cliffs.
        res["checks"]["volumeSurge"] = bool(avg5 > 0.9 * avg30) if avg30 > 0 else bool(avg5 > 0)

    # Score (ported; see quant-service for rationale)
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
            or not v.get("macdHist4")
        ):
            res["score"] = None
        else:
            close = float(v["close"])
            ema5 = float(v["ema5"])
            ema20 = float(v["ema20"])
            ema60 = float(v["ema60"])
            rsi14 = float(v["rsi14"])
            avg5 = float(v["avgVol5"])
            avg30 = float(v["avgVol30"])
            macd_last = float(v["macd"])
            h4 = [float(x) for x in (v.get("macdHist4") or [])]

            ema_pairs = 0
            if ema5 > ema20:
                ema_pairs += 1
            if ema20 > ema60:
                ema_pairs += 1
            s_ema = float(ema_pairs) / 2.0

            hpos = [max(0.0, x) for x in h4] if len(h4) == 4 else [0.0, 0.0, 0.0, 0.0]
            inc = 0
            if hpos[1] > hpos[0]:
                inc += 1
            if hpos[2] > hpos[1]:
                inc += 1
            if hpos[3] > hpos[2]:
                inc += 1
            hist_min = 0.0005 * close if close > 0 else 0.0
            has_hist_strength = bool(hpos[3] >= hist_min and hpos[3] > 0.0)
            s_hist = (float(inc) / 3.0) if has_hist_strength else 0.0
            s_macd = 0.0 if macd_last <= 0.0 else _clip01(0.5 + 0.5 * s_hist)

            high20_high = max(highs[-20:]) if len(highs) >= 20 else float(v["high20"])
            ratio_hi = close / high20_high if high20_high > 0 else 0.0
            s_break = _clip01((ratio_hi - 0.85) / 0.10)
            bonus_new_high = 3.0 if (high20_high > 0 and close >= high20_high) else 0.0

            # RSI subscore: momentum-friendly (do not penalize strong uptrends).
            # Center at 70, linearly decays to 0 at 55/85 (then clipped).
            s_rsi = _clip01(1.0 - (abs(rsi14 - 70.0) / 15.0))

            ratio_vol = (avg5 / avg30) if avg30 > 0 else (1.0 if avg5 > 0 else 0.0)
            s_vol = _clip01((ratio_vol - 1.0) / 0.30)

            # Weights: emphasize breakout/new-high as primary right-side signal.
            w_ema, w_macd, w_break, w_rsi, w_vol = 0.25, 0.15, 0.25, 0.15, 0.20
            pts_ema = 100.0 * w_ema * s_ema
            pts_macd = 100.0 * w_macd * s_macd
            pts_break = 100.0 * w_break * s_break
            pts_rsi = 100.0 * w_rsi * s_rsi
            pts_vol = 100.0 * w_vol * s_vol

            parts: dict[str, float] = {
                "ema": round(pts_ema, 3),
                "macd": round(pts_macd, 3),
                "breakout": round(pts_break, 3),
                "rsi": round(pts_rsi, 3),
                "volume": round(pts_vol, 3),
            }
            if bonus_new_high > 0:
                parts["bonus_new_high20"] = round(bonus_new_high, 3)

            penalty = 0.0
            atr14 = _atr14(highs, lows, closes, 14)
            if atr14 is not None and close > 0:
                atr_ratio = float(atr14) / float(close)
                # Volatility penalty: tolerate high ATR in strong themes.
                # New rule: start penalizing above 3% ATR/close, softer slope, half max penalty.
                p_vol = _clip01((atr_ratio - 0.03) / 0.05) * 5.0
                penalty += p_vol
                parts["penalty_volatility_atr"] = -round(p_vol, 3)
            if ema20 > 0 and close < ema20:
                dd = (ema20 - close) / ema20
                p_below = _clip01(dd / 0.05) * 10.0
                penalty += p_below
                parts["penalty_below_ema20"] = -round(p_below, 3)

            total = pts_ema + pts_macd + pts_break + pts_rsi + pts_vol + bonus_new_high - penalty
            total2 = max(0.0, min(100.0, total))
            res["score"] = round(total2, 3)
            res["scoreParts"] = parts
            if industry and flow_ctx:
                delta, flow_parts, flow_reasons = _industry_flow_score_adjustment(industry, flow_ctx)
                if flow_parts:
                    res["scoreParts"].update(flow_parts)
                    res["values"]["industryFlowAsOfDate"] = flow_ctx.get("asOfDate")
                    res["values"]["industryFlowReasons"] = flow_reasons
                if delta != 0.0 and res.get("score") is not None:
                    res["score"] = round(max(0.0, min(100.0, float(res["score"]) + delta)), 3)
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
                res["stopLossPrice"] = round(current, 6)
                stop_parts["final_stop_loss"] = round(current, 6)
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
                    stop_parts["atr14"] = round(atr14, 6)
                    stop_parts["buffer"] = round(buffer, 6)
                    stop_parts["hard_stop"] = round(hard_stop, 6)
                    stop_parts["stop_loss_support_minus_buffer"] = round(stop_loss_support, 6)
                    stop_parts["final_stop_loss"] = round(final_stop, 6)
                    res["stopLossPrice"] = round(final_stop, 6)
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
    return res

