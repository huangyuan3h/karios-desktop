from __future__ import annotations

import math
import random
import sys
import time
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast
from zoneinfo import ZoneInfo

from cachetools import TTLCache

from data_sync_service.db import get_connection
from data_sync_service.db.daily import ensure_table as ensure_daily
from data_sync_service.db.market_sentiment import get_latest_date, list_days, upsert_daily_rows
from data_sync_service.db.stock_basic import ensure_table as ensure_stock_basic
from data_sync_service.db.stock_basic import fetch_stock_ts_codes
from data_sync_service.db.trade_calendar import get_open_dates, is_trading_day
from data_sync_service.service.realtime_quote import (
    fetch_realtime_quotes,
    fetch_realtime_quotes_batched,
)
from data_sync_service.service.trade_calendar_utils import (
    is_cn_trading_day,
    last_open_date_on_or_before,
    shanghai_today,
)

BREADTH_DECLINE_RED_THRESHOLD = 3000
CN_INDEX_TRAFFIC_LIGHT_NAMES = frozenset({"上证指数", "创业板指", "中证500"})
INTRADAY_BREADTH_CACHE_TTL_SECONDS = 600
_INTRADAY_BREADTH_CACHE = cast(
    TTLCache[str, dict[str, Any]],
    TTLCache(maxsize=64, ttl=INTRADAY_BREADTH_CACHE_TTL_SECONDS),
)


def now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def breadth_panic_active(down_count: int) -> bool:
    return int(down_count) >= BREADTH_DECLINE_RED_THRESHOLD


def breadth_panic_rule(down_count: int) -> str:
    return (
        f"breadth_panic(down>={BREADTH_DECLINE_RED_THRESHOLD} => red + extreme_caution)"
        f"[down={int(down_count)}]"
    )


def apply_breadth_panic_risk_mode(risk_mode: str, down_count: int, rules: list[str]) -> str:
    if not breadth_panic_active(down_count):
        return risk_mode
    rule = breadth_panic_rule(down_count)
    if rule not in rules:
        rules.append(rule)
    return "extreme_caution"


def apply_breadth_panic_index_signals(
    index_signals: list[dict[str, Any]], down_count: int
) -> list[dict[str, Any]]:
    if not breadth_panic_active(down_count):
        return index_signals
    out: list[dict[str, Any]] = []
    for sig in index_signals:
        s = dict(sig)
        if str(s.get("name") or "") in CN_INDEX_TRAFFIC_LIGHT_NAMES:
            s["signal"] = "red"
            s["positionRange"] = "0%-10%"
            r = [str(x) for x in (s.get("rules") or [])]
            override = f"breadth_panic override(down={int(down_count)})"
            if override not in r:
                r.append(override)
            s["rules"] = r
        out.append(s)
    return out


def apply_breadth_panic_sentiment_items(
    items: list[dict[str, Any]], down_count: int
) -> list[dict[str, Any]]:
    if not items or not breadth_panic_active(down_count):
        return items
    out = [dict(x) for x in items]
    latest = dict(out[-1])
    latest["riskMode"] = "extreme_caution"
    rules = [str(x) for x in (latest.get("rules") or [])]
    rule = breadth_panic_rule(down_count)
    if rule not in rules:
        rules.append(rule)
    latest["rules"] = rules
    out[-1] = latest
    return out


# ---------- Capitulation V-Bottom (V5.7) ----------
CAPITULATION_DOWN_THRESHOLD = 3500
CAPITULATION_IV_THRESHOLD = 20.0  # 300ETF Put IV > 20%
CAPITULATION_FLOW_THRESHOLD_YI = 20.0  # +20亿 CNY


def check_capitulation_bottom(*, down: int, as_of: date) -> dict[str, Any]:
    """
    Detect extreme capitulation V-bottom resonance.

    All three conditions must be satisfied simultaneously:
      1. Market breadth extreme panic: down >= 3500
      2. Panic IV extreme: 300ETF Put IV > 20.0%
      3. Broad-based national-team inflow: 510300 main or super-large net inflow > +20亿
    """
    reasons: list[str] = []
    triggered = True

    # Condition 1: breadth extreme panic
    cond_breadth = int(down) >= CAPITULATION_DOWN_THRESHOLD
    if not cond_breadth:
        triggered = False
    reasons.append(f"breadth_down={int(down)}(>={CAPITULATION_DOWN_THRESHOLD}):{cond_breadth}")

    # Condition 2: 300ETF Put IV
    iv_pct: float | None = None
    cond_iv = False
    try:
        from data_sync_service.db.macro_daily import get_latest_row
        from data_sync_service.service.macro_daily import SID_510300_PUT_IV

        row = get_latest_row(SID_510300_PUT_IV)
        if row and row.get("close") is not None:
            iv_pct = float(row["close"])
            cond_iv = iv_pct > CAPITULATION_IV_THRESHOLD
    except Exception:
        pass
    if not cond_iv:
        triggered = False
    reasons.append(f"put_iv={iv_pct}(>{CAPITULATION_IV_THRESHOLD}):{cond_iv}")

    # Condition 3: 510300 ETF national-team inflow (main or super-large)
    main_flow_yi: float | None = None
    super_flow_yi: float | None = None
    cond_flow = False
    try:
        from data_sync_service.db.etf_fund_flow import fetch_row, get_last_trade_date

        last_date = get_last_trade_date("510300.SH")
        if last_date:
            fr = fetch_row("510300.SH", last_date.isoformat())
            if fr:
                main_yi = (fr.get("main_net_inflow") or 0.0) / 1e8
                super_yi = (fr.get("super_large_net_inflow") or 0.0) / 1e8
                main_flow_yi = round(main_yi, 2)
                super_flow_yi = round(super_yi, 2)
                cond_flow = (
                    main_yi > CAPITULATION_FLOW_THRESHOLD_YI
                    or super_yi > CAPITULATION_FLOW_THRESHOLD_YI
                )
    except Exception:
        pass
    if not cond_flow:
        triggered = False
    reasons.append(
        f"510300_main={main_flow_yi}亿/super={super_flow_yi}亿"
        f"(>{CAPITULATION_FLOW_THRESHOLD_YI}亿):{cond_flow}"
    )

    rule = (
        "capitulation_v_bottom(breadth>=3500 && put_iv>20% && 510300_flow>+20亿)"
        f"[{'|'.join(reasons)}]"
    )
    return {
        "triggered": triggered,
        "rule": rule,
        "raw": {
            "down": int(down),
            "ivPct": iv_pct,
            "mainFlowYi": main_flow_yi,
            "superLargeFlowYi": super_flow_yi,
            "reasons": reasons,
        },
    }


# ---------- Follow-Through Day (V5.8) ----------
FTD_LOOKBACK_TRADING_DAYS = 10
FTD_INDEX_CHG_THRESHOLD = 1.5  # %
FTD_INDEX_TS_CODES = ("000001.SH", "399006.SZ")  # SSE Composite, ChiNext


def _capitulation_in_lookback(as_of: date, lookback_days: int = FTD_LOOKBACK_TRADING_DAYS) -> bool:
    """True if capitulation_v_bottom occurred within the lookback window."""
    try:
        items = list_days(as_of_date=as_of.isoformat(), days=lookback_days)
        for item in items:
            if str(item.get("riskMode") or "") == "capitulation_v_bottom":
                return True
    except Exception:
        pass
    return False


def _compute_index_max_chg_pct(as_of: date) -> float | None:
    """Max daily pct change across SSE / ChiNext indices for as_of date."""
    max_chg: float | None = None
    d_str = as_of.isoformat()
    try:
        from data_sync_service.db.index_daily import fetch_last_closes_upto

        for ts_code in FTD_INDEX_TS_CODES:
            closes = fetch_last_closes_upto(ts_code, d_str, days=2)
            if len(closes) < 2:
                continue
            prev_c = closes[-2][1]
            cur_c = closes[-1][1]
            if prev_c > 0 and math.isfinite(cur_c):
                chg = (cur_c - prev_c) / prev_c * 100.0
                if math.isfinite(chg) and (max_chg is None or chg > max_chg):
                    max_chg = chg
    except Exception:
        pass
    return max_chg


def _read_prev_day_turnover(as_of: date) -> float | None:
    """Previous open-day market turnover from persisted sentiment rows."""
    try:
        from data_sync_service.service.trade_calendar_utils import previous_open_date

        prev = previous_open_date(as_of)
        if not prev:
            return None
        items = list_days(as_of_date=prev.isoformat(), days=1)
        if items:
            return float(items[-1].get("marketTurnoverCny") or 0.0)
    except Exception:
        pass
    return None


def check_follow_through_day(
    *,
    as_of: date,
    index_chg_max_pct: float | None,
    today_turnover_cny: float,
    prev_turnover_cny: float | None,
) -> dict[str, Any]:
    """
    Follow-Through Day (FTD): confirm right-side uptrend after capitulation bottom.

    Requires all three:
      1. capitulation_v_bottom within past 10 trading days
      2. SSE or ChiNext index daily gain > +1.5%
      3. today total turnover > previous trading day
    """
    reasons: list[str] = []
    triggered = True

    cond_cap = _capitulation_in_lookback(as_of)
    if not cond_cap:
        triggered = False
    reasons.append(f"capitulation_10d:{cond_cap}")

    cond_chg = index_chg_max_pct is not None and float(index_chg_max_pct) > FTD_INDEX_CHG_THRESHOLD
    if not cond_chg:
        triggered = False
    reasons.append(
        f"index_chg={index_chg_max_pct}(>{FTD_INDEX_CHG_THRESHOLD}%):{cond_chg}"
    )

    cond_vol = (
        prev_turnover_cny is not None
        and float(prev_turnover_cny) > 0.0
        and float(today_turnover_cny) > float(prev_turnover_cny)
    )
    if not cond_vol:
        triggered = False
    reasons.append(
        f"turnover={today_turnover_cny}>{prev_turnover_cny}:{cond_vol}"
    )

    rule = (
        "follow_through_day(capitulation_10d && index_chg>1.5% && turnover_up)"
        f"[{'|'.join(reasons)}]"
    )
    return {
        "triggered": triggered,
        "rule": rule,
        "raw": {
            "capitulationInLookback": cond_cap,
            "indexChgMaxPct": index_chg_max_pct,
            "todayTurnoverCny": today_turnover_cny,
            "prevTurnoverCny": prev_turnover_cny,
            "reasons": reasons,
        },
    }


def _with_retry(fn, *, tries: int = 3, base_sleep_s: float = 0.4, max_sleep_s: float = 2.0):
    tries2 = max(1, min(int(tries), 5))
    last: Exception | None = None
    for i in range(tries2):
        try:
            return fn()
        except Exception as e:
            last = e
            if i >= tries2 - 1:
                raise
            sleep_s = min(float(max_sleep_s), float(base_sleep_s) * (2**i))
            sleep_s = sleep_s * (0.7 + random.random() * 0.6)
            time.sleep(max(0.0, sleep_s))
    if last is not None:
        raise last
    raise RuntimeError("Retry wrapper failed unexpectedly.")


def _akshare():
    try:
        import akshare as ak  # type: ignore[import-not-found]

        return ak
    except Exception as e:
        raise RuntimeError(
            "AkShare is required for market sentiment sync.\n"
            "Install in data-sync-service: cd services/data-sync-service && uv add akshare\n"
            f"Original error: {e}"
        ) from e


def _tushare_pro():
    try:
        import tushare as ts  # type: ignore[import-not-found]

        from data_sync_service.config import get_settings

        settings = get_settings()
        if not settings.tu_share_api_key:
            raise RuntimeError("TU_SHARE_API_KEY is not set")
        return ts.pro_api(settings.tu_share_api_key)
    except Exception as e:
        raise RuntimeError(f"Tushare is required for fallback. Original error: {e}") from e


def _to_records(df: Any) -> list[dict[str, Any]]:
    if hasattr(df, "to_dict"):
        return list(df.to_dict("records"))  # type: ignore[arg-type]
    raise RuntimeError("Unexpected AkShare return type (expected DataFrame).")


def _parse_money_to_cny(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        f = float(value)
        return f if math.isfinite(f) else 0.0
    s = str(value).strip()
    if not s or s in ("-", "—", "N/A", "None"):
        return 0.0
    s2 = s.replace(",", "").replace(" ", "")
    mult = 1.0
    if "亿" in s2:
        mult = 1e8
        s2 = s2.replace("亿", "")
    elif "万" in s2:
        mult = 1e4
        s2 = s2.replace("万元", "").replace("万", "")
    keep = []
    for ch in s2:
        if ch.isdigit() or ch in (".", "-", "+"):
            keep.append(ch)
    num_s = "".join(keep)
    try:
        return float(num_s) * mult
    except Exception:
        return 0.0


def fetch_cn_market_breadth_eod(as_of: date) -> dict[str, Any]:
    d = as_of.strftime("%Y-%m-%d")
    # Use tushare to avoid native crashes from AkShare's JS decoder (mini_racer).
    # This is EOD breadth, so tushare daily is sufficient and more stable.
    pro = _tushare_pro()
    td = _safe_trade_date(as_of)
    limit = 5000
    offset = 0
    up = 0
    down = 0
    flat = 0
    total_turnover_cny = 0.0
    total_volume = 0.0
    rows_n = 0
    while True:
        df = _with_retry(
            lambda offset=offset: pro.daily(
                trade_date=td, limit=limit, offset=offset, fields="ts_code,pct_chg,vol,amount"
            ),
            tries=2,
            base_sleep_s=0.6,
        )
        if df is None or getattr(df, "empty", False):
            break
        rows = _to_records(df)
        rows_n += len(rows)
        for r in rows:
            try:
                pct = float(r.get("pct_chg"))
            except Exception:
                pct = 0.0
            if pct > 0:
                up += 1
            elif pct < 0:
                down += 1
            else:
                flat += 1

            try:
                vol = float(r.get("vol") or 0.0)
            except Exception:
                vol = 0.0
            try:
                amt = float(r.get("amount") or 0.0)
            except Exception:
                amt = 0.0
            # Tushare daily.amount is in thousand RMB (K CNY).
            if math.isfinite(amt):
                total_turnover_cny += float(amt) * 1000.0
            if math.isfinite(vol):
                total_volume += float(vol)
        if len(rows) < limit:
            break
        offset += limit

    total = up + down + flat
    ratio = float(up) / float(down) if down > 0 else float(up)
    return {
        "date": d,
        "up_count": up,
        "down_count": down,
        "flat_count": flat,
        "total_count": total,
        "up_down_ratio": ratio,
        "total_turnover_cny": total_turnover_cny,
        "total_volume": total_volume,
        "raw": {"source": "tushare.daily", "trade_date": td, "rows": rows_n},
    }


def fetch_cn_market_breadth_intraday(as_of: date) -> dict[str, Any]:
    """
    Best-effort intraday breadth using realtime quotes (Tushare).
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    d = as_of.strftime("%Y-%m-%d")
    cached = _INTRADAY_BREADTH_CACHE.get(d)
    if cached is not None:
        return cached

    ensure_stock_basic()
    ts_codes = fetch_stock_ts_codes()
    requested = len(ts_codes)
    if not ts_codes:
        empty = {
            "date": d,
            "up_count": 0,
            "down_count": 0,
            "flat_count": 0,
            "total_count": 0,
            "up_down_ratio": 0.0,
            "total_turnover_cny": 0.0,
            "total_volume": 0.0,
            "raw": {"source": "tushare.realtime_quote", "requested": 0, "matched": 0},
        }
        _INTRADAY_BREADTH_CACHE[d] = empty
        return empty

    parts = [ts_codes[i : i + 50] for i in range(0, len(ts_codes), 50)]

    def _fetch_part(part: list[str]) -> tuple[int, int, int, int, float, float, list[str]]:
        local_up = 0
        local_down = 0
        local_flat = 0
        local_matched = 0
        local_turnover = 0.0
        local_volume = 0.0
        local_errors: list[str] = []
        try:
            r = fetch_realtime_quotes(part)
            if not isinstance(r, dict) or not bool(r.get("ok")):
                err = r.get("error") if isinstance(r, dict) else "realtime_quote_failed"
                local_errors.append(str(err))
                return 0, 0, 0, 0, 0.0, 0.0, local_errors
            for it in r.get("items", []) or []:
                local_matched += 1
                pct = _realtime_pct_chg(it)
                if pct is not None:
                    if pct > 0:
                        local_up += 1
                    elif pct < 0:
                        local_down += 1
                    else:
                        local_flat += 1
                local_volume += _finite_float(it.get("volume"), 0.0)
                local_turnover += _finite_float(it.get("amount"), 0.0)
        except Exception as exc:
            local_errors.append(str(exc))
        return local_up, local_down, local_flat, local_matched, local_turnover, local_volume, local_errors

    up = 0
    down = 0
    flat = 0
    total_turnover_cny = 0.0
    total_volume = 0.0
    matched = 0
    batches = len(parts)
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(_fetch_part, part) for part in parts]
        for future in as_completed(futures):
            pu, pd, pf, pm, pt, pv, pe = future.result()
            up += pu
            down += pd
            flat += pf
            matched += pm
            total_turnover_cny += pt
            total_volume += pv
            errors.extend(pe)

    total = up + down + flat
    ratio = float(up) / float(down) if down > 0 else float(up)
    raw = {
        "source": "tushare.realtime_quote",
        "requested": requested,
        "matched": matched,
        "batches": batches,
    }
    if errors:
        raw["errors"] = errors[:3]
    result = {
        "date": d,
        "up_count": up,
        "down_count": down,
        "flat_count": flat,
        "total_count": total,
        "up_down_ratio": ratio,
        "total_turnover_cny": total_turnover_cny,
        "total_volume": total_volume,
        "raw": raw,
    }
    _INTRADAY_BREADTH_CACHE[d] = result
    return result


def _safe_trade_date(x: date) -> str:
    return x.strftime("%Y%m%d")


def _tushare_daily_pct_chg_map(as_of: date) -> dict[str, float]:
    """
    Return {ts_code -> pct_chg} for the given trade date (EOD).
    """
    pro = _tushare_pro()
    d = _safe_trade_date(as_of)
    # Prefer a minimal fields set to reduce payload size.
    df = _with_retry(lambda: pro.daily(trade_date=d, fields="ts_code,pct_chg"), tries=2, base_sleep_s=0.6)
    if df is None:
        return {}
    rows = _to_records(df)
    out: dict[str, float] = {}
    for r in rows:
        ts_code = str(r.get("ts_code") or "").strip()
        if not ts_code:
            continue
        try:
            v = float(r.get("pct_chg"))
        except Exception:
            continue
        if math.isfinite(v):
            out[ts_code] = v
    return out


def _tushare_yesterday_limitup_codes(as_of: date) -> tuple[date | None, list[str]]:
    """
    Find the most recent prior trade date with any limit-up list, and return its ts_codes.
    """
    pro = _tushare_pro()
    chosen_y: date | None = None
    codes: list[str] = []

    def _try_limit_list(trade_date: str) -> list[dict[str, Any]]:
        # Tushare provides different names across versions; try a few.
        for fn_name in ("limit_list_d", "limit_list"):
            fn = getattr(pro, fn_name, None)
            if not fn:
                continue
            # Try common signatures.
            for kwargs in (
                {"trade_date": trade_date, "limit_type": "U", "fields": "ts_code"},
                {"trade_date": trade_date, "limit_type": "U"},
                {"trade_date": trade_date, "fields": "ts_code"},
                {"trade_date": trade_date},
            ):
                try:
                    df = fn(**kwargs)  # type: ignore[misc]
                except TypeError:
                    continue
                if df is None:
                    continue
                rs = _to_records(df)
                if rs:
                    return rs
        return []

    for back in range(1, 10):
        y = as_of - timedelta(days=back)
        trade_date = _safe_trade_date(y)
        try:
            rs = _with_retry(
                lambda trade_date=trade_date: _try_limit_list(trade_date),
                tries=2,
                base_sleep_s=0.6,
            )
        except Exception:
            rs = []
        codes2: list[str] = []
        for r in rs:
            ts_code = str(r.get("ts_code") or "").strip()
            if ts_code:
                codes2.append(ts_code)
        if codes2:
            chosen_y = y
            codes = codes2
            break
    return chosen_y, codes


def fetch_cn_yesterday_limitup_premium_tushare(as_of: date) -> dict[str, Any]:
    """
    Fallback implementation for yesterday limit-up premium using tushare (EOD).
    """
    d = as_of.strftime("%Y-%m-%d")
    chosen_y, codes = _tushare_yesterday_limitup_codes(as_of)
    if not codes:
        return {"date": d, "premium": 0.0, "count": 0, "raw": {"source": "tushare", "y": None}}

    pct_map = _tushare_daily_pct_chg_map(as_of)
    vals: list[float] = []
    for ts_code in codes:
        if ts_code in pct_map:
            vals.append(float(pct_map[ts_code]))
    premium = float(sum(vals) / len(vals)) if vals else 0.0
    return {
        "date": d,
        "premium": premium,
        "count": len(codes),
        "raw": {
            "source": "tushare",
            "y": chosen_y.strftime("%Y-%m-%d") if chosen_y else None,
            "matched": len(vals),
        },
    }

def _fetch_cn_a_spot_change_pct() -> dict[str, float]:
    ak = _akshare()
    if not hasattr(ak, "stock_zh_a_spot_em"):
        raise RuntimeError("AkShare missing stock_zh_a_spot_em. Please upgrade AkShare.")
    try:
        df = _with_retry(lambda: ak.stock_zh_a_spot_em(), tries=3)
    except Exception:
        if not hasattr(ak, "stock_zh_a_spot"):
            raise
        df = _with_retry(lambda: ak.stock_zh_a_spot(), tries=2, base_sleep_s=0.8)
    rows = _to_records(df)
    out: dict[str, float] = {}
    for r in rows:
        code = str(r.get("代码") or r.get("code") or "").strip()
        if not code:
            continue
        chg = str(r.get("涨跌幅") or r.get("change_pct") or "").strip().replace("%", "")
        try:
            out[code] = float(chg)
        except Exception:
            continue
    return out


def fetch_cn_yesterday_limitup_premium(as_of: date) -> dict[str, Any]:
    d = as_of.strftime("%Y-%m-%d")
    # On macOS, AkShare's JS decoder may crash the whole process (mini_racer / V8 fatal).
    # Prefer tushare to keep the backend stable.
    if sys.platform == "darwin":
        return fetch_cn_yesterday_limitup_premium_tushare(as_of)
    try:
        ak = _akshare()
        if not hasattr(ak, "stock_zt_pool_em"):
            raise RuntimeError("AkShare missing stock_zt_pool_em. Please upgrade AkShare.")
        chosen_y: datetime.date | None = None
        codes: list[str] = []
        for back in range(1, 8):
            y = as_of - timedelta(days=back)
            try:
                df = ak.stock_zt_pool_em(date=_safe_trade_date(y))  # type: ignore[misc]
                rows = _to_records(df)
            except Exception:
                continue
            codes = []
            for r in rows:
                code = str(r.get("代码") or r.get("code") or r.get("股票代码") or "").strip()
                if code:
                    codes.append(code)
            if codes:
                chosen_y = y
                break
        if not codes:
            return {"date": d, "premium": 0.0, "count": 0, "raw": {"y": None, "searchedBackDays": 7}}

        chg_map = _fetch_cn_a_spot_change_pct()
        vals: list[float] = []
        for code in codes:
            if code in chg_map:
                vals.append(float(chg_map[code]))
        premium = float(sum(vals) / len(vals)) if vals else 0.0
        return {
            "date": d,
            "premium": premium,
            "count": len(codes),
            "raw": {
                "source": "akshare",
                "y": chosen_y.strftime("%Y-%m-%d") if chosen_y else None,
                "matched": len(vals),
            },
        }
    except Exception as e:
        # AkShare occasionally gets blocked/rate-limited and returns HTML ("<..."), causing decode errors.
        # Fallback to tushare (EOD) to keep the sentiment pipeline stable.
        try:
            out = fetch_cn_yesterday_limitup_premium_tushare(as_of)
            raw = out.get("raw") if isinstance(out, dict) else {}
            if isinstance(raw, dict):
                raw["akshareError"] = str(e)
            return out
        except Exception as e2:
            # Final fallback: return a safe default without throwing, to avoid polluting the whole sync step.
            return {
                "date": d,
                "premium": 0.0,
                "count": 0,
                "raw": {"source": "fallback", "akshareError": str(e), "tushareError": str(e2)},
            }

def fetch_cn_failed_limitup_rate(as_of: date) -> dict[str, Any]:
    d = as_of.strftime("%Y-%m-%d")

    def _codes(rs: list[dict[str, Any]]) -> set[str]:
        s: set[str] = set()
        for r in rs:
            code = str(r.get("代码") or r.get("code") or r.get("股票代码") or "").strip()
            if code:
                s.add(code)
        return s

    try:
        # Same safety consideration as premium: avoid AkShare on macOS to prevent native crashes.
        if sys.platform == "darwin":
            raise RuntimeError("akshare_disabled_on_darwin")
        ak = _akshare()
        if not hasattr(ak, "stock_zt_pool_em"):
            raise RuntimeError("AkShare missing stock_zt_pool_em. Please upgrade AkShare.")
        df_close = ak.stock_zt_pool_em(date=_safe_trade_date(as_of))  # type: ignore[misc]
        close_rows = _to_records(df_close)
        failed_rows: list[dict[str, Any]] = []
        method = "fallback_strong_minus_close"
        if hasattr(ak, "stock_zt_pool_zbgc_em"):
            try:
                df_failed = ak.stock_zt_pool_zbgc_em(date=_safe_trade_date(as_of))  # type: ignore[misc]
                failed_rows = _to_records(df_failed)
                method = "zbgc_over_zbgc_plus_close"
            except Exception:
                failed_rows = []
                method = "fallback_strong_minus_close"
        elif hasattr(ak, "stock_zt_pool_zb_em"):
            try:
                df_failed = ak.stock_zt_pool_zb_em(date=_safe_trade_date(as_of))  # type: ignore[misc]
                failed_rows = _to_records(df_failed)
                method = "zb_over_zb_plus_close"
            except Exception:
                failed_rows = []
                method = "fallback_strong_minus_close"

        close = _codes(close_rows)
        close_count = len(close)
        failed = _codes(failed_rows)
        failed_count = len(failed)
        if method in ("zbgc_over_zbgc_plus_close", "zb_over_zb_plus_close"):
            denom = failed_count + close_count
            rate = (float(failed_count) / float(denom) * 100.0) if denom > 0 else 0.0
            ever_count = denom
        else:
            if not hasattr(ak, "stock_zt_pool_strong_em"):
                raise RuntimeError("AkShare missing stock_zt_pool_strong_em. Please upgrade AkShare.")
            df_ever = ak.stock_zt_pool_strong_em(date=_safe_trade_date(as_of))  # type: ignore[misc]
            ever_rows = _to_records(df_ever)
            ever = _codes(ever_rows)
            ever_count = len(ever)
            failed_count = max(0, ever_count - close_count)
            rate = (float(failed_count) / float(ever_count) * 100.0) if ever_count > 0 else 0.0
        return {
            "date": d,
            "failed_rate": rate,
            "ever_count": ever_count,
            "close_count": close_count,
            "raw": {
                "source": "akshare",
                "method": method,
                "failedRows": len(failed_rows),
                "closeRows": len(close_rows),
            },
        }
    except Exception as e:
        # Keep pipeline stable if AkShare is blocked/rate-limited and returns HTML ("<...").
        # We do not have a reliable cross-source definition here, so return a conservative fallback.
        return {
            "date": d,
            "failed_rate": 0.0,
            "ever_count": 0,
            "close_count": 0,
            "raw": {"source": "fallback", "note": "akshare_failed", "akshareError": str(e)},
        }


def _finite_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except Exception:
        return default


def _try_float(v: Any) -> float | None:
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except Exception:
        return None


def _realtime_pct_chg(item: dict[str, Any]) -> float | None:
    pct = _try_float(item.get("pct_chg"))
    if pct is not None:
        return pct
    price = _try_float(item.get("price"))
    pre_close = _try_float(item.get("pre_close"))
    if price is None or pre_close is None or pre_close == 0:
        return None
    return (price - pre_close) / pre_close * 100.0


def _is_shanghai_trading_time() -> bool:
    """
    Best-effort CN A-share trading time check in Asia/Shanghai.
    """
    now = datetime.now(tz=ZoneInfo("Asia/Shanghai"))
    if now.weekday() >= 5:  # 5/6 = weekend
        return False
    minutes = now.hour * 60 + now.minute
    in_morning = minutes >= 9 * 60 + 30 and minutes <= 11 * 60 + 30
    in_afternoon = minutes >= 13 * 60 and minutes <= 15 * 60
    return in_morning or in_afternoon


def _limit_pct_for(ts_code: str, name: str | None) -> float:
    n = (name or "").upper()
    if "ST" in n:
        return 5.0
    t = (ts_code or "").upper()
    if t.endswith(".BJ"):
        return 30.0
    code = t.split(".", 1)[0]
    if code.startswith(("300", "301", "688")):
        return 20.0
    return 10.0


def _prev_open_date(exchange: str, d0: date) -> date | None:
    """
    Return previous open trading date before d0, or None if calendar missing.
    """
    # Prefer trade calendar when available.
    if is_trading_day(exchange, d0) is not None:
        xs = get_open_dates(exchange=exchange, start_date=d0 - timedelta(days=40), end_date=d0)
        xs2 = [x for x in xs if x < d0]
        if xs2:
            return xs2[-1]

    # Fallback: derive from daily table.
    ensure_daily()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(trade_date) FROM daily WHERE trade_date < %s",
                (d0.isoformat(),),
            )
            row = cur.fetchone()
    return row[0] if row and row[0] else None


def _daily_rows_for_date(d0: date) -> list[tuple[str, float | None, float | None, float | None, float | None, str | None]]:
    """
    Return tuples: (ts_code, pre_close, high, close, pct_chg, name).
    """
    ensure_daily()
    ensure_stock_basic()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT d.ts_code, d.pre_close, d.high, d.close, d.pct_chg, b.name
                FROM daily d
                LEFT JOIN stock_basic b ON b.ts_code = d.ts_code
                WHERE d.trade_date = %s
                """,
                (d0.isoformat(),),
            )
            rows = cur.fetchall()
    out: list[tuple[str, float | None, float | None, float | None, float | None, str | None]] = []
    for r in rows:
        ts_code = str(r[0] or "")
        if not ts_code:
            continue
        out.append((ts_code, r[1], r[2], r[3], r[4], str(r[5]) if r[5] is not None else None))
    return out


def _close_limit_up_pool_codes(d0: date) -> list[str]:
    """
    Derive "close-at-limit-up" pool from daily table (DB-first).
    """
    rows = _daily_rows_for_date(d0)
    codes: list[str] = []
    for ts_code, pre_close, _high, close, pct_chg, name in rows:
        if pre_close is None or close is None:
            continue
        try:
            pre = float(pre_close)
            c = float(close)
        except Exception:
            continue
        if not (pre > 0.0 and math.isfinite(pre) and math.isfinite(c)):
            continue
        limit_pct = _limit_pct_for(ts_code, name)
        limit_price = pre * (1.0 + limit_pct / 100.0)
        tol = max(0.01, abs(limit_price) * 0.0015)
        if abs(c - limit_price) <= tol:
            codes.append(ts_code)
            continue
        # Fallback: some data sources round pct_chg; allow a pct-based check.
        try:
            p = float(pct_chg) if pct_chg is not None else None
        except Exception:
            p = None
        if p is not None and math.isfinite(p) and p >= (limit_pct - 0.2):
            codes.append(ts_code)
    return codes


def _avg_pct_chg_from_db(trade_date: date, ts_codes: list[str]) -> tuple[float, int]:
    if not ts_codes:
        return 0.0, 0
    ensure_daily()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts_code, pct_chg
                FROM daily
                WHERE trade_date = %s AND ts_code = ANY(%s)
                """,
                (trade_date.isoformat(), ts_codes),
            )
            rows = cur.fetchall()
    vals: list[float] = []
    for _ts, pct in rows:
        try:
            v = float(pct)
        except Exception:
            continue
        if math.isfinite(v):
            vals.append(v)
    return (float(sum(vals) / len(vals)) if vals else 0.0), len(vals)


def _avg_pct_chg_from_realtime(ts_codes: list[str]) -> tuple[float, int]:
    if not ts_codes:
        return 0.0, 0
    vals: list[float] = []
    for it in fetch_realtime_quotes_batched(ts_codes):
        v = _realtime_pct_chg(it)
        if v is not None:
            vals.append(v)
    return (float(sum(vals) / len(vals)) if vals else 0.0), len(vals)


def _failed_limitup_rate_from_db(trade_date: date) -> tuple[float, int, int]:
    """
    Approximate failed limit-up rate using daily table:
      ever = high touched limit price
      close = close at limit price
    """
    rows = _daily_rows_for_date(trade_date)
    ever = 0
    close = 0
    for ts_code, pre_close, high, close0, pct_chg, name in rows:
        if pre_close is None or high is None or close0 is None:
            continue
        try:
            pre = float(pre_close)
            h = float(high)
            c = float(close0)
        except Exception:
            continue
        if not (pre > 0.0 and math.isfinite(pre) and math.isfinite(h) and math.isfinite(c)):
            continue
        limit_pct = _limit_pct_for(ts_code, name)
        limit_price = pre * (1.0 + limit_pct / 100.0)
        tol = max(0.01, abs(limit_price) * 0.0015)
        touched = h >= (limit_price - tol)
        closed = abs(c - limit_price) <= tol
        if touched:
            ever += 1
            if closed:
                close += 1
            continue
        # Fallback: pct-based touched check (weaker).
        try:
            p = float(pct_chg) if pct_chg is not None else None
        except Exception:
            p = None
        if p is not None and math.isfinite(p) and p >= (limit_pct - 0.2):
            ever += 1
            close += 1
    failed = max(0, ever - close)
    rate = (float(failed) / float(ever) * 100.0) if ever > 0 else 0.0
    return rate, ever, close


def compute_cn_sentiment_for_date(d: str) -> dict[str, Any]:
    ts = now_iso()
    as_of = d
    dt = datetime.strptime(d, "%Y-%m-%d").date()
    raw: dict[str, Any] = {}
    errors: list[str] = []
    up = 0
    down = 0
    flat = 0
    ratio = 0.0
    market_turnover_cny = 0.0
    market_volume = 0.0

    breadth: dict[str, Any] | None = None
    try:
        breadth = fetch_cn_market_breadth_eod(dt)
    except Exception as e:
        errors.append(f"breadth_failed: {e}")
        raw["breadthError"] = str(e)

    today_cn = datetime.now(tz=ZoneInfo("Asia/Shanghai")).date()
    should_try_intraday = dt == today_cn and (
        not breadth
        or int(breadth.get("total_count") or 0) == 0
        or _finite_float(breadth.get("total_turnover_cny"), 0.0) == 0.0
    )
    if should_try_intraday:
        try:
            breadth_rt = fetch_cn_market_breadth_intraday(dt)
            if int(breadth_rt.get("total_count") or 0) > 0 or _finite_float(
                breadth_rt.get("total_turnover_cny"), 0.0
            ) > 0.0:
                breadth = breadth_rt
        except Exception as e:
            errors.append(f"breadth_intraday_failed: {e}")

    if breadth:
        raw["breadth"] = breadth
        up = int(breadth.get("up_count") or 0)
        down = int(breadth.get("down_count") or 0)
        flat = int(breadth.get("flat_count") or 0)
        ratio = _finite_float(breadth.get("up_down_ratio"), 0.0)
        market_turnover_cny = _finite_float(breadth.get("total_turnover_cny"), 0.0)
        market_volume = _finite_float(breadth.get("total_volume"), 0.0)

    # Premium%: DB-first. Derive yesterday close-limit-up pool from DB, then:
    # - If today's daily pct_chg exists in DB: use it
    # - If today's daily is not ready: use realtime_quote
    premium = 0.0
    try:
        y = _prev_open_date("SSE", dt)
        if y is None:
            raise RuntimeError("trade calendar missing for premium computation")
        pool = _close_limit_up_pool_codes(y)
        premium_db, matched_db = _avg_pct_chg_from_db(dt, pool)
        if matched_db > 0:
            premium = premium_db
            raw["yesterdayLimitUpPremium"] = {
                "date": dt.isoformat(),
                "premium": premium,
                "count": len(pool),
                "matched": matched_db,
                "y": y.isoformat(),
                "source": "db.daily",
            }
        else:
            # Intraday/near-close realtime fallback for "today" when daily is not ready.
            now_cn = datetime.now(tz=ZoneInfo("Asia/Shanghai"))
            if now_cn.date() == dt:
                premium_rt, matched_rt = _avg_pct_chg_from_realtime(pool)
                premium = premium_rt
                raw["yesterdayLimitUpPremium"] = {
                    "date": dt.isoformat(),
                    "premium": premium,
                    "count": len(pool),
                    "matched": matched_rt,
                    "y": y.isoformat(),
                    "source": "tushare.realtime_quote",
                }
                errors.append(f"premium_realtime_from: {y.isoformat()}")
            else:
                raw["yesterdayLimitUpPremium"] = {
                    "date": dt.isoformat(),
                    "premium": 0.0,
                    "count": len(pool),
                    "matched": 0,
                    "y": y.isoformat(),
                    "source": "db.daily",
                }
                errors.append(f"premium_missing_daily_for: {dt.isoformat()}")
    except Exception as e:
        errors.append(f"yesterday_limitup_premium_failed: {e}")
        raw["yesterdayLimitUpPremiumError"] = str(e)

    failed_rate = 0.0
    try:
        # Failed% (炸板率): DB-first from daily table. Intraday not reliable; keep 0 and mark.
        # If daily rows for today are not ready, return 0 with a rule so UI doesn't misinterpret it.
        rate, ever_cnt, close_cnt = _failed_limitup_rate_from_db(dt)
        failed_rate = _finite_float(rate, 0.0)
        raw["failedLimitUpRate"] = {
            "date": dt.isoformat(),
            "failed_rate": failed_rate,
            "ever_count": ever_cnt,
            "close_count": close_cnt,
            "source": "db.daily",
        }
        if ever_cnt == 0:
            now_cn = datetime.now(tz=ZoneInfo("Asia/Shanghai"))
            if now_cn.date() == dt and _is_shanghai_trading_time():
                errors.append("failed_rate_intraday_unavailable")
    except Exception as e:
        errors.append(f"failed_limitup_rate_failed: {e}")
        raw["failedLimitUpRateError"] = str(e)

    rules: list[str] = []
    risk_mode = "normal"
    turnover_high = market_turnover_cny >= 1.5e12
    turnover_hot = market_turnover_cny >= 1.8e12
    turnover_euphoric = market_turnover_cny >= 2.5e12
    breadth_good = ratio >= 1.2
    breadth_hot = ratio >= 1.5
    breadth_euphoric = ratio >= 2.0
    premium_good = premium >= 0.0
    premium_hot = premium >= 0.5
    premium_euphoric = premium >= 3.0
    bullish_override = turnover_high and breadth_good and premium_good

    if turnover_euphoric and breadth_euphoric and premium_euphoric and failed_rate <= 35.0:
        risk_mode = "euphoric"
        rules.append("euphoric(turnover>=2.5T && breadth>=2.0 && premium>=3.0 && failed<=35)")
    elif turnover_hot and breadth_hot and premium_hot and failed_rate <= 50.0:
        risk_mode = "hot"
        rules.append("hot(turnover>=1.8T && breadth>=1.5 && premium>=0.5 && failed<=50)")
    else:
        if premium < 0.0 and failed_rate >= 70.0:
            risk_mode = "no_new_positions"
            rules.append("premium<0 && failedLimitUpRate>=70 => no_new_positions")
        elif failed_rate >= 70.0:
            risk_mode = "caution"
            rules.append("failedLimitUpRate>=70 => caution")
        elif premium < 0.0:
            risk_mode = "caution"
            rules.append("premium<0 => caution")
        if risk_mode in ("caution", "no_new_positions") and bullish_override and failed_rate <= 85.0:
            risk_mode = "normal"
            rules.append("bullish_override(turnover_high && breadth_ratio>=1.2 && premium>=0)")
    if errors and risk_mode == "normal":
        risk_mode = "caution"
    if errors:
        rules.extend(errors[:3])

    risk_mode = apply_breadth_panic_risk_mode(risk_mode, down, rules)

    # ---------- Capitulation V-Bottom resonance override (V5.7) ----------
    capitulation = check_capitulation_bottom(down=down, as_of=dt)
    if capitulation["triggered"]:
        risk_mode = "capitulation_v_bottom"
        rules.append(capitulation["rule"])
        raw["capitulation"] = capitulation["raw"]

    # ---------- Follow-Through Day (V5.8) — highest priority ----------
    index_chg_max = _compute_index_max_chg_pct(dt)
    prev_turnover = _read_prev_day_turnover(dt)
    ftd = check_follow_through_day(
        as_of=dt,
        index_chg_max_pct=index_chg_max,
        today_turnover_cny=market_turnover_cny,
        prev_turnover_cny=prev_turnover,
    )
    raw["ftd"] = ftd["raw"]
    if ftd["triggered"]:
        risk_mode = "confirmed_uptrend"
        rules.append(ftd["rule"])

    return {
        "date": d,
        "asOfDate": as_of,
        "up": up,
        "down": down,
        "flat": flat,
        "ratio": ratio,
        "marketTurnoverCny": market_turnover_cny,
        "marketVolume": market_volume,
        "premium": premium,
        "failedRate": failed_rate,
        "riskMode": risk_mode,
        "rules": rules,
        "updatedAt": ts,
        "raw": raw,
    }


def _sentiment_row_from_compute(out: dict[str, Any], d: str) -> dict[str, Any]:
    rules_raw = out.get("rules") or []
    rules_list = [str(x) for x in rules_raw] if isinstance(rules_raw, list) else [str(rules_raw)]
    return {
        "date": out.get("date") or d,
        "as_of_date": out.get("asOfDate") or d,
        "up_count": out.get("up") or 0,
        "down_count": out.get("down") or 0,
        "flat_count": out.get("flat") or 0,
        "total_count": int(out.get("up", 0)) + int(out.get("down", 0)) + int(out.get("flat", 0)),
        "up_down_ratio": out.get("ratio") or 0.0,
        "market_turnover_cny": out.get("marketTurnoverCny") or 0.0,
        "market_volume": out.get("marketVolume") or 0.0,
        "yesterday_limitup_premium": out.get("premium") or 0.0,
        "failed_limitup_rate": out.get("failedRate") or 0.0,
        "risk_mode": out.get("riskMode") or "caution",
        "rules": rules_list,
        "updated_at": out.get("updatedAt") or now_iso(),
        "raw": out.get("raw") if isinstance(out.get("raw"), dict) else {"raw": out.get("raw")},
    }


def _sentiment_item_from_compute(out: dict[str, Any], d: str, *, rules_list: list[str]) -> dict[str, Any]:
    return {
        "date": str(out.get("date") or d),
        "upCount": int(out.get("up") or 0),
        "downCount": int(out.get("down") or 0),
        "flatCount": int(out.get("flat") or 0),
        "totalCount": int(out.get("up", 0)) + int(out.get("down", 0)) + int(out.get("flat", 0)),
        "upDownRatio": float(out.get("ratio") or 0.0),
        "marketTurnoverCny": float(out.get("marketTurnoverCny") or 0.0),
        "marketVolume": float(out.get("marketVolume") or 0.0),
        "yesterdayLimitUpPremium": float(out.get("premium") or 0.0),
        "failedLimitUpRate": float(out.get("failedRate") or 0.0),
        "riskMode": str(out.get("riskMode") or "caution"),
        "rules": rules_list,
        "updatedAt": str(out.get("updatedAt") or now_iso()),
    }


def _resolve_sentiment_sync_dates(*, request_date: date, force: bool) -> tuple[list[date], dict[str, Any] | None]:
    """Return open dates to sync through the latest trading day, or a skip response."""
    target_end = last_open_date_on_or_before(request_date)
    if target_end is None:
        return [], {
            "ok": False,
            "error": "trade calendar missing; cannot determine last trading day",
            "asOfDate": request_date.isoformat(),
        }

    target_iso = target_end.isoformat()
    latest_db = (get_latest_date() or "").strip()
    is_trading = is_cn_trading_day(request_date) is True

    if latest_db >= target_iso:
        if force:
            return [target_end], None
        if is_trading:
            cached = list_days(as_of_date=request_date.isoformat(), days=1)
            if cached and str(cached[-1].get("date") or "") == request_date.isoformat():
                return [], {
                    "ok": True,
                    "skipped": True,
                    "reason": "already_synced",
                    "message": "sentiment already synced for today",
                    "asOfDate": request_date.isoformat(),
                    "days": 1,
                    "items": [cached[-1]],
                }
            return [target_end], None
        cached = list_days(as_of_date=target_iso, days=5)
        return [], {
            "ok": True,
            "skipped": True,
            "reason": "not_trading_day",
            "message": "not a trading day; data already up to date",
            "asOfDate": target_iso,
            "days": len(cached),
            "items": cached,
        }

    if latest_db:
        start = date.fromisoformat(latest_db[:10]) + timedelta(days=1)
    else:
        start = target_end
    dates = get_open_dates(exchange="SSE", start_date=start, end_date=target_end)
    return (dates if dates else [target_end]), None


def _persist_sentiment_for_date(d: str) -> dict[str, Any]:
    out = compute_cn_sentiment_for_date(d)
    row = _sentiment_row_from_compute(out, d)
    rules_list = [str(x) for x in row.get("rules") or []]
    upsert_daily_rows([row])
    cached = list_days(as_of_date=d, days=1)
    if cached:
        return cached[-1]
    return _sentiment_item_from_compute(out, d, rules_list=rules_list)


def sync_cn_sentiment(*, date_str: str, force: bool) -> dict[str, Any]:
    try:
        cal_d = date.fromisoformat(str(date_str).strip()[:10])
    except ValueError:
        cal_d = shanghai_today()

    dates_to_sync, skip_out = _resolve_sentiment_sync_dates(request_date=cal_d, force=bool(force))
    if skip_out is not None:
        return skip_out
    if not dates_to_sync:
        target_iso = (last_open_date_on_or_before(cal_d) or cal_d).isoformat()
        cached = list_days(as_of_date=target_iso, days=5)
        return {
            "ok": True,
            "asOfDate": target_iso,
            "days": len(cached),
            "items": cached,
        }

    synced_items: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for td in dates_to_sync:
        d_iso = td.isoformat()
        try:
            synced_items.append(_persist_sentiment_for_date(d_iso))
        except Exception as e:
            errors.append({"date": d_iso, "error": str(e)})
            cached = list_days(as_of_date=d_iso, days=1)
            if cached and str(cached[-1].get("date") or "") == d_iso:
                synced_items.append(cached[-1])
            elif synced_items:
                break

    as_of = dates_to_sync[-1].isoformat()
    if errors and not synced_items:
        cached2 = list_days(as_of_date=as_of, days=5)
        return {
            "ok": False,
            "skipped": True,
            "reason": "compute_failed",
            "error": errors[0]["error"],
            "asOfDate": str(cached2[-1].get("date") or as_of) if cached2 else as_of,
            "days": len(cached2),
            "items": cached2,
            "errors": errors,
        }

    recent = list_days(as_of_date=as_of, days=min(5, len(dates_to_sync)))
    out: dict[str, Any] = {
        "ok": True,
        "asOfDate": as_of,
        "days": len(recent) if recent else len(synced_items),
        "items": recent if recent else synced_items,
    }
    if len(dates_to_sync) > 1 or (not is_cn_trading_day(cal_d) and synced_items):
        out["catchup"] = True
        out["syncedDates"] = [td.isoformat() for td in dates_to_sync]
        out["message"] = "catchup: synced missing sentiment days through latest open day"
    if errors:
        out["ok"] = False
        out["errors"] = errors
    return out


def get_cn_sentiment(*, days: int = 10, as_of_date: str | None = None) -> dict[str, Any]:
    d = (as_of_date or "").strip() or (get_latest_date() or "")
    if not d:
        return {"asOfDate": "", "days": days, "items": []}
    items = list_days(as_of_date=d, days=days)
    return {"asOfDate": d, "days": max(1, min(int(days), 30)), "items": items}


def get_panic_cooldown(
    *, days: int = 10, cooldown_days: int = 3, as_of_date: str | None = None
) -> dict[str, Any]:
    """S-3 panic protection status (same semantics as the backtest engine).

    Most recent panic day (risk_mode in no_new_positions/extreme_caution)
    within ``days``, cooldown end computed over the CN trade calendar
    (panic day + cooldown_days trading days — matches
    BacktestConfig.panic_cooldown_days). ``active`` True when ``as_of_date``
    (or today) is still inside the cooldown window: no new S-3 entries then.
    """
    from datetime import date, timedelta

    from data_sync_service.db import get_connection
    from data_sync_service.service.backtest_engine import SENTIMENT_BLOCK_MODES

    items = get_cn_sentiment(days=days, as_of_date=as_of_date)["items"]
    panic_dates = [i["date"] for i in items if i.get("riskMode") in SENTIMENT_BLOCK_MODES]
    if not panic_dates:
        return {"lastPanicDate": None, "cooldownEndDate": None, "active": False}
    last_panic = panic_dates[-1]
    today = (as_of_date or "").strip() or date.today().isoformat()
    cooldown_end = last_panic
    if cooldown_days > 0:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT trade_date FROM daily
                    WHERE trade_date > %s AND trade_date <= %s
                    ORDER BY trade_date
                    """,
                    (last_panic, (date.fromisoformat(last_panic) + timedelta(days=60)).isoformat()),
                )
                nxt = [str(r[0]) for r in cur.fetchall()]
        cooldown_end = nxt[min(cooldown_days - 1, len(nxt) - 1)] if nxt else last_panic
    return {
        "lastPanicDate": last_panic,
        "cooldownEndDate": cooldown_end,
        "active": today <= cooldown_end,
    }
