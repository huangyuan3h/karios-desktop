from __future__ import annotations

import math
import random
import time
from datetime import UTC, date, datetime, timedelta
from typing import Any

from data_sync_service.db.market_sentiment import get_latest_date, list_days, upsert_daily_rows


def now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


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
    ak = _akshare()
    d = as_of.strftime("%Y-%m-%d")
    if not hasattr(ak, "stock_zh_a_spot_em"):
        raise RuntimeError("AkShare missing stock_zh_a_spot_em. Please upgrade AkShare.")
    try:
        df = _with_retry(lambda: ak.stock_zh_a_spot_em(), tries=3)
    except Exception:
        if not hasattr(ak, "stock_zh_a_spot"):
            raise
        df = _with_retry(lambda: ak.stock_zh_a_spot(), tries=2, base_sleep_s=0.8)
    rows = _to_records(df)
    up = 0
    down = 0
    flat = 0
    total_turnover_cny = 0.0
    total_volume = 0.0

    def _to_float0(v: Any) -> float:
        if v is None:
            return 0.0
        if isinstance(v, (int, float)):
            return float(v) if float(v) == float(v) else 0.0
        s = str(v).strip().replace(",", "").replace("%", "")
        if not s or s in ("-", "—", "N/A", "None"):
            return 0.0
        keep = []
        for ch in s:
            if ch.isdigit() or ch in (".", "-", "+"):
                keep.append(ch)
        try:
            return float("".join(keep))
        except Exception:
            return 0.0

    for r in rows:
        turnover0 = r.get("成交额") or r.get("turnover") or r.get("成交额(元)") or ""
        vol0 = r.get("成交量") or r.get("volume") or ""
        total_turnover_cny += _parse_money_to_cny(turnover0)
        total_volume += _to_float0(vol0)

        chg = r.get("涨跌幅") or r.get("change_pct") or r.get("涨跌幅%") or ""
        s = str(chg).strip().replace("%", "")
        try:
            v = float(s)
        except Exception:
            continue
        if v > 0:
            up += 1
        elif v < 0:
            down += 1
        else:
            flat += 1
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
        "raw": {"source": "stock_zh_a_spot_em", "rows": len(rows)},
    }


def _safe_trade_date(x: date) -> str:
    return x.strftime("%Y%m%d")

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
    ak = _akshare()
    d = as_of.strftime("%Y-%m-%d")
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
        "raw": {"y": chosen_y.strftime("%Y-%m-%d") if chosen_y else None, "matched": len(vals)},
    }

def fetch_cn_failed_limitup_rate(as_of: date) -> dict[str, Any]:
    ak = _akshare()
    d = as_of.strftime("%Y-%m-%d")
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

    def _codes(rs: list[dict[str, Any]]) -> set[str]:
        s: set[str] = set()
        for r in rs:
            code = str(r.get("代码") or r.get("code") or r.get("股票代码") or "").strip()
            if code:
                s.add(code)
        return s

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
        "raw": {"method": method, "failedRows": len(failed_rows), "closeRows": len(close_rows)},
    }


def _finite_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except Exception:
        return default


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

    try:
        breadth = fetch_cn_market_breadth_eod(dt)
        raw["breadth"] = breadth
        up = int(breadth.get("up_count") or 0)
        down = int(breadth.get("down_count") or 0)
        flat = int(breadth.get("flat_count") or 0)
        ratio = _finite_float(breadth.get("up_down_ratio"), 0.0)
        market_turnover_cny = _finite_float(breadth.get("total_turnover_cny"), 0.0)
        market_volume = _finite_float(breadth.get("total_volume"), 0.0)
    except Exception as e:
        errors.append(f"breadth_failed: {e}")
        raw["breadthError"] = str(e)

    premium = 0.0
    try:
        premium_obj = fetch_cn_yesterday_limitup_premium(dt)
        raw["yesterdayLimitUpPremium"] = premium_obj
        premium_raw = premium_obj.get("premium")
        premium = _finite_float(premium_raw, 0.0)
    except Exception as e:
        errors.append(f"yesterday_limitup_premium_failed: {e}")
        raw["yesterdayLimitUpPremiumError"] = str(e)

    failed_rate = 0.0
    try:
        failed_obj = fetch_cn_failed_limitup_rate(dt)
        raw["failedLimitUpRate"] = failed_obj
        failed_raw = failed_obj.get("failed_rate")
        failed_rate = _finite_float(failed_raw, 0.0)
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


def sync_cn_sentiment(*, date_str: str, force: bool) -> dict[str, Any]:
    d = date_str
    if not force:
        cached = list_days(as_of_date=d, days=1)
        if cached and str(cached[-1].get("date") or "") == d:
            return {"asOfDate": d, "days": 1, "items": [cached[-1]]}

    try:
        out = compute_cn_sentiment_for_date(d)
    except Exception as e:
        cached2 = list_days(as_of_date=d, days=1)
        if cached2:
            last = cached2[-1]
            last_rules = (last.get("rules") if isinstance(last.get("rules"), list) else []) or []
            last["rules"] = [*last_rules, f"stale_sync_failed: {type(e).__name__}: {e}"]
            return {"asOfDate": d, "days": 1, "items": [last]}
        out = {
            "date": d,
            "asOfDate": d,
            "up": 0,
            "down": 0,
            "flat": 0,
            "ratio": 0.0,
            "premium": 0.0,
            "failedRate": 0.0,
            "riskMode": "caution",
            "rules": [f"compute_failed: {type(e).__name__}: {e}"],
            "updatedAt": now_iso(),
            "raw": {"error": str(e)},
        }

    rules_raw = out.get("rules") or []
    rules_list = [str(x) for x in rules_raw] if isinstance(rules_raw, list) else [str(rules_raw)]
    breadth_failed = any(("breadth_failed" in r) for r in rules_list)
    compute_failed = any(("compute_failed" in r) for r in rules_list)
    premium_failed = any(("yesterday_limitup_premium_failed" in r) for r in rules_list)
    failed_rate_failed = any(("failed_limitup_rate_failed" in r) for r in rules_list)
    should_persist = not compute_failed and not breadth_failed and not (premium_failed or failed_rate_failed)
    if should_persist:
        row = {
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
        upsert_daily_rows([row])
        cached = list_days(as_of_date=d, days=1)
        if cached:
            return {"asOfDate": d, "days": 1, "items": [cached[-1]]}

    items = [
        {
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
    ]
    return {"asOfDate": d, "days": 1, "items": items}


def get_cn_sentiment(*, days: int = 10, as_of_date: str | None = None) -> dict[str, Any]:
    d = (as_of_date or "").strip() or (get_latest_date() or "")
    if not d:
        return {"asOfDate": "", "days": days, "items": []}
    items = list_days(as_of_date=d, days=days)
    return {"asOfDate": d, "days": max(1, min(int(days), 30)), "items": items}
