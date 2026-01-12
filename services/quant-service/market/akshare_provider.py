from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any


@dataclass(frozen=True)
class StockRow:
    symbol: str
    market: str  # "CN" | "HK"
    ticker: str
    name: str
    currency: str
    quote: dict[str, str]


@dataclass(frozen=True)
class BarRow:
    date: str  # ISO date
    open: str
    high: str
    low: str
    close: str
    volume: str
    amount: str


def _akshare():
    try:
        import akshare as ak  # type: ignore

        return ak
    except Exception as e:
        raise RuntimeError(
            "AkShare is required for market data. Please install it in quant-service:\n"
            "  cd services/quant-service && uv add akshare\n"
            f"Original error: {e}"
        ) from e


def _to_records(df: Any) -> list[dict[str, Any]]:
    # AkShare returns pandas.DataFrame. Convert robustly without importing pandas types.
    if hasattr(df, "to_dict"):
        return list(df.to_dict("records"))  # type: ignore[arg-type]
    raise RuntimeError("Unexpected AkShare return type (expected DataFrame).")


def fetch_cn_a_spot() -> list[StockRow]:
    ak = _akshare()
    df = ak.stock_zh_a_spot_em()
    out: list[StockRow] = []
    for r in _to_records(df):
        code = str(r.get("代码") or r.get("code") or "").strip()
        name = str(r.get("名称") or r.get("name") or "").strip()
        if not code or not name:
            continue
        symbol = f"CN:{code}"
        quote = {
            "price": str(r.get("最新价") or ""),
            "change_pct": str(r.get("涨跌幅") or ""),
            "open_pct": str(r.get("今开") or r.get("开盘") or ""),
            "vol_ratio": str(r.get("量比") or ""),
            "volume": str(r.get("成交量") or ""),
            "turnover": str(r.get("成交额") or ""),
            "market_cap": str(r.get("总市值") or ""),
        }
        out.append(
            StockRow(
                symbol=symbol,
                market="CN",
                ticker=code,
                name=name,
                currency="CNY",
                quote=quote,
            ),
        )
    return out


def fetch_cn_a_minute_bars(
    ticker: str,
    *,
    trade_date: str,
    interval: str = "1",
) -> list[dict[str, Any]]:
    """
    Fetch CN A-share minute bars using AkShare (best-effort).

    Notes:
    - AkShare APIs vary by version; we intentionally keep this function resilient.
    - We filter rows by `trade_date` (YYYY-MM-DD) after fetch to avoid relying on optional date params.
    - Returned rows are plain dicts with normalized keys:
      - ts (ISO-like string), open, high, low, close, volume, amount
    """
    ak = _akshare()
    if not hasattr(ak, "stock_zh_a_hist_min_em"):
        raise RuntimeError("AkShare missing stock_zh_a_hist_min_em. Please upgrade AkShare.")

    # Best-effort call: some versions accept (symbol, period, adjust), some require named args.
    try:
        df = ak.stock_zh_a_hist_min_em(symbol=ticker, period=interval, adjust="")  # type: ignore[misc]
    except TypeError:
        df = ak.stock_zh_a_hist_min_em(ticker, interval)  # type: ignore[misc]

    rows = _to_records(df)
    out: list[dict[str, Any]] = []
    d_prefix = trade_date.strip()
    for r in rows:
        # Common time column names: "时间" or "日期时间"
        ts0 = r.get("时间") or r.get("日期时间") or r.get("datetime") or r.get("time") or ""
        ts = str(ts0).strip().replace("/", "-")
        if not ts:
            continue
        if d_prefix not in ts:
            # AkShare may include multiple days; keep only the requested trade_date.
            continue
        out.append(
            {
                "ts": ts,
                "open": r.get("开盘") or r.get("open") or r.get("Open") or "",
                "high": r.get("最高") or r.get("high") or r.get("High") or "",
                "low": r.get("最低") or r.get("low") or r.get("Low") or "",
                "close": r.get("收盘") or r.get("close") or r.get("Close") or "",
                "volume": r.get("成交量") or r.get("volume") or r.get("Volume") or "",
                "amount": r.get("成交额") or r.get("amount") or r.get("Amount") or "",
            }
        )
    return out


def fetch_cn_market_breadth_eod(as_of: date) -> dict[str, Any]:
    """
    CN A-share market breadth (EOD-style snapshot).

    Returns:
      - date (YYYY-MM-DD)
      - up_count, down_count, flat_count, total_count
      - up_down_ratio (up/down, down==0 -> up)
      - raw (dict) optional
    """
    ak = _akshare()
    d = as_of.strftime("%Y-%m-%d")
    # Use spot snapshot as a stable source.
    df = ak.stock_zh_a_spot_em()
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
        # Keep digits / sign / dot only
        keep = []
        for ch in s:
            if ch.isdigit() or ch in (".", "-", "+"):
                keep.append(ch)
        try:
            return float("".join(keep))
        except Exception:
            return 0.0

    for r in rows:
        # Market activity (best-effort): sum turnover/volume from spot snapshot.
        # Note: this is NOT true historical EOD for arbitrary dates; it's the current snapshot when called.
        # We keep it consistent with breadth which is also computed from spot.
        turnover0 = r.get("成交额") or r.get("turnover") or r.get("成交额(元)") or ""
        vol0 = r.get("成交量") or r.get("volume") or ""
        total_turnover_cny += _parse_money_to_cny(turnover0)
        total_volume += _to_float0(vol0)

        # Breadth: only count rows with parseable change_pct.
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


def fetch_cn_yesterday_limitup_premium(as_of: date) -> dict[str, Any]:
    """
    Simplified yesterday limit-up premium:
      - take yesterday's limit-up pool
      - compute today's average change_pct of that pool

    Returns:
      - date (YYYY-MM-DD)
      - premium (percent, e.g. -1.2 means -1.2%)
      - count (pool size)
      - raw (dict)
    """
    ak = _akshare()
    d = as_of.strftime("%Y-%m-%d")
    # AkShare interfaces can vary; keep best-effort.
    if not hasattr(ak, "stock_zt_pool_em"):
        raise RuntimeError("AkShare missing stock_zt_pool_em. Please upgrade AkShare.")

    # "Yesterday" may be a non-trading day (weekend/holiday). Walk back to find the most recent
    # day with a non-empty limit-up pool (best-effort).
    chosen_y: date | None = None
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
        return {
            "date": d,
            "premium": 0.0,
            "count": 0,
            "raw": {"y": None, "searchedBackDays": 7},
        }

    # Map today's spot change_pct for those codes.
    spot = fetch_cn_a_spot()
    chg_map: dict[str, float] = {}
    for srow in spot:
        # srow.ticker already.
        v = str(srow.quote.get("change_pct") or "").strip().replace("%", "")
        try:
            chg_map[srow.ticker] = float(v)
        except Exception:
            continue
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
    """
    Simplified failed limit-up rate:
      - failed = count(ever limit-up today) - count(close limit-up today)
      - rate = failed / ever

    Returns:
      - date (YYYY-MM-DD)
      - failed_rate (percent)
      - ever_count, close_count
      - raw (dict)
    """
    ak = _akshare()
    d = as_of.strftime("%Y-%m-%d")
    # Preferred method: use dedicated "炸板" (failed limit-up) pool if available.
    # This avoids relying on "strong pool" APIs whose semantics vary by AkShare versions.
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
        # Some versions use a shorter name.
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
        # Fallback: infer "ever" via strong pool minus close pool (legacy behavior).
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
            "method": method,
            "failedRows": len(failed_rows),
            "closeRows": len(close_rows),
        },
    }


def fetch_cn_limitup_pool(as_of: date) -> list[dict[str, Any]]:
    """
    CN A-share limit-up pool (best-effort).

    Returns a list of dict with:
      - ticker
      - name (optional)
      - raw (original fields)
    """
    ak = _akshare()
    if not hasattr(ak, "stock_zt_pool_em"):
        raise RuntimeError("AkShare missing stock_zt_pool_em. Please upgrade AkShare.")
    df = ak.stock_zt_pool_em(date=_safe_trade_date(as_of))  # type: ignore[misc]
    rows = _to_records(df)
    out: list[dict[str, Any]] = []
    for r in rows:
        code = str(r.get("代码") or r.get("code") or r.get("股票代码") or "").strip()
        if not code:
            continue
        name = str(r.get("名称") or r.get("name") or r.get("股票简称") or "").strip()
        out.append({"ticker": code, "name": name, "raw": r})
    return out


def fetch_cn_industry_members(industry_name: str) -> list[str]:
    """
    Fetch industry board members (tickers) by industry name (best-effort).
    """
    ak = _akshare()
    fn = None
    if hasattr(ak, "stock_board_industry_cons_em"):
        fn = ak.stock_board_industry_cons_em  # type: ignore[attr-defined]
    elif hasattr(ak, "stock_board_industry_cons_ths"):
        fn = ak.stock_board_industry_cons_ths  # type: ignore[attr-defined]
    if fn is None:
        raise RuntimeError("AkShare missing industry constituents API. Please upgrade AkShare.")
    df = fn(industry_name)  # type: ignore[misc]
    rows = _to_records(df)
    out: list[str] = []
    for r in rows:
        code = str(r.get("代码") or r.get("code") or r.get("股票代码") or "").strip()
        if code:
            out.append(code)
    return out


def fetch_cn_concept_members(concept_name: str) -> list[str]:
    """
    Fetch concept board members (tickers) by concept name (best-effort).
    """
    ak = _akshare()
    fn = None
    if hasattr(ak, "stock_board_concept_cons_em"):
        fn = ak.stock_board_concept_cons_em  # type: ignore[attr-defined]
    elif hasattr(ak, "stock_board_concept_cons_ths"):
        fn = ak.stock_board_concept_cons_ths  # type: ignore[attr-defined]
    if fn is None:
        raise RuntimeError("AkShare missing concept constituents API. Please upgrade AkShare.")
    df = fn(concept_name)  # type: ignore[misc]
    rows = _to_records(df)
    out: list[str] = []
    for r in rows:
        code = str(r.get("代码") or r.get("code") or r.get("股票代码") or "").strip()
        if code:
            out.append(code)
    return out


def fetch_cn_industry_boards_spot() -> list[dict[str, Any]]:
    """
    CN A-share industry board spot rank (best-effort).
    Returns list of dict with:
      - name
      - change_pct
      - turnover
      - raw
    """
    ak = _akshare()
    fn = None
    if hasattr(ak, "stock_board_industry_name_em"):
        fn = ak.stock_board_industry_name_em  # type: ignore[attr-defined]
    elif hasattr(ak, "stock_board_industry_name_ths"):
        fn = ak.stock_board_industry_name_ths  # type: ignore[attr-defined]
    if fn is None:
        raise RuntimeError("AkShare missing industry board spot API. Please upgrade AkShare.")
    df = fn()  # type: ignore[misc]
    rows = _to_records(df)
    out: list[dict[str, Any]] = []
    for r in rows:
        name = str(r.get("板块名称") or r.get("行业名称") or r.get("name") or r.get("名称") or "").strip()
        if not name:
            continue
        chg = r.get("涨跌幅") or r.get("涨跌幅%") or r.get("change_pct") or ""
        turnover = r.get("成交额") or r.get("成交额(元)") or r.get("turnover") or ""
        out.append({"name": name, "change_pct": str(chg), "turnover": str(turnover), "raw": r})
    return out


def fetch_cn_concept_boards_spot() -> list[dict[str, Any]]:
    """
    CN A-share concept board spot rank (best-effort).
    Returns list of dict with:
      - name
      - change_pct
      - turnover
      - raw
    """
    ak = _akshare()
    fn = None
    if hasattr(ak, "stock_board_concept_name_em"):
        fn = ak.stock_board_concept_name_em  # type: ignore[attr-defined]
    elif hasattr(ak, "stock_board_concept_name_ths"):
        fn = ak.stock_board_concept_name_ths  # type: ignore[attr-defined]
    if fn is None:
        raise RuntimeError("AkShare missing concept board spot API. Please upgrade AkShare.")
    df = fn()  # type: ignore[misc]
    rows = _to_records(df)
    out: list[dict[str, Any]] = []
    for r in rows:
        name = str(r.get("板块名称") or r.get("概念名称") or r.get("name") or r.get("名称") or "").strip()
        if not name:
            continue
        chg = r.get("涨跌幅") or r.get("涨跌幅%") or r.get("change_pct") or ""
        turnover = r.get("成交额") or r.get("成交额(元)") or r.get("turnover") or ""
        out.append({"name": name, "change_pct": str(chg), "turnover": str(turnover), "raw": r})
    return out


def fetch_hk_spot() -> list[StockRow]:
    ak = _akshare()
    # NOTE: AkShare naming may change; keep this isolated.
    if not hasattr(ak, "stock_hk_spot_em"):
        raise RuntimeError("AkShare missing stock_hk_spot_em. Please upgrade AkShare.")
    df = ak.stock_hk_spot_em()
    out: list[StockRow] = []
    for r in _to_records(df):
        code = str(r.get("代码") or r.get("code") or "").strip()
        name = str(r.get("名称") or r.get("name") or "").strip()
        if not code or not name:
            continue
        # Some sources use 5-digit tickers; keep as-is.
        symbol = f"HK:{code}"
        quote = {
            "price": str(r.get("最新价") or ""),
            "change_pct": str(r.get("涨跌幅") or ""),
            "volume": str(r.get("成交量") or ""),
            "turnover": str(r.get("成交额") or ""),
            "market_cap": str(r.get("总市值") or ""),
        }
        out.append(
            StockRow(
                symbol=symbol,
                market="HK",
                ticker=code,
                name=name,
                currency="HKD",
                quote=quote,
            ),
        )
    return out


def fetch_cn_a_daily_bars(ticker: str, *, days: int = 60) -> list[BarRow]:
    ak = _akshare()
    end = date.today()
    start = end - timedelta(days=max(120, days * 2))
    start_date = start.strftime("%Y%m%d")
    end_date = end.strftime("%Y%m%d")

    df = ak.stock_zh_a_hist(
        symbol=ticker,
        period="daily",
        start_date=start_date,
        end_date=end_date,
        adjust="",
    )
    rows = _to_records(df)

    # Common columns: 日期 开盘 收盘 最高 最低 成交量 成交额
    out: list[BarRow] = []
    for r in rows:
        d = str(r.get("日期") or r.get("date") or "").strip()
        if not d:
            continue
        out.append(
            BarRow(
                date=d,
                open=str(r.get("开盘") or r.get("open") or ""),
                high=str(r.get("最高") or r.get("high") or ""),
                low=str(r.get("最低") or r.get("low") or ""),
                close=str(r.get("收盘") or r.get("close") or ""),
                volume=str(r.get("成交量") or r.get("volume") or ""),
                amount=str(r.get("成交额") or r.get("amount") or ""),
            ),
        )
    return out[-days:]


def fetch_hk_daily_bars(ticker: str, *, days: int = 60) -> list[BarRow]:
    ak = _akshare()
    # AkShare HK history APIs are less stable across versions; keep best-effort.
    if hasattr(ak, "stock_hk_hist"):
        end = date.today()
        start = end - timedelta(days=max(120, days * 2))
        try:
            df = ak.stock_hk_hist(
                symbol=ticker,
                start_date=start.strftime("%Y%m%d"),
                end_date=end.strftime("%Y%m%d"),
            )
        except Exception as e:
            raise RuntimeError(f"AkShare stock_hk_hist failed for {ticker}: {e}") from e
        rows = _to_records(df)
        out: list[BarRow] = []
        for r in rows:
            d = str(r.get("日期") or r.get("date") or "").strip()
            if not d:
                continue
            out.append(
                BarRow(
                    date=d,
                    open=str(r.get("开盘") or r.get("open") or ""),
                    high=str(r.get("最高") or r.get("high") or ""),
                    low=str(r.get("最低") or r.get("low") or ""),
                    close=str(r.get("收盘") or r.get("close") or ""),
                    volume=str(r.get("成交量") or r.get("volume") or ""),
                    amount=str(r.get("成交额") or r.get("amount") or ""),
                ),
            )
        return out[-days:]
    raise RuntimeError("AkShare missing stock_hk_hist. Please upgrade AkShare.")


def fetch_cn_a_chip_summary(
    ticker: str,
    *,
    days: int = 60,
    adjust: str = "",
) -> list[dict[str, str]]:
    """
    Eastmoney chip/cost distribution summary time series.

    Returns rows with:
    日期, 获利比例, 平均成本, 90成本-低/高/集中度, 70成本-低/高/集中度
    """
    ak = _akshare()
    # AkShare APIs can change across versions; keep best-effort compatibility.
    try:
        df = ak.stock_cyq_em(symbol=ticker, adjust=adjust)
    except TypeError:
        try:
            df = ak.stock_cyq_em(symbol=ticker)
        except TypeError:
            df = ak.stock_cyq_em(ticker)
    except Exception as e:
        raise RuntimeError(f"AkShare stock_cyq_em failed for {ticker}: {e}") from e
    rows = _to_records(df)
    out: list[dict[str, str]] = []
    for r in rows:
        d = str(r.get("日期") or "").strip()
        if not d:
            continue
        out.append(
            {
                "date": d,
                "profitRatio": str(r.get("获利比例") or ""),
                "avgCost": str(r.get("平均成本") or ""),
                "cost90Low": str(r.get("90成本-低") or ""),
                "cost90High": str(r.get("90成本-高") or ""),
                "cost90Conc": str(r.get("90集中度") or ""),
                "cost70Low": str(r.get("70成本-低") or ""),
                "cost70High": str(r.get("70成本-高") or ""),
                "cost70Conc": str(r.get("70集中度") or ""),
            },
        )
    return out[-max(1, int(days)) :]


def fetch_cn_a_fund_flow(ticker: str, *, days: int = 60) -> list[dict[str, str]]:
    """
    Eastmoney individual stock fund flow breakdown.

    Columns (AkShare):
    日期, 收盘价, 涨跌幅,
    主力净流入-净额/净占比,
    超大单净流入-净额/净占比,
    大单净流入-净额/净占比,
    中单净流入-净额/净占比,
    小单净流入-净额/净占比
    """
    ak = _akshare()
    # Best-effort market inference for A shares.
    market = "sh" if ticker.startswith("6") else "sz"
    try:
        df = ak.stock_individual_fund_flow(stock=ticker, market=market)
    except TypeError:
        df = ak.stock_individual_fund_flow(ticker, market=market)
    except Exception as e:
        raise RuntimeError(f"AkShare stock_individual_fund_flow failed for {ticker}: {e}") from e
    rows = _to_records(df)
    out: list[dict[str, str]] = []
    for r in rows:
        d = str(r.get("日期") or "").strip()
        if not d:
            continue
        out.append(
            {
                "date": d,
                "close": str(r.get("收盘价") or ""),
                "changePct": str(r.get("涨跌幅") or ""),
                "mainNetAmount": str(r.get("主力净流入-净额") or ""),
                "mainNetRatio": str(r.get("主力净流入-净占比") or ""),
                "superNetAmount": str(r.get("超大单净流入-净额") or ""),
                "superNetRatio": str(r.get("超大单净流入-净占比") or ""),
                "largeNetAmount": str(r.get("大单净流入-净额") or ""),
                "largeNetRatio": str(r.get("大单净流入-净占比") or ""),
                "mediumNetAmount": str(r.get("中单净流入-净额") or ""),
                "mediumNetRatio": str(r.get("中单净流入-净占比") or ""),
                "smallNetAmount": str(r.get("小单净流入-净额") or ""),
                "smallNetRatio": str(r.get("小单净流入-净占比") or ""),
            },
        )
    return out[-max(1, int(days)) :]


def _parse_money_to_cny(value: Any) -> float:
    """
    Parse common CN money formats to CNY (RMB) numeric value.

    Supported examples:
    - 12345.67
    - "56.71亿" -> 5.671e9
    - "123.4万" -> 1.234e6
    - "9876.5万元" -> 9.8765e7
    - "1.2亿(元)" (best-effort, strips non-numeric suffix)
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        f = float(value)
        return f if math.isfinite(f) else 0.0
    s = str(value).strip()
    if not s or s in ("-", "—", "N/A", "None"):
        return 0.0
    # Strip commas and whitespace
    s2 = s.replace(",", "").replace(" ", "")
    mult = 1.0
    # Normalize units
    if "亿" in s2:
        mult = 1e8
        s2 = s2.replace("亿", "")
    elif "万" in s2:
        mult = 1e4
        s2 = s2.replace("万元", "").replace("万", "")
    # Remove remaining non-numeric characters
    keep = []
    for ch in s2:
        if ch.isdigit() or ch in (".", "-", "+"):
            keep.append(ch)
    num_s = "".join(keep)
    try:
        return float(num_s) * mult
    except Exception:
        return 0.0


def _stable_industry_code(name: str) -> str:
    """
    Create a deterministic industry code when the data source does not provide one.
    """
    n = (name or "").strip()
    if not n:
        return ""
    return hashlib.sha1(n.encode("utf-8")).hexdigest()[:12]


def fetch_cn_industry_fund_flow_eod(as_of: date) -> list[dict[str, Any]]:
    """
    CN A-share industry fund flow (EOD-style snapshot).

    Source: Eastmoney sector fund flow rank (行业资金流, 今日).
    Returns a normalized list of:
    - date (YYYY-MM-DD)
    - industry_code
    - industry_name
    - net_inflow (CNY)
    - raw (dict) original row
    """
    ak = _akshare()

    # Eastmoney: board/sector fund flow rank (industry dimension)
    if not hasattr(ak, "stock_sector_fund_flow_rank"):
        raise RuntimeError("AkShare missing stock_sector_fund_flow_rank. Please upgrade AkShare.")
    df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
    rows = _to_records(df)

    out: list[dict[str, Any]] = []
    d = as_of.strftime("%Y-%m-%d")
    for r in rows:
        # Try common column names across AkShare versions / sources.
        name = str(
            r.get("名称")
            or r.get("行业名称")
            or r.get("行业")
            or r.get("板块名称")
            or r.get("板块")
            or ""
        ).strip()
        if not name:
            continue
        code = str(
            r.get("代码")
            or r.get("行业代码")
            or r.get("板块代码")
            or r.get("BK代码")
            or ""
        ).strip()
        if not code:
            code = _stable_industry_code(name)

        # Net inflow: best-effort mapping.
        # Eastmoney sector fund flow rank uses fields like "今日主力净流入-净额" (CNY).
        net = (
            r.get("今日主力净流入-净额")
            or r.get("今日主力净流入")
            or r.get("主力净流入-净额")
            or r.get("主力净流入")
            or r.get("资金净流入")
            or r.get("净流入")
            or r.get("今日净流入")
            or r.get("净额")
            or r.get("净流入额")
        )
        net_cny = _parse_money_to_cny(net)

        out.append(
            {
                "date": d,
                "industry_code": code,
                "industry_name": name,
                "net_inflow": net_cny,
                "raw": r,
            }
        )
    return out


def fetch_cn_industry_fund_flow_hist(industry_name: str, *, days: int = 10) -> list[dict[str, Any]]:
    """
    CN A-share industry historical fund flow time series (daily).

    Source: Eastmoney industry fund flow history.
    Returns a normalized list of:
    - date (YYYY-MM-DD)
    - net_inflow (CNY)
    - raw (dict)
    """
    ak = _akshare()
    if not hasattr(ak, "stock_sector_fund_flow_hist"):
        raise RuntimeError("AkShare missing stock_sector_fund_flow_hist. Please upgrade AkShare.")
    name = (industry_name or "").strip()
    if not name:
        return []
    df = ak.stock_sector_fund_flow_hist(symbol=name)
    rows = _to_records(df)
    out: list[dict[str, Any]] = []
    for r in rows:
        d = str(r.get("日期") or r.get("date") or "").strip()
        if not d:
            continue
        net = (
            r.get("主力净流入-净额")
            or r.get("主力净流入")
            or r.get("资金净流入")
            or r.get("净流入")
            or r.get("净额")
            or r.get("净流入额")
        )
        out.append({"date": d, "net_inflow": _parse_money_to_cny(net), "raw": r})
    return out[-max(1, int(days)) :]


