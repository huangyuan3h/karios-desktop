from __future__ import annotations

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
        try:
            df = ak.stock_individual_fund_flow(symbol=ticker, market=market)
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


