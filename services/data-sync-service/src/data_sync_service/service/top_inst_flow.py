"""Sync dragon-tiger institutional flow for watchlist symbols (East Money HTTP only)."""

from __future__ import annotations

import json
import logging
import os
import random
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from data_sync_service.db.sync_job_record import get_today_run, insert_record
from data_sync_service.db.top_inst import (
    ensure_table,
    fetch_summaries_for_codes,
    upsert_daily_rows,
    upsert_summary_rows,
)
from data_sync_service.db.trade_calendar import get_open_dates, is_trading_day
from data_sync_service.db.watchlist_automation import list_registry

logger = logging.getLogger(__name__)


JOB_TYPE = "top_inst_watchlist"
YI = 100_000_000.0  # 1亿 CNY
LHASA_KEYWORDS = ("拉萨",)
INST_SEAT_KEYWORDS = ("机构专用",)
EM_DATACENTER_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
EM_REPORT_ORG_TRADE = "RPT_ORGANIZATION_TRADE_DETAILSNEW"
EM_REPORT_LHB_LIST = "RPT_DAILYBILLBOARD_DETAILSNEW"
EM_REPORT_BUY_SEATS = "RPT_BILLBOARD_DAILYDETAILSBUY"
EM_REPORT_SELL_SEATS = "RPT_BILLBOARD_DAILYDETAILSSELL"
EM_PAGE_SIZE = 500
EM_MAX_PAGES = 20
EM_SEAT_FETCH_MAX_WORKERS = 4
TOP_INST_PROVIDER_ENV = "TOP_INST_PROVIDER"
TUSHARE_TOKEN_ENV = "TUSHARE_TOKEN"
DEFAULT_TOP_INST_PROVIDERS = ("tushare", "eastmoney")
SUPPORTED_TOP_INST_PROVIDERS = {"tushare", "eastmoney"}


@dataclass(frozen=True)
class TopInstProviderResult:
    source: str
    lhb_tickers: set[str]
    org_by_ticker: dict[str, dict[str, Any]] = field(default_factory=dict)
    buy_seats_by_ts_code: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    inst_seats_by_ts_code: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    lhb_count: int | None = None
    org_trade_count: int | None = None
    seat_fetch_failures: int = 0


@dataclass(frozen=True)
class EastMoneySeatBundle:
    buy_seats: list[dict[str, Any]] = field(default_factory=list)
    inst_seats: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _today_yyyymmdd() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%d")


def _yyyymmdd_to_iso(s: str) -> str:
    s2 = str(s).strip()
    if len(s2) == 8 and s2.isdigit():
        return f"{s2[:4]}-{s2[4:6]}-{s2[6:8]}"
    return s2


def _symbol_to_ts_code(symbol: str) -> str | None:
    from data_sync_service.service.market_quotes import normalize_market_symbol

    s = normalize_market_symbol(symbol)
    if not s.startswith("CN:"):
        return None
    ticker = s.split(":", 1)[1].strip()
    if len(ticker) != 6 or not ticker.isdigit():
        return None
    suffix = "SH" if ticker.startswith("6") else "SZ"
    return f"{ticker}.{suffix}"


def _ts_code_to_ticker(ts_code: str) -> str | None:
    parts = (ts_code or "").strip().split(".")
    if len(parts) != 2:
        return None
    ticker = parts[0].strip()
    return ticker if len(ticker) == 6 and ticker.isdigit() else None


def _ticker_to_ts_code(ticker: str) -> str:
    t = str(ticker or "").strip()
    suffix = "SH" if t.startswith("6") else "SZ"
    return f"{t}.{suffix}"


def _parse_cal_date(s: str) -> date:
    s2 = str(s).strip()
    if len(s2) == 8 and s2.isdigit():
        return date(int(s2[:4]), int(s2[4:6]), int(s2[6:8]))
    if len(s2) == 10 and s2[4] == "-":
        y, m, d = s2.split("-")
        return date(int(y), int(m), int(d))
    raise ValueError(f"invalid cal_date: {s}")


def _em_trade_date_filter(trade_date_iso: str) -> str:
    """East Money filter for exact trade date (date part only)."""
    d = trade_date_iso[:10]
    return f"(TRADE_DATE='{d}')"


def _with_retry(fn, *, tries: int = 3, base_sleep_s: float = 0.5, max_sleep_s: float = 3.0):
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


def _is_lhasa_seat(exalter: str) -> bool:
    name = str(exalter or "")
    return any(k in name for k in LHASA_KEYWORDS)


def _is_inst_seat(exalter: str) -> bool:
    name = str(exalter or "")
    return any(k in name for k in INST_SEAT_KEYWORDS)


def _em_request(params: dict[str, str]) -> dict[str, Any]:
    url = f"{EM_DATACENTER_URL}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://data.eastmoney.com/stock/lhb.html",
            "Connection": "close",
        },
    )
    with urllib.request.urlopen(req, timeout=25) as resp:
        raw = resp.read()
    j = json.loads(raw.decode("utf-8", errors="replace"))
    if not isinstance(j, dict):
        return {}
    if not j.get("success"):
        msg = str(j.get("message") or "eastmoney_request_failed")
        code = j.get("code")
        if code in (9201,):  # empty result
            return {"success": True, "result": {"data": [], "pages": 0, "count": 0}}
        raise RuntimeError(f"eastmoney_error: {msg} (code={code})")
    return j


def _em_fetch_pages(
    *,
    report_name: str,
    filter_expr: str,
    sort_columns: str = "TRADE_DATE",
    sort_types: str = "-1",
    page_size: int = EM_PAGE_SIZE,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    page_number = 1
    total_pages = 1
    while page_number <= total_pages and page_number <= EM_MAX_PAGES:
        params = {
            "reportName": report_name,
            "columns": "ALL",
            "pageSize": str(page_size),
            "pageNumber": str(page_number),
            "sortColumns": sort_columns,
            "sortTypes": sort_types,
            "source": "WEB",
            "client": "WEB",
            "filter": filter_expr,
        }

        def _fetch(request_params: dict[str, str] = params) -> dict[str, Any]:
            return _em_request(request_params)

        j = _with_retry(_fetch)
        result = j.get("result") if isinstance(j, dict) else None
        data = result.get("data") if isinstance(result, dict) else None
        if not isinstance(data, list) or not data:
            break
        for row in data:
            if isinstance(row, dict):
                rows.append(row)
        try:
            total_pages = int((result or {}).get("pages") or 1)
        except (TypeError, ValueError):
            total_pages = 1
        page_number += 1
        time.sleep(0.04 + random.random() * 0.02)
    return rows


def fetch_em_lhb_tickers_on_date(trade_date_iso: str) -> set[str]:
    """All A-share tickers on dragon-tiger list for trade_date."""
    date_filter = _em_trade_date_filter(trade_date_iso)
    rows = _em_fetch_pages(report_name=EM_REPORT_LHB_LIST, filter_expr=date_filter)
    out: set[str] = set()
    for row in rows:
        code = str(row.get("SECURITY_CODE") or "").strip()
        if len(code) == 6 and code.isdigit():
            out.add(code)
    return out


def fetch_em_org_trades_on_date(trade_date_iso: str) -> dict[str, dict[str, Any]]:
    """Map SECURITY_CODE -> org trade row for trade_date."""
    date_filter = _em_trade_date_filter(trade_date_iso)
    rows = _em_fetch_pages(
        report_name=EM_REPORT_ORG_TRADE,
        filter_expr=date_filter,
        sort_columns="NET_BUY_AMT",
        sort_types="-1",
    )
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = str(row.get("SECURITY_CODE") or "").strip()
        if code:
            out[code] = row
    return out


def _seat_rows_from_report(
    *,
    report_name: str,
    ts_code: str,
    trade_date_iso: str,
    side: str,
) -> list[dict[str, Any]]:
    ticker = _ts_code_to_ticker(ts_code)
    if not ticker:
        return []
    filter_expr = (
        f'(SECURITY_CODE="{ticker}")'
        f"{_em_trade_date_filter(trade_date_iso)}"
    )
    sort_col = "BUY" if side == "buy" else "SELL"
    try:
        rows = _em_fetch_pages(
            report_name=report_name,
            filter_expr=filter_expr,
            sort_columns=sort_col,
            sort_types="-1",
            page_size=50,
        )
    except Exception:
        logger.warning("top inst flow fetch failed, returning empty", exc_info=True)
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        exalter = str(row.get("OPERATEDEPT_NAME") or row.get("OPERATEDEPT_NAME_ABBR") or "").strip()
        if not exalter:
            continue
        buy = row.get("BUY")
        sell = row.get("SELL")
        net = row.get("NET")
        try:
            buy_v = float(buy) if buy is not None else None
        except (TypeError, ValueError):
            buy_v = None
        try:
            sell_v = float(sell) if sell is not None else None
        except (TypeError, ValueError):
            sell_v = None
        try:
            net_v = float(net) if net is not None else None
        except (TypeError, ValueError):
            net_v = None
        out.append(
            {
                "exalter": exalter,
                "buy": buy_v,
                "sell": sell_v,
                "net_buy": net_v,
                "side": side,
                "reason": str(row.get("EXPLANATION") or row.get("EXPLAIN") or "") or None,
            }
        )
    return out


def fetch_em_lhb_buy_seats(*, ts_code: str, trade_date_iso: str) -> list[dict[str, Any]]:
    """Top buy seats from East Money for one symbol on one trade date."""
    rows = _seat_rows_from_report(
        report_name=EM_REPORT_BUY_SEATS,
        ts_code=ts_code,
        trade_date_iso=trade_date_iso,
        side="buy",
    )
    return [{"exalter": r["exalter"], "buy": r.get("buy") or 0.0} for r in rows if r.get("exalter")]


def fetch_em_inst_seat_rows(*, ts_code: str, trade_date_iso: str) -> list[dict[str, Any]]:
    """Institutional seat rows (机构专用) for daily table persistence."""
    buy_rows = _seat_rows_from_report(
        report_name=EM_REPORT_BUY_SEATS,
        ts_code=ts_code,
        trade_date_iso=trade_date_iso,
        side="buy",
    )
    sell_rows = _seat_rows_from_report(
        report_name=EM_REPORT_SELL_SEATS,
        ts_code=ts_code,
        trade_date_iso=trade_date_iso,
        side="sell",
    )
    out: list[dict[str, Any]] = []
    for row in buy_rows + sell_rows:
        if _is_inst_seat(str(row.get("exalter") or "")):
            out.append(row)
    return out


def fetch_em_seat_bundle(*, ts_code: str, trade_date_iso: str) -> EastMoneySeatBundle:
    try:
        return EastMoneySeatBundle(
            buy_seats=fetch_em_lhb_buy_seats(ts_code=ts_code, trade_date_iso=trade_date_iso),
            inst_seats=fetch_em_inst_seat_rows(ts_code=ts_code, trade_date_iso=trade_date_iso),
        )
    except Exception as e:  # noqa: BLE001
        return EastMoneySeatBundle(error=str(e)[:300])


def fetch_em_seat_bundles_parallel(
    ts_codes: list[str],
    *,
    trade_date_iso: str,
    max_workers: int = EM_SEAT_FETCH_MAX_WORKERS,
) -> dict[str, EastMoneySeatBundle]:
    wanted = [str(code).strip() for code in ts_codes if str(code).strip()]
    if not wanted:
        return {}
    workers = max(1, min(int(max_workers), len(wanted)))
    out: dict[str, EastMoneySeatBundle] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(fetch_em_seat_bundle, ts_code=ts_code, trade_date_iso=trade_date_iso): ts_code
            for ts_code in wanted
        }
        for future in as_completed(futures):
            ts_code = futures[future]
            try:
                out[ts_code] = future.result()
            except Exception as e:  # noqa: BLE001
                out[ts_code] = EastMoneySeatBundle(error=str(e)[:300])
    return out


def _df_records(df: Any) -> list[dict[str, Any]]:
    if df is None:
        return []
    if hasattr(df, "to_dict"):
        records = df.to_dict("records")
    elif isinstance(df, list):
        records = df
    else:
        return []
    return [r for r in records if isinstance(r, dict)]


def _side_label(value: Any) -> str:
    raw = str(value if value is not None else "").strip()
    if raw in {"0", "买入", "buy", "BUY"}:
        return "buy"
    if raw in {"1", "卖出", "sell", "SELL"}:
        return "sell"
    return raw or "buy"


def _tushare_trade_date(trade_date_iso: str) -> str:
    d = str(trade_date_iso or "").strip()
    if len(d) == 10 and d[4] == "-":
        return d.replace("-", "")
    return d[:8]


def _tushare_code_to_ticker(ts_code: Any) -> str | None:
    code = str(ts_code or "").strip().upper()
    ticker = code.split(".", 1)[0]
    return ticker if len(ticker) == 6 and ticker.isdigit() else None


def normalize_tushare_top_list_rows(rows: list[dict[str, Any]]) -> set[str]:
    out: set[str] = set()
    for row in rows:
        ticker = _tushare_code_to_ticker(row.get("ts_code") or row.get("code"))
        if ticker:
            out.add(ticker)
    return out


def normalize_tushare_top_inst_rows(
    rows: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    org_net_by_ticker: dict[str, float] = {}
    reasons_by_ticker: dict[str, str] = {}
    buy_seats_by_ts_code: dict[str, list[dict[str, Any]]] = {}
    inst_seats_by_ts_code: dict[str, list[dict[str, Any]]] = {}

    for row in rows:
        ticker = _tushare_code_to_ticker(row.get("ts_code") or row.get("code"))
        if not ticker:
            continue
        ts_code = str(row.get("ts_code") or _ticker_to_ts_code(ticker)).strip().upper()
        exalter = str(row.get("exalter") or row.get("营业部名称") or "").strip()
        if not exalter:
            continue
        side = _side_label(row.get("side") or row.get("买卖类型"))
        buy = _safe_float(row.get("buy") or row.get("买入额") or row.get("买入金额"))
        sell = _safe_float(row.get("sell") or row.get("卖出额") or row.get("卖出金额"))
        net_buy = _safe_float(row.get("net_buy") or row.get("net") or row.get("净额"))
        reason = str(row.get("reason") or row.get("上榜理由") or "").strip() or None
        normalized = {
            "exalter": exalter,
            "buy": buy,
            "sell": sell,
            "net_buy": net_buy,
            "side": side,
            "reason": reason,
        }
        if side == "buy":
            buy_seats_by_ts_code.setdefault(ts_code, []).append(normalized)
        if _is_inst_seat(exalter):
            inst_seats_by_ts_code.setdefault(ts_code, []).append(normalized)
            org_net_by_ticker[ticker] = org_net_by_ticker.get(ticker, 0.0) + float(net_buy or 0.0)
            if reason and ticker not in reasons_by_ticker:
                reasons_by_ticker[ticker] = reason

    org_by_ticker = {
        ticker: {"NET_BUY_AMT": net, "EXPLANATION": reasons_by_ticker.get(ticker)}
        for ticker, net in org_net_by_ticker.items()
    }
    return org_by_ticker, buy_seats_by_ts_code, inst_seats_by_ts_code


def fetch_tushare_top_inst_on_date(trade_date_iso: str) -> TopInstProviderResult:
    token = os.getenv(TUSHARE_TOKEN_ENV, "").strip()
    try:
        import tushare as ts  # type: ignore[import-not-found]
    except Exception as e:
        raise RuntimeError(f"tushare_import_failed: {e}") from e

    if token:
        pro = ts.pro_api(token)
    else:
        pro = ts.pro_api()
    trade_date = _tushare_trade_date(trade_date_iso)
    top_list_rows = _df_records(pro.top_list(trade_date=trade_date))
    top_inst_rows = _df_records(pro.top_inst(trade_date=trade_date))
    lhb_tickers = normalize_tushare_top_list_rows(top_list_rows)
    org_by_ticker, buy_seats_by_ts_code, inst_seats_by_ts_code = normalize_tushare_top_inst_rows(
        top_inst_rows
    )
    return TopInstProviderResult(
        source="tushare",
        lhb_tickers=lhb_tickers,
        org_by_ticker=org_by_ticker,
        buy_seats_by_ts_code=buy_seats_by_ts_code,
        inst_seats_by_ts_code=inst_seats_by_ts_code,
        lhb_count=len(lhb_tickers),
        org_trade_count=len(org_by_ticker),
    )


def fetch_eastmoney_top_inst_on_date(trade_date_iso: str) -> TopInstProviderResult:
    lhb_tickers = fetch_em_lhb_tickers_on_date(trade_date_iso)
    org_by_ticker = fetch_em_org_trades_on_date(trade_date_iso)
    return TopInstProviderResult(
        source="eastmoney",
        lhb_tickers=lhb_tickers,
        org_by_ticker=org_by_ticker,
        lhb_count=len(lhb_tickers),
        org_trade_count=len(org_by_ticker),
    )


def _configured_top_inst_providers() -> list[str]:
    raw = os.getenv(TOP_INST_PROVIDER_ENV, "").strip()
    names = [p.strip().lower() for p in raw.split(",") if p.strip()] if raw else list(DEFAULT_TOP_INST_PROVIDERS)
    out: list[str] = []
    for name in names:
        if name in SUPPORTED_TOP_INST_PROVIDERS and name not in out:
            out.append(name)
    return out or ["eastmoney"]


def fetch_top_inst_provider_result(trade_date_iso: str) -> tuple[TopInstProviderResult, list[str]]:
    errors: list[str] = []
    for provider in _configured_top_inst_providers():
        try:
            if provider == "tushare":
                return fetch_tushare_top_inst_on_date(trade_date_iso), errors
            if provider == "eastmoney":
                return fetch_eastmoney_top_inst_on_date(trade_date_iso), errors
        except Exception as e:
            errors.append(f"{provider}: {e}")
    raise RuntimeError("; ".join(errors) or "top_inst_provider_unavailable")


def detect_lhasa_dominant(buy_seats: list[dict[str, Any]]) -> bool:
    """True when Lhasa seats dominate the buy side (top seat or >50% of top-5 buy)."""
    if not buy_seats:
        return False
    sorted_seats = sorted(buy_seats, key=lambda x: float(x.get("buy") or 0.0), reverse=True)
    if _is_lhasa_seat(str(sorted_seats[0].get("exalter") or "")):
        return True
    total_buy = sum(float(s.get("buy") or 0.0) for s in sorted_seats[:5])
    if total_buy <= 0:
        return False
    lhasa_buy = sum(
        float(s.get("buy") or 0.0)
        for s in sorted_seats[:5]
        if _is_lhasa_seat(str(s.get("exalter") or ""))
    )
    return lhasa_buy / total_buy >= 0.5


def classify_seat_label(*, inst_net_buy: float, lhasa_dominant: bool) -> str:
    if inst_net_buy > 0:
        return "机构主买"
    if inst_net_buy < 0 and lhasa_dominant:
        return "机构净卖/拉萨主买"
    if inst_net_buy < 0:
        return "机构净卖"
    return "机构持平"


def format_inst_flow_display(*, inst_net_buy_yi: float, label: str) -> str:
    sign = "+" if inst_net_buy_yi >= 0 else ""
    return f"{sign}{inst_net_buy_yi:.1f}亿 ({label})"


def build_top_buy_seats_payload(
    seats: list[dict[str, Any]] | None,
    *,
    limit: int = 3,
) -> list[dict[str, Any]]:
    """Top buy-side LHB seats for API payload (name + tags)."""
    if not seats:
        return []
    ranked = sorted(
        [s for s in seats if isinstance(s, dict)],
        key=lambda s: float(s.get("buy") or 0.0),
        reverse=True,
    )
    out: list[dict[str, Any]] = []
    for seat in ranked[:limit]:
        exalter = str(seat.get("exalter") or "").strip()
        if not exalter:
            continue
        try:
            buy_amt = float(seat.get("buy") or 0.0)
        except (TypeError, ValueError):
            buy_amt = 0.0
        out.append(
            {
                "name": exalter,
                "buyAmt": buy_amt,
                "isLhasa": _is_lhasa_seat(exalter),
                "isInst": _is_inst_seat(exalter),
            }
        )
    return out


def _format_seats_summary(seats: list[dict[str, Any]]) -> str:
    if not seats:
        return ""
    parts: list[str] = []
    for seat in seats:
        name = str(seat.get("name") or "")
        short = "拉萨" if seat.get("isLhasa") else ("机构" if seat.get("isInst") else name[:8])
        parts.append(short)
    return " | 买: " + ", ".join(parts)


def build_inst_flow_payload(
    summary: dict[str, Any] | None,
    *,
    buy_seats: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build instFlow JSON; distinguish synced-off-board vs not-synced."""
    if summary is None:
        return {
            "onBoard": False,
            "synced": False,
            "display": "未同步",
        }

    trade_date = str(summary.get("trade_date") or "")
    if not summary.get("on_board"):
        return {
            "tradeDate": trade_date,
            "onBoard": False,
            "synced": True,
            "lhasaDominant": False,
            "display": "未上榜",
            "topBuySeats": [],
        }

    yi = summary.get("inst_net_buy_yi")
    label = str(summary.get("seat_label") or "").strip()
    if yi is None or not label:
        return {
            "tradeDate": trade_date,
            "onBoard": True,
            "synced": True,
            "display": "上榜(数据不完整)",
            "topBuySeats": build_top_buy_seats_payload(buy_seats),
        }
    try:
        yi_f = float(yi)
    except (TypeError, ValueError):
        return {
            "tradeDate": trade_date,
            "onBoard": True,
            "synced": True,
            "display": "上榜(数据不完整)",
            "topBuySeats": build_top_buy_seats_payload(buy_seats),
        }

    top_seats = build_top_buy_seats_payload(buy_seats)
    display = format_inst_flow_display(inst_net_buy_yi=yi_f, label=label)
    if top_seats:
        display += _format_seats_summary(top_seats)
    return {
        "tradeDate": trade_date,
        "onBoard": True,
        "synced": True,
        "instNetBuyYi": round(yi_f, 2),
        "label": label,
        "lhasaDominant": bool(summary.get("lhasa_dominant")),
        "display": display,
        "topBuySeats": top_seats,
    }


def _latest_cn_trade_date_yyyymmdd() -> str | None:
    today = _parse_cal_date(_today_yyyymmdd())
    open_dates = get_open_dates(exchange="SSE", start_date=date(2020, 1, 1), end_date=today)
    if not open_dates:
        return None
    last = open_dates[-1]
    return last.strftime("%Y%m%d") if hasattr(last, "strftime") else str(last)


def _watchlist_ts_codes() -> list[str]:
    registry = list_registry()
    codes: list[str] = []
    seen: set[str] = set()
    for item in registry:
        ts = _symbol_to_ts_code(str(item.get("symbol") or ""))
        if ts and ts not in seen:
            seen.add(ts)
            codes.append(ts)
    return codes


def _safe_float(v: Any) -> float | None:
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def _missing_summary_codes(ts_codes: list[str], *, trade_date_iso: str) -> list[str]:
    existing = fetch_summaries_for_codes(ts_codes, trade_date=trade_date_iso)
    return [code for code in ts_codes if code not in existing]


def sync_top_inst_watchlist(*, force: bool = False, trade_date: str | None = None) -> dict[str, Any]:
    """Sync dragon-tiger institutional flow for watchlist CN symbols."""
    ensure_table()

    td = str(trade_date or _latest_cn_trade_date_yyyymmdd() or "").strip()
    if not td:
        return {"ok": False, "error": "no_trade_date", "jobType": JOB_TYPE}

    td_iso = _yyyymmdd_to_iso(td)
    cal = _parse_cal_date(td)
    open_flag = is_trading_day(exchange="SSE", cal_date=cal)
    if open_flag is False:
        return {"ok": True, "skipped": True, "reason": "not_trading_day", "tradeDate": td_iso}

    watchlist_codes = _watchlist_ts_codes()
    if not watchlist_codes:
        insert_record(
            job_type=JOB_TYPE,
            success=True,
            last_ts_code=None,
            error_message=None,
        )
        return {"ok": True, "skipped": True, "reason": "empty_watchlist", "tradeDate": td_iso}

    if not force:
        existing = get_today_run(JOB_TYPE)
        missing_codes = _missing_summary_codes(watchlist_codes, trade_date_iso=td_iso)
        if existing and existing.get("success") and not missing_codes:
            return {
                "ok": True,
                "skipped": True,
                "reason": "already_synced_today",
                "jobType": JOB_TYPE,
                "tradeDate": td_iso,
                "covered": len(watchlist_codes),
            }

    try:
        provider_result, provider_errors = fetch_top_inst_provider_result(td_iso)
    except Exception as e:
        insert_record(
            job_type=JOB_TYPE,
            success=False,
            last_ts_code=None,
            error_message=str(e)[:500],
        )
        return {
            "ok": False,
            "error": str(e),
            "jobType": JOB_TYPE,
            "tradeDate": td_iso,
            "source": "provider",
        }

    lhb_tickers = provider_result.lhb_tickers
    org_by_ticker = provider_result.org_by_ticker
    if not lhb_tickers:
        error = "suspicious_empty_lhb"
        msg = f"{error}: source={provider_result.source}, tradeDate={td_iso}"
        insert_record(
            job_type=JOB_TYPE,
            success=False,
            last_ts_code=None,
            error_message=msg[:500],
        )
        return {
            "ok": False,
            "skipped": True,
            "reason": error,
            "jobType": JOB_TYPE,
            "tradeDate": td_iso,
            "source": provider_result.source,
            "provider": provider_result.source,
            "fallbackUsed": bool(provider_errors),
            "providerErrors": provider_errors,
            "lhbCount": 0,
            "orgTradeCount": provider_result.org_trade_count or len(org_by_ticker),
            "summaryRows": 0,
            "dailyRows": 0,
        }

    daily_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    on_board_count = 0
    seat_fetch_failures = provider_result.seat_fetch_failures
    on_board_codes = [
        ts_code
        for ts_code in watchlist_codes
        if (ticker := _ts_code_to_ticker(ts_code)) and ticker in lhb_tickers
    ]
    em_seat_bundles = (
        fetch_em_seat_bundles_parallel(on_board_codes, trade_date_iso=td_iso)
        if provider_result.source == "eastmoney"
        else {}
    )

    for ts_code in watchlist_codes:
        ticker = _ts_code_to_ticker(ts_code)
        if not ticker:
            continue
        on_board = ticker in lhb_tickers
        if not on_board:
            summary_rows.append(
                {
                    "trade_date": td_iso,
                    "ts_code": ts_code,
                    "inst_net_buy": None,
                    "inst_net_buy_yi": None,
                    "seat_label": None,
                    "lhasa_dominant": False,
                    "on_board": False,
                }
            )
            continue

        on_board_count += 1
        org_row = org_by_ticker.get(ticker) or {}
        inst_net = _safe_float(org_row.get("NET_BUY_AMT"))
        if inst_net is None:
            inst_net = 0.0
        inst_net_yi = inst_net / YI

        buy_seats = provider_result.buy_seats_by_ts_code.get(ts_code, [])
        inst_seats = provider_result.inst_seats_by_ts_code.get(ts_code, [])
        if provider_result.source == "eastmoney":
            bundle = em_seat_bundles.get(ts_code, EastMoneySeatBundle(error="missing_seat_bundle"))
            if bundle.error:
                seat_fetch_failures += 1
                buy_seats = []
                inst_seats = []
            else:
                buy_seats = bundle.buy_seats
                inst_seats = bundle.inst_seats

        lhasa = detect_lhasa_dominant(buy_seats)
        label = classify_seat_label(inst_net_buy=inst_net, lhasa_dominant=lhasa)
        reason = str(org_row.get("EXPLANATION") or "") or None

        for seat in inst_seats:
            daily_rows.append(
                {
                    "trade_date": td_iso,
                    "ts_code": ts_code,
                    "exalter": seat.get("exalter"),
                    "buy": seat.get("buy"),
                    "sell": seat.get("sell"),
                    "net_buy": seat.get("net_buy"),
                    "side": seat.get("side"),
                    "reason": seat.get("reason") or reason,
                }
            )

        summary_rows.append(
            {
                "trade_date": td_iso,
                "ts_code": ts_code,
                "inst_net_buy": inst_net,
                "inst_net_buy_yi": round(inst_net_yi, 2),
                "seat_label": label,
                "lhasa_dominant": lhasa,
                "on_board": True,
            }
        )

    daily_n = upsert_daily_rows(daily_rows)
    summary_n = upsert_summary_rows(summary_rows)
    insert_record(
        job_type=JOB_TYPE,
        success=True,
        last_ts_code=None,
        error_message=None,
    )
    return {
        "ok": True,
        "tradeDate": td_iso,
        "source": provider_result.source,
        "provider": provider_result.source,
        "fallbackUsed": bool(provider_errors),
        "providerErrors": provider_errors,
        "onBoardCount": on_board_count,
        "lhbCount": provider_result.lhb_count if provider_result.lhb_count is not None else len(lhb_tickers),
        "orgTradeCount": (
            provider_result.org_trade_count
            if provider_result.org_trade_count is not None
            else len(org_by_ticker)
        ),
        "seatFetchFailures": seat_fetch_failures,
        "dailyRows": daily_n,
        "summaryRows": summary_n,
    }
