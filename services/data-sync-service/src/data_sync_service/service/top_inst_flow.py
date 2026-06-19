"""Sync dragon-tiger institutional flow for watchlist symbols (Tushare + East Money)."""

from __future__ import annotations

import json
import random
import time
import urllib.parse
import urllib.request
from datetime import UTC, date, datetime
from typing import Any

import pandas as pd  # type: ignore[import-not-found, import-untyped]
import tushare as ts  # type: ignore[import-not-found]

from data_sync_service.config import get_settings
from data_sync_service.db.sync_job_record import get_today_run, insert_record
from data_sync_service.db.top_inst import (
    ensure_table,
    upsert_daily_rows,
    upsert_summary_rows,
)
from data_sync_service.db.trade_calendar import get_open_dates, is_trading_day
from data_sync_service.db.watchlist_automation import list_registry

JOB_TYPE = "top_inst_watchlist"
YI = 100_000_000.0  # 1亿 CNY
LHASA_KEYWORDS = ("拉萨",)
EM_DATACENTER_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _today_yyyymmdd() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%d")


def _yyyymmdd_to_iso(s: str) -> str:
    s2 = str(s).strip()
    if len(s2) == 8 and s2.isdigit():
        return f"{s2[:4]}-{s2[4:6]}-{s2[6:8]}"
    return s2


def _iso_to_yyyymmdd(s: str) -> str:
    s2 = str(s).strip()
    if len(s2) == 10 and s2[4] == "-":
        return s2.replace("-", "")
    return s2


def _symbol_to_ts_code(symbol: str) -> str | None:
    s = (symbol or "").strip().upper()
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


def _fetch_top_inst_df(pro: Any, trade_date: str) -> pd.DataFrame:
    df = _with_retry(
        lambda: pro.top_inst(
            trade_date=trade_date,
            fields="trade_date,ts_code,exalter,buy,sell,net_buy,side,reason",
        )
    )
    if df is None or not isinstance(df, pd.DataFrame):
        return pd.DataFrame()
    return df


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
    with urllib.request.urlopen(req, timeout=20) as resp:
        raw = resp.read()
    j = json.loads(raw.decode("utf-8", errors="replace"))
    if not isinstance(j, dict):
        return {}
    return j


def fetch_em_lhb_buy_seats(*, ts_code: str, trade_date_iso: str) -> list[dict[str, Any]]:
    """Fetch top buy seats from East Money for one symbol on one trade date."""
    ticker = _ts_code_to_ticker(ts_code)
    if not ticker:
        return []
    params = {
        "sortColumns": "BUY",
        "sortTypes": "-1",
        "pageSize": "10",
        "pageNumber": "1",
        "reportName": "RPT_BILLBOARD_DAILYDETAILSBUY",
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "filter": f'(SECURITY_CODE="{ticker}")(TRADE_DATE=\'{trade_date_iso}\')',
    }
    try:
        j = _em_request(params)
    except Exception:
        return []
    result = j.get("result") if isinstance(j, dict) else None
    data = result.get("data") if isinstance(result, dict) else None
    if not isinstance(data, list):
        return []
    out: list[dict[str, Any]] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        exalter = str(
            row.get("OPERATEDEPT_NAME")
            or row.get("OPERATEDEPT_NAME_ABBR")
            or row.get("DEPT_NAME")
            or ""
        ).strip()
        buy = row.get("BUY") or row.get("BUY_AMT") or row.get("BUY_AMOUNT")
        try:
            buy_val = float(buy) if buy is not None else 0.0
        except (TypeError, ValueError):
            buy_val = 0.0
        if exalter:
            out.append({"exalter": exalter, "buy": buy_val})
    return out


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


def build_inst_flow_payload(summary: dict[str, Any] | None) -> dict[str, Any] | None:
    if not summary or not summary.get("on_board"):
        return None
    yi = summary.get("inst_net_buy_yi")
    label = str(summary.get("seat_label") or "").strip()
    if yi is None or not label:
        return None
    try:
        yi_f = float(yi)
    except (TypeError, ValueError):
        return None
    return {
        "tradeDate": str(summary.get("trade_date") or ""),
        "onBoard": True,
        "instNetBuyYi": round(yi_f, 2),
        "label": label,
        "lhasaDominant": bool(summary.get("lhasa_dominant")),
        "display": format_inst_flow_display(inst_net_buy_yi=yi_f, label=label),
    }


def _parse_cal_date(s: str) -> date:
    s2 = str(s).strip()
    if len(s2) == 8 and s2.isdigit():
        return date(int(s2[:4]), int(s2[4:6]), int(s2[6:8]))
    if len(s2) == 10 and s2[4] == "-":
        y, m, d = s2.split("-")
        return date(int(y), int(m), int(d))
    raise ValueError(f"invalid cal_date: {s}")


def _latest_cn_trade_date_yyyymmdd() -> str | None:
    today = _parse_cal_date(_today_yyyymmdd())
    open_dates = get_open_dates(exchange="SSE", start_date=date(2020, 1, 1), end_date=today)
    if not open_dates:
        return None
    return str(open_dates[-1])


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


def sync_top_inst_watchlist(*, force: bool = False, trade_date: str | None = None) -> dict[str, Any]:
    """
    Sync top_inst rows + summaries for watchlist CN symbols on trade_date.
    Requires TU_SHARE_API_KEY; East Money used for Lhasa seat detection.
    """
    ensure_table()
    settings = get_settings()
    if not settings.tu_share_api_key:
        return {"ok": False, "error": "missing_tu_share_api_key", "jobType": JOB_TYPE}

    if not force:
        existing = get_today_run(JOB_TYPE)
        if existing and existing.get("success"):
            return {
                "ok": True,
                "skipped": True,
                "reason": "already_synced_today",
                "jobType": JOB_TYPE,
            }

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
            message="empty_watchlist",
            last_ts_code=None,
        )
        return {"ok": True, "skipped": True, "reason": "empty_watchlist", "tradeDate": td_iso}

    watch_set = set(watchlist_codes)
    pro = ts.pro_api(settings.tu_share_api_key)

    try:
        df = _fetch_top_inst_df(pro, td)
    except Exception as e:
        insert_record(
            job_type=JOB_TYPE,
            success=False,
            message=str(e)[:500],
            last_ts_code=None,
        )
        return {"ok": False, "error": str(e), "jobType": JOB_TYPE, "tradeDate": td_iso}

    if df.empty:
        summaries = [
            {
                "trade_date": td_iso,
                "ts_code": code,
                "inst_net_buy": None,
                "inst_net_buy_yi": None,
                "seat_label": None,
                "lhasa_dominant": False,
                "on_board": False,
            }
            for code in watchlist_codes
        ]
        upsert_summary_rows(summaries)
        insert_record(job_type=JOB_TYPE, success=True, message="no_top_inst_rows", last_ts_code=None)
        return {
            "ok": True,
            "tradeDate": td_iso,
            "onBoardCount": 0,
            "dailyRows": 0,
            "summaryRows": len(summaries),
        }

    daily_rows: list[dict[str, Any]] = []
    on_board_codes: set[str] = set()
    grouped = df.groupby("ts_code") if "ts_code" in df.columns else []

    for ts_code, group in grouped:
        code = str(ts_code).strip()
        if code not in watch_set:
            continue
        on_board_codes.add(code)
        for _, row in group.iterrows():
            daily_rows.append(
                {
                    "trade_date": td_iso,
                    "ts_code": code,
                    "exalter": str(row.get("exalter") or "").strip(),
                    "buy": row.get("buy"),
                    "sell": row.get("sell"),
                    "net_buy": row.get("net_buy"),
                    "side": row.get("side"),
                    "reason": row.get("reason"),
                }
            )

    summary_rows: list[dict[str, Any]] = []
    for code in watchlist_codes:
        if code not in on_board_codes:
            summary_rows.append(
                {
                    "trade_date": td_iso,
                    "ts_code": code,
                    "inst_net_buy": None,
                    "inst_net_buy_yi": None,
                    "seat_label": None,
                    "lhasa_dominant": False,
                    "on_board": False,
                }
            )
            continue

        group = df[df["ts_code"] == code]
        inst_net = 0.0
        for _, row in group.iterrows():
            try:
                inst_net += float(row.get("net_buy") or 0.0)
            except (TypeError, ValueError):
                pass
        inst_net_yi = inst_net / YI

        buy_seats: list[dict[str, Any]] = []
        try:
            buy_seats = fetch_em_lhb_buy_seats(ts_code=code, trade_date_iso=td_iso)
        except Exception:
            buy_seats = []
        lhasa = detect_lhasa_dominant(buy_seats)
        label = classify_seat_label(inst_net_buy=inst_net, lhasa_dominant=lhasa)
        summary_rows.append(
            {
                "trade_date": td_iso,
                "ts_code": code,
                "inst_net_buy": inst_net,
                "inst_net_buy_yi": round(inst_net_yi, 2),
                "seat_label": label,
                "lhasa_dominant": lhasa,
                "on_board": True,
            }
        )
        time.sleep(0.05 + random.random() * 0.03)

    daily_n = upsert_daily_rows(daily_rows)
    summary_n = upsert_summary_rows(summary_rows)
    insert_record(
        job_type=JOB_TYPE,
        success=True,
        message=f"daily={daily_n} summary={summary_n}",
        last_ts_code=None,
    )
    return {
        "ok": True,
        "tradeDate": td_iso,
        "onBoardCount": len(on_board_codes),
        "dailyRows": daily_n,
        "summaryRows": summary_n,
    }
