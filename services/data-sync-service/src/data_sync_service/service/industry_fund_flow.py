from __future__ import annotations

import hashlib
import json
import random
import time
import urllib.parse
import urllib.request
from datetime import UTC, date, datetime
from typing import Any

from data_sync_service.db.industry_fund_flow import (
    get_dates_upto,
    get_latest_date,
    get_series_for_industry,
    get_top_rows,
    upsert_daily_rows,
)


def _now_iso() -> str:
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


def _parse_money_to_cny(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        f = float(value)
        return f if f == f else 0.0
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


def _stable_industry_code(name: str) -> str:
    n = (name or "").strip()
    if not n:
        return ""
    return hashlib.sha1(n.encode("utf-8")).hexdigest()[:12]


def _dataapi_getbkzj(key: str, code: str) -> list[dict[str, Any]]:
    url = "https://data.eastmoney.com/dataapi/bkzj/getbkzj"
    qs = urllib.parse.urlencode({"key": key, "code": code})
    req = urllib.request.Request(
        f"{url}?{qs}",
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://data.eastmoney.com/bkzj/hy.html",
            "Connection": "close",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        raw = resp.read()
    j = json.loads(raw.decode("utf-8", errors="replace"))
    data = j.get("data") if isinstance(j, dict) else None
    diff = (data or {}).get("diff") if isinstance(data, dict) else None
    return diff if isinstance(diff, list) else []


def _eastmoney_board_fund_flow_daykline(*, secid: str) -> list[dict[str, Any]]:
    url = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
    params = {
        "lmt": "0",
        "klt": "101",
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "secid": secid,
        "_": int(time.time() * 1000),
    }
    qs = urllib.parse.urlencode(params)
    req = urllib.request.Request(
        f"{url}?{qs}",
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://data.eastmoney.com/",
            "Connection": "close",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        raw = resp.read()
    j = json.loads(raw.decode("utf-8", errors="replace"))
    data = j.get("data") if isinstance(j, dict) else None
    klines = (data or {}).get("klines") if isinstance(data, dict) else None
    if not isinstance(klines, list):
        return []
    out: list[dict[str, Any]] = []
    for item in klines:
        s = str(item or "")
        if not s:
            continue
        parts = s.split(",")
        if len(parts) < 2:
            continue
        d = parts[0].strip()
        net = parts[1].strip()
        out.append({"date": d, "net_inflow": _parse_money_to_cny(net), "raw": {"kline": s}})
    return out


def _try_akshare_hist(industry_name: str, *, days: int) -> list[dict[str, Any]]:
    try:
        import akshare as ak  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "AkShare is required for industry fund flow history fallback.\n"
            "Install in data-sync-service: cd services/data-sync-service && uv add akshare\n"
            f"Original error: {e}"
        ) from e
    if not hasattr(ak, "stock_sector_fund_flow_hist"):
        raise RuntimeError("AkShare missing stock_sector_fund_flow_hist. Please upgrade AkShare.")
    df = _with_retry(lambda: ak.stock_sector_fund_flow_hist(symbol=industry_name), tries=3)
    rows = list(df.to_dict("records")) if hasattr(df, "to_dict") else []
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
    return out[-days:]


def fetch_cn_industry_fund_flow_eod(as_of: date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        rows = _with_retry(lambda: _dataapi_getbkzj("f62", "m:90 t:2"), tries=3)
    except Exception:
        rows = []
    out: list[dict[str, Any]] = []
    d = as_of.strftime("%Y-%m-%d")
    for r in rows:
        name = str(
            r.get("名称")
            or r.get("行业名称")
            or r.get("行业")
            or r.get("板块名称")
            or r.get("板块")
            or r.get("f14")
            or ""
        ).strip()
        if not name:
            continue
        code = str(
            r.get("代码")
            or r.get("行业代码")
            or r.get("板块代码")
            or r.get("BK代码")
            or r.get("f12")
            or ""
        ).strip()
        if not code:
            code = _stable_industry_code(name)
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
            or r.get("f62")
        )
        out.append(
            {
                "date": d,
                "industry_code": code,
                "industry_name": name,
                "net_inflow": _parse_money_to_cny(net),
                "raw": r,
            }
        )
    return out


def fetch_cn_industry_fund_flow_hist(
    industry_name: str,
    *,
    industry_code: str | None = None,
    days: int = 10,
) -> list[dict[str, Any]]:
    days2 = max(1, min(int(days), 60))
    code = (industry_code or "").strip()
    if code:
        if "." in code:
            secid = code
        else:
            secid = f"90.{code}"
        try:
            items = _with_retry(lambda: _eastmoney_board_fund_flow_daykline(secid=secid), tries=3)
            return items[-days2:]
        except Exception:
            pass
    name = (industry_name or "").strip()
    if not name:
        return []
    return _try_akshare_hist(name, days=days2)


def sync_cn_industry_fund_flow(*, days: int = 10, top_n: int = 10) -> dict[str, Any]:
    as_of = date.today()
    items = fetch_cn_industry_fund_flow_eod(as_of)
    updated_at = _now_iso()
    daily_rows = [
        {
            "date": it["date"],
            "industry_code": it["industry_code"],
            "industry_name": it["industry_name"],
            "net_inflow": it["net_inflow"],
            "updated_at": updated_at,
            "raw": it.get("raw") or {},
        }
        for it in items
    ]
    upsert_daily_rows(daily_rows)

    top_rows = sorted(items, key=lambda x: float(x.get("net_inflow") or 0.0), reverse=True)[: max(1, int(top_n))]
    hist_rows: list[dict[str, Any]] = []
    hist_failures = 0
    for r in top_rows:
        try:
            hist = fetch_cn_industry_fund_flow_hist(
                r.get("industry_name") or "",
                industry_code=r.get("industry_code") or None,
                days=days,
            )
            for h in hist:
                hist_rows.append(
                    {
                        "date": h.get("date") or "",
                        "industry_code": r.get("industry_code") or "",
                        "industry_name": r.get("industry_name") or "",
                        "net_inflow": h.get("net_inflow") or 0.0,
                        "updated_at": updated_at,
                        "raw": h.get("raw") or {},
                    }
                )
        except Exception:
            hist_failures += 1
            continue

    if hist_rows:
        upsert_daily_rows(hist_rows)
    return {
        "asOfDate": as_of.strftime("%Y-%m-%d"),
        "rows": len(daily_rows),
        "histRows": len(hist_rows),
        "histFailures": hist_failures,
    }


def get_cn_industry_fund_flow(*, days: int = 10, top_n: int = 30, as_of_date: str | None = None) -> dict[str, Any]:
    d = (as_of_date or "").strip() or (get_latest_date() or "")
    if not d:
        return {"asOfDate": "", "days": days, "topN": top_n, "dates": [], "top": []}
    dates = get_dates_upto(d, days)
    top_rows = get_top_rows(d, top_n)
    top: list[dict[str, Any]] = []
    for r in top_rows:
        name = r.get("industry_name") or ""
        series = get_series_for_industry(industry_name=name, dates=dates)
        sum10d = sum(float(x.get("net_inflow") or 0.0) for x in series)
        top.append(
            {
                "industryCode": r.get("industry_code") or "",
                "industryName": name,
                "netInflow": float(r.get("net_inflow") or 0.0),
                "sum10d": float(sum10d),
                "series10d": [{"date": x["date"], "netInflow": float(x["net_inflow"])} for x in series],
            }
        )
    return {"asOfDate": d, "days": days, "topN": top_n, "dates": dates, "top": top}
