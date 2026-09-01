"""Twin-Star intraday approximate satellite signal (12:30 snapshot → 14:30 buy).

User execution model: buy at 14:30 at (approximately) the closing price. The
frozen backtest signal uses t-1 close, so to line the live signal up with the
14:30 execution we approximate the CURRENT day's close with a 12:30 (lunch
break) full-market snapshot and re-run the S-gap satellite screen against it.

- Snapshot: East Money push2 clist, full A-share list. open/high/low/pre_close
  are the REAL session values; the 12:30 price stands in for today's close.
- Signal: same formulas as state_bucket_track._day_features + twin_star_daily
  (gap = open/pre_close-1, amp = (high-low)/close, R-wide = close>MA20 share,
  low-vol bottom-1/3 bucket, limit-locked filter applied to the snapshot price).
- mv: t-1 stock_dailybasic (live mv is unavailable intraday — approximation).
- Cache: data/twin_star_intraday/{date}.json; the API serves it after 14:30
  and falls back to the t-1 signal when the snapshot is missing.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, timedelta
from typing import Any

from data_sync_service.service.em_push2_http import em_get_json
from data_sync_service.service.state_bucket_track import (
    R_WIDE_THRESHOLD,
    _day_features,
    _load_calendar,
    _load_mv,
    _load_rows,
)

logger = logging.getLogger(__name__)

TOP_N = 5
WARMUP_DAYS = 120
SNAPSHOT_PAGE_SIZE = 200
SNAPSHOT_MAX_PAGES = 40
_EM_SPOT_URLS = (
    "https://push2delay.eastmoney.com/api/qt/clist/get",
    "https://push2.eastmoney.com/api/qt/clist/get",
)
_EM_SPOT_URL = _EM_SPOT_URLS[1]
_EM_REFERER = "https://quote.eastmoney.com/center/gridlist.html"
# A-share markets: 深主板 m:0+t:6 · 创业板 m:0+t:80 · 沪主板 m:1+t:2 · 科创板 m:1+t:23
_EM_A_SHARE_FS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
_EM_FIELDS = ",".join(["f12", "f13", "f2", "f15", "f16", "f17", "f18", "f6"])

_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
    "data",
    "twin_star_intraday",
)


def _cache_path(today: date) -> str:
    return os.path.join(_CACHE_DIR, f"{today.isoformat()}.json")


def _em_snapshot_params(page_number: int) -> dict[str, str]:
    return {
        "pn": str(page_number),
        "pz": str(SNAPSHOT_PAGE_SIZE),
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "wbp2u": "|0|0|0|web",
        "fid": "f12",
        "fs": _EM_A_SHARE_FS,
        "fields": _EM_FIELDS,
        "_": str(int(time.time() * 1000)),
    }


def _em_snapshot_request(params: dict[str, str]) -> dict[str, Any]:
    errors: list[str] = []
    for url in _EM_SPOT_URLS:
        try:
            return em_get_json(url, params=params, referer=_EM_REFERER)
        except Exception as exc:  # noqa: BLE001
            host = url.split("//", 1)[-1].split("/", 1)[0]
            errors.append(f"{host}:{exc}")
    raise RuntimeError("; ".join(errors))


def fetch_market_snapshot() -> dict[str, dict[str, float | None]]:
    """Full A-share quote snapshot: {ts_code: {open, high, low, close, pre_close, amount}}.

    ``close`` = the price at snapshot time (12:30 lunch break → simulated close).
    """
    out: dict[str, dict[str, float | None]] = {}
    total = None
    for page in range(1, SNAPSHOT_MAX_PAGES + 1):
        j = _em_snapshot_request(_em_snapshot_params(page))
        data = (j or {}).get("data") or {}
        if total is None:
            total = int(data.get("total") or 0)
        diff = data.get("diff") or []
        if not diff:
            break
        for row in diff:
            code = str(row.get("f12") or "")
            market = str(row.get("f13") or "")
            if not code or code.startswith(("BJ", "SH", "SZ")):
                continue
            exch = "SH" if market == "1" else "SZ"
            ts = f"{code}.{exch}"
            close = _f(row.get("f2"))
            pre_close = _f(row.get("f18"))
            if close is None or close <= 0 or pre_close is None or pre_close <= 0:
                continue
            out[ts] = {
                "open": _f(row.get("f17")),
                "high": _f(row.get("f15")),
                "low": _f(row.get("f16")),
                "close": close,
                "pre_close": pre_close,
                "amount": _f(row.get("f6")),
            }
        if total and len(out) >= total:
            break
    logger.info("twin_star_intraday: snapshot %s rows", len(out))
    return out


def _is_trading_day(day: str) -> bool:
    """True when the trade_calendar says the SH exchange is open that day.

    The daily table has no bar for the current day intraday, so the calendar
    table (not `_load_calendar`) is the source of truth here.
    """
    try:
        import psycopg

        from data_sync_service.config import get_settings

        conn = psycopg.connect(get_settings().database_url)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT is_open FROM trade_calendar WHERE exchange='SSE' AND cal_date=%s",
                (day,),
            )
            row = cur.fetchone()
            return bool(row and row[0])
        finally:
            conn.close()
    except Exception:  # noqa: BLE001
        return True


def _f(val: Any) -> float | None:
    try:
        if val is None or val == "-":
            return None
        return float(val)
    except (TypeError, ValueError):
        return None


def build_intraday_sat(today: date | None = None) -> dict[str, Any] | None:
    """Simulated-close satellite signal from the 12:30 snapshot.

    Returns the same shape as twin_star_daily._sat_signal plus ``approx: True``,
    or None when the snapshot / data is unavailable.
    """
    today = today or date.today()
    w_start = (today - timedelta(days=WARMUP_DAYS)).isoformat()
    try:
        snapshot = fetch_market_snapshot()
        if not snapshot:
            logger.warning("twin_star_intraday: empty snapshot for %s", today)
            return None
        cal = _load_calendar(w_start, today.isoformat())
        per_ts = _load_rows(w_start, (today - timedelta(days=1)).isoformat())
        mv_map = _load_mv(w_start, (today - timedelta(days=1)).isoformat())
    except Exception as exc:  # noqa: BLE001
        logger.warning("twin_star_intraday: data load failed for %s (%s)", today, exc)
        return None
    if not cal:
        return None
    today_s = today.isoformat()
    if not _is_trading_day(today_s):
        logger.info("twin_star_intraday: %s not a trading day", today_s)
        return None

    # Merge the snapshot as today's approximate bar.
    mv_latest = sorted(mv_map.keys())[-1] if mv_map else None
    for ts, row in snapshot.items():
        series = per_ts.setdefault(ts, [])
        series.append(
            {
                "date": today_s,
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "pre_close": row["pre_close"],
                "amount": row["amount"],
            }
        )
        if mv_latest is not None:
            mv = mv_map.get(mv_latest, {}).get(ts)
            if mv is not None:
                mv_map.setdefault(today_s, {})[ts] = float(mv)

    date_idx = {ts: {r["date"]: i for i, r in enumerate(series)} for ts, series in per_ts.items()}
    day_all, breadth = _day_features(per_ts, mv_map, cal, today_s, date_idx)

    def _limit_locked(ts: str) -> bool:
        di = date_idx.get(ts, {}).get(today_s, -1)
        if di < 0:
            return False
        r = per_ts.get(ts, [])[di]
        pc = r.get("pre_close")
        if not pc or pc <= 0:
            return False
        lim = 0.20 if str(ts).startswith(("3", "68")) else 0.10
        return float(r["close"]) >= pc * (1 + lim - 0.004)

    gap_stocks = [(ts, d["amp"], d["gap"]) for ts, d in day_all.items() if d["is_gap"]]
    gap_stocks = [g for g in gap_stocks if not _limit_locked(g[0])]
    gap_stocks.sort(key=lambda x: x[1])
    qn = max(1, len(gap_stocks) // 3)
    candidates = []
    for ts, amp, gap in gap_stocks[: min(qn, TOP_N)]:
        series = per_ts.get(ts)
        idx = date_idx.get(ts, {}).get(today_s, -1)
        close = series[idx]["close"] if idx >= 0 and series else None
        candidates.append(
            {
                "ts": ts,
                "amp": round(float(amp) * 100, 2),
                "gapPct": round(float(gap) * 100, 2),
                "close": float(close) if close else None,
            }
        )
    return {
        "asOf": today_s,
        "gateOpen": breadth > R_WIDE_THRESHOLD,
        "breadth": round(float(breadth), 3),
        "gapCount": len(gap_stocks),
        "candidates": candidates,
        "approx": True,
        "note": None,
    }


def cache_intraday_sat(sat: dict[str, Any], today: date | None = None) -> None:
    today = today or date.today()
    os.makedirs(_CACHE_DIR, exist_ok=True)
    with open(_cache_path(today), "w", encoding="utf-8") as fh:
        json.dump(sat, fh, ensure_ascii=False)


def load_intraday_sat(today: date | None = None) -> dict[str, Any] | None:
    today = today or date.today()
    path = _cache_path(today)
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None