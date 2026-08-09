"""HK daily K-line sync via Tencent ifzq (qfq), avoiding akshare's V8 decoder.

akshare's ``stock_hk_daily`` (Sina) needs py_mini_racer (V8) to decode the
response, and that V8 build crashes the whole process on macOS 26 (SIGTRAP
in partition_alloc). Tencent's ``web.ifzq.gtimg.cn`` serves HK daily K-lines
as plain JSON — no JS decoding, no native deps. Data matches the Sina
realtime feed (verified against hq.sinajs.cn: open/close/high/low align).

Source priority in hk_daily.py: tencent → akshare (non-darwin only) →
yfinance → tushare.

API notes:
  - URL: ``/appstock/app/hkfqkline/get?param={symbol},day,{start},{end},{count},qfq``
  - One page caps at ~1000 rows; paging walks backwards via ``end``.
  - Row format: [date, open, close, high, low, volume, extra, pct_chg, amount]
  - ``amount`` is in 万元 (CNY 10k); we convert to raw units.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

from data_sync_service.db.daily import get_last_trade_date, upsert_from_dataframe

# Output columns for upsert_from_dataframe (must match db/daily.py order).
_DAILY_UPSERT_COLS = [
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount",
]

# Default look-back window for first-time full sync (matches hk_daily_ak).
_DEFAULT_BACKFILL_YEARS = 5

_KLINE_ENDPOINT = "https://web.ifzq.gtimg.cn/appstock/app/hkfqkline/get"
_PAGE_SIZE = 1000
_REQUEST_TIMEOUT_S = 15.0

# Row indices in the Tencent kline page format:
# [0] date, [1] open, [2] close, [3] high, [4] low, [5] volume,
# [6] extra dict, [7] pct_chg, [8] amount.
_IDX_DATE, _IDX_OPEN, _IDX_CLOSE, _IDX_HIGH, _IDX_LOW, _IDX_VOL, _IDX_AMOUNT = (0, 1, 2, 3, 4, 5, 8)

_AMOUNT_UNIT = 10_000.0  # Tencent amount is 万元.


def _ts_code_to_tx(ts_code: str) -> str | None:
    """Convert padded ts_code '00700.HK' to Tencent symbol 'hk00700'."""
    code = (ts_code or "").strip().upper()
    if not code.endswith(".HK"):
        return None
    ticker = code[:-3].strip()
    if not ticker or not ticker.isdigit():
        return None
    return f"hk{ticker.zfill(5)}"


def _today_iso() -> str:
    return datetime.now(UTC).date().isoformat()


def _default_backfill_cutoff(years: int = _DEFAULT_BACKFILL_YEARS) -> date:
    return datetime.now(UTC).date() - timedelta(days=365 * years)


def _fetch_kline_page(
    symbol: str,
    start: date,
    end: date,
    count: int = _PAGE_SIZE,
) -> list[list[Any]]:
    """Fetch one ascending page of HK daily bars [start, end] from Tencent.

    Returns rows oldest-first; empty list on any error (callers fall back).
    """
    import requests  # type: ignore[import-not-found]

    param = f"{symbol},day,{start.isoformat()},{end.isoformat()},{count},qfq"
    try:
        resp = requests.get(
            _KLINE_ENDPOINT,
            params={"param": param},
            timeout=_REQUEST_TIMEOUT_S,
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com/"},
            proxies={"http": None, "https": None},
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return []
    data = payload.get("data") or {}
    node = data.get(symbol) or {}
    if isinstance(node, dict):
        rows = node.get("qfqday") or node.get("day") or []
        return [list(r) for r in rows if isinstance(r, (list, tuple)) and len(r) >= 9]
    return []


def _fetch_kline_since(symbol: str, since: date, end: date) -> list[list[Any]]:
    """Fetch all bars on/after ``since``, paging backwards if truncated.

    Tencent caps a page at ~1000 rows; when a page returns exactly 1000 rows
    we walk an earlier window until we reach rows older than ``since``.
    """
    out: list[list[Any]] = []
    window_end = end
    while True:
        rows = _fetch_kline_page(symbol, since, window_end)
        if not rows:
            break
        out = rows + out  # rows ascending; earlier pages prepend
        if len(rows) < _PAGE_SIZE:
            break  # window fully covered
        oldest = rows[0][_IDX_DATE]
        try:
            prev_day = datetime.fromisoformat(str(oldest)).date() - timedelta(days=1)
        except ValueError:
            break
        if prev_day < since:
            break
        window_end = prev_day
    return out


def _rows_to_daily_rows(
    ts_code: str,
    rows: list[list[Any]],
    since: date | None,
) -> list[dict[str, Any]]:
    """Convert Tencent rows to our daily upsert dict list.

    Only rows on/after ``since + 1 day`` are kept (incremental sync).
    pre_close / change / pct_chg derive from the prior row's close.
    """
    out: list[dict[str, Any]] = []
    prev_close: float | None = None

    for row in rows:
        raw_date = row[_IDX_DATE]
        try:
            d = datetime.fromisoformat(str(raw_date).strip()).date()
        except ValueError:
            continue
        if since is not None and d <= since:
            try:
                prev_close = float(row[_IDX_CLOSE])
            except (TypeError, ValueError):
                prev_close = None
            continue

        try:
            o = float(row[_IDX_OPEN])
            c = float(row[_IDX_CLOSE])
            h = float(row[_IDX_HIGH])
            lo = float(row[_IDX_LOW])
            v = float(row[_IDX_VOL])
            amt_raw = row[_IDX_AMOUNT]
            amt = float(amt_raw) * _AMOUNT_UNIT if amt_raw is not None else None
        except (TypeError, ValueError):
            continue

        pre_close: float | None = prev_close
        change_val: float | None = None
        pct_chg: float | None = None
        if pre_close is not None and pre_close != 0:
            change_val = round(c - pre_close, 6)
            pct_chg = round((change_val / pre_close) * 100.0, 6)

        out.append(
            {
                "ts_code": ts_code,
                "trade_date": d.strftime("%Y-%m-%d"),
                "open": o,
                "high": h,
                "low": lo,
                "close": c,
                "pre_close": pre_close,
                "change": change_val,
                "pct_chg": pct_chg,
                "vol": v,
                "amount": amt,
            }
        )
        prev_close = c

    return out


def sync_hk_daily_for_ts_code_tx(
    ts_code: str,
    backfill_years: int = _DEFAULT_BACKFILL_YEARS,
) -> dict[str, Any]:
    """Incremental Tencent (ifzq) HK K-line sync for one ts_code.

    Behaviour mirrors hk_daily_ak:
      - If bars are cached, only fetch rows newer than last_trade_date.
      - If nothing cached, backfill up to ``backfill_years`` (default 5y).
      - Existing rows are never removed; upsert only inserts / updates.

    Returns ``{ok, updated, ts_code, source: 'tencent'}`` or ``{ok: False,
    error, ts_code}`` on failure (callers fall back to yfinance / tushare).
    """
    code = (ts_code or "").strip().upper()
    if not code.endswith(".HK"):
        return {"ok": False, "error": "ts_code must end with .HK", "ts_code": code}

    symbol = _ts_code_to_tx(code)
    if symbol is None:
        return {"ok": False, "error": "could not derive Tencent symbol", "ts_code": code}

    last_date = get_last_trade_date(code)
    end_date = datetime.now(UTC).date()
    if last_date is None:
        since = _default_backfill_cutoff(int(backfill_years))
    else:
        since = last_date
        if since >= end_date:
            return {"ok": True, "updated": 0, "skipped": True, "ts_code": code, "source": "tencent"}

    rows = _fetch_kline_since(symbol, since, end_date)
    if not rows:
        return {
            "ok": True,
            "updated": 0,
            "skipped": True,
            "ts_code": code,
            "source": "tencent",
            "message": "no new bars from tencent",
        }

    daily_rows = _rows_to_daily_rows(code, rows, since=last_date)
    if not daily_rows:
        return {
            "ok": True,
            "updated": 0,
            "skipped": True,
            "ts_code": code,
            "source": "tencent",
            "message": "no new bars from tencent",
        }

    import pandas as pd  # type: ignore[import-not-found]

    rows_df = pd.DataFrame(daily_rows, columns=_DAILY_UPSERT_COLS)
    updated = upsert_from_dataframe(rows_df)
    if updated > 0:
        from data_sync_service.service.trendok import clear_trendok_cache

        clear_trendok_cache()
    return {
        "ok": True,
        "updated": updated,
        "ts_code": code,
        "source": "tencent",
        "latest_trade_date": daily_rows[-1]["trade_date"],
        "backfill_years": backfill_years,
    }
