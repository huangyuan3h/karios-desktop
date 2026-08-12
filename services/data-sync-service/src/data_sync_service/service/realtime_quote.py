"""Realtime quote via tushare `realtime_quote` (query-only; no DB writes).

HK tickers prefer Sina Finance `hq.sinajs.cn` because tushare `realtime_quote`
does not reliably return HK rows on every key/region, and East Money push2 HK
prices drift from the feeds used by Tonghuashun / Xueqiu / Futu. Sina is the
same source most popular CN stock apps use for HK, so the displayed price
matches the user's mental model of "the HK closing price".
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pandas as pd
import tushare as ts

from data_sync_service.config import get_settings
from data_sync_service.service.em_push2_http import em_get_json
from data_sync_service.service.sina_http import build_hq_url, parse_hq_lines, sina_get_text

logger = logging.getLogger(__name__)

_SINA_HK_QUOTE_TTL_S = 30.0
_SINA_HK_QUOTE_BATCH_SIZE = 50
_SINA_HK_QUOTE_CACHE_LOCK = threading.Lock()
_SINA_HK_QUOTE_CACHE: dict[str, Any] = {
    "fetched_at": 0.0,
    "quotes": {},
}

# HK indices served by Sina as `hq_str_hkHSI` / `hq_str_hkHSTECH` (bare codes
# with no exchange suffix). Tushare's realtime_quote crashes on them (it
# split()s on "." internally) and East Money needs a different market prefix,
# so they get their own Sina path (2026-08-11).
_HK_INDEX_CODES: frozenset[str] = frozenset({"HSI", "HSTECH"})
_HK_INDEX_TTL_S = 30.0
_HK_INDEX_CACHE_LOCK = threading.Lock()
_HK_INDEX_CACHE: dict[str, Any] = {
    "fetched_at": 0.0,
    "quotes": {},
}


def _clear_sina_hk_quote_cache_impl() -> None:
    with _SINA_HK_QUOTE_CACHE_LOCK:
        _SINA_HK_QUOTE_CACHE["fetched_at"] = 0.0
        _SINA_HK_QUOTE_CACHE["quotes"] = {}
    with _HK_INDEX_CACHE_LOCK:
        _HK_INDEX_CACHE["fetched_at"] = 0.0
        _HK_INDEX_CACHE["quotes"] = {}


def _as_str(val: Any) -> str | None:
    if val is None or pd.isna(val):
        return None
    s = str(val).strip()
    return s or None


def _get(obj: Any, *keys: str) -> Any:
    for k in keys:
        if k in obj and obj[k] is not None:
            return obj[k]
    return None


def _is_hk(ts_code: str) -> bool:
    return ts_code.strip().upper().endswith(".HK")


def _em_hk_fields() -> str:
    # f43 price, f44 high, f45 low, f46 open, f47 volume, f48 amount,
    # f57 code, f58 name, f60 pre_close, f169 change, f170 pct_chg.
    return "f43,f44,f45,f46,f47,f48,f57,f58,f60,f169,f170"


def _fetch_em_hk_quote(ts_code: str) -> dict[str, Any] | None:
    """Try East Money push2 for a single HK ticker. Returns normalized quote or None."""
    code = ts_code.strip().upper()
    if not code.endswith(".HK"):
        return None
    ticker = code[:-3]
    secid = f"116.{ticker}"
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    params = {"secid": secid, "fields": _em_hk_fields(), "invt": "2", "fltt": "2"}
    try:
        j = em_get_json(url, params=params, referer="https://quote.eastmoney.com/")
    except Exception:
        return None
    data = j.get("data") if isinstance(j, dict) else None
    if not isinstance(data, dict):
        return None

    def _num(key: str) -> float | None:
        v = data.get(key)
        if v in (None, "", "-"):
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    price = _num("f43")
    if price is None or price <= 0:
        return None
    high = _num("f44")
    low = _num("f45")
    open_ = _num("f46")
    pre_close = _num("f60")
    change = _num("f169")
    pct_chg = _num("f170")
    volume = _num("f47")
    amount = _num("f48")

    return {
        "ts_code": code,
        "price": _as_str(price),
        "open": _as_str(open_),
        "high": _as_str(high),
        "low": _as_str(low),
        "pre_close": _as_str(pre_close),
        "change": _as_str(change),
        "pct_chg": _as_str(pct_chg),
        "volume": _as_str(volume),
        "amount": _as_str(amount),
        "trade_time": None,
    }


def _split_hk(codes: list[str]) -> tuple[list[str], list[str]]:
    """Split codes into (hk_codes, other_codes), preserving order, dedup.

    HK = ``*.HK`` stocks plus the bare HK index codes (HSI / HSTECH).
    """
    hk: list[str] = []
    other: list[str] = []
    seen_hk: set[str] = set()
    seen_other: set[str] = set()
    for c in codes:
        up = c.strip().upper()
        if not up:
            continue
        if up.endswith(".HK") or up in _HK_INDEX_CODES:
            if up not in seen_hk:
                seen_hk.add(up)
                hk.append(up)
        else:
            if up not in seen_other:
                seen_other.add(up)
                other.append(up)
    return hk, other


def _parse_sina_hk_payload(ticker: str, payload: str) -> dict[str, Any] | None:
    """Parse a single `hq_str_hk<5digit>="..."` payload into the normalized quote dict.

    Field layout (Sina HK):
      0  English name
      1  Chinese name (GBK-decoded upstream)
      2  open (今开)
      3  pre_close (昨收)
      4  high (最高)
      5  low (最低)
      6  latest price (最新价)
      7  change amount (涨跌额)
      8  change pct (涨跌幅, %)
      9  bid price (买一)
      10 ask price (卖一)
      11 amount (成交额, CNY/HKD)
      12 volume (成交量, shares)
      13-14 unused
      15 52-week high
      16 52-week low
      17 trade date (YYYY/MM/DD)
      18 trade time (HH:MM or HH:MM:SS)
    """
    fields = payload.split(",")
    if len(fields) < 19:
        return None

    def _num(idx: int) -> float | None:
        try:
            v = float(fields[idx])
        except (ValueError, IndexError):
            return None
        # Sina uses 0.000 as "missing" for some fields; treat as None to avoid
        # noise in pct_chg / pre_close comparisons on the frontend.
        return v if v != 0 else None

    price = _num(6)
    if price is None:
        return None

    open_ = _num(2)
    pre_close = _num(3)
    high = _num(4)
    low = _num(5)
    change = _num(7)
    pct_chg = _num(8)
    amount = _num(11)
    volume = _num(12)

    date_raw = fields[17].strip() if len(fields) > 17 else ""
    time_raw = fields[18].strip() if len(fields) > 18 else ""
    trade_time: str | None = None
    if date_raw and time_raw:
        date_iso = date_raw.replace("/", "-")
        # Some rows report HH:MM only; pad to HH:MM:SS so frontend parsing matches tushare.
        if len(time_raw) == 5 and time_raw.count(":") == 1:
            time_raw = f"{time_raw}:00"
        trade_time = f"{date_iso} {time_raw}"

    code = ticker.zfill(5)
    return {
        "ts_code": f"{code}.HK",
        "price": _as_str(price),
        "open": _as_str(open_),
        "high": _as_str(high),
        "low": _as_str(low),
        "pre_close": _as_str(pre_close),
        "change": _as_str(change),
        "pct_chg": _as_str(pct_chg),
        "volume": _as_str(volume),
        "amount": _as_str(amount),
        "trade_time": trade_time,
    }


def _sina_hk_quotes_fresh(
    tickers: list[str], *, force: bool = False
) -> dict[str, dict[str, Any]]:
    """Fetch the given HK tickers from Sina Finance. Returns ticker -> quote.

    Result is cached per ticker for `_SINA_HK_QUOTE_TTL_S` so repeated calls
    (e.g. market_regime polling every few seconds) don't hammer Sina.
    """
    requested = sorted({t.zfill(5) for t in tickers if t})
    if not requested:
        return {}

    with _SINA_HK_QUOTE_CACHE_LOCK:
        cached = _SINA_HK_QUOTE_CACHE
        now = time.monotonic()
        cache_age = now - float(cached["fetched_at"])
        out: dict[str, dict[str, Any]] = {}
        missing: list[str] = []
        if cache_age < _SINA_HK_QUOTE_TTL_S:
            for t in requested:
                hit = cached["quotes"].get(t)
                if hit is not None:
                    out[t] = hit
                else:
                    missing.append(t)
            if not missing and not force:
                return out
        else:
            missing = list(requested)

    if not missing:
        return out

    fetched: dict[str, dict[str, Any]] = {}
    last_error: str | None = None
    # Sina accepts long `list=` strings but truncates around 60 tickers, so chunk.
    for i in range(0, len(missing), _SINA_HK_QUOTE_BATCH_SIZE):
        part = missing[i : i + _SINA_HK_QUOTE_BATCH_SIZE]
        try:
            text = sina_get_text(build_hq_url(part), timeout=10.0)
            for ticker, payload in parse_hq_lines(text):
                quote = _parse_sina_hk_payload(ticker, payload)
                if quote is not None:
                    fetched[ticker.zfill(5)] = quote
        except Exception as e:  # noqa: BLE001
            last_error = str(e)

    with _SINA_HK_QUOTE_CACHE_LOCK:
        if fetched:
            _SINA_HK_QUOTE_CACHE["quotes"].update(fetched)
            _SINA_HK_QUOTE_CACHE["fetched_at"] = time.monotonic()
            _SINA_HK_QUOTE_CACHE["last_error"] = None
        elif last_error:
            _SINA_HK_QUOTE_CACHE["last_error"] = last_error
    out.update(fetched)
    return out


def _fetch_sina_hk_quote(ts_code: str) -> dict[str, Any] | None:
    """Fetch a single HK ticker from Sina Finance (cached). Returns None on miss."""
    code = ts_code.strip().upper()
    if not code.endswith(".HK"):
        return None
    ticker = code[:-3]
    return _sina_hk_quotes_fresh([ticker]).get(ticker.zfill(5))


def _parse_sina_hk_index_payload(code: str, payload: str) -> dict[str, Any] | None:
    """Parse a single `hq_str_hkHSI="..."` payload into the normalized quote dict.

    HK INDEX layout differs from HK stocks (empirically verified 2026-08-11:
    HSI payload price 25773.561, pre_close 25937.49, change -163.93 — exact):
      0  code          1  name
      2  open          3  pre_close
      4  high          5  low
      6  latest price  7  change amount
      8  change pct    9-10 zeros
      11 volume        12 amount
      13-14 zeros      15 52w high   16 52w low
      17 trade date (YYYY/MM/DD)      18 trade time (HH:MM[:SS])
    """
    fields = payload.split(",")
    if len(fields) < 17:
        return None

    def _num(idx: int) -> float | None:
        try:
            v = float(fields[idx])
        except (ValueError, IndexError):
            return None
        return v if v != 0 else None

    price = _num(6)
    if price is None:
        return None

    date_raw = fields[17].strip() if len(fields) > 17 else ""
    time_raw = fields[18].strip() if len(fields) > 18 else ""
    trade_time: str | None = None
    if date_raw and time_raw:
        date_iso = date_raw.replace("/", "-")
        if len(time_raw) == 5 and time_raw.count(":") == 1:
            time_raw = f"{time_raw}:00"
        trade_time = f"{date_iso} {time_raw}"

    return {
        "ts_code": code,
        "price": _as_str(price),
        "open": _as_str(_num(2)),
        "high": _as_str(_num(4)),
        "low": _as_str(_num(5)),
        "pre_close": _as_str(_num(3)),
        "change": _as_str(_num(7)),
        "pct_chg": _as_str(_num(8)),
        "volume": _as_str(_num(11)),
        "amount": _as_str(_num(12)),
        "trade_time": trade_time,
    }


def _sina_hk_index_quotes(codes: list[str]) -> list[dict[str, Any]]:
    """Realtime quotes for HK indices (HSI / HSTECH) from Sina Finance.

    Tushare crashes on bare index codes and East Money uses a different
    market prefix for indices — Sina `hq_str_hkHSI` is the matching feed.
    Cached for ``_HK_INDEX_TTL_S`` like the stock quotes.
    """
    requested = sorted({str(c).strip().upper() for c in codes} & set(_HK_INDEX_CODES))
    if not requested:
        return []
    with _HK_INDEX_CACHE_LOCK:
        cached = _HK_INDEX_CACHE
        now = time.monotonic()
        fresh = now - float(cached["fetched_at"]) < _HK_INDEX_TTL_S
        hits = [cached["quotes"][c] for c in requested if cached["quotes"].get(c) is not None]
        if fresh and len(hits) == len(requested):
            return hits
        missing = [c for c in requested if not fresh or cached["quotes"].get(c) is None]

    fetched: dict[str, dict[str, Any]] = {}
    try:
        text = sina_get_text(build_hq_url(missing), timeout=10.0)
        for ticker, payload in parse_hq_lines(text, allow_index=True):
            quote = _parse_sina_hk_index_payload(ticker, payload)
            if quote is not None:
                fetched[str(quote.get("ts_code"))] = quote
    except Exception as exc:  # noqa: BLE001
        logger.warning("sina HK index quotes failed for %s: %s", ",".join(missing), exc)

    out = list(hits)
    if fetched:
        with _HK_INDEX_CACHE_LOCK:
            _HK_INDEX_CACHE["quotes"].update(fetched)
            _HK_INDEX_CACHE["fetched_at"] = time.monotonic()
        seen = {str(q.get("ts_code")) for q in out}
        for c in requested:
            q = fetched.get(c)
            if q is not None and c not in seen:
                out.append(q)
    return out


def clear_sina_hk_quote_cache() -> None:
    """Reset the Sina HK cache (tests only)."""
    with _SINA_HK_QUOTE_CACHE_LOCK:
        _SINA_HK_QUOTE_CACHE["fetched_at"] = 0.0
        _SINA_HK_QUOTE_CACHE["quotes"] = {}
        _SINA_HK_QUOTE_CACHE["last_error"] = None
    with _HK_INDEX_CACHE_LOCK:
        _HK_INDEX_CACHE["fetched_at"] = 0.0
        _HK_INDEX_CACHE["quotes"] = {}


def _tushare_quotes(codes: list[str], *, api_key: str) -> list[dict[str, Any]]:
    """Call tushare realtime_quote for non-HK codes; returns normalized items list."""
    # Tushare's realtime_quote proxies to Sina and internally split()s every
    # code on "." — bare codes (HSI / HSTECH / anything without an exchange
    # suffix) raise IndexError inside the library. Drop them up front.
    codes = [c for c in codes if "." in c]
    if not codes:
        return []
    # 2026-08-12: tushare's realtime_quote is wrapped by verify_token which
    # calls get_token() — env vars first, then ~/.tushare/tk.csv. The file is
    # not reliably present/writable on this Mac, so set the token via set_token:
    # the key from settings is the single source of truth (same one pro_api uses).
    # NOTE: do NOT write os.environ["TS_TOKEN"] — that mutates a process-global
    # shared by every concurrent caller; set_token keeps the token scoped.
    ts.set_token(api_key)
    try:
        if hasattr(ts, "realtime_quote"):
            df = ts.realtime_quote(ts_code=",".join(codes))
        else:
            pro = ts.pro_api(api_key)
            df = pro.realtime_quote(ts_code=",".join(codes))
    except Exception:
        # 2026-08-10: observed as a silent black hole — a transient failure here
        # returned {"ok": true, "items": []}, which frontend "copy" syncs read
        # as missing realtime quotes and aborted. Log it so the failure is
        # observable instead of invisible.
        logger.warning("tushare realtime_quote failed for %s", ",".join(codes)[:120], exc_info=True)
        return []

    if not isinstance(df, pd.DataFrame) or df.empty:
        return []

    rows = df.to_dict(orient="records")
    out: list[dict[str, Any]] = []
    for r in rows:
        r2 = {str(k).lower(): v for k, v in r.items()}
        ts_code = _as_str(_get(r2, "ts_code", "code"))
        trade_time = _as_str(_get(r2, "trade_time", "time", "datetime"))
        if not trade_time:
            date_raw = _as_str(_get(r2, "date"))
            time_raw = _as_str(_get(r2, "time"))
            if date_raw and len(date_raw) == 8 and date_raw.isdigit():
                date_raw = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
            if date_raw and time_raw:
                trade_time = f"{date_raw} {time_raw}"
        else:
            date_raw = _as_str(_get(r2, "date"))
            if date_raw and len(date_raw) == 8 and date_raw.isdigit():
                date_raw = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
            if date_raw and len(trade_time) <= 8 and ":" in trade_time:
                trade_time = f"{date_raw} {trade_time}"
        out.append(
            {
                "ts_code": ts_code,
                "price": _as_str(_get(r2, "price", "current", "last")),
                "open": _as_str(_get(r2, "open")),
                "high": _as_str(_get(r2, "high")),
                "low": _as_str(_get(r2, "low")),
                "pre_close": _as_str(_get(r2, "pre_close", "prev_close")),
                "change": _as_str(_get(r2, "change")),
                "pct_chg": _as_str(_get(r2, "pct_change", "pct_chg", "change_pct")),
                "volume": _as_str(_get(r2, "vol", "volume")),
                "amount": _as_str(_get(r2, "amount", "turnover")),
                "trade_time": trade_time,
            }
        )

    return [x for x in out if x.get("ts_code")]


def fetch_realtime_quotes(ts_codes: list[str]) -> dict[str, Any]:
    """
    Fetch realtime quotes for one or more ts_code.

    Returns:
      {"ok": True, "items": [...]} or {"ok": False, "error": "..."}.

    Notes:
    - Values are normalized to strings to avoid float precision issues in JSON.
    - Field names are normalized to: ts_code, price, open, high, low, pre_close, change, pct_chg, volume, amount, trade_time.
    - HK tickers (".HK" suffix) prefer Sina Finance hq.sinajs.cn (the same
      source used by Tonghuashun / Xueqiu for HK), then fall back to East
      Money push2, then tushare as a last resort.
    """
    codes = [c.strip() for c in ts_codes if c and c.strip()]
    if not codes:
        return {"ok": False, "error": "ts_code is required"}

    settings = get_settings()
    if not settings.tu_share_api_key:
        return {"ok": False, "error": "TU_SHARE_API_KEY is not set"}

    hk_codes, other_codes = _split_hk(codes)

    items: list[dict[str, Any]] = []
    if other_codes:
        items.extend(_tushare_quotes(other_codes, api_key=settings.tu_share_api_key))
    if hk_codes:
        # HK indices (HSI / HSTECH) use a different Sina feed/format — handle
        # them first so the stock path below never sees a bare index code.
        hk_index_codes = [c for c in hk_codes if c in _HK_INDEX_CODES]
        if hk_index_codes:
            items.extend(_sina_hk_index_quotes(hk_index_codes))
        hk_stock_codes = [c for c in hk_codes if c.endswith(".HK")]
        # Single batched Sina call covers all HK tickers in this request.
        sina_tickers = [c[:-3] for c in hk_stock_codes]
        sina_map = _sina_hk_quotes_fresh(sina_tickers)
        for code in hk_stock_codes:
            ticker = code[:-3]
            quote = sina_map.get(ticker.zfill(5))
            if quote is not None:
                items.append(quote)
                continue
            em_quote = _fetch_em_hk_quote(code)
            if em_quote is not None:
                items.append(em_quote)
                continue
            # Last-resort: try tushare in case the .HK tushare bug is fixed.
            tq = _tushare_quotes([code], api_key=settings.tu_share_api_key)
            if tq:
                items.extend(tq)

    return {"ok": True, "items": items}


def fetch_realtime_quotes_batched(
    ts_codes: list[str],
    *,
    batch_size: int = 50,
    max_workers: int = 6,
) -> list[dict[str, Any]]:
    """
    Fetch realtime quotes in parallel batches; returns merged items list.

    Stateless tushare calls are safe to run concurrently; max_workers caps
    inflight requests (similar rate limit intent as serial sleep between batches).
    """
    codes = [c.strip() for c in ts_codes if c and c.strip()]
    if not codes:
        return []
    parts = [codes[i : i + batch_size] for i in range(0, len(codes), batch_size)]
    items: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(fetch_realtime_quotes, part) for part in parts if part]
        for future in as_completed(futures):
            resp = future.result()
            if isinstance(resp, dict) and resp.get("ok"):
                items.extend(resp.get("items") or [])
    return items

