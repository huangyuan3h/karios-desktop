"""Tencent minute-line capture (TIP-014 Phase 3 / D7).

Fetches the current session's 1-minute bars for CN/HK symbols from Tencent
(web.ifzq.gtimg.cn) and stores them in bar_minute.

Endpoints (both verified working 2026-08-14):
- HK:  /appstock/app/hkMinute/query?code=hk02099
- CN:  /appstock/app/minute/query?code=sz000001

Response rows: "0930 192.000 70200 13478400.000" → time price vol amount.
Each minute row is a POINT; OHLC is derived by collapsing consecutive
1-min points at the SAME minute-of-session timestamp (Tencent minute rows
are already per-minute, but the first/last of the session may share a
minute with gap-fill — we keep the raw point prices as O/H/L/C with the
minute close as close).

Only the CURRENT session is available — bars accumulate forward from the
deployment date. There is no history endpoint on this source.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import urllib.request

from data_sync_service.db.bar_minute import upsert_minute_bars

logger = logging.getLogger(__name__)

TENCENT_MINUTE_URL = "https://web.ifzq.gtimg.cn/appstock/app"
TENCENT_REFERER = "https://gu.qq.com/"
REQUEST_DELAY_SECONDS = 0.8  # gentle pacing — this host is otherwise stable
REQUEST_TIMEOUT = 20

# Eastmoney historical 5-minute backfill (TIP-014 D7).
# NOTE (2026-08-14): EM's push2his works only when the request does NOT go
# through the ClashX system proxy (127.0.0.1:7890 — macOS urllib picks it up
# via _scproxy). The proxy-node IP is what got rate-limited (rc=102); the
# local broadband IP is clean. em_get_json proxies={"http": None} does this
# automatically. GENTLE pacing is mandatory — batch bursts are what triggered
# the block in the first place.
EM_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
EM_REFERER = "https://quote.eastmoney.com/"
EM_GENTLE_DELAY_SECONDS = 1.5     # between individual requests
EM_PAUSE_EVERY = 30               # every N requests…
EM_PAUSE_SECONDS = 10.0           # …pause this long
EM_MAX_DAYS_PER_CALL = 5          # beg..end span per request (5 trading days)

CN_DAYS = 5  # unused placeholder — no history on minute endpoints


def _fetch_tencent(code: str, kind: str) -> list[dict[str, Any]] | None:
    """Fetch minute rows for one symbol. ``kind`` in {"hk", "cn"}."""
    if kind == "hk":
        url = f"{TENCENT_MINUTE_URL}/hkMinute/query?_var=min_data_{code}&code={code}&r=0.99"
    else:
        url = f"{TENCENT_MINUTE_URL}/minute/query?code={code}"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0", "Referer": TENCENT_REFERER},
    )
    txt = urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT).read().decode("utf-8", "ignore")
    if "=" in txt[:200]:
        txt = txt[txt.index("=") + 1:]
    import json

    d = json.loads(txt)
    node = (d.get("data") or {}).get(code) or {}
    inner = node.get("data") or {}
    raw = inner.get("data") or []
    rows: list[dict[str, Any]] = []
    for line in raw:
        parts = str(line).split()
        if len(parts) < 4:
            continue
        t, price, vol, amount = parts[0], parts[1], parts[2], parts[3]
        try:
            px = float(price)
            v = float(vol)
            amt = float(amount)
        except (TypeError, ValueError):
            continue
        if px <= 0:
            continue
        rows.append({
            "time": t,
            "open": px, "high": px, "low": px, "close": px,
            "vol": v, "amount": amt,
        })
    return rows if rows else None


def capture_day_minute(
    *,
    ts_code: str,
    trade_date: str,
    kind: str,
) -> dict[str, Any]:
    """Fetch + store one symbol's minute bars for ``trade_date``.

    Returns {"ok", "stored", "skipped", "reason"}. ``ts_code`` like
    "02099.HK" / "000001.SZ"; ``kind`` must match ("hk"/"cn").
    """
    code = ts_code.split(".")[0].lower() if kind == "cn" else ts_code.split(".")[0]
    if kind == "hk":
        code = f"hk{ts_code.split('.')[0].zfill(5)}"
    elif kind == "cn":
        prefix = "sh" if ts_code.split(".")[1].startswith("SH") else "sz"
        code = f"{prefix}{ts_code.split('.')[0]}"
    else:
        return {"ok": False, "stored": 0, "skipped": True, "reason": "bad kind"}

    try:
        rows = _fetch_tencent(code, kind)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "stored": 0, "skipped": False, "reason": str(exc)[:200]}
    if not rows:
        return {"ok": False, "stored": 0, "skipped": True, "reason": "empty response"}

    stored = upsert_minute_bars(ts_code, trade_date, rows)
    time.sleep(REQUEST_DELAY_SECONDS)
    return {"ok": True, "stored": stored, "skipped": False, "reason": None}


def capture_symbols(
    *,
    trade_date: str,
    symbols: list[dict[str, str]],
    max_symbols: int = 60,
) -> dict[str, Any]:
    """Batch capture for a list of {ts_code, kind} — gentle-paced."""
    out = {"ok": 0, "failed": 0, "skipped": 0, "stored": 0}
    for i, sym in enumerate(symbols[:max_symbols]):
        res = capture_day_minute(
            ts_code=sym["ts_code"],
            trade_date=trade_date,
            kind=sym["kind"],
        )
        if res["ok"]:
            out["ok"] += 1
            out["stored"] += res["stored"]
        elif res.get("skipped"):
            out["skipped"] += 1
        else:
            out["failed"] += 1
        if i % 10 == 9:
            time.sleep(1.5)
    return out


# ---------------------------------------------------------------------------
# Eastmoney historical 5-minute backfill (TIP-014 D7 · gentle)
# ---------------------------------------------------------------------------


def _em_secid(ts_code: str, kind: str) -> str | None:
    """EM secid for a ts_code: HK→116.02099 · CN→0.000001/1.600000."""
    code, _, suffix = ts_code.partition(".")
    if kind == "hk":
        return f"116.{code.zfill(5)}"
    if kind == "cn":
        market = "1" if suffix.upper().startswith("SH") else "0"
        return f"{market}.{code.zfill(6)}"
    return None


def _em_fetch_5m(secid: str, beg: str, end: str) -> list[dict[str, Any]] | None:
    """One gentle 5m request. Returns rows WITH trade_date or None on failure."""
    from data_sync_service.service.em_push2_http import em_get_json

    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "klt": "5",
        "fqt": "1",
        "beg": beg,
        "end": end,
    }
    try:
        d = em_get_json(EM_KLINE_URL, params=params, referer=EM_REFERER, timeout=20)
    except Exception:  # noqa: BLE001
        return None
    klines = ((d or {}).get("data") or {}).get("klines") or []
    rows: list[dict[str, Any]] = []
    for line in klines:
        parts = str(line).split(",")
        if len(parts) < 8:
            continue
        # "2026-08-12 15:55", o, c, h, l, vol, amount, _
        try:
            dt, o, c, h, l, v, amt = (
                parts[0], float(parts[1]), float(parts[2]),
                float(parts[3]), float(parts[4]), float(parts[5]), float(parts[6]),
            )
        except (TypeError, ValueError):
            continue
        if c <= 0:
            continue
        trade_date, _, hm = dt.partition(" ")
        rows.append({
            "trade_date": trade_date,
            "time": hm.replace(":", "")[:4],
            "open": o, "high": h, "low": l, "close": c,
            "vol": v, "amount": amt,
        })
    return rows


def backfill_em_history(
    *,
    ts_code: str,
    kind: str,
    start_date: str,
    end_date: str,
    max_requests: int = 120,
) -> dict[str, Any]:
    """Gentle Eastmoney 5m backfill for one symbol over a date range.

    - one request per ≤5-calendar-day window, 1.5s apart, 10s pause every 30
    - per (ts_code, trade_date) upsert; rows carry their own trade_date
    - max_requests caps a single run so the job never bursts EM
    - idempotent (ON CONFLICT upsert) — safe to re-run after interruptions
    """
    from datetime import date, timedelta

    from data_sync_service.db.bar_minute import upsert_minute_bars

    secid = _em_secid(ts_code, kind)
    if secid is None:
        return {"ok": False, "stored": 0, "reason": "bad ts_code/kind"}

    out = {"ok": False, "stored": 0, "failed": 0, "requests": 0}
    d = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    n_requests = 0
    while d <= end and n_requests < max_requests:
        d_end = min(d + timedelta(days=EM_MAX_DAYS_PER_CALL - 1), end)
        rows = _em_fetch_5m(secid, d.strftime("%Y%m%d"), d_end.strftime("%Y%m%d"))
        n_requests += 1
        out["requests"] = n_requests
        if rows:
            by_date: dict[str, list[dict[str, Any]]] = {}
            for r in rows:
                by_date.setdefault(r["trade_date"], []).append(r)
            for td, day_rows in by_date.items():
                stored = upsert_minute_bars(ts_code, td, day_rows)
                out["stored"] += stored
        else:
            out["failed"] += 1
        d = d_end + timedelta(days=1)
        if n_requests % EM_PAUSE_EVERY == 0:
            time.sleep(EM_PAUSE_SECONDS)
        else:
            time.sleep(EM_GENTLE_DELAY_SECONDS)

    out["ok"] = out["requests"] > 0
    return out
