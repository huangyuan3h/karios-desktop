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
