"""Sync East Money industry board membership (ts_code -> industry board name)."""

from __future__ import annotations

import json
import os
import random
import time
import urllib.parse
import urllib.request
import warnings
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

# 2026-08-09: home-line IP is rate-limited/banned by eastmoney (backfill
# storm fallout); route push2/emweb via the ClashX node exit when
# EASTMONEY_PROXY is set (mirrors em_push2_http.py / industry_fund_flow.py).
from dotenv import load_dotenv  # noqa: E402

load_dotenv(Path(__file__).resolve().parents[5] / ".env")  # noqa: E402

from data_sync_service.db.stock_basic import ensure_table as ensure_stock_basic  # noqa: E402
from data_sync_service.db.stock_eastmoney_industry import (  # noqa: E402
    count_rows,
    coverage_stats,
    list_missing_cn_ts_codes,
    list_stale_cn_ts_codes,
    lookup_by_ts_codes,
    upsert_rows,
)
from data_sync_service.db.sync_job_record import get_today_run, insert_record  # noqa: E402

_PROXY = os.environ.get("EASTMONEY_PROXY", "").strip()
_COOKIE = os.environ.get("EASTMONEY_COOKIE", "").strip()


def _em_opener() -> urllib.request.OpenerDirector | None:
    if not _PROXY:
        return None
    return urllib.request.build_opener(
        urllib.request.ProxyHandler({"http": _PROXY, "https": _PROXY})
    )


def _open_url(req: urllib.request.Request, timeout: float):
    opener = _em_opener()
    return opener.open(req, timeout=timeout) if opener else urllib.request.urlopen(req, timeout=timeout)

JOB_TYPE = "eastmoney_industry_sync"


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _ts_code_to_secid(ts_code: str) -> str | None:
    parts = (ts_code or "").strip().split(".")
    if len(parts) != 2:
        return None
    ticker, suffix = parts[0].strip(), parts[1].strip().upper()
    if len(ticker) != 6 or not ticker.isdigit():
        return None
    market = "1" if suffix == "SH" else "0"
    return f"{market}.{ticker}"


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


def _fetch_em_industry_for_ts_code(ts_code: str) -> str | None:
    """
    Fetch East Money industry label for one A-share.

    Primary: push2 stock/get (field f127, EM board name).
    Fallback chain when the host is unreachable: push2delay mirror (same
    payload) → emweb F10 CompanySurvey (EM2016, three-level path). The EM2016
    label resolves to the second level (``医药生物-化学制药-化学制剂`` →
    ``化学制药``), matching the correlation cluster rules.
    """
    for label in (
        _fetch_em_industry_push2(ts_code),
        _fetch_em_industry_push2delay(ts_code),
        _fetch_em_industry_emweb(ts_code),
    ):
        if label:
            return label
    return None


def _push2_get_label(url: str, ts_code: str) -> str | None:
    params = {
        "secid": _ts_code_to_secid(ts_code) or "",
        "fields": "f127",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "_": str(int(time.time() * 1000)),
    }
    req = urllib.request.Request(
        f"{url}?{urllib.parse.urlencode(params)}",
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://quote.eastmoney.com/",
            "Connection": "close",
        },
    )
    with _open_url(req, timeout=10) as resp:
        raw = resp.read()
    j = json.loads(raw.decode("utf-8", errors="replace"))
    data = j.get("data") if isinstance(j, dict) else None
    if not isinstance(data, dict):
        return None
    name = str(data.get("f127") or "").strip()
    return name or None


def _fetch_em_industry_push2(ts_code: str) -> str | None:
    if not _ts_code_to_secid(ts_code):
        return None
    try:
        return _push2_get_label("https://push2.eastmoney.com/api/qt/stock/get", ts_code)
    except Exception:  # noqa: BLE001 - main host down → next in fallback chain
        return None


def _fetch_em_industry_push2delay(ts_code: str) -> str | None:
    if not _ts_code_to_secid(ts_code):
        return None
    try:
        return _push2_get_label("https://push2delay.eastmoney.com/api/qt/stock/get", ts_code)
    except Exception:  # noqa: BLE001 - mirror down → emweb fallback
        return None


def _em2016_to_board_name(em2016: str) -> str | None:
    """``医药生物-化学制药-化学制剂`` → ``化学制药``; single level → as-is."""
    parts = [p.strip() for p in (em2016 or "").split("-") if p and p.strip()]
    if not parts:
        return None
    return parts[1] if len(parts) >= 2 else parts[0]


def _fetch_em_industry_emweb(ts_code: str) -> str | None:
    """East Money F10 CompanySurvey (EM2016 industry path) fallback."""
    parts = (ts_code or "").strip().split(".")
    if len(parts) != 2:
        return None
    ticker, suffix = parts[0].strip(), parts[1].strip().upper()
    if len(ticker) != 6 or not ticker.isdigit():
        return None
    url = "https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/PageAjax"
    req = urllib.request.Request(
        f"{url}?code={suffix}{ticker}",
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://emweb.securities.eastmoney.com/",
            "Connection": "close",
        },
    )
    try:
        with _open_url(req, timeout=15) as resp:
            raw = resp.read()
    except Exception:  # noqa: BLE001 - emweb down/limited → treat as no label
        return None
    try:
        j = json.loads(raw.decode("utf-8", errors="replace"))
    except ValueError:
        return None
    jbzl = j.get("jbzl") if isinstance(j, dict) else None
    if not isinstance(jbzl, list) or not jbzl:
        return None
    row = jbzl[0]
    if not isinstance(row, dict):
        return None
    return _em2016_to_board_name(str(row.get("EM2016") or ""))


def fetch_em_industries_for_ts_codes(
    ts_codes: list[str],
    *,
    sleep_s: float = 0.05,
) -> dict[str, str]:
    """Return ts_code -> East Money industry label for codes that resolve."""
    out: dict[str, str] = {}
    codes = [str(c or "").strip() for c in ts_codes if c and str(c).strip()]
    for i, code in enumerate(codes):
        try:
            name = _fetch_em_industry_for_ts_code(code)
        except Exception:
            name = None
        if name:
            out[code] = name
        if i + 1 < len(codes):
            time.sleep(max(0.0, float(sleep_s)) + random.random() * 0.02)
    return out


def _list_cn_ts_codes(*, limit: int | None = None) -> list[str]:
    ensure_stock_basic()
    from data_sync_service.db import get_connection

    lim = int(limit) if limit is not None and int(limit) > 0 else 100000
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts_code
                FROM stock_basic
                WHERE market IN ('主板', '中小板', '创业板', '科创板', '北交所', 'CN')
                ORDER BY ts_code
                LIMIT %s
                """,
                (lim,),
            )
            rows = cur.fetchall()
    return [str(r[0]) for r in rows if r and r[0]]


def _result_with_coverage(**extra: Any) -> dict[str, Any]:
    stats = coverage_stats()
    total = stats["totalCnStocks"]
    mapped = stats["emMapped"]
    stats["missingCount"]
    coverage_pct = round(100.0 * mapped / total, 2) if total > 0 else 0.0
    return {
        **extra,
        **stats,
        "coveragePct": coverage_pct,
        "totalInDb": count_rows(),
    }


def _resume_after_ts_code() -> str | None:
    run = get_today_run(JOB_TYPE)
    if run and run.get("success") is False and run.get("last_ts_code"):
        return str(run["last_ts_code"])
    return None


def sync_eastmoney_industry_incremental(
    *,
    mode: Literal["missing", "stale"] = "missing",
    batch_size: int = 500,
    max_batches: int = 1,
    sleep_s: float = 0.04,
    max_stale_days: int = 30,
) -> dict[str, Any]:
    """
    Offline incremental sync for stock_eastmoney_industry.

    - missing: stock_basic CN codes without EM row
    - stale: EM rows older than max_stale_days
    """
    batches = max(1, int(max_batches))
    size = max(1, min(int(batch_size), 5000))
    after = _resume_after_ts_code()
    total_requested = 0
    total_resolved = 0
    total_updated = 0
    batches_run = 0
    updated_at = _now_iso()
    last_resolved: dict[str, str] = {}
    any_empty_batch = False

    for _ in range(batches):
        if mode == "stale":
            ts_codes = list_stale_cn_ts_codes(
                after_ts_code=after,
                limit=size,
                max_stale_days=max_stale_days,
            )
        else:
            ts_codes = list_missing_cn_ts_codes(after_ts_code=after, limit=size)

        if not ts_codes:
            if total_requested == 0:
                return _result_with_coverage(
                    ok=True,
                    skipped=True,
                    message="no codes to sync",
                    mode=mode,
                    requested=0,
                    resolved=0,
                    updated=0,
                    batchesRun=0,
                    updatedAt=updated_at,
                )
            break

        try:
            resolved = fetch_em_industries_for_ts_codes(ts_codes, sleep_s=sleep_s)
            rows = [
                {
                    "ts_code": code,
                    "industry_name": name,
                    "industry_code": "",
                    "updated_at": updated_at,
                }
                for code, name in resolved.items()
            ]
            updated = upsert_rows(rows)
            if resolved:
                insert_record(job_type=JOB_TYPE, success=True, last_ts_code=None, error_message=None)
            else:
                # H10/H1 lesson: an all-empty batch used to be recorded as
                # success — the job looked green while the upstream source was
                # unreachable. Mark it failed so health checks can see it.
                any_empty_batch = True
                insert_record(
                    job_type=JOB_TYPE,
                    success=False,
                    last_ts_code=after or (ts_codes[0] if ts_codes else None),
                    error_message=(
                        f"no industry resolved for {len(ts_codes)} codes "
                        "(push2/emweb unreachable or malformed response)"
                    ),
                )
            total_requested += len(ts_codes)
            total_resolved += len(resolved)
            total_updated += updated
            last_resolved = resolved
            batches_run += 1
            after = ts_codes[-1]
        except Exception as e:  # noqa: BLE001
            insert_record(
                job_type=JOB_TYPE,
                success=False,
                last_ts_code=after or (ts_codes[0] if ts_codes else None),
                error_message=str(e),
            )
            return _result_with_coverage(
                ok=False,
                error=str(e),
                mode=mode,
                requested=total_requested + len(ts_codes),
                resolved=total_resolved,
                updated=total_updated,
                batchesRun=batches_run,
                lastTsCode=after,
                updatedAt=updated_at,
            )

    return _result_with_coverage(
        ok=not any_empty_batch,
        mode=mode,
        requested=total_requested,
        resolved=total_resolved,
        updated=total_updated,
        batchesRun=batches_run,
        updatedAt=updated_at,
        sample=[{"ts_code": k, "industry_name": v} for k, v in list(last_resolved.items())[:5]],
    )


def sync_eastmoney_industry(
    *,
    symbols: list[str] | None = None,
    limit: int | None = None,
    sleep_s: float = 0.05,
) -> dict[str, Any]:
    """
    Sync ts_code -> East Money industry labels.

    - symbols: optional CN:xxxxxx list (fast path for watchlist smoke tests)
    - limit: when symbols omitted, number of CN stocks from stock_basic to refresh
    """
    updated_at = _now_iso()
    ts_codes: list[str] = []
    if symbols:
        for sym in symbols:
            code = _symbol_to_ts_code(sym)
            if code:
                ts_codes.append(code)
    else:
        ts_codes = _list_cn_ts_codes(limit=limit if limit is not None else 500)

    if not ts_codes:
        return _result_with_coverage(ok=False, error="no_ts_codes", updated=0)

    resolved = fetch_em_industries_for_ts_codes(ts_codes, sleep_s=sleep_s)
    rows = [
        {
            "ts_code": code,
            "industry_name": name,
            "industry_code": "",
            "updated_at": updated_at,
        }
        for code, name in resolved.items()
    ]
    updated = upsert_rows(rows)
    return _result_with_coverage(
        ok=True,
        requested=len(ts_codes),
        resolved=len(resolved),
        updated=updated,
        sample=[{"ts_code": k, "industry_name": v} for k, v in list(resolved.items())[:5]],
        updatedAt=updated_at,
    )


def lookup_em_industries_for_ts_codes(ts_codes: list[str]) -> dict[str, str]:
    """DB lookup only; never HTTP. Use on TrendOK hot path."""
    codes = [str(c or "").strip() for c in ts_codes if c and str(c).strip()]
    if not codes:
        return {}
    return lookup_by_ts_codes(codes)


def _sync_missing_em_industries(ts_codes: list[str]) -> None:
    """Offline-only: fetch and cache missing EM labels. Do not call from request paths."""
    codes = [str(c or "").strip() for c in ts_codes if c and str(c).strip()]
    if not codes:
        return
    existing = lookup_by_ts_codes(codes)
    missing = [c for c in codes if c not in existing]
    if not missing:
        return
    resolved = fetch_em_industries_for_ts_codes(missing, sleep_s=0.04)
    if not resolved:
        return
    updated_at = _now_iso()
    upsert_rows(
        [
            {
                "ts_code": code,
                "industry_name": name,
                "industry_code": "",
                "updated_at": updated_at,
            }
            for code, name in resolved.items()
        ]
    )


def ensure_em_industries_for_ts_codes(ts_codes: list[str]) -> None:
    """
    Deprecated: do not use on TrendOK or other user-facing request paths.

    Use lookup_em_industries_for_ts_codes on hot paths and sync_eastmoney_industry_incremental offline.
    """
    warnings.warn(
        "ensure_em_industries_for_ts_codes is deprecated for request paths; "
        "use offline sync_eastmoney_industry_incremental instead",
        DeprecationWarning,
        stacklevel=2,
    )
    _sync_missing_em_industries(ts_codes)


def get_eastmoney_industry_sync_status() -> dict[str, Any]:
    """Coverage stats plus latest scheduler job record."""
    stats = coverage_stats()
    total = stats["totalCnStocks"]
    mapped = stats["emMapped"]
    coverage_pct = round(100.0 * mapped / total, 2) if total > 0 else 0.0
    today_run = get_today_run(JOB_TYPE)
    return {
        "ok": True,
        **stats,
        "coveragePct": coverage_pct,
        "totalInDb": count_rows(),
        "todayRun": today_run,
        "jobType": JOB_TYPE,
    }
