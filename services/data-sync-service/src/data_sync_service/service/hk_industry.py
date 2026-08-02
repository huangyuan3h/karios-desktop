"""Sync HK stock industry labels from East Money datacenter (primary) + Xueqiu (fallback).

EM endpoint `RPT_HKF10_INFO_ORGPROFILE` returns `BELONG_INDUSTRY` for every HK
listed security (incl. warrants, prefs) without auth. We paginate 500/page,
14 pages = 6895 rows covering all 2803 HK stocks in our DB.

Xueqiu `ak.stock_individual_basic_info_hk_xq` is kept as a per-stock fallback
for codes that EM omits (warrants), but is no longer the primary source because
the bundled Xueqiu token expired (400016).
"""

from __future__ import annotations

import time
from typing import Any

from data_sync_service.db.stock_basic import (
    ensure_table as ensure_stock_basic,
)
from data_sync_service.db.stock_basic import (
    fetch_ts_codes_by_market,
    update_industry,
)
from data_sync_service.db.sync_job_record import insert_record

JOB_TYPE = "hk_industry_sync"

INDUSTRY_MAX_LEN = 24
"""Truncate Xueqiu mbu description to first N chars for industry display."""

DEFAULT_SLEEP_S = 1.0
"""Per-call delay between Xueqiu fetches (Xueqiu has soft rate limits)."""

DEFAULT_LIMIT = 500
"""Max codes to update per run; default safe batch for manual / scheduler use."""

EM_HK_REPORT_NAME = "RPT_HKF10_INFO_ORGPROFILE"
EM_HK_INDUSTRY_COLUMN = "BELONG_INDUSTRY"
EM_HK_SECUCODE_COLUMN = "SECUCODE"
EM_PAGE_SIZE = 500
EM_PAGE_TIMEOUT = 25.0
"""East Money datacenter HK org profile endpoint."""


def _truncate_mbu(mbu: object) -> str | None:
    """Normalize Xueqiu mbu text → short industry label.

    - Strip whitespace
    - Take the first non-empty Chinese / English sentence
    - Truncate to INDUSTRY_MAX_LEN chars
    - Return None when text is empty / placeholder
    """
    if mbu is None:
        return None
    text = str(mbu).strip()
    if not text or text in {"None", "—", "-", "暂无", "N/A"}:
        return None
    text = text.replace("\n", " ").replace("\r", " ")
    # Collapse multi-space
    text = " ".join(text.split())
    # Prefer first sentence (Chinese / English); keep going if it ends with a connector.
    for sep in ("。", ".", "！", "!", "\n"):
        if sep in text:
            text = text.split(sep, 1)[0].strip()
            break
    if not text:
        return None
    if len(text) > INDUSTRY_MAX_LEN:
        text = text[:INDUSTRY_MAX_LEN].rstrip()
    return text or None


def _fetch_em_page(page_number: int, *, page_size: int = EM_PAGE_SIZE) -> list[dict[str, Any]]:
    """Fetch one page of HK org profile from East Money datacenter.

    Returns a list of raw `{SECUCODE: "00001.HK", BELONG_INDUSTRY: "综合企业"}` rows.
    Raises on network / parse errors so the caller can retry / log.
    """
    import requests  # noqa: PLC0415 — local import keeps top-level deps light

    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "reportName": EM_HK_REPORT_NAME,
        "columns": "ALL",
        "pageSize": str(page_size),
        "pageNumber": str(page_number),
        "source": "WEB",
        "client": "WEB",
    }
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Referer": "https://data.eastmoney.com/",
    }
    resp = requests.get(
        url,
        params=params,
        headers=headers,
        timeout=EM_PAGE_TIMEOUT,
        proxies={"http": None, "https": None},
    )
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict) or not data.get("success"):
        raise RuntimeError(
            f"eastmoney_hk_industry: bad response page={page_number} body={data!r}"
        )
    result = data.get("result") or {}
    return list(result.get("data") or [])


def fetch_eastmoney_hk_industry_map(
    *,
    page_size: int = EM_PAGE_SIZE,
    max_pages: int = 30,
    sleep_s: float = 0.05,
) -> tuple[dict[str, str], dict[str, Any]]:
    """Build ts_code -> BELONG_INDUSTRY map for all HK stocks (paginated).

    Returns `(resolved_map, stats)`. `stats` includes `emPages`, `emRows`, `emEmpty`.
    """
    resolved: dict[str, str] = {}
    pages_fetched = 0
    rows_total = 0
    rows_empty = 0

    for page in range(1, max_pages + 1):
        rows = _fetch_em_page(page, page_size=page_size)
        pages_fetched += 1
        if not rows:
            break
        rows_total += len(rows)
        for row in rows:
            code = str(row.get(EM_HK_SECUCODE_COLUMN) or "").strip()
            ind = row.get(EM_HK_INDUSTRY_COLUMN)
            if not code or not code.endswith(".HK"):
                continue
            if ind is None or str(ind).strip() == "":
                rows_empty += 1
                continue
            resolved[code] = str(ind).strip()
        # Last page: EM returns < page_size rows when total count reached.
        if len(rows) < page_size:
            break
        if sleep_s > 0:
            time.sleep(sleep_s)

    stats = {
        "emPages": pages_fetched,
        "emRows": rows_total,
        "emEmpty": rows_empty,
        "emResolved": len(resolved),
    }
    return resolved, stats


def fetch_xueqiu_mbu(
    ts_code: str,
    *,
    sleep_s: float = DEFAULT_SLEEP_S,
    retries: int = 2,
) -> str | None:
    """Fetch HK stock 'mbu' (主营业务) from Xueqiu via akshare.

    Returns the truncated industry label (≤ INDUSTRY_MAX_LEN chars), or None on failure.
    Retries up to `retries` times when Xueqiu returns a row whose 'mbu' is None
    (its soft rate-limit response).
    """
    symbol = (ts_code or "").strip().upper()
    if not symbol.endswith(".HK"):
        return None
    ticker = symbol.split(".", 1)[0].lstrip("0") or "0"
    if not ticker:
        return None
    try:
        import akshare as ak  # type: ignore[import-not-found]
    except Exception:
        return None

    last_label: str | None = None
    for attempt in range(retries + 1):
        try:
            df = ak.stock_individual_basic_info_hk_xq(symbol=ticker)
        except Exception:
            time.sleep(sleep_s * (attempt + 1))
            continue
        if df is None or df.empty:
            time.sleep(sleep_s * (attempt + 1))
            continue
        try:
            rows = dict(zip(df["item"].tolist(), df["value"].tolist(), strict=False))
        except Exception:
            time.sleep(sleep_s * (attempt + 1))
            continue
        label = _truncate_mbu(rows.get("mbu") or rows.get("comintr") or rows.get("comcnname"))
        if label:
            if sleep_s > 0:
                time.sleep(sleep_s)
            return label
        # Xueqiu soft rate-limit: every field is None. Back off and retry.
        last_label = None
        time.sleep(sleep_s * (attempt + 1) * 2)
    if sleep_s > 0:
        time.sleep(sleep_s)
    return last_label


def _iter_missing_hk_codes(*, limit: int | None) -> list[str]:
    """Return HK ts_codes that have no industry set, ordered by ts_code."""
    ensure_stock_basic()
    all_hk = fetch_ts_codes_by_market("HK")
    if not all_hk:
        return []
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts_code FROM stock_basic
                WHERE market = 'HK'
                  AND (industry IS NULL OR BTRIM(industry) = '')
                ORDER BY ts_code
                """
            )
            rows = cur.fetchall()
    missing = [str(r[0]) for r in rows if r and r[0]]
    if limit and limit > 0:
        missing = missing[: int(limit)]
    return missing


def _iter_all_hk_codes() -> list[str]:
    """Return all HK ts_codes currently in stock_basic."""
    ensure_stock_basic()
    return fetch_ts_codes_by_market("HK") or []


def sync_hk_industry(
    *,
    symbols: list[str] | None = None,
    limit: int = DEFAULT_LIMIT,
    sleep_s: float = DEFAULT_SLEEP_S,
) -> dict[str, Any]:
    """
    Resolve HK stock industry labels and upsert into stock_basic.industry.

    Strategy:
      - If `symbols` is given, fall back to Xueqiu per-stock (manual override path).
      - Otherwise pull the full EM map (one paginated fetch, ~14 pages) and
        supplement per-stock with Xueqiu for codes EM omits (warrants, prefs).

    - symbols: explicit ts_code list (e.g. ['00700.HK', '01810.HK']); overrides limit.
    - limit: max number of HK codes to process via Xueqiu fallback.
    - sleep_s: per-call delay between Xueqiu fetches.

    Returns dict with ok, requested, resolved, updated, sample, emStats.
    """
    if symbols:
        ts_codes = [s for s in symbols if s and s.strip()]
        resolved_map = _resolve_via_xueqiu_only(ts_codes, sleep_s=sleep_s)
        em_stats: dict[str, Any] = {"emPages": 0, "emRows": 0, "emResolved": 0}
        requested = len(ts_codes)
    else:
        db_codes = _iter_all_hk_codes()
        if not db_codes:
            insert_record(job_type=JOB_TYPE, success=True, last_ts_code=None, error_message=None)
            return {
                "ok": True,
                "skipped": True,
                "message": "no HK codes in stock_basic",
                "requested": 0,
                "resolved": 0,
                "updated": 0,
                "emResolved": 0,
                "xueqiuResolved": 0,
            }

        # Step 1: EM map (primary, fast, complete).
        em_map, em_stats = fetch_eastmoney_hk_industry_map()
        # Restrict to codes we actually have in stock_basic.
        resolved_map = {code: em_map[code] for code in db_codes if code in em_map}
        em_resolved_for_db = len(resolved_map)

        # Step 2: Xueqiu fallback for codes EM didn't cover (warrants, prefs).
        missing_in_em = [code for code in db_codes if code not in resolved_map]
        xueqiu_resolved: dict[str, str] = {}
        if missing_in_em:
            xueqiu_resolved = _resolve_via_xueqiu_only(
                missing_in_em[: int(limit)] if limit else missing_in_em,
                sleep_s=sleep_s,
            )
            resolved_map.update(xueqiu_resolved)
        em_stats["emResolved"] = em_resolved_for_db
        em_stats["xueqiuResolved"] = len(xueqiu_resolved)
        requested = len(db_codes)

    if not resolved_map:
        insert_record(
            job_type=JOB_TYPE,
            success=False,
            last_ts_code=symbols[-1] if symbols else None,
            error_message="no labels resolved from East Money or Xueqiu",
        )
        return {
            "ok": False,
            "error": "no labels resolved from East Money or Xueqiu",
            "requested": requested,
            "resolved": 0,
            "updated": 0,
            "emResolved": em_stats.get("emResolved", 0),
            "emPages": em_stats.get("emPages", 0),
            "emEmpty": em_stats.get("emEmpty", 0),
        }

    updated = update_industry(resolved_map)
    insert_record(job_type=JOB_TYPE, success=True, last_ts_code=None, error_message=None)
    return {
        "ok": True,
        "requested": requested,
        "resolved": len(resolved_map),
        "updated": updated,
        "sample": [{"ts_code": k, "industry": v} for k, v in list(resolved_map.items())[:5]],
        "emPages": em_stats.get("emPages", 0),
        "emRows": em_stats.get("emRows", 0),
        "emEmpty": em_stats.get("emEmpty", 0),
        "emResolved": em_stats.get("emResolved", 0),
        "xueqiuResolved": em_stats.get("xueqiuResolved", 0),
    }


def _resolve_via_xueqiu_only(
    ts_codes: list[str], *, sleep_s: float = DEFAULT_SLEEP_S
) -> dict[str, str]:
    """Resolve industry for each ts_code via Xueqiu only (legacy path)."""
    resolved_map: dict[str, str] = {}
    for code in ts_codes:
        label = fetch_xueqiu_mbu(code, sleep_s=sleep_s)
        if label:
            resolved_map[code] = label
    return resolved_map


def get_hk_industry_status() -> dict[str, Any]:
    """How many HK codes already have industry filled vs total."""
    ensure_stock_basic()
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE industry IS NOT NULL AND BTRIM(industry) <> '') AS mapped
                FROM stock_basic
                WHERE market = 'HK'
                """
            )
            row = cur.fetchone()
    total = int(row[0] or 0) if row else 0
    mapped = int(row[1] or 0) if row else 0
    return {
        "ok": True,
        "totalHk": total,
        "mappedHk": mapped,
        "missingHk": total - mapped,
        "coveragePct": round(100.0 * mapped / total, 2) if total > 0 else 0.0,
        "jobType": JOB_TYPE,
    }


def main() -> dict[str, Any]:  # pragma: no cover - manual CLI helper
    """CLI entrypoint: PYTHONPATH=src python -m data_sync_service.service.hk_industry."""
    return sync_hk_industry(limit=DEFAULT_LIMIT)


if __name__ == "__main__":  # pragma: no cover
    print(main())
