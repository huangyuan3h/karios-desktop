"""Sync HK stock industry labels from Xueqiu mbu (主营业务) into stock_basic.industry."""

from __future__ import annotations

import time
from typing import Any

from data_sync_service.config import get_settings
from data_sync_service.db.stock_basic import (
    ensure_table as ensure_stock_basic,
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
            rows = dict(zip(df["item"].tolist(), df["value"].tolist()))
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


def sync_hk_industry(
    *,
    symbols: list[str] | None = None,
    limit: int = DEFAULT_LIMIT,
    sleep_s: float = DEFAULT_SLEEP_S,
) -> dict[str, Any]:
    """
    Fetch Xueqiu mbu for HK tickers and upsert into stock_basic.industry.

    - symbols: explicit ts_code list (e.g. ['00700.HK', '01810.HK']); overrides limit.
    - limit: max number of HK codes to process when symbols is None.
    - sleep_s: per-call delay (Xueqiu has soft rate limits).

    Returns dict with ok, requested, resolved, updated, sample.
    """
    if symbols:
        ts_codes = [s for s in symbols if s and s.strip()]
    else:
        ts_codes = _iter_missing_hk_codes(limit=limit)

    if not ts_codes:
        insert_record(job_type=JOB_TYPE, success=True, last_ts_code=None, error_message=None)
        return {
            "ok": True,
            "skipped": True,
            "message": "no HK codes to update",
            "requested": 0,
            "resolved": 0,
            "updated": 0,
        }

    resolved_map: dict[str, str] = {}
    last_ts_code: str | None = None
    for code in ts_codes:
        last_ts_code = code
        label = fetch_xueqiu_mbu(code, sleep_s=sleep_s)
        if label:
            resolved_map[code] = label

    if not resolved_map:
        insert_record(
            job_type=JOB_TYPE,
            success=False,
            last_ts_code=last_ts_code,
            error_message="no labels resolved from Xueqiu",
        )
        return {
            "ok": False,
            "error": "no labels resolved from Xueqiu",
            "requested": len(ts_codes),
            "resolved": 0,
            "updated": 0,
        }

    updated = update_industry(resolved_map)
    insert_record(job_type=JOB_TYPE, success=True, last_ts_code=None, error_message=None)
    return {
        "ok": True,
        "requested": len(ts_codes),
        "resolved": len(resolved_map),
        "updated": updated,
        "sample": [{"ts_code": k, "industry": v} for k, v in list(resolved_map.items())[:5]],
    }


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
    settings = get_settings()
    return sync_hk_industry(limit=settings.hk_industry_sync_limit or DEFAULT_LIMIT)


if __name__ == "__main__":  # pragma: no cover
    print(main())