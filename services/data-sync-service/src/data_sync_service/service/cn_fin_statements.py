"""CN raw financial statements sync — tushare balancesheet/income/cashflow.

4-year window backfill + incremental refresh. One call per stock per table
(full period history, filtered locally on end_date) — same pacing idiom as
cn_extra_sync (0.35s sleep, 35s back-off on rate limit).
"""

from __future__ import annotations

import logging
import re
import time
from datetime import date

from data_sync_service.clients.tushare_pool import get_pool
from data_sync_service.db import cn_balance, cn_cashflow, cn_income

logger = logging.getLogger(__name__)

TABLES = ("balancesheet", "income", "cashflow")

#: A-share listings only (excludes ETFs/funds/REITs/B shares which have no
#: statements — they return empty frames and waste quota).
_STOCK_PAT = re.compile(r"^(60[0138]|68[89]|00[0123]|30[01])\d{3}\.(SH|SZ)$|^\d{6}\.BJ$")


def _get_stock_codes() -> list[str]:
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ts_code FROM stock_basic "
                "WHERE ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ' OR ts_code LIKE '%.BJ' "
                "ORDER BY ts_code"
            )
            codes = [str(r[0]) for r in cur.fetchall() if r[0]]
    return [c for c in codes if _STOCK_PAT.match(c)]


def _pro():
    return get_pool().pro()


def _with_retry(fn, tries=3, base=1.5):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if i < tries - 1:
                time.sleep(base * (2**i))
    raise last  # type: ignore[misc]


def _to_iso8(d: date) -> str:
    return d.strftime("%Y%m%d")


def _row_end_iso8(val) -> str | None:
    s = str(val or "").strip().replace("-", "")
    return s if len(s) == 8 and s.isdigit() else None


def fresh_codes(cutoff: date) -> set[str]:
    """ts_codes already complete through the latest quarter end (>= cutoff).

    One aggregate query per table; a stock is fresh only when all three
    tables cover it. Makes reruns/resumes idempotent.
    """
    from data_sync_service.db import get_connection

    cutoff_s = cutoff.isoformat()
    per_table: list[set[str]] = []
    for mod in (cn_balance, cn_income, cn_cashflow):
        mod.ensure_table()
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT ts_code, max(end_date) FROM {mod.TABLE_NAME} "
                        "WHERE end_date >= %s GROUP BY ts_code",
                        (cutoff_s,),
                    )
                    per_table.append({str(r[0]) for r in cur.fetchall() if r[0]})
        except Exception as e:
            logger.warning("fresh check %s failed: %s", mod.TABLE_NAME, e)
            return set()
    if not per_table:
        return set()
    fresh = per_table[0]
    for s in per_table[1:]:
        fresh &= s
    return fresh


def sync_statements_for_codes(
    codes: list[str],
    cutoff: date,
    *,
    sleep: float = 0.4,
    progress_every: int = 50,
    log_prefix: str = "",
) -> dict:
    """Sync 4y-window statements for codes. Returns {updated, stocks, failed}."""
    pro = _pro()
    for mod in (cn_balance, cn_income, cn_cashflow):
        mod.ensure_table()
    cutoff8 = _to_iso8(cutoff)
    updated = {"balancesheet": 0, "income": 0, "cashflow": 0}
    done = 0
    failed: list[str] = []
    for i, ts_code in enumerate(codes):
        try:
            frames: dict[str, list[dict]] = {}
            for endpoint in TABLES:
                df = _with_retry(
                    lambda endpoint=endpoint, ts_code=ts_code: getattr(pro, endpoint)(ts_code=ts_code)
                )
                rows = []
                if df is not None and not df.empty:
                    for r in df.itertuples():
                        end8 = _row_end_iso8(getattr(r, "end_date", None))
                        if end8 is None or end8 < cutoff8:
                            continue
                        rows.append({c: getattr(r, c, None) for c in df.columns})
                frames[endpoint] = rows
                time.sleep(0.15)
            updated["balancesheet"] += cn_balance.upsert_rows(frames["balancesheet"])
            updated["income"] += cn_income.upsert_rows(frames["income"])
            updated["cashflow"] += cn_cashflow.upsert_rows(frames["cashflow"])
            done += 1
        except Exception as e:
            logger.warning("statements %s failed: %s", ts_code, e)
            failed.append(ts_code)
            if "频率超限" in str(e):
                time.sleep(35)
            else:
                time.sleep(0.6)
        if (i + 1) % progress_every == 0:
            logger.info(
                "%sprogress %d/%d done=%d bal=%d inc=%d cf=%d failed=%d",
                log_prefix, i + 1, len(codes), done,
                updated["balancesheet"], updated["income"], updated["cashflow"],
                len(failed),
            )
        time.sleep(sleep)
    return {"updated": updated, "stocks": done, "failed": failed}


def sync_4y_window(
    *,
    cutoff: date,
    limit_codes: int | None = None,
    offset: int = 0,
    sleep: float = 0.4,
    skip_fresh: bool = True,
) -> dict:
    """Resolve A-share universe, optionally skip fresh, sync."""
    codes = _get_stock_codes()
    codes = codes[offset:]
    if limit_codes:
        codes = codes[:limit_codes]
    skipped = 0
    if skip_fresh:
        fresh = fresh_codes(cutoff)
        if fresh:
            before = len(codes)
            codes = [c for c in codes if c not in fresh]
            skipped = before - len(codes)
    out = sync_statements_for_codes(codes, cutoff, sleep=sleep)
    out["skipped_fresh"] = skipped
    out["universe"] = len(codes) + skipped
    return out
