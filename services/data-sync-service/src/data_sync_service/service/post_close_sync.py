"""Post-close tasks after CN market daily sync: index daily + macro daily."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any

from data_sync_service.service.eastmoney_industry import sync_eastmoney_industry_incremental
from data_sync_service.service.index_daily import sync_index_daily_full
from data_sync_service.service.macro_daily import sync_macro_daily_full


def run_post_close_sync() -> dict[str, Any]:
    """
    Run index, macro, and eastmoney industry incremental syncs in parallel.
    Each sub-job has its own skip-if-today-ok logic.
    """
    with ThreadPoolExecutor(max_workers=3) as pool:
        f_index = pool.submit(sync_index_daily_full)
        f_macro = pool.submit(sync_macro_daily_full)
        f_em = pool.submit(
            sync_eastmoney_industry_incremental,
            mode="missing",
            batch_size=500,
            max_batches=1,
        )
        index_result = f_index.result()
        macro_result = f_macro.result()
        em_result = f_em.result()
    return {"indexDaily": index_result, "macroDaily": macro_result, "eastmoneyIndustry": em_result}
