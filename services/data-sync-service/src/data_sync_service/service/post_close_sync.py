"""Post-close tasks after CN market daily sync: index daily + macro daily."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from data_sync_service.service.eastmoney_industry import sync_eastmoney_industry_incremental
from data_sync_service.service.etf_fund_flow import sync_etf_fund_flow_watchlist
from data_sync_service.service.index_daily import sync_index_daily_full
from data_sync_service.service.macro_daily import sync_macro_daily_full
from data_sync_service.service.option_iv import sync_option_iv_daily
from data_sync_service.service.top_inst_flow import sync_top_inst_watchlist

logger = logging.getLogger(__name__)



def run_post_close_sync() -> dict[str, Any]:
    """
    Run index, macro, and eastmoney industry incremental syncs in parallel.
    Each sub-job has its own skip-if-today-ok logic.
    """
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {
            "indexDaily": pool.submit(sync_index_daily_full),
            "macroDaily": pool.submit(sync_macro_daily_full),
            "eastmoneyIndustry": pool.submit(
                sync_eastmoney_industry_incremental,
                mode="missing",
                batch_size=500,
                max_batches=1,
            ),
            "etfFundFlow": pool.submit(sync_etf_fund_flow_watchlist),
            "topInstWatchlist": pool.submit(sync_top_inst_watchlist),
            "optionIvDaily": pool.submit(sync_option_iv_daily),
        }
        results: dict[str, Any] = {}
        for name, fut in futures.items():
            try:
                results[name] = fut.result()
            except Exception as exc:  # noqa: BLE001 - one failure must not abort the rest
                logger.warning("post_close_sync sub-task %s failed: %s", name, exc, exc_info=True)
                results[name] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    return results
