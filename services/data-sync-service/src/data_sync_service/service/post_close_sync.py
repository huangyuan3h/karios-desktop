"""Post-close tasks after CN market daily sync: index daily + macro daily."""

from __future__ import annotations

from typing import Any

from data_sync_service.service.index_daily import sync_index_daily_full
from data_sync_service.service.macro_daily import sync_macro_daily_full


def run_post_close_sync() -> dict[str, Any]:
    """
    Run index and macro incremental syncs (each has its own skip-if-today-ok logic).
    Call after successful sync_close when you want parity with POST /sync/close.
    """
    index_result = sync_index_daily_full()
    macro_result = sync_macro_daily_full()
    return {"indexDaily": index_result, "macroDaily": macro_result}
