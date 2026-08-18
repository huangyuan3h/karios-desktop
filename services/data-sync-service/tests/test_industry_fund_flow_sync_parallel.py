"""Parallel industry fund flow hist sync tests (OPT-027)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

import data_sync_service.service.industry_fund_flow as iflow
from data_sync_service.service.industry_fund_flow import sync_cn_industry_fund_flow


@pytest.fixture(autouse=True)
def _reset_hist_short_circuit():
    """Reset the module-level eastmoney short-circuit latches between tests —
    they are process-global and would leak failure streaks across tests."""
    iflow._EM_HIST_FAIL_STREAK = 0
    iflow._EM_HIST_SKIP = False
    yield
    iflow._EM_HIST_FAIL_STREAK = 0
    iflow._EM_HIST_SKIP = False

_TOP_NAMES = ["电子", "计算机", "有色金属", "非银金融", "银行", "通信", "汽车", "医药生物", "电力设备", "机械设备"]

_TOP_ITEMS = [
    {
        "date": "2024-06-18",
        "industry_code": f"c{i}",
        "industry_name": name,
        "net_inflow": float(100 - i),
        "raw": {},
    }
    for i, name in enumerate(_TOP_NAMES)
]


def test_sync_cn_industry_fund_flow_parallel_hist_fetch() -> None:
    inflight = 0
    max_inflight = 0

    def _hist(name: str, *, industry_code: str | None = None, days: int = 10) -> list[dict]:
        nonlocal inflight, max_inflight
        inflight += 1
        max_inflight = max(max_inflight, inflight)
        import time

        time.sleep(0.05)
        inflight -= 1
        return [{"date": "2024-06-18", "net_inflow": 1.0, "raw": {}}]

    with (
        patch(
            "data_sync_service.service.industry_fund_flow.is_cn_trading_day",
            return_value=True,
        ),
        patch(
            "data_sync_service.service.industry_fund_flow.fetch_cn_industry_fund_flow_eod",
            return_value=_TOP_ITEMS,
        ),
        patch(
            "data_sync_service.service.industry_fund_flow.upsert_daily_rows",
        ),
        patch(
            "data_sync_service.service.industry_fund_flow.fetch_cn_industry_fund_flow_hist",
            side_effect=_hist,
        ),
    ):
        out = sync_cn_industry_fund_flow(days=10, top_n=10)

    assert max_inflight > 1
    assert out["histFailures"] == 0
    assert out["histRows"] == 10


def test_sync_cn_industry_fund_flow_returns_ok_flag() -> None:
    """Success path must carry ok=True so cn_industry_post_close_sync job succeeds."""
    with (
        patch(
            "data_sync_service.service.industry_fund_flow.is_cn_trading_day",
            return_value=True,
        ),
        patch(
            "data_sync_service.service.industry_fund_flow.fetch_cn_industry_fund_flow_eod",
            return_value=_TOP_ITEMS[:2],
        ),
        patch(
            "data_sync_service.service.industry_fund_flow.upsert_daily_rows",
        ),
        patch(
            "data_sync_service.service.industry_fund_flow.fetch_cn_industry_fund_flow_hist",
            return_value=[],
        ),
    ):
        out = sync_cn_industry_fund_flow(days=10, top_n=2)

    assert out.get("ok") is True
    assert out["asOfDate"] is not None


def test_sync_cn_industry_fund_flow_hist_failure_isolation() -> None:
    def _hist(name: str, *, industry_code: str | None = None, days: int = 10) -> list[dict]:
        if name == "电子":
            raise RuntimeError("boom")
        return [{"date": "2024-06-18", "net_inflow": 1.0, "raw": {}}]

    with (
        patch(
            "data_sync_service.service.industry_fund_flow.is_cn_trading_day",
            return_value=True,
        ),
        patch(
            "data_sync_service.service.industry_fund_flow.fetch_cn_industry_fund_flow_eod",
            return_value=_TOP_ITEMS[:3],
        ),
        patch(
            "data_sync_service.service.industry_fund_flow.upsert_daily_rows",
        ),
        patch(
            "data_sync_service.service.industry_fund_flow.fetch_cn_industry_fund_flow_hist",
            side_effect=_hist,
        ),
    ):
        out = sync_cn_industry_fund_flow(days=10, top_n=3)

    assert out["histFailures"] == 1
    assert out["histRows"] == 2
