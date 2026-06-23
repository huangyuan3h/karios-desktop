from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service.industry_fund_flow import get_cn_industry_fund_flow, sync_cn_industry_fund_flow
from data_sync_service.service.industry_fund_flow_read import (
    build_trendok_flow_context_from_rows,
    positive_days_from_rows,
    series_map_from_rows,
    sum_by_industry_from_rows,
    top_by_date_from_rows,
)
from data_sync_service.service.mainline import _flow_context


FIXTURE_ROWS = [
    {"date": "2024-01-01", "industry_code": "c1", "industry_name": "电子", "net_inflow": 10.0},
    {"date": "2024-01-02", "industry_code": "c1", "industry_name": "电子", "net_inflow": -5.0},
    {"date": "2024-01-03", "industry_code": "c1", "industry_name": "电子", "net_inflow": 3.0},
    {"date": "2024-01-01", "industry_code": "c2", "industry_name": "计算机", "net_inflow": 20.0},
    {"date": "2024-01-02", "industry_code": "c2", "industry_name": "计算机", "net_inflow": 1.0},
]


def test_series_map_from_rows_orders_and_filters() -> None:
    dates = ["2024-01-01", "2024-01-02", "2024-01-03"]
    m = series_map_from_rows(FIXTURE_ROWS, dates)
    assert [x["date"] for x in m["电子"]] == ["2024-01-01", "2024-01-02", "2024-01-03"]
    assert m["电子"][0]["net_inflow"] == 10.0
    assert m["计算机"][0]["net_inflow"] == 20.0
    assert len(m["计算机"]) == 2


def test_sum_by_industry_from_rows() -> None:
    dates = ["2024-01-01", "2024-01-02", "2024-01-03"]
    sums = sum_by_industry_from_rows(FIXTURE_ROWS, dates)
    assert sums["电子"] == 8.0
    assert sums["计算机"] == 21.0


def test_positive_days_from_rows() -> None:
    dates = ["2024-01-01", "2024-01-02", "2024-01-03"]
    pos = positive_days_from_rows(FIXTURE_ROWS, dates)
    assert pos["电子"] == 2
    assert pos["计算机"] == 2


def test_get_cn_industry_fund_flow_json_shape() -> None:
    dates = ["2024-01-01", "2024-01-02"]
    top_rows = [
        {"industry_code": "c1", "industry_name": "电子", "net_inflow": 99.0},
    ]
    batch_rows = [
        {"date": "2024-01-01", "industry_code": "c1", "industry_name": "电子", "net_inflow": 10.0},
        {"date": "2024-01-02", "industry_code": "c1", "industry_name": "电子", "net_inflow": 5.0},
    ]
    with (
        patch(
            "data_sync_service.service.industry_fund_flow.get_dates_upto",
            return_value=dates,
        ),
        patch(
            "data_sync_service.service.industry_fund_flow.get_top_rows",
            return_value=top_rows,
        ),
        patch(
            "data_sync_service.service.industry_fund_flow.get_rows_for_dates",
            return_value=batch_rows,
        ) as mock_batch,
    ):
        out = get_cn_industry_fund_flow(days=2, top_n=1, as_of_date="2024-01-02")

    assert out["asOfDate"] == "2024-01-02"
    assert out["dates"] == dates
    assert len(out["top"]) == 1
    row = out["top"][0]
    assert row["industryCode"] == "c1"
    assert row["industryName"] == "电子"
    assert row["netInflow"] == 99.0
    assert row["sum10d"] == 15.0
    assert row["series10d"] == [
        {"date": "2024-01-01", "netInflow": 10.0},
        {"date": "2024-01-02", "netInflow": 5.0},
    ]
    mock_batch.assert_called_once_with(dates)


def test_get_cn_industry_fund_flow_no_per_industry_series_queries() -> None:
    dates = ["2024-01-01"]
    with (
        patch(
            "data_sync_service.service.industry_fund_flow.get_dates_upto",
            return_value=dates,
        ),
        patch(
            "data_sync_service.service.industry_fund_flow.get_top_rows",
            return_value=[
                {"industry_code": "a", "industry_name": "A", "net_inflow": 1.0},
                {"industry_code": "b", "industry_name": "B", "net_inflow": 2.0},
            ],
        ),
        patch(
            "data_sync_service.service.industry_fund_flow.get_rows_for_dates",
            return_value=[],
        ),
        patch(
            "data_sync_service.db.industry_fund_flow.get_series_for_industry",
        ) as mock_series,
    ):
        get_cn_industry_fund_flow(days=1, top_n=2, as_of_date="2024-01-01")

    mock_series.assert_not_called()


def test_get_cn_industry_fund_flow_db_call_count_with_as_of_date() -> None:
    """With as_of_date fixed: dates_upto + top_rows + get_rows_for_dates = 3 DB reads."""
    dates = ["2024-01-01", "2024-01-02"]
    with (
        patch(
            "data_sync_service.service.industry_fund_flow.get_latest_date",
        ) as mock_latest,
        patch(
            "data_sync_service.service.industry_fund_flow.get_dates_upto",
            return_value=dates,
        ) as mock_dates,
        patch(
            "data_sync_service.service.industry_fund_flow.get_top_rows",
            return_value=[],
        ) as mock_top,
        patch(
            "data_sync_service.service.industry_fund_flow.get_rows_for_dates",
            return_value=[],
        ) as mock_batch,
    ):
        get_cn_industry_fund_flow(days=2, top_n=30, as_of_date="2024-01-02")

    mock_latest.assert_not_called()
    mock_dates.assert_called_once()
    mock_top.assert_called_once()
    mock_batch.assert_called_once()


def test_flow_context_uses_batch_read_and_memory_agg() -> None:
    dates_20 = [f"2024-01-{i:02d}" for i in range(1, 21)]
    batch = [
        {
            "date": "2024-01-19",
            "industry_code": "c1",
            "industry_name": "电子",
            "net_inflow": 5.0,
        },
        {
            "date": "2024-01-20",
            "industry_code": "c1",
            "industry_name": "电子",
            "net_inflow": -1.0,
        },
    ]
    with (
        patch(
            "data_sync_service.service.mainline.flow_dates_upto",
            return_value=dates_20,
        ) as mock_dates_upto,
        patch(
            "data_sync_service.service.mainline.flow_rows_for_dates",
            return_value=batch,
        ) as mock_batch,
    ):
        ctx = _flow_context("2024-01-20")

    mock_dates_upto.assert_called_once_with("2024-01-20", 20)
    mock_batch.assert_called_once_with(dates_20)
    assert ctx["dates_20"] == dates_20
    assert ctx["dates_10"] == dates_20[-10:]
    assert ctx["dates_5"] == dates_20[-5:]
    assert ctx["sum20"]["电子"] == 4.0
    assert ctx["pos10"]["电子"] == 1


def test_build_trendok_flow_context_from_rows_hotspots_and_5d_ranks() -> None:
    dates_5 = ["2024-01-16", "2024-01-17", "2024-01-18", "2024-01-19", "2024-01-20"]
    rows = [
        {"date": "2024-01-19", "industry_name": "电子", "net_inflow": 50.0},
        {"date": "2024-01-19", "industry_name": "计算机", "net_inflow": 30.0},
        {"date": "2024-01-19", "industry_name": "医药生物", "net_inflow": 10.0},
        {"date": "2024-01-20", "industry_name": "电子", "net_inflow": 100.0},
        {"date": "2024-01-20", "industry_name": "计算机", "net_inflow": 80.0},
        {"date": "2024-01-20", "industry_name": "医药生物", "net_inflow": 60.0},
        {"date": "2024-01-20", "industry_name": "银行", "net_inflow": 40.0},
        {"date": "2024-01-20", "industry_name": "房地产", "net_inflow": 20.0},
        {"date": "2024-01-16", "industry_name": "电子", "net_inflow": 1.0},
        {"date": "2024-01-16", "industry_name": "环保", "net_inflow": -100.0},
        {"date": "2024-01-17", "industry_name": "煤炭", "net_inflow": -90.0},
        {"date": "2024-01-18", "industry_name": "钢铁", "net_inflow": -80.0},
    ]
    ctx = build_trendok_flow_context_from_rows(
        flow_date="2024-01-20",
        dates_5=dates_5,
        rows=rows,
    )
    assert ctx["ok"] is True
    assert ctx["today"] == "2024-01-20"
    assert ctx["yesterday"] == "2024-01-19"
    assert ctx["top_today_3"] == {"电子", "计算机", "医药生物"}
    assert ctx["top_today_5"] == {"电子", "计算机", "医药生物", "银行", "房地产"}
    assert ctx["top_yesterday_3"] == {"电子", "计算机", "医药生物"}
    assert ctx["net_today"]["电子"] == 100.0
    assert ctx["net_yesterday"]["电子"] == 50.0
    assert "电子" in ctx["top_5d_3"]
    assert "环保" in ctx["bottom_5d_5"]


def test_build_trendok_flow_context_from_rows_single_day_no_yesterday() -> None:
    dates_5 = ["2024-01-20"]
    rows = [{"date": "2024-01-20", "industry_name": "电子", "net_inflow": 5.0}]
    ctx = build_trendok_flow_context_from_rows(
        flow_date="2024-01-20",
        dates_5=dates_5,
        rows=rows,
    )
    assert ctx["today"] == "2024-01-20"
    assert ctx["yesterday"] is None
    assert ctx["top_yesterday_3"] == set()
    assert ctx["net_yesterday"] == {}


def test_top_by_date_filters_nested_sw_child_industries() -> None:
    rows = [
        {"date": "2026-06-18", "industry_code": "l1-a", "industry_name": "非银金融", "net_inflow": 123.77e8},
        {"date": "2026-06-18", "industry_code": "l2-a", "industry_name": "证券Ⅱ", "net_inflow": 105.61e8},
        {"date": "2026-06-18", "industry_code": "l3-a", "industry_name": "证券Ⅲ", "net_inflow": 99.0e8},
        {"date": "2026-06-18", "industry_code": "l1-b", "industry_name": "有色金属", "net_inflow": 112.10e8},
        {"date": "2026-06-18", "industry_code": "l1-c", "industry_name": "电子", "net_inflow": 70.0e8},
    ]
    out = top_by_date_from_rows(rows, ["2026-06-18"], top_k=5)
    assert out == [{"date": "2026-06-18", "top": ["非银金融", "有色金属", "电子"]}]


def test_top_by_date_dedupes_same_industry_name_slots() -> None:
    rows = [
        {"date": "2026-06-18", "industry_code": "c1", "industry_name": "非银金融", "net_inflow": 100.0},
        {"date": "2026-06-18", "industry_code": "c2", "industry_name": "非银金融", "net_inflow": 90.0},
        {"date": "2026-06-18", "industry_code": "c3", "industry_name": "有色金属", "net_inflow": 80.0},
    ]
    out = top_by_date_from_rows(rows, ["2026-06-18"], top_k=2)
    assert out == [{"date": "2026-06-18", "top": ["非银金融", "有色金属"]}]


def test_trendok_flow_context_filters_nested_sw_child_industries() -> None:
    dates_5 = ["2026-06-16", "2026-06-17", "2026-06-18"]
    rows = [
        {"date": "2026-06-18", "industry_name": "非银金融", "net_inflow": 123.77e8},
        {"date": "2026-06-18", "industry_name": "有色金属", "net_inflow": 112.10e8},
        {"date": "2026-06-18", "industry_name": "证券Ⅱ", "net_inflow": 105.61e8},
        {"date": "2026-06-18", "industry_name": "证券Ⅲ", "net_inflow": 99.0e8},
        {"date": "2026-06-17", "industry_name": "非银金融", "net_inflow": 50.0},
        {"date": "2026-06-16", "industry_name": "电子", "net_inflow": 10.0},
    ]
    ctx = build_trendok_flow_context_from_rows(flow_date="2026-06-18", dates_5=dates_5, rows=rows)
    assert "证券Ⅱ" not in ctx["top_today_5"]
    assert "证券Ⅲ" not in ctx["top_today_5"]
    assert ctx["top_today_3"] == {"非银金融", "有色金属"}


def test_sync_cn_industry_fund_flow_fetches_history_for_all_industries() -> None:
    items = [
        {"date": "2024-01-20", "industry_code": "c1", "industry_name": "电子", "net_inflow": 30.0, "raw": {}},
        {"date": "2024-01-20", "industry_code": "c2", "industry_name": "计算机", "net_inflow": 20.0, "raw": {}},
        {"date": "2024-01-20", "industry_code": "c3", "industry_name": "有色金属", "net_inflow": 10.0, "raw": {}},
    ]
    calls: list[str] = []

    def fake_hist(name: str, *, industry_code: str | None = None, days: int = 10) -> list[dict]:
        calls.append(name)
        return [{"date": "2024-01-19", "net_inflow": 1.0, "raw": {"name": name}}]

    with (
        patch("data_sync_service.service.industry_fund_flow.is_cn_trading_day", return_value=True),
        patch("data_sync_service.service.industry_fund_flow.fetch_cn_industry_fund_flow_eod", return_value=items),
        patch("data_sync_service.service.industry_fund_flow.fetch_cn_industry_fund_flow_hist", side_effect=fake_hist),
        patch("data_sync_service.service.industry_fund_flow.upsert_daily_rows") as upsert,
    ):
        out = sync_cn_industry_fund_flow(days=2, top_n=1)

    assert sorted(calls) == ["有色金属", "电子", "计算机"]
    assert out["histRows"] == 3
    assert out["filteredRows"] == 0
    assert upsert.call_count == 2


def test_sync_cn_industry_fund_flow_filters_nested_child_industries() -> None:
    items = [
        {"date": "2026-06-18", "industry_code": "l1-a", "industry_name": "非银金融", "net_inflow": 123.77e8, "raw": {}},
        {"date": "2026-06-18", "industry_code": "l2-a", "industry_name": "证券Ⅱ", "net_inflow": 105.61e8, "raw": {}},
        {"date": "2026-06-18", "industry_code": "l3-a", "industry_name": "证券Ⅲ", "net_inflow": 99.0e8, "raw": {}},
        {"date": "2026-06-18", "industry_code": "l1-b", "industry_name": "有色金属", "net_inflow": 112.10e8, "raw": {}},
    ]
    calls: list[str] = []

    def fake_hist(name: str, *, industry_code: str | None = None, days: int = 10) -> list[dict]:
        calls.append(name)
        return [{"date": "2026-06-17", "net_inflow": 1.0, "raw": {}}]

    with (
        patch("data_sync_service.service.industry_fund_flow.is_cn_trading_day", return_value=True),
        patch("data_sync_service.service.industry_fund_flow.fetch_cn_industry_fund_flow_eod", return_value=items),
        patch("data_sync_service.service.industry_fund_flow.fetch_cn_industry_fund_flow_hist", side_effect=fake_hist),
        patch("data_sync_service.service.industry_fund_flow.upsert_daily_rows") as upsert,
    ):
        out = sync_cn_industry_fund_flow(days=2, top_n=10)

    daily_rows = upsert.call_args_list[0].args[0]
    assert [r["industry_name"] for r in daily_rows] == ["非银金融", "有色金属"]
    assert sorted(calls) == ["有色金属", "非银金融"]
    assert out["rows"] == 2
    assert out["filteredRows"] == 2
    assert out["histRows"] == 2
