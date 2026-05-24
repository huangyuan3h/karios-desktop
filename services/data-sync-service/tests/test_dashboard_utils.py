import pytest
from data_sync_service.service.dashboard import (
    _daily_rankings_by_date_from_items,
    _now_iso,
    _today_iso_date,
)


def test_now_iso_format():
    result = _now_iso()
    assert "T" in result


def test_today_iso_date_format():
    result = _today_iso_date()
    assert len(result) == 10
    assert result[4] == "-"
    assert result[7] == "-"


def test_daily_rankings_include_negative_inflow_for_rank_delta() -> None:
    items = [
        {
            "industryName": "Leader-A",
            "series": [
                {"date": "2026-05-22", "netInflow": 30e8},
                {"date": "2026-05-23", "netInflow": 10e8},
            ],
        },
        {
            "industryName": "Rebound-B",
            "series": [
                {"date": "2026-05-22", "netInflow": -5e8},
                {"date": "2026-05-23", "netInflow": 35e8},
            ],
        },
    ]
    rankings = _daily_rankings_by_date_from_items(items, ["2026-05-22", "2026-05-23"])
    prev = next(x for x in rankings if x["date"] == "2026-05-22")["ranked"]
    latest = next(x for x in rankings if x["date"] == "2026-05-23")["ranked"]
    assert any(x["industryName"] == "Rebound-B" and x["rank"] == 2 for x in prev)
    assert any(x["industryName"] == "Rebound-B" and x["rank"] == 1 for x in latest)