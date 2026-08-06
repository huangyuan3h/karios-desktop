"""Research channel (研报 → Alpha) tests — TIP-012."""

from __future__ import annotations

from datetime import date, timedelta

from data_sync_service.db import research as db  # noqa: F401  (patch target)
from data_sync_service.service import research as rs


def _today() -> date:
    return date.today()


def test_compute_report_score_buy_80() -> None:
    score = rs.compute_report_score(
        rating="买入", target_price=None, current_close=None,
        publish_date=_today(), today=_today(),
    )
    assert score == 80.0


def test_compute_report_score_hold_60() -> None:
    score = rs.compute_report_score(
        rating="增持", target_price=None, current_close=None,
        publish_date=_today(), today=_today(),
    )
    assert score == 60.0


def test_compute_report_score_target_price_bonus() -> None:
    score = rs.compute_report_score(
        rating="买入", target_price=120.0, current_close=100.0,
        publish_date=_today(), today=_today(),
    )
    # 20% upside → (0.2/0.5)×20 = 8 pts → 88
    assert score == 88.0


def test_compute_report_score_full_target_bonus_caps() -> None:
    score = rs.compute_report_score(
        rating="买入", target_price=200.0, current_close=100.0,
        publish_date=_today(), today=_today(),
    )
    # 100% upside → capped at 20 pts → 100
    assert score == 100.0


def test_compute_report_score_partial_target_bonus() -> None:
    score = rs.compute_report_score(
        rating="买入", target_price=110.0, current_close=100.0,
        publish_date=_today(), today=_today(),
    )
    # 10% upside → (0.1/0.5)×20 = 4 pts → 84
    assert score == 84.0


def test_compute_report_score_target_below_price_no_bonus() -> None:
    score = rs.compute_report_score(
        rating="买入", target_price=90.0, current_close=100.0,
        publish_date=_today(), today=_today(),
    )
    assert score == 80.0


def test_compute_report_score_unknown_rating_default() -> None:
    score = rs.compute_report_score(
        rating="", target_price=None, current_close=None,
        publish_date=_today(), today=_today(),
    )
    assert score == 40.0


def test_compute_report_score_recency_decay_half_life() -> None:
    old = _today() - timedelta(days=14)
    score = rs.compute_report_score(
        rating="买入", target_price=None, current_close=None,
        publish_date=old, today=_today(),
    )
    assert score == 40.0  # 80 × 0.5


def test_symbol_from_code_markets() -> None:
    assert rs._symbol_from_code("603606", "SHANGHAI") == "CN:603606.SH"
    assert rs._symbol_from_code("000001", "SHENZHEN") == "CN:000001.SZ"
    assert rs._symbol_from_code("920002", "BEIJING") is None
    assert rs._symbol_from_code("", "SHANGHAI") is None
    assert rs._symbol_from_code("12345", "SHANGHAI") is None


def test_build_research_catalyst_payload_aggregates(monkeypatch) -> None:
    today = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    reports = [
        {
            "stock_code": "603606", "stock_name": "东方电缆", "market": "SHANGHAI",
            "rating": "买入", "target_price": None, "org_name": "东吴证券",
            "title": "中报点评", "publish_date": yesterday,
        },
        {
            "stock_code": "603606", "stock_name": "东方电缆", "market": "SHANGHAI",
            "rating": "买入", "target_price": None, "org_name": "国泰海通",
            "title": "重申买入", "publish_date": today,
        },
        {
            "stock_code": "300001", "stock_name": "特锐德", "market": "SHENZHEN",
            "rating": "增持", "target_price": None, "org_name": "中金",
            "title": "增持评级", "publish_date": today,
        },
    ]
    monkeypatch.setattr(db, "fetch_reports_for_score_window", lambda window_days=14: reports)
    monkeypatch.setattr(rs, "_current_closes", lambda symbols: {})

    payload = rs.build_research_catalyst_payload()
    assert payload["total"] == 2
    by_sym = {x["symbol"]: x for x in payload["items"]}
    east = by_sym["CN:603606.SH"]
    # best 80 + confirm bonus 5 → 85
    assert east["catalystScore"] == 85.0
    assert east["articleCount"] == 2
    assert east["channel"] == "research"
    assert east["articles"][0]["catalystGrade"] == "S"
    assert east["articles"][0]["orgName"] == "国泰海通"
    assert by_sym["CN:300001.SZ"]["catalystScore"] == 60.0


def test_build_research_catalyst_payload_empty(monkeypatch) -> None:
    monkeypatch.setattr(db, "fetch_reports_for_score_window", lambda window_days=14: [])
    payload = rs.build_research_catalyst_payload()
    assert payload["total"] == 0
    assert payload["items"] == []


def test_build_research_catalyst_payload_skips_bj(monkeypatch) -> None:
    reports = [
        {
            "stock_code": "920002", "stock_name": "万达轴承", "market": "BEIJING",
            "rating": "买入", "target_price": None, "org_name": "开源证券",
            "title": "北交所更新", "publish_date": "2026-08-05",
        },
    ]
    monkeypatch.setattr(db, "fetch_reports_for_score_window", lambda window_days=14: reports)
    monkeypatch.setattr(rs, "_current_closes", lambda symbols: {})
    payload = rs.build_research_catalyst_payload()
    assert payload["total"] == 0


def test_sync_research_reports_parses_and_filters(monkeypatch) -> None:
    api_rows = [
        {
            "infoCode": "AP1", "stockCode": "603606", "stockName": "东方电缆",
            "title": "中报点评", "orgSName": "东吴证券", "emRatingName": "买入",
            "indvAimPriceT": "", "indvAimPriceL": "",
            "predictThisYearEps": "1.81", "predictThisYearPe": "21.95",
            "indvInduName": "电网设备", "market": "SHANGHAI",
            "publishDate": "2026-08-05 00:00:00.000", "encodeUrl": "abc",
        },
        {
            "infoCode": "AP2", "stockCode": "920002", "stockName": "万达轴承",
            "title": "北交所更新", "orgSName": "开源证券", "emRatingName": "增持",
            "indvAimPriceT": "", "indvAimPriceL": "",
            "predictThisYearEps": "", "predictThisYearPe": "",
            "indvInduName": "通用设备", "market": "BEIJING",
            "publishDate": "2026-08-05 00:00:00.000", "encodeUrl": "def",
        },
    ]

    captured: dict[str, object] = {}

    def fake_get_json(url, *, params, referer):
        captured["url"] = url
        captured["params"] = params
        return {"hits": 2, "data": api_rows}

    monkeypatch.setattr(rs, "em_get_json", fake_get_json)
    monkeypatch.setattr(db, "upsert_research_reports", lambda rows: len(rows))

    result = rs.sync_research_reports(days=3)
    assert result["fetched"] == 2
    # BEIJING row dropped before upsert
    assert result["inserted"] == 1
    assert captured["url"] == rs.REPORT_API_URL
    assert captured["params"]["qType"] == 0
    assert captured["params"]["beginTime"] == (date.today() - timedelta(days=3)).isoformat()


def test_refresh_report_scores_persists_scores(monkeypatch) -> None:
    reports = [
        {
            "id": 1, "stock_code": "603606", "market": "SHANGHAI",
            "rating": "买入", "target_price": None, "publish_date": _today().isoformat(),
        },
        {
            "id": 2, "stock_code": "300001", "market": "SHENZHEN",
            "rating": "增持", "target_price": None, "publish_date": _today().isoformat(),
        },
    ]
    monkeypatch.setattr(db, "fetch_reports_for_score_window", lambda window_days=14: reports)
    monkeypatch.setattr(rs, "_current_closes", lambda symbols: {"CN:603606.SH": 40.0})
    updates: list[tuple[float, int]] = []
    monkeypatch.setattr(
        db, "update_report_scores", lambda rows: (updates.extend(rows) or len(rows))
    )
    updated = rs.refresh_report_scores()
    assert updated == 2
    assert len(updates) == 2
    by_id = {rid: score for score, rid in updates}
    assert by_id[1] == 80.0  # 买入, fresh
    assert by_id[2] == 60.0  # 增持, fresh


def test_sync_research_reports_failure_reports_error(monkeypatch) -> None:
    def fake_get_json(url, *, params, referer):
        raise RuntimeError("boom")

    monkeypatch.setattr(rs, "em_get_json", fake_get_json)
    result = rs.sync_research_reports(days=3)
    assert result["ok"] is False
    assert result["error"] is not None
