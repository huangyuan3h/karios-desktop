"""mainline score/context drivers (mocked db rows)."""

from __future__ import annotations

from data_sync_service.service import mainline as ml


def test_rank_map() -> None:
    assert ml._rank_map({"A": 3.0, "B": 1.0, "C": 2.0}) == {"A": 1, "C": 2, "B": 3}
    assert ml._rank_map({"A": 1.0, "B": 3.0}, desc=False) == {"B": 2, "A": 1}


def test_flow_context_buckets(monkeypatch) -> None:
    monkeypatch.setattr(
        ml, "flow_dates_upto", lambda d, n: [f"2026-07-{x:02d}" for x in range(1, 21)]
    )
    monkeypatch.setattr(
        ml,
        "flow_rows_for_dates",
        lambda dates: [
            {"date": d, "industry_name": "银行", "net_inflow": 100.0} for d in dates[:5]
        ],
    )
    monkeypatch.setattr(
        ml, "sum_by_industry_from_rows", lambda rows, dates: {"银行": 500.0}
    )
    monkeypatch.setattr(ml, "positive_days_from_rows", lambda rows, dates: {"银行": 5})
    ctx = ml._flow_context("2026-07-21")
    assert len(ctx["dates_20"]) == 20
    assert len(ctx["dates_10"]) == 10
    assert len(ctx["dates_5"]) == 5
    assert ctx["rank20"] == {"银行": 1}
    assert ctx["pos10"] == {"银行": 5}


def test_breadth_context(monkeypatch) -> None:
    monkeypatch.setattr(
        ml,
        "metrics_rows_by_date",
        lambda d: [
            {"industry_name": "银行", "limit_up_count": 5},
            {"industry_name": "半导体", "limit_up_count": 2},
        ],
    )
    ctx = ml._breadth_context("2026-07-21")
    assert ctx["limit_rank"] == {"银行": 1, "半导体": 2}
    assert ctx["rows"]["银行"]["limit_up_count"] == 5


def test_trend_context(monkeypatch) -> None:
    monkeypatch.setattr(ml, "_trade_dates_upto", lambda d, n: ["2026-07-01", "2026-07-02"])
    monkeypatch.setattr(
        ml,
        "metrics_rows_for_dates",
        lambda dates: [
            {"date": "2026-07-01", "industry_name": "银行", "avg_close": 10.0, "total_count": 2},
            {"date": "2026-07-02", "industry_name": "银行", "avg_close": 10.5, "total_count": 2},
        ],
    )
    ctx = ml._trend_context("2026-07-21")
    assert ctx["series"]["银行"] == [("2026-07-01", 10.0), ("2026-07-02", 10.5)]
    assert abs(ctx["market_avg_close"]["2026-07-01"] - 10.0) < 1e-9


def test_compute_scores_for_date_drives_all_factors(monkeypatch) -> None:
    monkeypatch.setattr(ml, "_flow_context", lambda d: {
        "dates_20": ["d"], "dates_10": ["d"], "dates_5": ["d"],
        "sum20": {"银行": 1.0, "半导体": 2.0}, "sum5": {},
        "rank20": {"银行": 2, "半导体": 1}, "rank5": {}, "pos10": {"银行": 3},
    })
    monkeypatch.setattr(ml, "_breadth_context", lambda d: {
        "rows": {"银行": {"limit_up_count": 1}, "半导体": {"limit_up_count": 0}},
        "limit_rank": {"银行": 1, "半导体": 2},
    })
    monkeypatch.setattr(ml, "_trend_context", lambda d: {
        "dates": ["d"], "series": {}, "market_avg_close": {},
    })
    monkeypatch.setattr(ml, "_score_flow", lambda industry, ctx: (1.0, {"f": 1}))
    monkeypatch.setattr(ml, "_score_breadth", lambda industry, ctx: (2.0, {}))
    monkeypatch.setattr(ml, "_score_trend", lambda industry, ctx: (3.0, {}))
    out = ml.compute_scores_for_date("2026-07-21")
    industries = {r["industry_name"] for r in out}
    assert industries == {"银行", "半导体"}
    for r in out:
        assert r["total_score"] == 6.0


def test_ensure_scores_for_dates_skips_existing(monkeypatch) -> None:
    monkeypatch.setattr(ml, "scores_rows_by_date", lambda d: [{"x": 1}])
    monkeypatch.setattr(ml, "compute_scores_for_date", lambda d: [])
    monkeypatch.setattr(ml, "scores_upsert_rows", lambda rows: len(rows))
    assert ml.ensure_scores_for_dates(["2026-07-21"]) == {"ensured": 0}


def test_is_mainline_thresholds() -> None:
    assert ml._is_mainline(
        [{"industry_name": "银行"}],
        {"2026-07-21": {"total_score": 85.0}},
    ) is True
    assert ml._is_mainline(
        [{"industry_name": "银行"}],
        {"2026-07-21": {"total_score": 60.0}},
    ) is False
    assert ml._is_mainline([], {}) is False


def test_get_cn_industry_mainline_no_date() -> None:
    assert ml.get_cn_industry_mainline(as_of_date="")["currentMainline"] == []
