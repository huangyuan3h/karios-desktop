from data_sync_service.service.mainline import _is_mainline, _score_breadth, _score_flow, _score_trend  # type: ignore[import-not-found]


def test_flow_score_full() -> None:
    ctx = {
        "sum20": {"A": 100.0},
        "sum5": {"A": 50.0},
        "rank20": {"A": 1},
        "rank5": {"A": 1},
        "pos10": {"A": 6},
    }
    score, flags = _score_flow("A", ctx)
    assert score == 40.0
    assert flags["midAccumulation"] is True
    assert flags["shortIntensity"] is True
    assert flags["consistency"] is True


def test_breadth_score_full() -> None:
    ctx = {
        "rows": {
            "A": {
                "limit_up_count": 3,
                "limit_up_2d_count": 1,
                "surge_ratio": 0.06,
            }
        },
        "limit_rank": {"A": 2},
    }
    score, flags = _score_breadth("A", ctx)
    assert score == 40.0
    assert flags["limitUpQualified"] is True
    assert flags["dragonQualified"] is True
    assert flags["surgeQualified"] is True


def test_trend_score_full() -> None:
    dates = [f"2024-01-{i:02d}" for i in range(1, 22)]
    series = {"A": [(d, float(i + 1)) for i, d in enumerate(dates)]}
    market_avg_close = {d: 1.0 for d in dates}
    ctx = {"dates": dates, "series": series, "market_avg_close": market_avg_close}
    score, flags = _score_trend("A", ctx)
    assert score == 20.0
    assert flags["indexAboveMa20"] is True
    assert flags["rpsQualified"] is True


def test_is_mainline_streak() -> None:
    recent_scores = {
        "2024-01-01": {"total_score": 81.0},
        "2024-01-02": {"total_score": 82.0},
        "2024-01-03": {"total_score": 83.0},
    }
    assert _is_mainline([{"total_score": 83.0}], recent_scores) is True
