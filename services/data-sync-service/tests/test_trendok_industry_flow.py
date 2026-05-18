from data_sync_service.service.trendok import _industry_flow_score_adjustment  # type: ignore[import-not-found]


def _ctx() -> dict:
    return {
        "ok": True,
        "top_today_3": {"C"},
        "top_today_5": {"C", "D", "E"},
        "top_yesterday_3": {"F"},
        "top_5d_3": {"A"},
        "bottom_5d_5": {"B"},
        "net_today": {
            "F": -2.0e8,
            "G": -2.0e8,
            "H": -2.0e8,
        },
        "net_yesterday": {
            "G": -2.0e8,
        },
    }


def test_industry_flow_top_5d_bonus() -> None:
    delta, parts, _ = _industry_flow_score_adjustment("A", _ctx())
    assert delta == 10.0
    assert parts["industry_flow_5d_top3"] == 10.0


def test_industry_flow_bottom_5d_penalty() -> None:
    delta, parts, _ = _industry_flow_score_adjustment("B", _ctx())
    assert delta == -20.0
    assert parts["industry_flow_5d_bottom5"] == -20.0


def test_hotspot_today_top3() -> None:
    delta, parts, _ = _industry_flow_score_adjustment("C", _ctx())
    assert delta == 5.0
    assert parts["hotspots_today_top3"] == 5.0


def test_hotspot_today_top4_5() -> None:
    delta, parts, _ = _industry_flow_score_adjustment("D", _ctx())
    assert delta == 3.0
    assert parts["hotspots_today_top4_5"] == 3.0


def test_hotspot_falloff_big_outflow() -> None:
    delta, parts, _ = _industry_flow_score_adjustment("F", _ctx())
    assert delta == -15.0
    assert parts["hotspot_falloff_big_outflow"] == -15.0


def test_hotspot_absent_two_day_outflow() -> None:
    delta, parts, _ = _industry_flow_score_adjustment("G", _ctx())
    assert delta == -10.0
    assert parts["hotspot_absent_2d_big_outflow"] == -10.0

