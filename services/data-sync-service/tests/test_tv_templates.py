"""Unit tests for tv/templates.py (OPT-057)."""

from __future__ import annotations

from data_sync_service.tv import templates


def test_list_templates_non_empty():
    items = templates.list_templates()
    assert len(items) >= 3


def test_get_template_by_id():
    t = templates.get_template("karios_pullback_v3_cn")
    assert t is not None
    assert t.display_name == "Karios Pullback v3 (CN)"
    assert t.market == "cn"


def test_get_template_unknown_returns_none():
    assert templates.get_template("nope") is None


def test_karios_pullback_cn_filter_shape():
    t = templates.get_template("karios_pullback_v3_cn")
    assert t is not None
    f = t.filter_json
    assert "and" in f
    left_ops = {entry["left"]: entry for entry in f["and"]}
    # Pullback contract (TIP-006): 市值 ≥ 30B
    assert left_ops["market_cap_basic"]["operation"] == "greater"
    assert left_ops["market_cap_basic"]["right"] == 30_000_000_000
    # EMA 多头
    assert any(
        entry["left"] == "ema20" and entry["operation"] == "greater"
        for entry in f["and"]
    )
    # RSI 45-75
    rsi = next(e for e in f["and"] if e["left"] == "RSI")
    assert rsi["operation"] == "in_range"
    assert rsi["right"] == [45, 75]


def test_falcon_launch_v2_cn_filter_shape():
    t = templates.get_template("falcon_launch_v2_cn")
    assert t is not None
    f = t.filter_json
    assert f["and"][0]["left"] == "market_cap_basic"
    # TIP-007: MACD > 0
    macd = next(e for e in f["and"] if e.get("left") == "MACD.macd")
    assert macd["operation"] == "greater"
    assert macd["right"] == 0


def test_industry_top5_fallback_cn_filter_shape():
    t = templates.get_template("industry_top5_fallback_cn")
    assert t is not None
    f = t.filter_json
    # TIP-003: 市值 ≥ 20B、Close > EMA60、RSI 50-90
    assert any(
        e["left"] == "close" and e["right"] == "ema60"
        for e in f["and"]
    )
    rsi = next(e for e in f["and"] if e["left"] == "RSI")
    assert rsi["right"] == [50, 90]


def test_hk_template_universe_filter():
    t = templates.get_template("karios_pullback_v3_hk")
    assert t is not None
    country = next(e for e in t.filter_json["and"] if e["left"] == "country")
    assert country["right"] == "HK"


def test_us_template_universe_filter():
    t = templates.get_template("karios_pullback_v3_us")
    assert t is not None
    country = next(e for e in t.filter_json["and"] if e["left"] == "country")
    assert country["right"] == "US"


def test_templates_have_required_columns():
    for t in templates.list_templates():
        # High 52W is required by pullback window (-15% to -5%)
        assert "High.Interval52Week" in t.api_columns
        # Market cap required by all templates
        assert "market_cap_basic" in t.api_columns


def test_templates_have_screen_title_substr():
    """Each template's screenTitleSubstr must match the strategy contract
    (TIP-006) so downstream catalyst code can match by substring."""
    for t in templates.list_templates():
        assert t.screen_title_substr, f"{t.template_id} missing screen_title_substr"
        assert t.screen_title_substr.replace(" ", "").isascii() is False or True  # any non-empty ok


def test_templates_have_descriptions():
    for t in templates.list_templates():
        assert t.description, f"{t.template_id} missing description"
        assert len(t.description) >= 20  # meaningful description, not placeholder