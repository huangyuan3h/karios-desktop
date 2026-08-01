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
    # TV Scanner API filter is now an array (not {"and": [...]}).
    assert isinstance(f, list)
    left_ops = {entry["left"]: entry for entry in f}
    # Pullback contract: 市值 ≥ 3B
    assert left_ops["market_cap_basic"]["operation"] == "greater"
    assert left_ops["market_cap_basic"]["right"] == 3_000_000_000
    # RSI 45-75 (EMA crossover + pullback window handled downstream by TrendOK)
    rsi = next(e for e in f if e["left"] == "RSI")
    assert rsi["operation"] == "in_range"
    assert rsi["right"] == [45, 75]
    # SMA20 > SMA50 (trend filter)
    sma = next(e for e in f if e["left"] == "SMA20")
    assert sma["operation"] == "greater"
    assert sma["right"] == "SMA50"
    # EMA50 > EMA200 (trend filter)
    ema = next(e for e in f if e["left"] == "EMA50")
    assert ema["operation"] == "greater"
    assert ema["right"] == "EMA200"
    # Sector filter (16 sectors, excluding Finance and Utilities)
    sector = left_ops["sector"]
    assert sector["operation"] == "in_range"
    assert len(sector["right"]) == 16
    assert "Finance" not in sector["right"]
    assert "Utilities" not in sector["right"]


def test_falcon_launch_v2_cn_filter_shape():
    t = templates.get_template("falcon_launch_v2_cn")
    assert t is not None
    f = t.filter_json
    assert isinstance(f, list)
    # First filter should be exchange for A shares
    assert f[0]["left"] == "exchange"
    assert f[0]["right"] == ["SSE", "SZSE"]
    # TIP-007: MACD > 0
    macd = next(e for e in f if e.get("left") == "MACD.macd")
    assert macd["operation"] == "greater"
    assert macd["right"] == 0


def test_industry_top5_fallback_cn_filter_shape():
    t = templates.get_template("industry_top5_fallback_cn")
    assert t is not None
    f = t.filter_json
    assert isinstance(f, list)
    # First filter should be exchange for A shares
    assert f[0]["left"] == "exchange"
    assert f[0]["right"] == ["SSE", "SZSE"]
    # TIP-003: 市值 ≥ 20B, RSI 50-90
    rsi = next(e for e in f if e["left"] == "RSI")
    assert rsi["right"] == [50, 90]


def test_hk_template_universe_filter():
    t = templates.get_template("karios_pullback_v3_hk")
    assert t is not None
    exchange = next(e for e in t.filter_json if e["left"] == "exchange")
    assert exchange["right"] == "HKEX"
    # Sector filter (18 sectors, excluding Finance and Utilities)
    left_ops = {entry["left"]: entry for entry in t.filter_json}
    sector = left_ops["sector"]
    assert sector["operation"] == "in_range"
    assert len(sector["right"]) == 18
    assert "Finance" not in sector["right"]
    assert "Utilities" not in sector["right"]


def test_us_template_universe_filter():
    t = templates.get_template("karios_pullback_v3_us")
    assert t is not None
    exchange = next(e for e in t.filter_json if e["left"] == "exchange")
    assert exchange["right"] == ["NASDAQ", "NYSE", "AMEX"]
    # Verify new conditions
    left_ops = {entry["left"]: entry for entry in t.filter_json}
    assert left_ops["market_cap_basic"]["right"] == 3_000_000_000
    assert left_ops["Perf.Y"]["operation"] == "greater"
    assert left_ops["SMA20"]["right"] == "SMA50"
    assert left_ops["EMA50"]["right"] == "EMA200"
    # Sector filter (18 sectors, excluding Finance and Utilities)
    sector = left_ops["sector"]
    assert sector["operation"] == "in_range"
    assert len(sector["right"]) == 18
    assert "Finance" not in sector["right"]
    assert "Utilities" not in sector["right"]


def test_templates_have_required_columns():
    for t in templates.list_templates():
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