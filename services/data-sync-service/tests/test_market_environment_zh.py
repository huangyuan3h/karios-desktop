"""Tests for Chinese one-line market environment summary."""

from data_sync_service.service.market_environment_zh import format_market_environment_zh

# Mirror macro_daily series ids (avoid importing macro_daily in tests — heavy deps).
SID_IXIC = "IXIC"
SID_USDCNH = "USDCNH.FXCM"
SID_A50 = "A50"
SID_COMM_ENERGY = "COMM_ENERGY"
SID_COMM_GOLD = "COMM_GOLD"
SID_COMM_COPPER = "COMM_COPPER"


def test_format_market_environment_zh_full_shape() -> None:
    snap = {
        "cnIndexSignals": [
            {
                "name": "上证指数",
                "tsCode": "000001.SH",
                "close": 3916.12,
                "signal": "red",
                "positionRange": "0%-10%",
            },
            {
                "name": "创业板指",
                "tsCode": "399006.SZ",
                "close": 3309.59,
                "signal": "yellow",
                "positionRange": "30%",
            },
        ],
        "macro": [
            {
                "seriesId": SID_IXIC,
                "close": 21929.83,
                "pctChg": 0.77,
                "asOfDate": "2026-03-25",
            },
            {
                "seriesId": SID_USDCNH,
                "close": 6.92,
                "pctChg": None,
                "asOfDate": "2026-03-26",
            },
            {
                "seriesId": SID_A50,
                "close": 14563.82,
                "pctChg": -0.82,
                "asOfDate": "2026-03-26",
            },
            {
                "seriesId": SID_COMM_ENERGY,
                "close": 702.8,
                "pctChg": None,
                "asOfDate": "2026-03-26",
            },
            {
                "seriesId": SID_COMM_GOLD,
                "close": 1009.38,
                "pctChg": None,
                "asOfDate": "2026-03-26",
            },
            {
                "seriesId": SID_COMM_COPPER,
                "close": 94740.0,
                "pctChg": None,
                "asOfDate": "2026-03-26",
            },
        ],
    }
    out = format_market_environment_zh(snap)
    assert out.startswith("市场环境摘要：")
    assert "上证指数收报3916.12" in out
    assert "创业板指收报3309.59" in out
    assert "纳指收报21929.83" in out
    assert "涨0.77%" in out
    assert "离岸人民币报6.92" in out
    assert "富时A50收报14563.82" in out
    assert "跌0.82%" in out
    assert "INE原油主力收报702.80" in out
    assert "沪金主力收报1009.38" in out
    assert "沪铜主力收报94740" in out


def test_format_market_environment_zh_empty() -> None:
    assert format_market_environment_zh(None) == ""
    assert format_market_environment_zh({}) == ""
