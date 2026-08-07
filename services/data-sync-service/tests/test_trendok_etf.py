"""ETF-specific trendok behavior: rule isolation from stock hard-exit + fallback stops."""
from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import patch

import pytest

from data_sync_service.service.trendok import _symbol_to_ts_code, _trendok_one  # type: ignore[import-not-found]


def test_symbol_to_ts_code_accepts_suffixed_cn():
    assert _symbol_to_ts_code("CN:002064.SZ") == ("CN", "002064", "002064.SZ")
    assert _symbol_to_ts_code("CN:603259.SH") == ("CN", "603259", "603259.SH")
    assert _symbol_to_ts_code("CN:688266.SH") == ("CN", "688266", "688266.SH")
    assert _symbol_to_ts_code("ETF:513180.SH") == ("ETF", "513180", "513180.SH")
    assert _symbol_to_ts_code("HK:00700.HK") == ("HK", "00700", "00700.HK")


def test_symbol_to_ts_code_still_rejects_invalid():
    assert _symbol_to_ts_code("CN:") is None
    assert _symbol_to_ts_code("CN:12345") is None
    assert _symbol_to_ts_code("CN:abcdef") is None


def _bars(
    prices: list[float],
    last_n_vol: tuple[int, float] | None = None,
) -> list[tuple[str, str, str, str, str, str]]:
    out: list[tuple[str, str, str, str, str, str]] = []
    start = date(2025, 1, 1)
    tail_n, tail_vol = last_n_vol or (0, 0.0)
    for i, close in enumerate(prices):
        open_p = close - 0.02
        high = close + 0.03
        low = close - 0.03
        if tail_n > 0 and i >= len(prices) - tail_n:
            vol = f"{tail_vol:.1f}"
        else:
            vol = f"{1000.0 + i * 10:.1f}"
        out.append(
            (
                (start + timedelta(days=i)).isoformat(),
                f"{open_p:.4f}",
                f"{high:.4f}",
                f"{low:.4f}",
                f"{close:.4f}",
                vol,
            )
        )
    return out


def _run(symbol: str, prices: list[float], **bars_kw) -> dict:
    with patch(
        "data_sync_service.service.trendok._compute_watchlist_score_v4",
        return_value=(88.0, {"base": 88.0}),
    ):
        return _trendok_one(
            symbol=symbol,
            name="Test",
            industry=None,
            bars=_bars(prices, **bars_kw),
        )


def _rise_then_dip_below_ema20() -> list[float]:
    """Steady uptrend then a shallow 4-day pullback that closes below EMA20."""
    prices = [20.0 + i * 0.25 for i in range(60)]
    peak = prices[-1]
    prices += [peak * (1 - 0.01), peak * (1 - 0.03), peak * (1 - 0.05), peak * (1 - 0.07)]
    return prices


def _expanding_uptrend(n: int = 70) -> list[float]:
    """Accelerating growth -> MACD hist expands; no shrink warning."""
    prices: list[float] = []
    p = 20.0
    for i in range(n):
        prices.append(round(p, 4))
        p *= 1 + 0.0005 + i * 0.00005
    return prices


def test_stock_trend_structure_break_still_exit_now() -> None:
    res = _run("CN:000001", _rise_then_dip_below_ema20())
    parts = res["stopLossParts"] or {}
    assert parts["exit_now"] is True
    assert any("trend_structure_break" in r for r in parts["exit_reasons"])


def test_etf_trend_structure_break_downgrades_to_trim_warning() -> None:
    res = _run("ETF:515880", _rise_then_dip_below_ema20())
    parts = res["stopLossParts"] or {}
    assert parts["exit_now"] is False
    assert parts["warn_reduce_half"] is True
    assert any("trend_structure_break" in r for r in parts["warn_reasons"])
    # Price-based stop remains a normal level below current (not "stop = current").
    assert res["stopLossPrice"] is not None
    assert parts["final_stop_loss"] < float(res["values"]["close"])


def test_etf_uptrend_no_exit_no_warn() -> None:
    res = _run("ETF:515880", _expanding_uptrend())
    parts = res["stopLossParts"] or {}
    assert parts["exit_now"] is False
    assert parts["warn_reduce_half"] is False


def test_etf_momentum_exhaustion_downgrades_to_trim() -> None:
    # MACD hist shrinks 3 days then flips negative + volume dries up
    # (shrink_then_flip + vol_dry) -> stock exits, ETF only trims.
    from data_sync_service.service import trendok as trendok_mod

    real_macd = trendok_mod._macd

    def fake_macd(values, *a, **k):
        macd_line, sig, hist = real_macd(values, *a, **k)
        if len(hist) < 4:
            return macd_line, sig, hist
        hist = list(hist)
        base = abs(hist[-1]) + 0.05
        hist[-4] = base
        hist[-3] = base * 0.6
        hist[-2] = base * 0.25
        hist[-1] = -base * 0.5
        return macd_line, sig, hist

    with patch(
        "data_sync_service.service.trendok._compute_watchlist_score_v4",
        return_value=(88.0, {"base": 88.0}),
    ), patch("data_sync_service.service.trendok._macd", side_effect=fake_macd):
        res = _trendok_one(
            symbol="ETF:515880",
            name="Test",
            industry=None,
            bars=_bars(_expanding_uptrend(), last_n_vol=(5, 150.0)),
        )
    parts = res["stopLossParts"] or {}
    assert parts["exit_now"] is False
    assert parts["warn_reduce_half"] is True
    assert any("momentum_exhaustion" in r for r in parts["warn_reasons"])


def test_etf_fallback_stop_when_ema20_missing() -> None:
    # Too few bars for EMA20 (only 10) -> ETF gets a -8% drawdown fallback stop.
    res = _run("ETF:512480", [20.0 + i * 0.1 for i in range(10)])
    parts = res["stopLossParts"] or {}
    assert parts.get("etf_fallback") is True
    assert parts["etf_fallback_reason"] == "no_ema20"
    assert res["stopLossPrice"] is not None
    assert parts["hard_stop"] == pytest.approx(20.9 * 0.92, rel=1e-3)
    assert res["stopLossPrice"] == parts["hard_stop"]


def test_etf_fallback_stop_when_atr14_missing() -> None:
    # EMA20 available but ATR-14 fails (data anomaly) -> price-drawdown fallback.
    with patch("data_sync_service.service.trendok._atr14", return_value=None):
        res = _run("ETF:512480", [20.0 + i * 0.1 for i in range(30)])
    parts = res["stopLossParts"] or {}
    assert parts.get("etf_fallback") is True
    assert parts["etf_fallback_reason"] == "no_atr14"
    assert res["stopLossPrice"] is not None
    assert parts["hard_stop"] is not None


def test_etf_full_bars_no_fallback() -> None:
    res = _run("ETF:515880", _expanding_uptrend())
    parts = res["stopLossParts"] or {}
    assert parts.get("etf_fallback") is not True
    assert parts["exit_now"] is False
    assert parts["warn_reduce_half"] is False
