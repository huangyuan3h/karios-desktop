from __future__ import annotations


def _make_bars(days: int = 40, start_price: float = 10.0, step: float = 0.3) -> list[tuple[str, str, str, str, str, str]]:
    out: list[tuple[str, str, str, str, str, str]] = []
    for i in range(days):
        date = f"2025-02-{i + 1:02d}"
        close = start_price + i * step
        open_p = close - 0.15
        high = close + 0.35
        low = close - 0.4
        vol = 1000 + i * 15
        out.append((date, f"{open_p:.3f}", f"{high:.3f}", f"{low:.3f}", f"{close:.3f}", f"{vol:.3f}"))
    return out


def test_trendok_strong_allows_mode_b() -> None:
    import data_sync_service.service.trendok as trendok  # type: ignore[import-not-found]

    bars = _make_bars()
    res = trendok._trendok_one(  # type: ignore[attr-defined]
        symbol="CN:000001",
        name="Test",
        industry=None,
        bars=bars,
        flow_ctx=None,
        market_regime="Strong",
    )
    assert res.get("buyChecks", {}).get("in_trend") is True
    assert res.get("buyChecks", {}).get("mode_b_allowed") is True
    assert res.get("buyMode") == "B_momentum"


def test_trendok_weak_disables_mode_b() -> None:
    import data_sync_service.service.trendok as trendok  # type: ignore[import-not-found]

    bars = _make_bars()
    res = trendok._trendok_one(  # type: ignore[attr-defined]
        symbol="CN:000001",
        name="Test",
        industry=None,
        bars=bars,
        flow_ctx=None,
        market_regime="Weak",
    )
    assert res.get("buyChecks", {}).get("mode_b_blocked") is True
    assert res.get("buyMode") == "A_pullback"
