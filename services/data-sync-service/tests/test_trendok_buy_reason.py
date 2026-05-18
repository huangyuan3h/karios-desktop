from __future__ import annotations


def _flat_bars(days: int = 40, price: float = 10.0, vol: float = 1000.0) -> list[tuple[str, str, str, str, str, str]]:
    out: list[tuple[str, str, str, str, str, str]] = []
    for i in range(days):
        date = f"2025-03-{i + 1:02d}"
        open_p = price
        high = price
        low = price
        close = price
        out.append((date, f"{open_p:.3f}", f"{high:.3f}", f"{low:.3f}", f"{close:.3f}", f"{vol:.3f}"))
    return out


def test_trendok_pullback_reason_missing_breakout() -> None:
    import data_sync_service.service.trendok as trendok  # type: ignore[import-not-found]

    bars = _flat_bars()
    res = trendok._trendok_one(  # type: ignore[attr-defined]
        symbol="CN:000001",
        name="Test",
        industry=None,
        bars=bars,
        flow_ctx=None,
        market_regime="Weak",
    )
    assert res.get("buyMode") == "A_pullback"
    assert res.get("buyWhy") == "模式A：未找到近5日突破日"
