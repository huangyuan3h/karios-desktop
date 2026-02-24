"""Tests for index signal rules: slope, volume, breadth, and deep green."""

def _make_series_uptrend(days: int = 80, base: float = 100.0, drift: float = 0.8) -> list[tuple[str, float, float]]:
    """Create (date, close, vol) series with MA20 slope up and MA5>MA20."""
    out: list[tuple[str, float, float]] = []
    for i in range(days):
        d = f"2026-01-{i+1:02d}" if i < 31 else f"2026-02-{i-30:02d}"
        close = base + i * drift
        vol = 1e9 + i * 1e7
        out.append((d, close, vol))
    return out


def _make_series_flat_volume(series: list[tuple[str, float, float]], vol: float = 1e9) -> list[tuple[str, float, float]]:
    """Keep closes, force flat volume so current vol is not above MA5."""
    return [(d, c, vol) for d, c, _ in series]


def _make_series_dead_cross(days: int = 80, base: float = 100.0) -> list[tuple[str, float, float]]:
    """Create series with MA5 < MA20 (downtrend)."""
    out: list[tuple[str, float, float]] = []
    for i in range(days):
        d = f"2026-01-{i+1:02d}" if i < 31 else f"2026-02-{i-30:02d}"
        close = base - i * 0.5
        vol = 1e9
        out.append((d, close, vol))
    return out


def test_green_when_ma20_up_ma5_above_and_vol_ratio(monkeypatch) -> None:
    import data_sync_service.service.market_regime as mr  # type: ignore[import-not-found]

    series = _make_series_uptrend()
    monkeypatch.setattr(
        mr,
        "fetch_last_closes_vol_upto",
        lambda ts_code, as_of, days=80: series,
    )
    monkeypatch.setattr(mr, "_get_breadth_above_ma20_ratio", lambda **_: {"ratio": 0.4, "total": 100, "above_count": 40})

    signals = mr.get_index_signals(as_of_date="2026-02-24")
    assert len(signals) >= 1
    s = signals[0]
    assert s["signal"] == "green"


def test_yellow_when_vol_below_ma5(monkeypatch) -> None:
    import data_sync_service.service.market_regime as mr  # type: ignore[import-not-found]

    series = _make_series_flat_volume(_make_series_uptrend())
    monkeypatch.setattr(
        mr,
        "fetch_last_closes_vol_upto",
        lambda ts_code, as_of, days=80: series,
    )
    monkeypatch.setattr(mr, "_get_breadth_above_ma20_ratio", lambda **_: {"ratio": 0.7, "total": 100, "above_count": 70})

    signals = mr.get_index_signals(as_of_date="2026-02-24")
    assert len(signals) >= 1
    s = signals[0]
    assert s["signal"] == "yellow"


def test_red_when_dead_cross(monkeypatch) -> None:
    import data_sync_service.service.market_regime as mr  # type: ignore[import-not-found]

    series = _make_series_dead_cross()
    monkeypatch.setattr(
        mr,
        "fetch_last_closes_vol_upto",
        lambda ts_code, as_of, days=80: series,
    )
    monkeypatch.setattr(mr, "_get_breadth_above_ma20_ratio", lambda **_: {"ratio": 0.9, "total": 100, "above_count": 90})

    signals = mr.get_index_signals(as_of_date="2026-02-24")
    assert len(signals) >= 1
    s = signals[0]
    assert s["signal"] == "red"


def test_deep_green_requires_breadth_and_volume(monkeypatch) -> None:
    import data_sync_service.service.market_regime as mr  # type: ignore[import-not-found]

    series = _make_series_uptrend()
    # Inflate last volumes to satisfy strong volume condition.
    series = [
        (d, c, v if i < len(series) - 3 else v * 2.0)
        for i, (d, c, v) in enumerate(series)
    ]
    monkeypatch.setattr(
        mr,
        "fetch_last_closes_vol_upto",
        lambda ts_code, as_of, days=80: series,
    )

    monkeypatch.setattr(mr, "_get_breadth_above_ma20_ratio", lambda **_: {"ratio": 0.55, "total": 100, "above_count": 55})
    signals = mr.get_index_signals(as_of_date="2026-02-24")
    assert len(signals) >= 1
    assert signals[0]["signal"] == "green"

    monkeypatch.setattr(mr, "_get_breadth_above_ma20_ratio", lambda **_: {"ratio": 0.7, "total": 100, "above_count": 70})
    signals2 = mr.get_index_signals(as_of_date="2026-02-24")
    assert len(signals2) >= 1
    assert signals2[0]["signal"] == "deep_green"


def test_signal_rank_treats_light_deep_green_as_green() -> None:
    import data_sync_service.service.market_regime as mr  # type: ignore[import-not-found]

    assert mr._signal_rank("green") == 3
    assert mr._signal_rank("deep_green") == 3
    assert mr._signal_rank("yellow") == 2
    assert mr._signal_rank("red") == 1
