"""Tests for index signal rules: 3-day confirmation, breadth gating, volume expansion."""

from datetime import datetime
from zoneinfo import ZoneInfo


def _make_series_3d_confirm(days: int = 80, base: float = 100.0, drift: float = 0.5) -> list[tuple[str, float, float]]:
    """Create (date, close, vol) series with 3-day MA20 confirmation and rising trend."""
    out: list[tuple[str, float, float]] = []
    for i in range(days):
        d = f"2026-01-{i+1:02d}" if i < 31 else f"2026-02-{i-30:02d}"
        close = base + i * drift
        vol = 1e9 + i * 1e7
        out.append((d, close, vol))
    return out


def _make_series_first_break(days: int = 80, base: float = 100.0) -> list[tuple[str, float, float]]:
    """Create series where only last close is above MA20 (first break, no 3-day confirm)."""
    out: list[tuple[str, float, float]] = []
    for i in range(days):
        d = f"2026-01-{i+1:02d}" if i < 31 else f"2026-02-{i-30:02d}"
        close = base - 2.0 if i < days - 1 else base + 5.0
        vol = 1e9
        out.append((d, close, vol))
    return out


def test_3day_confirmation_yields_light_green(monkeypatch) -> None:
    import data_sync_service.service.market_regime as mr  # type: ignore[import-not-found]

    series = _make_series_3d_confirm()
    monkeypatch.setattr(
        mr,
        "fetch_last_closes_vol_upto",
        lambda ts_code, as_of, days=80: series,
    )
    monkeypatch.setattr(mr, "_get_breadth_above_ma20_ratio", lambda **_: {"ratio": 0.6, "total": 100, "above_count": 60})

    signals = mr.get_index_signals(as_of_date="2026-02-24")
    assert len(signals) >= 1
    s = signals[0]
    assert s["signal"] in ("light_green", "deep_green")


def test_first_break_yields_yellow(monkeypatch) -> None:
    import data_sync_service.service.market_regime as mr  # type: ignore[import-not-found]

    series = _make_series_first_break()
    monkeypatch.setattr(
        mr,
        "fetch_last_closes_vol_upto",
        lambda ts_code, as_of, days=80: series,
    )
    monkeypatch.setattr(mr, "_get_breadth_above_ma20_ratio", lambda **_: {"ratio": 0.6, "total": 100, "above_count": 60})

    signals = mr.get_index_signals(as_of_date="2026-02-24")
    assert len(signals) >= 1
    s = signals[0]
    assert s["signal"] == "yellow"


def test_breadth_gating_downgrades_to_yellow(monkeypatch) -> None:
    import data_sync_service.service.market_regime as mr  # type: ignore[import-not-found]

    series = _make_series_3d_confirm()
    monkeypatch.setattr(
        mr,
        "fetch_last_closes_vol_upto",
        lambda ts_code, as_of, days=80: series,
    )
    monkeypatch.setattr(mr, "_get_breadth_above_ma20_ratio", lambda **_: {"ratio": 0.3, "total": 100, "above_count": 30})

    signals = mr.get_index_signals(as_of_date="2026-02-24")
    assert len(signals) >= 1
    s = signals[0]
    assert s["signal"] == "yellow"
    assert any("breadth" in str(r).lower() for r in (s.get("rules") or []))


def test_signal_rank_treats_light_deep_green_as_green() -> None:
    import data_sync_service.service.market_regime as mr  # type: ignore[import-not-found]

    assert mr._signal_rank("green") == 3
    assert mr._signal_rank("light_green") == 3
    assert mr._signal_rank("deep_green") == 3
    assert mr._signal_rank("yellow") == 2
    assert mr._signal_rank("red") == 1
