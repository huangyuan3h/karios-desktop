"""commodity_signals — pure RSI/MA + hint branches (DB mocked)."""

from __future__ import annotations

import pytest

from data_sync_service.service import commodity_signals as mod


def test_rsi_all_gains_is_100() -> None:
    assert mod._rsi(list(range(20))) == 100.0


def test_rsi_insufficient_is_none() -> None:
    assert mod._rsi([1.0, 2.0, 3.0]) is None


def test_rsi_mixed_series_between_0_and_100() -> None:
    closes = [10 + (i % 3) for i in range(30)]
    rsi = mod._rsi(closes)
    assert rsi is not None and 0 < rsi < 100


def test_ma_average_and_guard() -> None:
    assert mod._ma([1.0, 2.0, 3.0], 3) == pytest.approx(2.0)
    assert mod._ma([1.0], 5) is None


def test_closes_filters_bad_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    bars = [{"close": 10.0}, {"close": "11"}, {"close": 0}, {"close": None}, {"bad": 1}]
    monkeypatch.setattr(mod, "fetch_last_bars", lambda ts, days=250: bars)
    assert mod._closes("X") == [10.0, 11.0]


def test_closes_swallows_fetch_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(ts, days=250):
        raise RuntimeError("network")

    monkeypatch.setattr(mod, "fetch_last_bars", boom)
    assert mod._closes("X") == []


def _patch_signal(
    monkeypatch: pytest.MonkeyPatch,
    *,
    close: float,
    ma10: float,
    ma20: float,
    ma60: float,
    rsi: float | None,
) -> None:
    monkeypatch.setattr(mod, "_closes", lambda ts, days=250: [close] * 60)

    def fake_ma(closes, n):  # noqa: ANN001
        return {10: ma10, 20: ma20, 60: ma60, 200: ma60}[n]

    monkeypatch.setattr(mod, "_ma", fake_ma)
    monkeypatch.setattr(mod, "_rsi", lambda closes, period=14: rsi)


def test_signal_insufficient_bars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod, "_closes", lambda ts, days=250: [1.0] * 10)
    out = mod.signal_for("518880.SH")
    assert out["ok"] is False and out["n"] == 10


@pytest.mark.parametrize(
    "ts,close,ma10,ma20,ma60,rsi,action",
    [
        # gold: oversold -> BUY; trend -> HOLD; below MA10 -> HOLD; else DONT_BUY
        ("518880.SH", 1.0, 1.0, 2.0, 1.0, 20.0, "BUY"),
        ("518880.SH", 1.0, 1.0, 2.0, 1.0, 50.0, "HOLD"),
        ("518880.SH", 1.0, 2.0, 1.0, 2.0, 50.0, "HOLD"),
        ("518880.SH", 3.0, 1.0, 1.0, 2.0, 50.0, "DONT_BUY"),
        # oil QDII: oversold BUY; above MA10 SELL; below MA10 HOLD
        ("513350.SH", 1.0, 1.0, 1.0, 1.0, 20.0, "BUY"),
        ("513350.SH", 3.0, 1.0, 1.0, 1.0, 50.0, "SELL"),
        ("513350.SH", 1.0, 2.0, 1.0, 1.0, 50.0, "HOLD"),
        # bond: carry hold
        ("511260.SH", 1.0, 1.0, 1.0, 1.0, 50.0, "HOLD"),
        # unknown symbol: bare hold
        ("999999.SH", 1.0, 1.0, 1.0, 1.0, 50.0, "HOLD"),
    ],
)
def test_signal_branches(
    monkeypatch: pytest.MonkeyPatch,
    ts: str,
    close: float,
    ma10: float,
    ma20: float,
    ma60: float,
    rsi: float,
    action: str,
) -> None:
    _patch_signal(monkeypatch, close=close, ma10=ma10, ma20=ma20, ma60=ma60, rsi=rsi)
    out = mod.signal_for(ts)
    assert out["action"] == action


def test_all_signals_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod, "signal_for", lambda ts: {"ts": ts, "ok": True})
    out = mod.all_signals()
    assert out["as_of"] == "today"
    assert set(out["signals"]) == set(mod.COMMODITY_MAP)
    assert out["signals"]["GOLD"]["ts"] == "518880.SH"
