"""T2: regime strength score — shared CN/HK strength ruler."""

from __future__ import annotations

import pytest

from data_sync_service.service import market_regime as mr


def _sig(signal: str, code: str = "000001.SH") -> dict:
    return {
        "tsCode": code,
        "name": code,
        "signal": signal,
        "close": 100.0,
        "ma5": 100.0,
        "ma20": 100.0,
        "ma60": 100.0,
    }


def test_momentum_score_edges() -> None:
    assert mr._strength_momentum_score(5.0) == pytest.approx(40.0)
    assert mr._strength_momentum_score(0.0) == pytest.approx(20.0)
    assert mr._strength_momentum_score(-5.0) == pytest.approx(0.0)
    assert mr._strength_momentum_score(20.0) == pytest.approx(40.0)  # clamped
    assert mr._strength_momentum_score(-20.0) == pytest.approx(0.0)  # clamped
    assert mr._strength_momentum_score(2.5) == pytest.approx(30.0)


def test_structure_votes_uptrend() -> None:
    closes = [100.0 + i for i in range(70)]
    votes, total = mr._strength_structure_votes(closes)
    assert (votes, total) == (4, 4)


def test_structure_votes_downtrend() -> None:
    closes = [200.0 - i for i in range(70)]
    votes, total = mr._strength_structure_votes(closes)
    assert votes == 0 and total == 4


def test_structure_votes_short_series() -> None:
    assert mr._strength_structure_votes([100.0, 101.0]) == (0, 4)
    closes = [100.0 + i for i in range(10)]
    votes, total = mr._strength_structure_votes(closes)
    assert votes == 1 and total == 4  # close>MA5 only


def test_strength_score_hk_all_green(monkeypatch) -> None:
    monkeypatch.setattr(mr, "get_index_signals", lambda as_of_date=None, include_breadth=True: [
        _sig("green", "HSI"),
        _sig("deep_green", "HSTECH"),
    ])
    monkeypatch.setattr(mr, "fetch_macro_last_closes", lambda sid, days=80, as_of_date=None: [
        ("2026-08-10", 100.0 + 40 * (i / 70)) for i in range(71)
    ])
    out = mr.regime_strength_score(market="HK", as_of_date="2026-08-10")
    assert out["market"] == "HK"
    assert out["regime"] == "Strong"
    assert out["strength"] > 90.0
    assert out["components"]["greens"] == pytest.approx(30.0)


def test_strength_score_cn_weak(monkeypatch) -> None:
    monkeypatch.setattr(mr, "get_index_signals", lambda as_of_date=None, include_breadth=True: [
        _sig("red", "000001.SH"),
        _sig("yellow", "399006.SZ"),
        _sig("red", "000905.SH"),
    ])
    monkeypatch.setattr(mr, "fetch_last_closes_vol_batch", lambda codes, days=80, as_of_date=None: {
        c: [("2026-08-10", 100.0 - 30 * (i / 70), 1_000_000.0) for i in range(71)] for c in codes
    })
    out = mr.regime_strength_score(market="CN", as_of_date="2026-08-10")
    assert out["market"] == "CN"
    assert out["regime"] == "Weak"
    assert out["strength"] < 30.0
    assert out["components"]["greens"] == pytest.approx(0.0)


def test_strength_score_invalid_market(monkeypatch) -> None:
    monkeypatch.setattr(mr, "get_index_signals", lambda as_of_date=None, include_breadth=True: [])
    with pytest.raises(ValueError):
        mr.regime_strength_score(market="US")


def test_strength_score_no_signals(monkeypatch) -> None:
    monkeypatch.setattr(mr, "get_index_signals", lambda as_of_date=None, include_breadth=True: [])
    out = mr.regime_strength_score(market="HK", as_of_date="2025-01-05")
    assert out["strength"] == 0.0 and out["regime"] == "Weak"
