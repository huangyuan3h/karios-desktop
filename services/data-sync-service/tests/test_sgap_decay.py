"""Unit + contract tests for the S-gap decay display layer (no DB/network).

Covers the frozen H2k display method:
- rolling means/sums/win-rate/t/percentile on synthetic trades
- K2 two-step revival state (0/10/20, double-confirm + step gap + drop)
- crowding medians, large-order edge, equity/drawdown, monthly counts
- GET /api/backtest/sgap-decay file contract (404 / corrupt / ok)
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from data_sync_service.api import backtest_routes as br
from data_sync_service.service import sgap_decay as sd


def _trade(entry: str, contrib: float, **kw) -> dict:
    d = {"entry": entry, "contrib": contrib}
    d.update(kw)
    return d


def test_percentile_calibration_matches_h2k() -> None:
    # H2k Sec 3: valid +3.11/52 => ~59.8% (true 50.2), holdout -20.17 => ~0.8%
    # (true 10.9) with the long-R1 normal approx (+/-10pt declared error).
    assert sd.percentile_of_sum(3.11, 52) == pytest.approx(59.8, abs=1.0)
    assert sd.percentile_of_sum(-20.17, 52) == pytest.approx(0.8, abs=1.0)
    # Neutral zero window sits just below 50 (mu/trade is +0.0178).
    assert sd.percentile_of_sum(0.0, 40) == pytest.approx(46.3, abs=1.0)


def _day(i: int) -> str:
    # Monotonic fake date (2024-01-01 + i days) so entry sort keeps file order.
    from datetime import date, timedelta

    return (date(2024, 1, 1) + timedelta(days=i)).isoformat()


def test_rolling_mean_winrate_and_equity() -> None:
    trades = [_trade(_day(i), 1.0 if i % 2 == 0 else -0.5) for i in range(45)]
    out = sd.compute_sgap_decay(trades)
    assert out["summary"]["n_trades"] == 45
    assert len(out["rolling"]) == 45
    assert len(out["equity"]) == 45
    # First full 40-window: 20 x +1 and 20 x -0.5 => mean +0.25, win 50%.
    r40 = out["rolling"][39]["r40"]
    assert r40 is not None
    assert r40["mean"] == pytest.approx(0.25)
    assert r40["sum"] == pytest.approx(10.0)
    assert r40["win_rate"] == pytest.approx(0.5)
    # N=60 not ready yet at i=39.
    assert out["rolling"][39]["r60"] is None
    assert out["rolling"][44]["r60"] is None  # only 45 trades
    # Equity sums contributions in entry order.
    assert out["equity"][-1]["cum"] == pytest.approx(
        sum(1.0 if i % 2 == 0 else -0.5 for i in range(45))
    )
    assert all(e["drawdown"] <= 1e-9 for e in out["equity"])


def test_revival_two_step_needs_double_confirm_and_gap() -> None:
    # 60 strong trades (+2 each): first double-confirm steps 0 -> 10,
    # second step to 20 only after the 20-trade gap.
    trades = [_trade(_day(i), 2.0) for i in range(70)]
    out = sd.compute_sgap_decay(trades)
    states = [r["revival_pos"] for r in out["rolling"]]
    # Warmup: first 39 windows have no state.
    assert all(s is None for s in states[:39])
    # First eligible double-confirm at i=40 steps to 10.
    assert states[39] == 0
    assert states[40] == 10
    # Gap blocks the second step until i >= 60.
    assert states[59] == 10
    assert states[60] == 20


def test_revival_drops_one_level_per_window() -> None:
    # 60 strong then 45 weak (-2): climbs to 20, then steps 20 -> 10 -> 0.
    strong = [_trade(_day(i), 2.0) for i in range(60)]
    weak = [_trade(_day(60 + i), -2.0) for i in range(45)]
    out = sd.compute_sgap_decay(strong + weak)
    states = [r["revival_pos"] for r in out["rolling"] if r["revival_pos"] is not None]
    assert 20 in states
    assert states[-1] == 0
    # No jump 20 -> 0 in a single step.
    full = [r["revival_pos"] for r in out["rolling"]]
    for a, b in zip(full, full[1:], strict=False):
        if a == 20 and b is not None:
            assert b in (20, 10)


def test_crowding_and_large_edge() -> None:
    trades = [
        _trade(
            _day(i),
            1.0 if i % 2 == 0 else -1.0,
            large_pct=10.0 if i % 2 == 0 else -10.0,
            amt_w=float(100 + i),
            circ=float(1000 + 10 * i),
        )
        for i in range(40)
    ]
    out = sd.compute_sgap_decay(trades)
    last = out["rolling"][-1]
    assert last["crowd_amt_w"] == pytest.approx(119.5)
    assert last["crowd_circ"] == pytest.approx(1195.0)
    # High half (+1/-1 alternating, median split) edge is +2.0.
    assert last["large_edge"] == pytest.approx(2.0)
    # Too few valid large_pct => null edge.
    few = [_trade("2024-01-01", 1.0, large_pct=1.0)] * 39 + [_trade("2024-01-02", 1.0)]
    out2 = sd.compute_sgap_decay(few)
    assert out2["rolling"][-1]["large_edge"] is None


def test_monthly_counts() -> None:
    trades = [_trade("2024-01-05", 1.0)] * 3 + [_trade("2024-02-05", -1.0)] * 2
    out = sd.compute_sgap_decay(trades)
    assert out["monthly"] == [
        {"month": "2024-01", "count": 3, "sum": pytest.approx(3.0)},
        {"month": "2024-02", "count": 2, "sum": pytest.approx(-2.0)},
    ]


def test_sgap_decay_route_file_contract(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(br, "REPORTS_DIR", tmp_path)
    with pytest.raises(HTTPException) as exc:
        br.backtest_sgap_decay()
    assert exc.value.status_code == 404
    (tmp_path / "sgap_decay.json").write_text("{oops", encoding="utf-8")
    with pytest.raises(HTTPException) as exc:
        br.backtest_sgap_decay()
    assert exc.value.status_code == 500
    (tmp_path / "sgap_decay.json").write_text('{"summary": {"n_trades": 1}}', encoding="utf-8")
    assert br.backtest_sgap_decay() == {"ok": True, "decay": {"summary": {"n_trades": 1}}}
