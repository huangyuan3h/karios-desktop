"""Engine replay golden (OPT-173).

Freezes the pure ``_summarize`` output for a fixed synthetic trade set + nav
curve. If the engine's performance math (returns, drawdown, Sharpe, score
buckets) drifts, the serialized summary changes and this test fails loudly.

Regenerate after an intentional change::

    UPDATE_GOLDEN=1 uv run pytest tests/test_engine_golden.py
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from data_sync_service.service import backtest_engine as be

GOLDEN = Path(__file__).resolve().parent / "golden" / "engine_summary.json"

_CALENDAR = [
    "2026-06-18",
    "2026-06-19",
    "2026-06-22",
    "2026-06-23",
    "2026-06-24",
    "2026-06-25",
]

_NAV = [1.0, 1.02, 1.01, 1.05, 1.03, 1.06, 1.04, 1.08]


def _trade(pnl: float, score: float, close_date: str) -> be.BacktestTrade:
    return be.BacktestTrade(
        symbol="CN:600001",
        market="CN",
        entry_date="2026-06-18",
        entry_price=10.0,
        close_date=close_date,
        close_price=10.0 * (1.0 + pnl / 100.0),
        gross_pnl_pct=pnl + 0.3,
        costs_pct=0.3,
        pnl_pct=pnl,
        holding_days=3,
        close_reason="max_hold",
        score_at_entry=score,
        position_pct=0.05,
    )


def _summary_dict() -> dict:
    cfg = be.BacktestConfig(start_date="2026-06-18", end_date="2026-06-25")
    data = be.BacktestData.__new__(be.BacktestData)
    data.calendar = list(_CALENDAR)
    trades = [
        _trade(4.5, 91.0, "2026-06-22"),
        _trade(-2.1, 83.0, "2026-06-23"),
        _trade(1.7, 76.0, "2026-06-24"),
        _trade(-3.4, 88.0, "2026-06-25"),
    ]
    summary = be._summarize(
        cfg,
        data,
        trades,
        open_at_end=2,
        gated_blocks={"limit_up": 3, "st": 1},
        nav_curve=list(_NAV),
    )
    payload = summary.to_dict()
    # `config` carries non-JSON sets (frozenset repr order is not stable); the
    # replay contract is about the performance math, so freeze metrics only.
    payload.pop("config", None)
    return payload


def test_engine_summary_matches_golden() -> None:
    actual = json.dumps(_summary_dict(), indent=2, sort_keys=True)
    if os.environ.get("UPDATE_GOLDEN"):
        GOLDEN.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN.write_text(actual + "\n", encoding="utf-8")
        return
    assert GOLDEN.exists(), f"missing golden fixture {GOLDEN}; run UPDATE_GOLDEN=1"
    expected = GOLDEN.read_text(encoding="utf-8").rstrip("\n")
    assert actual == expected
