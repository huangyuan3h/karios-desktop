"""user_trades_stats expectancy math (pure) + integration over SELL rows."""

from __future__ import annotations

import pytest

from data_sync_service.service import user_trades_stats as st

TEST_PREFIX = "CN:99"

pytestmark = pytest.mark.requires_postgres


def _sell_rows() -> list[dict]:
    return [
        {"symbol": "CN:99a", "source": "ALPHA", "pnlPct": 10.0, "holdingDays": 5},
        {"symbol": "CN:99a", "source": "ALPHA", "pnlPct": -4.0, "holdingDays": 3},
        {"symbol": "CN:99b", "source": "TV", "pnlPct": 2.0, "holdingDays": 1},
        {"symbol": "CN:99b", "source": "TV", "pnlPct": -6.0, "holdingDays": 2},
    ]


def test_bucket_stats_empty() -> None:
    s = st._bucket_stats([])
    assert s["count"] == 0
    assert s["winRate"] is None
    assert s["expectancyPct"] is None
    assert s["profitFactor"] is None


def test_bucket_stats_mixed() -> None:
    s = st._bucket_stats(_sell_rows())
    assert s["count"] == 4
    assert s["wins"] == 2
    assert s["losses"] == 2
    assert s["winRate"] == 0.5
    assert s["avgWinPct"] == 6.0
    assert s["avgLossPct"] == 5.0
    # expectancy = 0.5*6 - 0.5*5 = 0.5
    assert s["expectancyPct"] == pytest.approx(0.5)
    # net = 0.5 - 0.3 cost
    assert s["netExpectancyPct"] == pytest.approx(0.2)
    # profit factor = (10+2) / (4+6) = 1.2
    assert s["profitFactor"] == pytest.approx(1.2)
    assert s["avgHoldingDays"] == pytest.approx(2.8)


def test_bucket_stats_all_wins_profit_factor_inf() -> None:
    rows = [
        {"symbol": "CN:99a", "source": "TV", "pnlPct": 5.0, "holdingDays": 1},
        {"symbol": "CN:99b", "source": "TV", "pnlPct": 3.0, "holdingDays": 2},
    ]
    s = st._bucket_stats(rows)
    assert s["winRate"] == 1.0
    assert s["profitFactor"] == float("inf")


def test_bucket_stats_all_losses_profit_factor_none() -> None:
    rows = [
        {"symbol": "CN:99a", "source": "TV", "pnlPct": -5.0, "holdingDays": 1},
    ]
    s = st._bucket_stats(rows)
    assert s["winRate"] == 0.0
    assert s["profitFactor"] == 0.0


def test_compute_trade_stats_by_source_and_symbol() -> None:
    from data_sync_service.db import user_trades as ut

    ut.ensure_tables()
    with ut.get_connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM user_trades WHERE symbol LIKE %s", (f"{TEST_PREFIX}%",))
    ut.insert_trade(
        symbol=f"{TEST_PREFIX}a",
        side="SELL",
        trade_date="2026-08-08",
        price=11.0,
        position_pct=5.0,
        cost_basis=10.0,
        entry_date="2026-08-01",
        pnl_pct=10.0,
        holding_days=7,
        source="ALPHA",
    )
    ut.insert_trade(
        symbol=f"{TEST_PREFIX}b",
        side="SELL",
        trade_date="2026-08-08",
        price=9.5,
        position_pct=3.0,
        cost_basis=10.0,
        entry_date="2026-08-02",
        pnl_pct=-5.0,
        holding_days=6,
        source="TV",
    )
    try:
        stats = st.compute_trade_stats()
        assert stats["total"] >= 2
        # DB discipline: real user rows may exist — assert only our prefixed
        # symbols, never whole-source counts.
        assert f"{TEST_PREFIX}a" in stats["bySymbol"]
        assert f"{TEST_PREFIX}b" in stats["bySymbol"]
        assert stats["roundTripCostPct"] == 0.3
        assert stats["bySymbol"][f"{TEST_PREFIX}a"]["count"] == 1
        assert stats["bySymbol"][f"{TEST_PREFIX}a"]["winRate"] == 1.0
        assert stats["bySymbol"][f"{TEST_PREFIX}b"]["count"] == 1
        assert stats["bySymbol"][f"{TEST_PREFIX}b"]["winRate"] == 0.0
        assert "ALPHA" in stats["bySource"]
        assert "TV" in stats["bySource"]
    finally:
        with ut.get_connection() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM user_trades WHERE symbol LIKE %s", (f"{TEST_PREFIX}%",))
