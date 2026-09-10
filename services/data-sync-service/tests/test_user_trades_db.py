"""user_trades db + routes + stats tests.

Integration tests write rows with the `CN:99` test-symbol prefix and MUST
clean them up (AGENTS.md DB hygiene discipline): the autouse fixture removes
any row whose symbol matches the prefix.
"""

from __future__ import annotations

import pytest

from data_sync_service.db import user_trades as ut

TEST_PREFIX = "CN:99"
TEST_SYMBOL = "CN:99ut1"

pytestmark = pytest.mark.requires_postgres


@pytest.fixture(autouse=True)
def _cleanup_test_rows():
    yield
    ut.ensure_tables()
    with ut.get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM user_trades WHERE symbol LIKE %s",
            (f"{TEST_PREFIX}%",),
        )


def test_insert_and_list_roundtrip() -> None:
    ut.ensure_tables()
    row = ut.insert_trade(
        symbol=TEST_SYMBOL,
        side="SELL",
        trade_date="2026-08-08",
        price=10.5,
        position_pct=5.0,
        cost_basis=10.0,
        entry_date="2026-08-01",
        pnl_pct=5.0,
        holding_days=7,
        source="ALPHA",
        market="CN",
    )
    assert row["side"] == "SELL"
    assert row["symbol"] == TEST_SYMBOL
    rows = ut.list_trades()
    assert any(r["id"] == row["id"] for r in rows)


def test_insert_rejects_invalid_side() -> None:
    ut.ensure_tables()
    with pytest.raises(ValueError):
        ut.insert_trade(
            symbol=TEST_SYMBOL,
            side="HOLD",
            trade_date="2026-08-08",
            price=1.0,
            position_pct=1.0,
        )


def test_leg_defaults_s3_and_accepts_sat() -> None:
    ut.ensure_tables()
    core = ut.insert_trade(
        symbol=TEST_SYMBOL, side="BUY", trade_date="2026-09-09", price=10.0, position_pct=10.0,
    )
    assert core["leg"] == "s3"
    sat = ut.insert_trade(
        symbol=f"{TEST_PREFIX}sat1", side="BUY", trade_date="2026-09-07",
        price=221.28, position_pct=12.5, leg="sat",
    )
    assert sat["leg"] == "sat"
    rows = ut.list_trades(symbol=f"{TEST_PREFIX}sat1")
    assert any(r["id"] == sat["id"] and r["leg"] == "sat" for r in rows)
    with pytest.raises(ValueError):
        ut.insert_trade(
            symbol=TEST_SYMBOL, side="BUY", trade_date="2026-09-09",
            price=1.0, position_pct=1.0, leg="x",
        )


def test_update_trade_leg_roundtrip() -> None:
    ut.ensure_tables()
    row = ut.insert_trade(
        symbol=f"{TEST_PREFIX}fix1", side="BUY", trade_date="2026-09-07",
        price=100.0, position_pct=12.5,
    )
    assert row["leg"] == "s3"
    fixed = ut.update_trade(row["id"], leg="sat")
    assert fixed is not None and fixed["leg"] == "sat"
    assert ut.update_trade("no-such-id", leg="sat") is None
    with pytest.raises(ValueError):
        ut.update_trade(row["id"], leg="x")
    with pytest.raises(ValueError):
        ut.update_trade(row["id"])


def test_delete_trade() -> None:
    ut.ensure_tables()
    row = ut.insert_trade(
        symbol=TEST_SYMBOL,
        side="BUY",
        trade_date="2026-08-08",
        price=10.0,
        position_pct=5.0,
    )
    assert ut.delete_trade(row["id"]) is True
    assert ut.delete_trade(row["id"]) is False


def test_list_filters_by_symbol_and_limit() -> None:
    ut.ensure_tables()
    for i in range(3):
        ut.insert_trade(
            symbol=f"{TEST_PREFIX}list{i}",
            side="BUY",
            trade_date=f"2026-08-0{i + 1}",
            price=10.0 + i,
            position_pct=5.0,
        )
    rows = ut.list_trades(symbol=f"{TEST_PREFIX}list1")
    assert len(rows) == 1
    rows2 = ut.list_trades(limit=2)
    assert len(rows2) <= 2


def test_fetch_sell_rows_only() -> None:
    ut.ensure_tables()
    ut.insert_trade(
        symbol=TEST_SYMBOL,
        side="BUY",
        trade_date="2026-08-01",
        price=10.0,
        position_pct=5.0,
    )
    ut.insert_trade(
        symbol=TEST_SYMBOL,
        side="SELL",
        trade_date="2026-08-08",
        price=11.0,
        position_pct=5.0,
        cost_basis=10.0,
        entry_date="2026-08-01",
        pnl_pct=10.0,
        holding_days=7,
    )
    sells = ut.fetch_sell_rows()
    mine = [r for r in sells if r["symbol"] == TEST_SYMBOL]
    assert len(mine) == 1
    assert mine[0]["symbol"] == TEST_SYMBOL
    assert mine[0]["pnlPct"] == 10.0


def test_alpha_snapshot_roundtrip() -> None:
    ut.ensure_tables()
    snap = {
        "asOf": "2026-08-13",
        "windowDays": 14,
        "nEvents": 1,
        "hasSA": True,
        "maxConfidence": 0.9,
        "riskStatuses": ["active"],
        "events": [
            {"trend": "x", "grade": "A", "confidence": 0.9, "daysAgo": 2,
             "riskStatus": "active", "focus": "y"},
        ],
    }
    row = ut.insert_trade(
        symbol=TEST_SYMBOL,
        side="BUY",
        trade_date="2026-08-13",
        price=10.0,
        position_pct=5.0,
        alpha_snapshot=snap,
    )
    assert row["alphaSnapshot"] == snap
    rows = ut.list_trades(symbol=TEST_SYMBOL)
    assert any(r["id"] == row["id"] and r["alphaSnapshot"] == snap for r in rows)
