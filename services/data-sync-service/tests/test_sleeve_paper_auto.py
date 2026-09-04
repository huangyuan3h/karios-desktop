"""Tests for the paper-book multi-asset sleeve auto-configuration."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from data_sync_service.db.paper_trading import (
    CLOSE_REASON_SLEEVE_EXIT,
    list_paper_trades,
)
from data_sync_service.service.sleeve_paper_auto import apply_sleeve_to_paper

TEST_SYMBOL = "ETF:513100"
TEST_TS = "513100.SH"


def _mk_open_leg(day: str = "2026-08-01") -> dict:
    from data_sync_service.db.paper_trading import insert_paper_trade

    return insert_paper_trade(
        symbol=TEST_SYMBOL,
        entry_date=day,
        side="BUY",
        entry_price=2.0,
        why_at_entry="test sleeve leg",
        sleeve_pct=50.0,
        source="S3",
        market="CN",
    )


def _multi(*, action: str, idle: float = 60.0, holding: bool = False) -> dict:
    return {
        "action": action,
        "idlePct": idle,
        "holding": holding,
        "pick": {
            "key": "NASDAQ",
            "symbol": TEST_SYMBOL,
            "ts": TEST_TS,
            "mom60": 12.0,
            "close": 2.25,
        },
    }


def _fill(day: str = "2026-08-20") -> dict:
    return {
        "entry_date": "2026-08-21",
        "entry_price": 2.30,
        "pending_open_fill": False,
        "signal_snapshot": {
            "entryMode": "next_open",
            "signalDate": day,
            "pendingOpenFill": False,
        },
    }


@pytest.fixture(autouse=True)
def _cleanup():
    yield
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM paper_trades WHERE symbol = %s "
                "AND (why_at_entry LIKE %s OR why_at_entry LIKE %s)",
                (TEST_SYMBOL, "test sleeve leg%", "multi-sleeve%"),
            )
        conn.commit()


@pytest.mark.requires_postgres
def test_buy_opens_sleeve_leg():
    with (
        patch(
            "data_sync_service.service.sleeve_paper_auto._build_multi_for_paper",
            return_value=_multi(action="BUY"),
        ),
        patch(
            "data_sync_service.service.sleeve_paper_auto.resolve_next_open_fill",
            return_value=_fill(),
        ),
    ):
        out = apply_sleeve_to_paper(day="2026-08-20")
    assert out.get("reason") != "no next_open fill", out
    assert out["changed"] is True
    assert out["reason"] == "multi opened"
    open_legs = [
        t for t in list_paper_trades(status="open")
        if str(t.get("symbol") or "").upper() == TEST_SYMBOL
    ]
    assert len(open_legs) == 1
    assert float(open_legs[0]["sleevePct"] or 0) == pytest.approx(60.0, abs=0.1)
    assert open_legs[0]["entryDate"] == "2026-08-21"
    assert float(open_legs[0]["entryPrice"] or 0) == pytest.approx(2.30)


@pytest.mark.requires_postgres
def test_buy_is_idempotent():
    _mk_open_leg()
    with patch(
        "data_sync_service.service.sleeve_paper_auto._build_multi_for_paper",
        return_value=_multi(action="BUY", holding=True),
    ):
        out = apply_sleeve_to_paper(day="2026-08-20")
    # already have open multi leg → BUY branch requires not open_multi
    assert out["changed"] is False


@pytest.mark.requires_postgres
def test_sell_to_repo_closes_leg():
    _mk_open_leg(day="2026-08-01")
    with patch(
        "data_sync_service.service.sleeve_paper_auto._build_multi_for_paper",
        return_value=_multi(action="SELL_TO_REPO", holding=True),
    ):
        out = apply_sleeve_to_paper(day="2026-08-20")
    assert out["changed"] is True
    assert "multi closed" in out["reason"]
    open_legs = [
        t for t in list_paper_trades(status="open")
        if str(t.get("symbol") or "").upper() == TEST_SYMBOL
    ]
    assert len(open_legs) == 0


@pytest.mark.requires_postgres
def test_hold_is_noop():
    with patch(
        "data_sync_service.service.sleeve_paper_auto._build_multi_for_paper",
        return_value=_multi(action="HOLD", idle=10.0, holding=True),
    ):
        out = apply_sleeve_to_paper(day="2026-08-20")
    assert out["changed"] is False
    assert out["reason"] == "multi no-op"


@pytest.mark.requires_postgres
def test_close_reason_is_sleeve_exit():
    leg = _mk_open_leg(day="2026-08-01")
    with patch(
        "data_sync_service.service.sleeve_paper_auto._build_multi_for_paper",
        return_value=_multi(action="SELL_TO_REPO", holding=True),
    ):
        apply_sleeve_to_paper(day="2026-08-20")
    rows = [
        t for t in list_paper_trades(status="closed")
        if str(t.get("id")) == str(leg["id"])
    ]
    assert rows and rows[0]["closeReason"] == CLOSE_REASON_SLEEVE_EXIT
