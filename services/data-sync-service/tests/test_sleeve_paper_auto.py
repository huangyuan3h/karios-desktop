"""Tests for the paper-book Harbor parking sleeve auto-configuration."""

from __future__ import annotations

from contextlib import ExitStack
from unittest.mock import patch

import pytest

from data_sync_service.db.paper_trading import (
    CLOSE_REASON_SLEEVE_EXIT,
    list_paper_trades,
)
from data_sync_service.service.sleeve_paper_auto import apply_sleeve_to_paper

# Fake candidate symbol — DB tests must never touch real book rows (AGENTS.md).
TEST_SYMBOL = "ETF:995100"
TEST_TS = "995100.SH"
FAKE_CANDIDATES = {TEST_SYMBOL}


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
            "key": "GOLD",
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


def _purge_test_rows() -> None:
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM paper_trades WHERE symbol = %s", (TEST_SYMBOL,))
        conn.commit()


@pytest.fixture(autouse=True)
def _cleanup():
    # Purge before AND after so a leftover fake leg cannot flip a BUY branch.
    _purge_test_rows()
    yield
    _purge_test_rows()


def _patches(stack: ExitStack, *, multi: dict, fill: dict | None = None) -> None:
    """Patch the candidate set (fake symbol only) + decision + next-open fills."""
    stack.enter_context(
        patch("data_sync_service.service.sleeve_paper_auto.CANDIDATE_SYMBOLS", FAKE_CANDIDATES)
    )
    stack.enter_context(
        patch(
            "data_sync_service.service.sleeve_paper_auto._build_multi_for_paper",
            return_value=multi,
        )
    )
    if fill is not None:
        stack.enter_context(
            patch(
                "data_sync_service.service.sleeve_paper_auto.resolve_next_open_fill",
                return_value=fill,
            )
        )


@pytest.mark.requires_postgres
def test_buy_opens_sleeve_leg():
    with ExitStack() as stack:
        _patches(stack, multi=_multi(action="BUY"), fill=_fill())
        out = apply_sleeve_to_paper(day="2026-08-20")
    assert out.get("reason") != "no next_open fill", out
    assert out["changed"] is True
    assert out["reason"] == "multi opened"
    open_legs = [
        t
        for t in list_paper_trades(status="open")
        if str(t.get("symbol") or "").upper() == TEST_SYMBOL
    ]
    assert len(open_legs) == 1
    assert float(open_legs[0]["sleevePct"] or 0) == pytest.approx(60.0, abs=0.1)
    assert open_legs[0]["entryDate"] == "2026-08-21"
    assert float(open_legs[0]["entryPrice"] or 0) == pytest.approx(2.30)


@pytest.mark.requires_postgres
def test_buy_is_idempotent():
    _mk_open_leg()
    with ExitStack() as stack:
        _patches(stack, multi=_multi(action="BUY", holding=True))
        out = apply_sleeve_to_paper(day="2026-08-20")
    # already have an open sleeve leg → BUY requires not open_multi
    assert out["changed"] is False


@pytest.mark.requires_postgres
def test_sell_to_repo_closes_leg():
    _mk_open_leg(day="2026-08-01")
    with ExitStack() as stack:
        _patches(stack, multi=_multi(action="SELL_TO_REPO", holding=True), fill=_fill())
        out = apply_sleeve_to_paper(day="2026-08-20")
    assert out["changed"] is True
    assert "multi closed" in out["reason"]
    open_legs = [
        t
        for t in list_paper_trades(status="open")
        if str(t.get("symbol") or "").upper() == TEST_SYMBOL
    ]
    assert len(open_legs) == 0


@pytest.mark.requires_postgres
def test_sell_books_next_open_pnl():
    """pnl/holding days come from the camelCase row fields (not恒 0)."""
    leg = _mk_open_leg(day="2026-08-01")
    with ExitStack() as stack:
        _patches(stack, multi=_multi(action="SELL_TO_REPO", holding=True), fill=_fill())
        apply_sleeve_to_paper(day="2026-08-20")
    rows = [t for t in list_paper_trades(status="closed") if str(t.get("id")) == str(leg["id"])]
    assert rows
    row = rows[0]
    assert row["closeDate"] == "2026-08-21"
    assert float(row["closePrice"] or 0) == pytest.approx(2.30)
    assert float(row["pnlPct"] or 0) == pytest.approx(15.0, abs=0.01)
    assert int(row["holdingDays"] or 0) >= 20


@pytest.mark.requires_postgres
def test_hold_is_noop():
    with ExitStack() as stack:
        _patches(stack, multi=_multi(action="HOLD", idle=10.0, holding=True))
        out = apply_sleeve_to_paper(day="2026-08-20")
    assert out["changed"] is False
    assert out["reason"] == "multi no-op"


@pytest.mark.requires_postgres
def test_close_reason_is_sleeve_exit():
    leg = _mk_open_leg(day="2026-08-01")
    with ExitStack() as stack:
        _patches(stack, multi=_multi(action="SELL_TO_REPO", holding=True), fill=_fill())
        apply_sleeve_to_paper(day="2026-08-20")
    rows = [t for t in list_paper_trades(status="closed") if str(t.get("id")) == str(leg["id"])]
    assert rows and rows[0]["closeReason"] == CLOSE_REASON_SLEEVE_EXIT
