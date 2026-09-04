"""H2 smoke test: end-to-end post-close chain (2026-08-08, K5 gate).

Simulates one full trading cycle through the REAL DB paths:

    journal BUY signal (ingest_snapshot)
        → paper intake (run_intake) opens a trade
        → next-day update (run_update) closes on target_hit
        → exit attribution (analyze_exit_attribution) sees the close
        → weekly review (build_weekly_review) counts it

Only the price feed (fetch_last_ohlcv_batch) is mocked; every DB read/write
goes through the real service/db code so key-shape regressions (K1 class)
surface here. Teardown deletes every row this test inserted.
"""

from __future__ import annotations

import uuid
from unittest.mock import patch

import pytest

from data_sync_service.db import get_connection
from data_sync_service.db import paper_trading as pt_db

pytestmark = pytest.mark.requires_postgres

ENTRY_DATE = "2026-08-03"  # fixed week so weekly review bounds are deterministic
UPDATE_DATE = "2026-08-07"
REVIEW_END = "2026-08-09"

_CREATED: dict[str, set[str]] = {"symbols": set(), "snapshot_ids": set()}


@pytest.fixture(autouse=True)
def _cleanup_smoke_rows() -> None:
    yield
    with get_connection() as conn:
        with conn.cursor() as cur:
            if _CREATED["symbols"]:
                cur.execute(
                    "DELETE FROM paper_trades WHERE symbol = ANY(%s)",
                    (sorted(_CREATED["symbols"]),),
                )
                cur.execute(
                    "DELETE FROM execution_decision_changes WHERE symbol = ANY(%s)",
                    (sorted(_CREATED["symbols"]),),
                )
            if _CREATED["snapshot_ids"]:
                cur.execute(
                    "DELETE FROM execution_snapshots WHERE id = ANY(%s)",
                    (sorted(_CREATED["snapshot_ids"]),),
                )
        conn.commit()
    _CREATED["symbols"].clear()
    _CREATED["snapshot_ids"].clear()


def _smoke_symbol() -> str:
    # Pure digits (uuid hex can contain a-f, which _SYMBOL_RE rejects).
    sym = f"CN:99{uuid.uuid4().int % 10**4:04d}"
    _CREATED["symbols"].add(sym)
    return sym


def _mock_prices(smoke_symbol: str, close_price: float):
    """Patch fetch_last_ohlcv_batch so the smoke symbol sees ``close_price``.

    2026-08-11 hardening: the fake feed ONLY serves bars for the smoke
    symbol's ts_code — anything else returns empty. Previously it returned
    ``entry_price`` (100.0) for every non-.SZ ts_code, so a run_update pass
    that scanned the REAL paper book closed every real open trade with a
    bogus close price (08-11 00:22 UTC wiped all 20 S3HK rows this way).
    """
    from data_sync_service.service import paper_trading as pt_svc

    ticker = smoke_symbol.split(":", 1)[1]
    smoke_ts = f"{ticker}.SZ"

    def fake_fetch(ts_codes, days=2):
        if not ts_codes or smoke_ts not in ts_codes:
            return {}
        return {smoke_ts: [(ENTRY_DATE, close_price, close_price, close_price, close_price, 10000)]}

    return patch.object(pt_svc, "fetch_last_ohlcv_batch", side_effect=fake_fetch)


def _mock_no_prev_snapshot():
    """Diff against our own empty baseline snapshot instead of the real one.

    ingest_snapshot diffs against fetch_latest_snapshot() (the real latest
    snapshot); a cross-day diff would emit reverse changes for every REAL
    watchlist symbol, indistinguishable from real journal rows. We insert an
    empty baseline snapshot first so the diff only covers our smoke card.
    """
    from data_sync_service.service import execution_journal as ej_svc

    base = ej_svc.ej_db.insert_snapshot(
        trade_date=ENTRY_DATE,
        source="TV",
        gate={"mode": "AGGRESSIVE", "allowNewEntries": True},
        cards=[],
        content_hash="smoke-baseline",
        meta={"smoke": True},
    )
    _CREATED["snapshot_ids"].add(base["id"])
    return base


def test_postclose_chain_end_to_end() -> None:
    from data_sync_service.service.execution_journal import ingest_snapshot
    from data_sync_service.service.exit_attribution import analyze_exit_attribution
    from data_sync_service.service.paper_trading import run_intake, run_update
    from data_sync_service.service.weekly_review import build_weekly_review

    sym = _smoke_symbol()
    ticker = sym.split(":", 1)[1]
    _ = f"{ticker}.SZ"  # 99xxxx → SZ

    # -- Step 1: journal BUY signal (real ingest: writes snapshot + changes) --
    _mock_no_prev_snapshot()  # empty baseline → diff covers only our card
    ingested = ingest_snapshot(
        trade_date=ENTRY_DATE,
        source="TV",
        gate={"mode": "AGGRESSIVE", "allowNewEntries": True},
        cards=[{"symbol": sym, "action": "BUY", "why": "SMOKE_TEST", "source": "TV"}],
        meta={"smoke": True},
    )
    assert ingested["changed"] is True
    _CREATED["snapshot_ids"].add(ingested["snapshotId"])

    # -- Step 2: intake opens the trade (real DB; only prices mocked) --
    with _mock_prices(sym, close_price=100.0):
        intake = run_intake(trade_date=ENTRY_DATE)
    # NOTE: candidates count varies with real journal data (yesterday's cron
    # may already have ingested real BUY cards → 'duplicate' skips); we only
    # assert that OUR smoke card is ingested.
    assert intake["inserted"] == 1, intake

    open_rows = pt_db.list_paper_trades(status="open", limit=100)
    opened = next((t for t in open_rows if t.get("symbol") == sym), None)
    assert opened is not None, "trade did not open"
    # next_open fill: signal ENTRY_DATE (08-03) fills on the next session open.
    assert opened["entryDate"] == "2026-08-04"
    assert opened["market"] == "CN"
    assert opened["source"] == "TV"

    # -- Step 3: update closes on target_hit (3x price → net pnl ≫ 100%) --
    # S-3 定案：TARGET_PNL_PCT=100（不止盈），+100% 仍持有；3x 才触发 target_hit。
    # 2026-08-11 hardening: get_open_paper_trades is patched to return ONLY
    # our smoke row. run_update is a GLOBAL scan of the real paper book —
    # running it unmocked against the dev DB closed every real open trade
    # (08-11 00:22 UTC wiped the 20 real S3HK rows with mocked prices).
    from data_sync_service.service import paper_trading as pt_svc

    open_row = next(
        (t for t in open_rows if t.get("symbol") == sym),
        None,
    )
    assert open_row is not None, "trade did not open"
    with (
        _mock_prices(sym, close_price=300.0),
        patch.object(pt_svc.pt_db, "get_open_paper_trades", return_value=[open_row]),
    ):
        upd = run_update(today_iso=UPDATE_DATE)
    assert upd["closed"] == 1, upd
    assert upd["closeReasons"].get("target_hit") == 1, upd

    closed_rows = pt_db.list_paper_trades(status="closed", limit=100)
    closed = next((t for t in closed_rows if t.get("symbol") == sym), None)
    assert closed is not None, "trade did not close"
    assert closed["closeReason"] == "target_hit"
    assert closed["pnlPct"] is not None, "pnlPct missing after close"
    assert closed["grossPnlPct"] is not None and closed["costsPct"] is not None

    # -- Step 4: exit attribution sees the close (K1 key-shape regression) --
    attribution = analyze_exit_attribution(days=30, limit=500)
    assert "error" not in attribution, attribution
    by_reason = attribution.get("byReason") or {}
    assert "target_hit" in by_reason, f"target_hit missing: {sorted(by_reason)}"
    assert by_reason["target_hit"]["count"] >= 1

    # -- Step 5: weekly review counts the close (NET-of-costs path) --
    review = build_weekly_review(end_date=REVIEW_END)
    assert "error" not in review, review
    paper = review.get("paper") or {}
    assert paper.get("closed", 0) >= 1
    assert (paper.get("byReason") or {}).get("target_hit", {}).get("count", 0) >= 1
    assert paper.get("avgNetPnlPct") is not None


def test_ingest_snapshot_is_idempotent_on_same_content() -> None:
    """H9: re-ingesting the identical gate+cards must NOT emit new changes
    (content-hash heartbeat). A cron double-fire must not duplicate rows."""
    from data_sync_service.service.execution_journal import ingest_snapshot

    sym = _smoke_symbol()
    _mock_no_prev_snapshot()
    gate = {"mode": "AGGRESSIVE", "allowNewEntries": True}
    cards = [{"symbol": sym, "action": "BUY", "why": "SMOKE_TEST", "source": "TV"}]

    first = ingest_snapshot(
        trade_date=ENTRY_DATE, source="TV", gate=gate, cards=cards, meta={"smoke": True},
    )
    _CREATED["snapshot_ids"].add(first["snapshotId"])
    assert first["changed"] is True

    second = ingest_snapshot(
        trade_date=ENTRY_DATE, source="TV", gate=gate, cards=cards, meta={"smoke": True},
    )
    assert second["changed"] is False
    assert second["heartbeat"] is True
    assert second["changes"] == []

    # Exactly one BUY change row for the symbol (no duplicates from re-fire).
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM execution_decision_changes
                WHERE symbol = %s AND field = 'action'
                """,
                (sym,),
            )
            assert cur.fetchone()[0] == 1
