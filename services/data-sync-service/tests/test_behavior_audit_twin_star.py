"""OPT-140: twin-star leg split in reconcile_registry — satellite holdings
must never count as S-3 extra. Pure unit (explicit leg_ctx, no live calls)."""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service import reconciliation as recon


def _fake_run():
    class _Trade:
        symbol = "CN:600002"
        entry_date = "2026-08-05"
        close_date = "2026-08-06"

    class _Run:
        positions_by_day = [
            {
                "date": "2026-08-07",
                "positions": [
                    {"symbol": "CN:600001", "market": "CN", "ts_code": "600001.SH",
                     "entry_date": "2026-08-05", "score_at_entry": 88.0, "position_pct": 0.1},
                ],
            }
        ]
        trades = [_Trade()]

    return _Run()


REGISTRY = [
    {"symbol": "CN:600001", "positionPct": 10.0, "entryDate": "2026-08-05",
     "name": "核心对齐", "costPrice": 10.0},
    {"symbol": "CN:600099", "positionPct": 12.5, "entryDate": "2026-08-06",
     "name": "卫星一号", "costPrice": 20.0},  # engine-book satellite, not S-3
    {"symbol": "CN:600003", "positionPct": 8.0, "entryDate": "2026-08-06",
     "name": "真不该买", "costPrice": 12.0},  # leftover S-3 name
]

# pick=STOCK day: only names in the sat set are satellite.
LEG_CTX = {"pick": "STOCK", "sat_ts": {"600099.SH"}, "book_ts": {"600099.SH", "600088.SH"}}


def test_satellite_holding_not_s3_extra(monkeypatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.db.watchlist_automation.list_registry", lambda: REGISTRY
    )
    with patch.object(recon, "simulate", return_value=_fake_run()), \
         patch.object(recon, "BacktestData", return_value=None):
        out = recon.reconcile_registry("2026-08-07", mode="twin_star", leg_ctx=LEG_CTX)
    m = out["markets"]["CN"]
    # core leg only: 600001 aligned, 600003 never_entered
    assert m["expected"] == 1
    assert m["extra"] == 1
    assert [e["symbol"] for e in m["extraList"]] == ["CN:600003"]
    assert m["missing"] == 0
    # satellite leg vs engine book: 600099 held (aligned), 600088 missing
    assert m["satExpected"] == 2
    assert m["actualSat"] == 1
    assert m["satExtra"] == 1
    assert m["satExtraList"][0]["symbol"] == "CN:600099"
    assert m["satExtraList"][0]["kind"] == "sat_leg"
    assert m["satMissing"] == 1
    assert m["satMissingList"] == [{"symbol": "CN:600088"}]


def test_non_stock_pick_all_cn_satellite(monkeypatch) -> None:
    """pick≠STOCK → every A-share is satellite (shared holding_book rule)."""
    monkeypatch.setattr(
        "data_sync_service.db.watchlist_automation.list_registry", lambda: REGISTRY
    )
    leg = {"pick": "NASDAQ", "sat_ts": set(), "book_ts": set()}
    with patch.object(recon, "simulate", return_value=_fake_run()), \
         patch.object(recon, "BacktestData", return_value=None):
        out = recon.reconcile_registry("2026-08-07", mode="twin_star", leg_ctx=leg)
    m = out["markets"]["CN"]
    assert m["extra"] == 0
    # 600001 is S-3-aligned (not extra anywhere); the other two are sat-leg
    assert m["satExtra"] == 2
    assert {e["symbol"] for e in m["satExtraList"]} == {"CN:600099", "CN:600003"}


def test_holding_book_shared_predicate() -> None:
    from data_sync_service.service.twin_star_daily import holding_book

    assert holding_book("twin_star", "NASDAQ", "CN", "CN:600001", set()) == "sat"
    assert holding_book("twin_star", "STOCK", "CN", "CN:600099", {"600099.SH"}) == "sat"
    assert holding_book("twin_star", "STOCK", "CN", "CN:600003", {"600099.SH"}) == "s3"
    assert holding_book("twin_star", "NASDAQ", "HK", "HK:00700", set()) == "idle"
    assert holding_book("twin_star", "STOCK", "HK", "HK:00700", set()) == "s3"
    assert holding_book("single_track", "STOCK", "CN", "CN:600001", set()) == "s3"
