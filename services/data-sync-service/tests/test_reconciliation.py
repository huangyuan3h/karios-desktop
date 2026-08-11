"""Reconciliation service tests (2026-08-11): reconcile_day + run_and_persist."""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service import reconciliation as recon


def _fake_run(**overrides):
    """A fake simulate() result: BacktestRun-like object with positions_by_day."""

    class _Run:
        positions_by_day = [
            {
                "date": "2026-08-07",
                "positions": [
                    {"symbol": "CN:600001", "market": "CN", "ts_code": "600001.SH",
                     "entry_date": "2026-08-05", "score_at_entry": 88.0, "position_pct": 0.1},
                    {"symbol": "CN:600002", "market": "CN", "ts_code": "600002.SH",
                     "entry_date": "2026-08-06", "score_at_entry": 80.0, "position_pct": 0.1},
                ],
            }
        ]

    for k, v in overrides.items():
        setattr(_Run, k, v)
    return _Run()


def test_reconcile_day_matches_paper(monkeypatch) -> None:
    paper = [
        {"symbol": "CN:600001", "status": "open", "entryDate": "2026-08-05",
         "market": "CN", "source": "S3"},
        # CN:600002 held per backtest but NOT in paper → missing.
    ]
    monkeypatch.setattr(recon, "list_paper_trades", lambda: paper)
    with patch.object(recon, "simulate", return_value=_fake_run()), \
         patch.object(recon, "BacktestData", return_value=None):
        out = recon.reconcile_day("2026-08-07")
    m = out["markets"]["CN"]
    assert m["expected"] == 2
    assert m["actual"] == 1
    assert m["aligned"] == 1
    assert m["missing"] == 1
    assert m["extra"] == 0
    assert m["alignedList"][0]["entrySkew"] is False
    assert m["missingList"][0]["symbol"] == "CN:600002"


def test_reconcile_day_entry_skew_and_extra(monkeypatch) -> None:
    paper = [
        {"symbol": "CN:600001", "status": "open", "entryDate": "2026-08-07",
         "market": "CN", "source": "S3"},  # entry skew vs backtest 08-05
        {"symbol": "CN:600003", "status": "open", "entryDate": "2026-08-06",
         "market": "CN", "source": "ALPHA"},  # extra vs backtest
    ]
    monkeypatch.setattr(recon, "list_paper_trades", lambda: paper)
    with patch.object(recon, "simulate", return_value=_fake_run()), \
         patch.object(recon, "BacktestData", return_value=None):
        out = recon.reconcile_day("2026-08-07")
    m = out["markets"]["CN"]
    assert m["aligned"] == 1 and m["alignedList"][0]["entrySkew"] is True
    assert m["extra"] == 1 and m["extraList"][0]["source"] == "ALPHA"
    assert m["missing"] == 1


def test_reconcile_day_hk_market(monkeypatch) -> None:
    """HK line runs its own market block."""
    paper = [
        {"symbol": "HK:00700", "status": "open", "entryDate": "2026-08-05",
         "market": "HK", "source": "S3HK"},
    ]
    monkeypatch.setattr(recon, "list_paper_trades", lambda: paper)

    def _hk_run():
        r = _fake_run()
        r.positions_by_day = [
            {"date": "2026-08-07", "positions": [
                {"symbol": "HK:00700", "market": "HK", "ts_code": "00700.HK",
                 "entry_date": "2026-08-05", "score_at_entry": 99.0, "position_pct": 0.1},
            ]}
        ]
        return r

    with patch.object(recon, "simulate", side_effect=[_fake_run(), _hk_run()]), \
         patch.object(recon, "BacktestData", return_value=None):
        out = recon.reconcile_day("2026-08-07")
    assert out["markets"]["CN"]["expected"] == 2
    assert out["markets"]["HK"]["expected"] == 1
    assert out["markets"]["HK"]["aligned"] == 1


def test_reconcile_day_bad_window() -> None:
    try:
        recon.reconcile_day("2026-08-07", window="nope")
        raise AssertionError("expected ValueError")
    except ValueError:
        pass


def test_run_and_persist(monkeypatch) -> None:
    paper = [{"symbol": "CN:600001", "status": "open", "entryDate": "2026-08-05",
              "market": "CN", "source": "S3"}]
    monkeypatch.setattr(recon, "list_paper_trades", lambda: paper)
    rows = []

    def _fake_insert(**kw):
        rows.append(kw)

    with patch.object(recon, "simulate", return_value=_fake_run()), \
         patch.object(recon, "BacktestData", return_value=None), \
         patch("data_sync_service.db.reconciliation.insert_recon", side_effect=_fake_insert):
        out = recon.run_and_persist("2026-08-07")
    assert out["reconDate"] == "2026-08-07"
    assert len(rows) == 2  # CN + HK
    assert rows[0]["expected"] == 2
    assert rows[0]["detail"][0]["type"] == "missing"
