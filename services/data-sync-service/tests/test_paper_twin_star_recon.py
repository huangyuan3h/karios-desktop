"""paper_twin_star_recon expected-vs-actual (H5). Hermetic, no DB."""

from __future__ import annotations

import pytest

from data_sync_service.service import paper_twin_star as pts


@pytest.fixture(autouse=True)
def _hermetic(monkeypatch):
    monkeypatch.setattr(pts, "_held_days", lambda entry, day: 3 if entry <= "2026-08-31" else 1)


def _action(monkeypatch, cands, gate=True, bad=False):
    monkeypatch.setattr(
        pts,
        "build_twin_star_daily_action",
        lambda day: {
            "sat": {
                "gateOpen": gate,
                "candidates": [{"ts": c} for c in cands],
                "snapshotMissing": bad,
                "snapshotStale": False,
            }
        },
    )


def _book(monkeypatch, opens, closed):
    monkeypatch.setattr(pts, "_open_twin_star", lambda: opens)
    monkeypatch.setattr(pts, "list_paper_trades", lambda **k: closed)


def _row(symbol, entry, close=None):
    r = {"symbol": symbol, "source": "twin_star", "entryDate": entry, "id": symbol}
    if close:
        r["closeDate"] = close
    return r


def test_recon_clean_day(monkeypatch) -> None:
    _action(monkeypatch, ["000001.SZ", "600000.SH"])
    _book(
        monkeypatch,
        opens=[_row("CN:000001", "2026-09-02"), _row("CN:600000", "2026-09-02")],
        closed=[],
    )
    out = pts.paper_twin_star_recon(day="2026-09-02")
    assert out["ok"] is True
    assert out["expectedBuys"] == ["000001.SZ", "600000.SH"]
    assert out["missedBuys"] == [] and out["extraOpens"] == []


def test_recon_missed_and_extra(monkeypatch) -> None:
    _action(monkeypatch, ["000001.SZ", "000002.SZ"])
    _book(
        monkeypatch,
        opens=[_row("CN:000001", "2026-09-02"), _row("CN:000003", "2026-09-02")],
        closed=[],
    )
    out = pts.paper_twin_star_recon(day="2026-09-02")
    assert out["ok"] is False
    assert out["missedBuys"] == ["000002.SZ"]
    assert out["extraOpens"] == ["000003.SZ"]


def test_recon_already_open_not_missed(monkeypatch) -> None:
    _action(monkeypatch, ["000001.SZ"])
    _book(monkeypatch, opens=[_row("CN:000001", "2026-08-31")], closed=[])
    out = pts.paper_twin_star_recon(day="2026-09-02")
    assert out["missedBuys"] == [] and out["heldBefore"] == ["000001.SZ"]


def test_recon_gate_closed_any_insert_is_extra(monkeypatch) -> None:
    _action(monkeypatch, ["000001.SZ"], gate=False)
    _book(monkeypatch, opens=[_row("CN:000001", "2026-09-02")], closed=[])
    out = pts.paper_twin_star_recon(day="2026-09-02")
    assert out["expectedBuys"] == [] and out["extraOpens"] == ["000001.SZ"]
    assert out["ok"] is False


def test_recon_missed_exit(monkeypatch) -> None:
    _action(monkeypatch, [])
    _book(
        monkeypatch,
        opens=[_row("CN:000001", "2026-08-31")],  # held 3 >= BODY
        closed=[],
    )
    out = pts.paper_twin_star_recon(day="2026-09-02")
    assert out["exitsDue"] == ["CN:000001"] and out["missedExits"] == ["CN:000001"]
    assert out["ok"] is False


def test_recon_closed_exit_ok(monkeypatch) -> None:
    _action(monkeypatch, [])
    _book(
        monkeypatch,
        opens=[],
        closed=[_row("CN:000001", "2026-08-31", close="2026-09-02")],
    )
    out = pts.paper_twin_star_recon(day="2026-09-02")
    assert out["missedExits"] == [] and out["closedToday"] == ["CN:000001"]
    assert out["ok"] is True


def test_recon_action_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        pts, "build_twin_star_daily_action", lambda day: (_ for _ in ()).throw(RuntimeError("down"))
    )
    out = pts.paper_twin_star_recon(day="2026-09-02")
    assert out["ok"] is False and "action failed" in out["error"]


def test_recon_list_failure(monkeypatch) -> None:
    _action(monkeypatch, [])
    monkeypatch.setattr(pts, "_open_twin_star", lambda: (_ for _ in ()).throw(RuntimeError("db")))
    out = pts.paper_twin_star_recon(day="2026-09-02")
    assert out["ok"] is False and "list paper failed" in out["error"]
