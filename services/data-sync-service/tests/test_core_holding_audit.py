"""core_holding_audit satellite-leg judges — pure unit tests, no DB."""

from __future__ import annotations

from data_sync_service.service import core_holding_audit as audit


def _op(symbol="CN:688525", side="BUY", date="2026-09-07", price=221.28, pct=12.5, leg="sat"):
    return {
        "id": "x", "symbol": symbol, "side": side, "trade_date": date,
        "price": price, "position_pct": pct, "cost_basis": None,
        "entry_date": None, "pnl_pct": None, "holding_days": None,
        "source": "RESEARCH", "market": "CN", "note": None, "leg": leg,
        "created_at": None,
    }


def test_sat_open_matches_paper_book() -> None:
    book = {("CN:688525", "2026-09-07")}
    v = audit._judge_sat_open(_op(), book)
    assert v["verdict"] == "ok"
    assert v["rule"] == "sat_signal"


def test_sat_open_off_book_is_self_directed_ok() -> None:
    """2026-09-10: a self-directed satellite buy is compliant, not a warn."""
    v = audit._judge_sat_open(_op(symbol="CN:600540", date="2026-09-02"), set())
    assert v["verdict"] == "ok"
    assert v["rule"] == "sat_manual"
    assert "自选" in v["detail"]


def test_sat_sell_on_due_day_ok(monkeypatch) -> None:
    monkeypatch.setattr(audit, "_sat_exit_due", lambda entry: "2026-09-09")
    v = audit._judge_sat_sell(_op(side="SELL", date="2026-09-09"), "2026-09-07")
    assert v["verdict"] == "ok"


def test_sat_sell_early_or_late_warns(monkeypatch) -> None:
    monkeypatch.setattr(audit, "_sat_exit_due", lambda entry: "2026-09-09")
    assert audit._judge_sat_sell(_op(side="SELL", date="2026-09-08"), "2026-09-07")["verdict"] == "warn"
    assert audit._judge_sat_sell(_op(side="SELL", date="2026-09-10"), "2026-09-07")["verdict"] == "warn"
    assert audit._judge_sat_sell(_op(side="SELL", date="2026-09-09"), None)["verdict"] == "warn"


def test_sat_add_always_warns() -> None:
    v = audit._judge_sat_add(_op(side="ADD"))
    assert v["verdict"] == "warn"


def test_replay_routes_sat_around_s3_rules(monkeypatch) -> None:
    """A sat BUY in a panic window must NOT get the S-3 panic warn."""
    monkeypatch.setattr(audit, "_sat_exit_due", lambda entry: "2026-09-09")
    ops = [_op(side="BUY"), _op(side="SELL", date="2026-09-09")]
    book = {("CN:688525", "2026-09-07")}
    gate = {"regime": "Diverging", "panicActive": True, "gateOpen": False}
    out = audit._replay_ops(ops, {"costPrice": 221.28, "positionPct": 12.5}, gate, book)
    assert [v["verdict"] for v in out] == ["ok", "ok"]
    assert all("恐慌" not in v["detail"] and "金字塔" not in v["detail"] for v in out)


def test_replay_verdicts_carry_row_ids(monkeypatch) -> None:
    """OPT-150: the UI needs op ids to patch a misfiled leg."""
    monkeypatch.setattr(audit, "_sat_exit_due", lambda entry: "2026-09-09")
    ops = [_op(side="BUY"), _op(side="SELL", date="2026-09-09")]
    book = {("CN:688525", "2026-09-07")}
    out = audit._replay_ops(ops, {"costPrice": 221.28, "positionPct": 12.5}, {}, book)
    assert [v.get("id") for v in out] == ["x", "x"]
