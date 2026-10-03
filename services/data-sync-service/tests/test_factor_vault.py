"""Unit + contract tests for the factor-vault display layer (no DB/network).

Covers the frozen H2k uniform rule:
- classify boundaries (cold <50 or net<=0 / watch 50-75 or <20 trades / revived >=75 and net>0)
- no true percentile => never revived
- position two-step (0/10/20, double-confirm + gap, drop to 0 on cold)
- spark/history shape + vault summary counts
- GET /api/backtest/factor-vault file contract (404 / corrupt / ok + history merge)
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from data_sync_service.api import backtest_routes as br
from data_sync_service.service import factor_vault as fvv


def test_classify_boundaries() -> None:
    assert fvv.classify_factor(10.9, -19.8, 40) == ("cold", "冷库")
    assert fvv.classify_factor(49.9, 5.0, 40) == ("cold", "冷库")
    assert fvv.classify_factor(60.0, 5.0, 40) == ("watch", "观察")
    assert fvv.classify_factor(80.0, 5.0, 40) == ("revived", "复活")
    assert fvv.classify_factor(80.0, 0.0, 40) == ("cold", "冷库")
    assert fvv.classify_factor(80.0, 5.0, 10) == ("watch", "观察")


def test_no_true_percentile_never_revived() -> None:
    # Net-positive pilot without a real random baseline stays in watch.
    assert fvv.classify_factor(None, 7.9, 40) == ("watch", "观察")
    assert fvv.classify_factor(None, -5.4, 40) == ("cold", "冷库")
    assert fvv.classify_factor(None, -5.4, 2) == ("watch", "观察")


def test_position_needs_double_confirm() -> None:
    assert fvv.position_for_factor([False, False, False, True], 40) == (0, 1)
    assert fvv.position_for_factor([False, False, True, True], 40) == (10, 2)
    assert fvv.position_for_factor([True, True, True, True], 40) == (20, 4)
    # Not enough trades pins to 0 even with confirmations.
    assert fvv.position_for_factor([True, True, True, True], 8) == (0, 4)


def test_percentile_calibration() -> None:
    # S-gap holdout tail is deep cold; strong tail is revived territory.
    assert fvv.percentile_of_sum(-20.17, 52, fvv.SGAP_MU, fvv.SGAP_SIGMA) < 5.0
    assert fvv.percentile_of_sum(30.0, 40, fvv.SGAP_MU, fvv.SGAP_SIGMA) > 75.0


def test_build_factors_cover_all_families() -> None:
    rows = fvv.build_factors()
    ids = {r["id"] for r in rows}
    for must in (
        "starship_b",
        "starship_cap50",
        "h11",
        "h08",
        "h01",
        "g_small",
        "g_event",
        "harbor",
        "m30",
        "bleg",
    ):
        assert must in ids
    by_id = {r["id"]: r for r in rows}
    # S-gap observation leg is cold on the holdout (10.9%ile, net negative).
    assert by_id["starship_b"]["status"] == "cold"
    assert by_id["starship_b"]["position"] == 0
    assert by_id["starship_b"]["badge"] == "冷库"
    # Monthly pilots with <20 periods stay in watch, never revived.
    assert by_id["g_small"]["trades"] < 20
    assert by_id["g_small"]["status"] in ("cold", "watch")
    # Every row carries the required table columns.
    for r in rows:
        for key in (
            "name",
            "family",
            "source",
            "status",
            "badge",
            "percentile",
            "net",
            "win_rate",
            "trades",
            "days_since_change",
            "spark",
            "updated",
            "position",
            "history",
        ):
            assert key in r
        assert len(r["spark"]) == 4
        assert len(r["history"]) == 4


def test_compute_vault_summary_counts() -> None:
    payload = fvv.compute_factor_vault()
    s = payload["summary"]
    assert s["n_factors"] == len(payload["factors"]) >= 20
    assert s["n_cold"] + s["n_watch"] + s["n_revived"] == s["n_factors"]
    assert payload["meta"]["revival"]["N"] == 40
    assert payload["meta"]["revival"]["X"] == 75.0


def test_apply_history_counts_days() -> None:
    rows = fvv.build_factors()
    prev = [
        {"date": "2026-10-01", "states": {r["id"]: r["status"] for r in rows}},
        {"date": "2026-10-02", "states": {r["id"]: r["status"] for r in rows}},
    ]
    out = fvv.apply_history(rows, prev)
    assert all(r["days_since_change"] == 2 for r in out)
    flipped = [
        {"date": "2026-10-02", "states": {r["id"]: ("watch" if r["status"] == "cold" else r["status"]) for r in rows}}
    ]
    out2 = fvv.apply_history(fvv.build_factors(), flipped)
    cold = [r for r in out2 if r["id"] == "starship_b"][0]
    assert cold["days_since_change"] == 0


def test_factor_vault_route_file_contract(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(br, "REPORTS_DIR", tmp_path)
    with pytest.raises(HTTPException) as exc:
        br.backtest_factor_vault()
    assert exc.value.status_code == 404
    (tmp_path / "factor_vault.json").write_text("{oops", encoding="utf-8")
    with pytest.raises(HTTPException) as exc:
        br.backtest_factor_vault()
    assert exc.value.status_code == 500
    (tmp_path / "factor_vault.json").write_text(
        '{"meta": {"n_factors": 1}, "summary": {"n_factors": 1}, "factors": []}',
        encoding="utf-8",
    )
    out = br.backtest_factor_vault()
    assert out == {
        "ok": True,
        "vault": {"meta": {"n_factors": 1}, "summary": {"n_factors": 1}, "factors": []},
        "history": [],
    }
    (tmp_path / "factor_vault_history.json").write_text(
        '{"history": [{"date": "2026-10-03", "states": {"a": "cold"}}]}', encoding="utf-8"
    )
    out2 = br.backtest_factor_vault()
    assert out2["history"] == [{"date": "2026-10-03", "states": {"a": "cold"}}]
