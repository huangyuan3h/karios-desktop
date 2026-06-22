from __future__ import annotations

import pytest

from data_sync_service.service import option_iv as svc


def test_fetch_510300_atm_put_iv_live_from_em_rows(monkeypatch) -> None:
    em_rows = [
        {
            "f14": "300ETF沽6月4000",
            "f249": 28.5,
            "f301": 20260625,
            "f334": 4.02,
        },
        {
            "f14": "50ETF沽6月2900",
            "f249": 19.0,
            "f301": 20260625,
            "f334": 2.74,
        },
    ]

    monkeypatch.setattr(svc, "_fetch_em_option_value_rows", lambda: em_rows)

    picked = svc.fetch_510300_atm_put_iv_live()
    assert picked is not None
    assert picked["ivPct"] == 28.5
    assert picked["source"] == "eastmoney"


def test_select_atm_put_iv_without_spot() -> None:
    rows = [
        {"f14": "300ETF沽6月3800", "f249": 21.0, "f301": 20260625},
        {"f14": "300ETF沽6月4000", "f249": 22.0, "f301": 20260625},
        {"f14": "300ETF沽6月4200", "f249": 23.0, "f301": 20260625},
    ]
    df = svc._em_rows_to_analysis_df(rows)
    picked = svc.select_atm_put_iv(df)
    assert picked is not None
    assert picked["ivPct"] == 22.0
    assert picked["strike"] == pytest.approx(4.0)


def test_resolve_put_iv_for_snapshot_live(monkeypatch) -> None:
    monkeypatch.setattr(
        svc,
        "fetch_510300_atm_put_iv_live",
        lambda **_: {"ivPct": 25.0, "source": "eastmoney", "contractName": "x"},
    )
    monkeypatch.setattr(svc, "get_latest_row", lambda *_: None)
    monkeypatch.setattr(svc, "upsert_from_dataframe", lambda *a, **k: 1)
    out = svc.resolve_put_iv_for_snapshot(write_db=False)
    assert out["close"] == pytest.approx(25.0)
    assert out["signalLabel"] == "Elevated Fear"
