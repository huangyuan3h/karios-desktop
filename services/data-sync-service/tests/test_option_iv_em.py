from __future__ import annotations

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


def test_em_rows_to_analysis_df_and_select(monkeypatch) -> None:
    rows = [
        {"f14": "300ETF沽6月4000", "f249": 22.0, "f301": 20260625, "f334": 4.0},
        {"f14": "300ETF沽7月3800", "f249": 24.0, "f301": 20260724, "f334": 4.0},
    ]
    df = svc._em_rows_to_analysis_df(rows)
    picked = svc.select_atm_put_iv(df)
    assert picked is not None
    assert picked["ivPct"] == 22.0
