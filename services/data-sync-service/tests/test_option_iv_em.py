from __future__ import annotations

pytestmark = pytest.mark.requires_postgres

import time

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
    svc._PUT_IV_SNAPSHOT_CACHE.update({"ts": 0.0, "value": None})
    monkeypatch.setattr(
        svc,
        "fetch_510300_atm_put_iv_live",
        lambda **_: {
            "ivPct": 25.0,
            "source": "eastmoney",
            "contractName": "x",
            "diagnostics": {"eastmoneyRows": 2, "eastmoneyPutRows": 1},
        },
    )
    monkeypatch.setattr(svc, "get_latest_row", lambda *_: None)
    monkeypatch.setattr(svc, "upsert_from_dataframe", lambda *a, **k: 1)
    out = svc.resolve_put_iv_for_snapshot(write_db=False, use_cache=False)
    assert out["close"] == pytest.approx(25.0)
    assert out["signalLabel"] == "Elevated Fear"
    assert out["diagnostics"]["eastmoneyPutRows"] == 1


def test_fetch_510300_atm_put_iv_live_no_candidate_has_diagnostics(monkeypatch) -> None:
    svc._LAST_PUT_IV_DIAGNOSTICS = {}
    monkeypatch.setattr(svc, "_fetch_em_option_value_rows", lambda: [{"f14": "50ETF沽6月2900", "f249": 19.0}])
    monkeypatch.setattr(svc.sys, "platform", "darwin")

    picked = svc.fetch_510300_atm_put_iv_live()

    assert picked is None
    assert svc._LAST_PUT_IV_DIAGNOSTICS["eastmoneyRows"] == 1
    assert svc._LAST_PUT_IV_DIAGNOSTICS["eastmoneyPutRows"] == 0
    assert svc._LAST_PUT_IV_DIAGNOSTICS["akshareSkippedReason"] == "akshare_disabled_on_darwin"
    assert svc._LAST_PUT_IV_DIAGNOSTICS["error"] == "no_510300_put_iv_candidate"


def test_resolve_put_iv_for_snapshot_falls_back_to_db(monkeypatch) -> None:
    svc._PUT_IV_SNAPSHOT_CACHE.update({"ts": 0.0, "value": None})
    svc._LAST_PUT_IV_DIAGNOSTICS = {"error": "no_510300_put_iv_candidate"}
    monkeypatch.setattr(svc, "fetch_510300_atm_put_iv_live", lambda **_: None)
    monkeypatch.setattr(
        svc,
        "get_latest_row",
        lambda *_: {
            "trade_date": "2024-06-18",
            "close": 18.5,
            "pct_chg": 1.2,
            "source": "macro_daily",
        },
    )

    out = svc.resolve_put_iv_for_snapshot(write_db=False, use_cache=False)

    assert out["close"] == pytest.approx(18.5)
    assert out["realtime"] is False
    assert out["warning"] == "put_iv_live_fetch_failed_using_db"
    assert out["diagnostics"]["error"] == "no_510300_put_iv_candidate"


def test_resolve_put_iv_for_snapshot_uses_short_cache(monkeypatch) -> None:
    svc._PUT_IV_SNAPSHOT_CACHE.update(
        {
            "ts": time.monotonic(),
            "value": {
                "close": 21.0,
                "asOfDate": "2024-06-18",
                "pctChg": None,
                "source": "eastmoney",
                "signal": "yellow",
                "signalLabel": "Elevated Fear",
                "underlyingTsCode": "510300.SH",
                "realtime": True,
                "warning": None,
                "diagnostics": {},
                "cached": False,
            },
        }
    )

    def _fail_live(**_: object) -> None:
        raise AssertionError("cache should avoid live fetch")

    monkeypatch.setattr(svc, "fetch_510300_atm_put_iv_live", _fail_live)

    out = svc.resolve_put_iv_for_snapshot(write_db=False)

    assert out["close"] == pytest.approx(21.0)
    assert out["cached"] is True
