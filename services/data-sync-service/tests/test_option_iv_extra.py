"""option_iv extra coverage: sync driver, akshare fallback, paging, utilities."""

from __future__ import annotations

import types

import pandas as pd
import pytest

from data_sync_service.service import option_iv as svc


def _live(**kw) -> dict:
    out = {"ivPct": 22.0, "source": "eastmoney", "contractName": "300ETF沽6月4000", "diagnostics": {"eastmoneyRows": 1}}
    out.update(kw)
    return out


# ---- sync_option_iv_daily --------------------------------------------------

def test_sync_skips_already_synced_today(monkeypatch) -> None:
    monkeypatch.setattr(svc, "get_today_run", lambda job: {"success": True})
    out = svc.sync_option_iv_daily()
    assert out["skipped"] is True
    assert out["reason"] == "already_synced_today"


def test_sync_skips_non_trading_day(monkeypatch) -> None:
    monkeypatch.setattr(svc, "get_today_run", lambda job: None)
    monkeypatch.setattr(svc, "is_trading_day", lambda exchange, cal_date: False)
    out = svc.sync_option_iv_daily(force=True)
    assert out["skipped"] is True
    assert out["reason"] == "not_trading_day"


def test_sync_trade_date_iso_input(monkeypatch) -> None:
    monkeypatch.setattr(svc, "get_today_run", lambda job: None)
    monkeypatch.setattr(svc, "is_trading_day", lambda exchange, cal_date: True)
    monkeypatch.setattr(svc, "fetch_510300_atm_put_iv_live", lambda **_: _live())
    monkeypatch.setattr(svc, "get_latest_row", lambda *_: None)
    monkeypatch.setattr(svc, "upsert_from_dataframe", lambda *a, **k: 1)
    monkeypatch.setattr(svc, "insert_record", lambda **kw: None)
    out = svc.sync_option_iv_daily(force=True, trade_date="2026-08-07")
    assert out["ok"] is True
    assert out["tradeDate"] == "2026-08-07"
    assert out["ivPct"] == pytest.approx(22.0)
    assert out["rowsUpserted"] == 1


def test_sync_fetch_exception_records_failure(monkeypatch) -> None:
    monkeypatch.setattr(svc, "get_today_run", lambda job: None)
    monkeypatch.setattr(svc, "is_trading_day", lambda exchange, cal_date: True)
    records: list[dict] = []

    def boom():
        raise RuntimeError("net down")

    monkeypatch.setattr(svc, "fetch_510300_atm_put_iv_live", boom)
    monkeypatch.setattr(svc, "insert_record", lambda **kw: records.append(kw))
    out = svc.sync_option_iv_daily(force=True)
    assert out["ok"] is False
    assert out["error"] == "net down"
    assert records[0]["success"] is False


def test_sync_no_iv_data(monkeypatch) -> None:
    svc._LAST_PUT_IV_DIAGNOSTICS = {"error": "no_510300_put_iv_candidate"}
    monkeypatch.setattr(svc, "get_today_run", lambda job: None)
    monkeypatch.setattr(svc, "is_trading_day", lambda exchange, cal_date: True)
    monkeypatch.setattr(svc, "fetch_510300_atm_put_iv_live", lambda **_: None)
    monkeypatch.setattr(svc, "insert_record", lambda **kw: None)
    out = svc.sync_option_iv_daily(force=True)
    assert out["ok"] is False
    assert out["error"] == "no_iv_data"
    assert out["diagnostics"]["error"] == "no_510300_put_iv_candidate"


def test_sync_success_with_prev_close(monkeypatch) -> None:
    monkeypatch.setattr(svc, "get_today_run", lambda job: None)
    monkeypatch.setattr(svc, "is_trading_day", lambda exchange, cal_date: True)
    monkeypatch.setattr(svc, "fetch_510300_atm_put_iv_live", lambda **_: _live(ivPct=24.2))
    monkeypatch.setattr(svc, "get_latest_row", lambda *_: {"trade_date": "2026-08-06", "close": 20.0})
    monkeypatch.setattr(svc, "upsert_from_dataframe", lambda *a, **k: 1)
    monkeypatch.setattr(svc, "insert_record", lambda **kw: None)
    out = svc.sync_option_iv_daily(force=True)
    assert out["pctChg"] == pytest.approx(21.0)
    assert out["signalLabel"] == "Elevated Fear"


# ---- fetch_510300_atm_put_iv_live: akshare fallback ------------------------

def test_live_em_fails_akshare_succeeds(monkeypatch) -> None:
    def boom():
        raise RuntimeError("em blocked")

    monkeypatch.setattr(svc, "_fetch_em_option_value_rows", boom)
    monkeypatch.setattr(svc.sys, "platform", "linux")
    monkeypatch.setattr(
        svc,
        "_akshare",
        lambda: types.SimpleNamespace(
            option_value_analysis_em=lambda: pd.DataFrame(
                [{"期权名称": "300ETF沽6月4000", "隐含波动率": 25.0, "到期日": "2026-06-25"}]
            )
        ),
    )
    picked = svc.fetch_510300_atm_put_iv_live()
    assert picked is not None
    assert picked["ivPct"] == 25.0
    assert picked["source"] == "akshare"
    assert picked["diagnostics"]["akshareAttempted"] is True
    assert picked["diagnostics"]["akshareRows"] == 1


def test_live_akshare_fails_too(monkeypatch) -> None:
    monkeypatch.setattr(svc, "_fetch_em_option_value_rows", lambda: [{"f14": "50ETF沽6月2900"}])
    monkeypatch.setattr(svc.sys, "platform", "linux")

    class _FakeAk:
        def option_value_analysis_em(self):
            raise RuntimeError("akshare down")

    monkeypatch.setattr(svc, "_akshare", lambda: _FakeAk())
    picked = svc.fetch_510300_atm_put_iv_live()
    assert picked is None
    assert svc._LAST_PUT_IV_DIAGNOSTICS["akshareError"]
    assert svc._LAST_PUT_IV_DIAGNOSTICS["error"] == "no_510300_put_iv_candidate"


def test_live_source_override(monkeypatch) -> None:
    monkeypatch.setattr(
        svc,
        "_fetch_em_option_value_rows",
        lambda: [{"f14": "300ETF沽6月4000", "f249": 21.0, "f301": 20260625}],
    )
    picked = svc.fetch_510300_atm_put_iv_live(source="manual")
    assert picked["source"] == "manual"


# ---- _fetch_em_option_value_rows (paging) ----------------------------------

def test_em_rows_paging(monkeypatch) -> None:
    calls: list[int] = []

    def fake_request(params):
        page = int(params["pn"])
        calls.append(page)
        if page == 1:
            return {"data": {"diff": [{"f14": "a"}], "total": 150}}
        return {"data": {"diff": [{"f14": "b"}], "total": 150}}

    monkeypatch.setattr(svc, "_em_option_value_request", fake_request)
    rows = svc._fetch_em_option_value_rows()
    assert len(rows) == 2
    assert calls == [1, 2]


def test_em_rows_empty_diff_breaks(monkeypatch) -> None:
    monkeypatch.setattr(svc, "_em_option_value_request", lambda params: {"data": {"diff": [], "total": 10}})
    assert svc._fetch_em_option_value_rows() == []


def test_em_rows_bad_total(monkeypatch) -> None:
    monkeypatch.setattr(
        svc, "_em_option_value_request", lambda params: {"data": {"diff": [{"f14": "a"}], "total": "junk"}}
    )
    rows = svc._fetch_em_option_value_rows()
    assert len(rows) == 1


def test_em_rows_non_dict_entries_skipped(monkeypatch) -> None:
    monkeypatch.setattr(
        svc, "_em_option_value_request", lambda params: {"data": {"diff": [{"f14": "a"}, "junk"], "total": 2}}
    )
    assert len(svc._fetch_em_option_value_rows()) == 1


# ---- em rows -> df / select -------------------------------------------------

def test_em_rows_to_df_field_variants() -> None:
    rows = [
        {"f14": "300ETF沽6月4000", "f249": "22.5", "f301": "20260625", "f334": None, "f335": "4.01"},
        {"f14": "", "f249": 1.0},  # empty name skipped
        {"f14": "300ETF沽6月3900", "f249": "bad", "f301": 20260626},
        {"f14": "300ETF沽6月3800", "f249": 19.0, "f301": "2026-06-27"},
        {"f14": "300ETF沽6月3700", "f249": 18.0, "f301": None},
    ]
    df = svc._em_rows_to_analysis_df(rows)
    assert len(df) == 4
    row0 = df.iloc[0]
    assert row0["期权名称"] == "300ETF沽6月4000"
    assert row0["隐含波动率"] == 22.5
    assert row0["到期日"] == "2026-06-25"
    assert row0["标的最新价"] == 4.01
    assert df.iloc[1]["隐含波动率"] != df.iloc[1]["隐含波动率"]  # NaN for "bad"
    assert df.iloc[2]["到期日"] == "2026-06-27"


def test_select_atm_put_iv_edge_cases() -> None:
    assert svc.select_atm_put_iv(None) is None
    import pandas as pd

    empty = pd.DataFrame({"期权名称": []})
    assert svc.select_atm_put_iv(empty) is None
    no_name_col = pd.DataFrame({"foo": [1]})
    assert svc.select_atm_put_iv(no_name_col) is None
    not_510300 = pd.DataFrame({"期权名称": ["50ETF沽6月2900"], "隐含波动率": [19.0]})
    assert svc.select_atm_put_iv(not_510300) is None
    bad_expiry = pd.DataFrame(
        {"期权名称": ["300ETF沽6月4000"], "隐含波动率": [22.0], "到期日": ["garbage"]}
    )
    assert svc.select_atm_put_iv(bad_expiry) is None
    zero_iv = pd.DataFrame(
        {"期权名称": ["300ETF沽6月4000", "300ETF沽6月3900"], "隐含波动率": [0.0, "bad"]}
    )
    assert svc.select_atm_put_iv(zero_iv) is None
    na_iv = pd.DataFrame({"期权名称": ["300ETF沽6月4000"], "隐含波动率": [float("nan")]})
    assert svc.select_atm_put_iv(na_iv) is None


def test_count_510300_put_rows() -> None:
    import pandas as pd

    assert svc._count_510300_put_rows(None) == 0
    df = pd.DataFrame({"期权名称": ["300ETF沽6月4000", "50ETF沽6月2900", "300ETF购6月4100", 123]})
    assert svc._count_510300_put_rows(df) == 1
    bad = pd.DataFrame({"foo": [1]})
    assert svc._count_510300_put_rows(bad) == 0


# ---- utilities -------------------------------------------------------------

def test_akshare_import_error(monkeypatch) -> None:
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kw):
        if name == "akshare":
            raise ImportError("no akshare")
        return real_import(name, *args, **kw)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(RuntimeError, match="akshare is not installed"):
        svc._akshare()


def test_parse_strike_from_name_variants() -> None:
    assert svc._parse_strike_from_name("300ETF沽6月4000") == pytest.approx(4.0)
    assert svc._parse_strike_from_name("300ETF沽6月290") == 290.0  # below 1000: raw value
    assert svc._parse_strike_from_name("沽X月") is None
    assert svc._parse_strike_from_name("") is None
    assert svc._parse_strike_from_name(None) is None


def test_parse_cal_date_variants() -> None:
    from datetime import date

    assert svc._parse_cal_date("20260807") == date(2026, 8, 7)
    assert svc._parse_cal_date("2026-08-07") == date(2026, 8, 7)
    with pytest.raises(ValueError):
        svc._parse_cal_date("nope")


def test_classify_iv_signal_all_bands() -> None:
    assert svc.classify_iv_signal(iv_pct=30.0, pct_chg=None) == ("red", "Deep Panic")
    assert svc.classify_iv_signal(iv_pct=25.0, pct_chg=12.0) == ("yellow", "Elevated Fear")
    assert svc.classify_iv_signal(iv_pct=25.0, pct_chg=3.0) == ("yellow", "Elevated Fear")
    assert svc.classify_iv_signal(iv_pct=25.0, pct_chg=None) == ("yellow", "Elevated Fear")
    assert svc.classify_iv_signal(iv_pct=16.0, pct_chg=99.0) == ("green", "Normal")
    assert svc.classify_iv_signal(iv_pct=12.0, pct_chg=99.0) == ("light_green", "Complacent")


def test_compute_iv_pct_chg_variants() -> None:
    assert svc.compute_iv_pct_chg(22.0, 20.0) == pytest.approx(10.0)
    assert svc.compute_iv_pct_chg(22.0, None) is None
    assert svc.compute_iv_pct_chg(22.0, 0.0) is None
    assert svc.compute_iv_pct_chg("bad", 20.0) is None


def test_safe_float_variants() -> None:
    assert svc._safe_float("1.5") == 1.5
    assert svc._safe_float(float("nan")) is None
    assert svc._safe_float("bad") is None
    assert svc._safe_float(None) is None


# ---- resolve_put_iv_for_snapshot extra branches ----------------------------

def test_resolve_snapshot_fetch_raises(monkeypatch) -> None:
    svc._PUT_IV_SNAPSHOT_CACHE.update({"ts": 0.0, "value": None})
    svc._LAST_PUT_IV_DIAGNOSTICS = {"error": "no_510300_put_iv_candidate"}

    def boom(**kw):
        raise RuntimeError("live crashed")

    monkeypatch.setattr(svc, "fetch_510300_atm_put_iv_live", boom)
    monkeypatch.setattr(svc, "get_latest_row", lambda *_: None)
    out = svc.resolve_put_iv_for_snapshot(write_db=False, use_cache=False)
    assert out["close"] is None
    assert out["warning"] == "live crashed"
    assert out["diagnostics"]["error"] == "no_510300_put_iv_candidate"


def test_resolve_snapshot_db_fallback_missing_pct_chg(monkeypatch) -> None:
    svc._PUT_IV_SNAPSHOT_CACHE.update({"ts": 0.0, "value": None})
    svc._LAST_PUT_IV_DIAGNOSTICS = {"error": "no_510300_put_iv_candidate"}
    monkeypatch.setattr(svc, "fetch_510300_atm_put_iv_live", lambda **_: None)
    monkeypatch.setattr(
        svc,
        "get_latest_row",
        lambda *_: {"trade_date": "2026-08-06", "close": 18.5, "pct_chg": None, "source": "macro_daily"},
    )
    out = svc.resolve_put_iv_for_snapshot(write_db=False, use_cache=False)
    assert out["close"] == pytest.approx(18.5)
    assert out["source"] == "macro_daily"
    assert out["realtime"] is False


def test_resolve_snapshot_db_fallback_bad_prev_row(monkeypatch) -> None:
    svc._PUT_IV_SNAPSHOT_CACHE.update({"ts": 0.0, "value": None})
    monkeypatch.setattr(svc, "fetch_510300_atm_put_iv_live", lambda **_: None)
    monkeypatch.setattr(svc, "get_latest_row", lambda *_: {"trade_date": "x", "close": None})
    out = svc.resolve_put_iv_for_snapshot(write_db=False, use_cache=False)
    assert out["close"] is None
    assert out["warning"] == "put_iv_fetch_failed"


def test_resolve_snapshot_write_db_failure_swallowed(monkeypatch) -> None:
    svc._PUT_IV_SNAPSHOT_CACHE.update({"ts": 0.0, "value": None})
    monkeypatch.setattr(
        svc,
        "fetch_510300_atm_put_iv_live",
        lambda **_: _live(diagnostics={"eastmoneyRows": 2}),
    )
    monkeypatch.setattr(svc, "get_latest_row", lambda *_: None)

    def boom(*a, **k):
        raise RuntimeError("upsert failed")

    monkeypatch.setattr(svc, "upsert_from_dataframe", boom)
    out = svc.resolve_put_iv_for_snapshot(write_db=True, use_cache=False)
    assert out["close"] == pytest.approx(22.0)
    assert out["realtime"] is True


def test_resolve_snapshot_bad_prev_close_type(monkeypatch) -> None:
    svc._PUT_IV_SNAPSHOT_CACHE.update({"ts": 0.0, "value": None})
    monkeypatch.setattr(
        svc,
        "fetch_510300_atm_put_iv_live",
        lambda **_: _live(ivPct=24.0, diagnostics={"eastmoneyRows": 2}),
    )
    monkeypatch.setattr(svc, "get_latest_row", lambda *_: {"trade_date": "x", "close": "bad"})
    monkeypatch.setattr(svc, "upsert_from_dataframe", lambda *a, **k: 1)
    out = svc.resolve_put_iv_for_snapshot(write_db=False, use_cache=False)
    assert out["pctChg"] is None
    assert out["close"] == pytest.approx(24.0)
