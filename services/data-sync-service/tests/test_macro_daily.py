"""macro_daily coverage: sync driver + normalization + paging helpers."""

from __future__ import annotations

import pandas as pd

from data_sync_service.service import macro_daily as md


def _us_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "trade_date": ["20260701", "20260702"],
            "close": [100.0, 101.0],
            "pct_chg": [0.5, 1.0],
        }
    )


def test_normalize_us_daily_df_renames() -> None:
    out = md._normalize_us_daily_df(_us_df())
    assert out is not None
    assert list(out.columns) == ["trade_date", "close", "pct_chg"]
    assert out.iloc[0]["close"] == 100.0


def test_normalize_us_daily_df_none() -> None:
    assert md._normalize_us_daily_df(None) is None
    assert md._normalize_us_daily_df(pd.DataFrame()) is not None


def test_normalize_fx_daily_df_keeps_required_cols() -> None:
    df = pd.DataFrame({"trade_date": ["20260701"], "close": [7.16], "open": [7.15]})
    out = md._normalize_fx_daily_df(df)
    assert out is not None
    assert "close" in out.columns and "trade_date" in out.columns


def test_try_tushare_pro_no_api_key(monkeypatch) -> None:
    monkeypatch.setattr(md, "get_settings", lambda: type("S", (), {"tu_share_api_key": ""})())
    assert md.try_tushare_pro() is None


def test_resolve_main_fut_by_prefix_matches_ts_code(monkeypatch) -> None:
    calls: list[tuple] = []

    class _FakePro:
        def fut_basic(self, **kw):
            calls.append(kw)
            return pd.DataFrame({"ts_code": ["AU2408.SHF", "AU2412.SHF"]})

    out = md.resolve_main_fut_by_prefix(_FakePro(), "SHFE", "AU")
    assert out in {"AU2408.SHF", "AU2412.SHF"}
    assert calls and calls[0]["exchange"] == "SHFE"


def test_sync_macro_daily_skips_when_already_synced(monkeypatch) -> None:
    monkeypatch.setattr(md, "get_today_run", lambda job: {"success": True})
    out = md.sync_macro_daily_full()
    assert out == {"ok": True, "skipped": True, "message": "already synced today"}


def test_sync_macro_daily_missing_api_key(monkeypatch) -> None:
    monkeypatch.setattr(md, "get_today_run", lambda job: None)
    monkeypatch.setattr(
        md, "get_settings", lambda: type("S", (), {"tu_share_api_key": ""})()
    )
    out = md.sync_macro_daily_full()
    assert out["ok"] is False
    assert "TU_SHARE_API_KEY" in out["error"]


def test_sync_macro_daily_full_driver(monkeypatch) -> None:
    """All series fetch nothing (start > end) → ok with updated=0."""
    monkeypatch.setattr(md, "get_today_run", lambda job: None)
    monkeypatch.setattr(
        md,
        "get_settings",
        lambda: type("S", (), {"tu_share_api_key": "x"})(),
    )
    monkeypatch.setattr(md, "_tushare_pro", lambda: object())
    monkeypatch.setattr(
        md, "get_last_trade_date", lambda sid: pd.Timestamp("2099-01-01").date()
    )
    monkeypatch.setattr(md, "_today_yyyymmdd", lambda: "20260808")
    monkeypatch.setattr(md, "insert_record", lambda **kw: None)

    out = md.sync_macro_daily_full()
    assert out["ok"] is True
    assert out["updated"] == 0
    assert out.get("skipped") is None


def test_sync_macro_daily_one_series_upserts(monkeypatch) -> None:
    """IXIC path: last date old → fetch paged → upsert."""
    monkeypatch.setattr(md, "get_today_run", lambda job: None)
    settings = type("S", (), {"tu_share_api_key": "x"})()
    monkeypatch.setattr(md, "get_settings", lambda: settings)
    monkeypatch.setattr(md, "_tushare_pro", lambda: object())

    def fake_last(sid: str):
        return pd.Timestamp("2026-06-30").date()

    monkeypatch.setattr(md, "get_last_trade_date", fake_last)
    monkeypatch.setattr(md, "_today_yyyymmdd", lambda: "20260702")

    monkeypatch.setattr(md, "_paged_index_global", lambda pro, code, s, e: _us_df())
    monkeypatch.setattr(md, "upsert_from_dataframe", lambda *a, **kw: 2)

    out = md.sync_macro_daily_full()
    assert out["ok"] is True
    assert out["updated"] >= 2
import sys

import pandas as pd

from data_sync_service.service import macro_daily as md


def test_tushare_pro_and_try(monkeypatch) -> None:
    monkeypatch.setattr(md, "get_settings", lambda: type("S", (), {"tu_share_api_key": "KEY"})())
    monkeypatch.setattr(md.ts, "set_token", lambda t: None)
    monkeypatch.setattr(md.ts, "pro_api", lambda k: ("pro", k))
    assert md._tushare_pro() == ("pro", "KEY")
    assert md.try_tushare_pro() == ("pro", "KEY")

    monkeypatch.setattr(md, "get_settings", lambda: type("S", (), {"tu_share_api_key": ""})())
    try:
        md._tushare_pro()
        assert False
    except RuntimeError:
        pass
    assert md.try_tushare_pro() is None


def test_normalize_fx_daily_df() -> None:
    df = pd.DataFrame({"bid_close": [1.0], "bid_open": [2.0], "bid_high": [3.0], "bid_low": [4.0]})
    out = md._normalize_fx_daily_df(df)
    assert out["close"][0] == 1.0 and out["open"][0] == 2.0
    assert md._normalize_fx_daily_df(None) is None
    assert md._normalize_fx_daily_df(pd.DataFrame()) is not None


def test_paged_index_global(monkeypatch) -> None:
    chunks = []
    pro = type("P", (), {"index_global": staticmethod(lambda ts_code, start_date, end_date: chunks.pop(0))})()
    d1 = pd.DataFrame({"trade_date": ["2026-01-01"], "close": [1.0]})
    d2 = pd.DataFrame({"trade_date": ["2026-01-01", "2026-02-01"], "close": [9.0, 2.0]})
    chunks.append(d1)
    chunks.append(d2)
    out = md._paged_index_global(pro, "IXIC", "20250101", "20260301")  # spans 2 page chunks
    assert out is not None and len(out) == 2 and out.iloc[0]["close"] == 9.0  # dedup keeps last

    chunks.append(pd.DataFrame())
    pro2 = type("P", (), {"index_global": staticmethod(lambda ts_code, start_date, end_date: None)})()
    assert md._paged_index_global(pro2, "IXIC", "20251201", "20260301") is None

    pro3 = type("P", (), {"index_global": staticmethod(lambda ts_code, start_date, end_date: (_ for _ in ()).throw(RuntimeError("x")))})()
    assert md._paged_index_global(pro3, "IXIC", "20251201", "20260301") is None


def test_paged_fut_daily(monkeypatch) -> None:
    d1 = pd.DataFrame({"trade_date": ["2026-01-05"], "close": [1.0]})
    chunks = [d1]
    pro = type("P", (), {"fut_daily": staticmethod(lambda ts_code, start_date, end_date: chunks.pop(0))})()
    out = md._paged_fut_daily(pro, "A50", "20251201", "20260110")
    assert out is not None and len(out) == 1

    pro2 = type("P", (), {"fut_daily": staticmethod(lambda ts_code, start_date, end_date: None)})()
    assert md._paged_fut_daily(pro2, "A50", "20251201", "20260110") is None


def test_resolve_sgx_a50_main() -> None:
    df = pd.DataFrame({
        "ts_code": ["C1", "A50C", "D2"],
        "name": ["X", "SGX FTSE China A50 Futures", "Y"],
        "list_date": ["2020-01-01", "2025-06-01", "2021-01-01"],
    })
    pro = type("P", (), {"fut_basic": staticmethod(lambda **kw: df)})()
    out = md.resolve_sgx_a50_main(pro)
    assert out == "A50C"

    pro2 = type("P", (), {"fut_basic": staticmethod(lambda **kw: (_ for _ in ()).throw(RuntimeError("x")))})()
    assert md.resolve_sgx_a50_main(pro2) is None

    pro3 = type("P", (), {"fut_basic": staticmethod(lambda **kw: pd.DataFrame())})()
    assert md.resolve_sgx_a50_main(pro3) is None


def test_resolve_ine_sc_main_and_prefix(monkeypatch) -> None:
    df = pd.DataFrame({"ts_code": ["SC2601.INE", "AU2606.SHFE", "CU2603.SHFE"], "name": ["原油", "金", "铜"], "list_date": ["2025-01-01", "2025-02-01", "2025-03-01"]})
    pro = type("P", (), {"fut_basic": staticmethod(lambda **kw: df)})()
    assert md.resolve_main_fut_by_prefix(pro, "SHFE", "au") == "AU2606.SHFE"
    assert md.resolve_ine_sc_main(pro) == "SC2601.INE"

    empty = type("P", (), {"fut_basic": staticmethod(lambda **kw: pd.DataFrame({"ts_code": []}))})()
    assert md.resolve_main_fut_by_prefix(empty, "INE", "SC") is None
    assert md.resolve_ine_sc_main(empty) is None

    failing = type("P", (), {"fut_basic": staticmethod(lambda **kw: (_ for _ in ()).throw(RuntimeError("x")))})()
    assert md.resolve_ine_sc_main(failing) is None


def test_fetch_hstech_via_ak_and_yf(monkeypatch) -> None:
    monkeypatch.setattr(sys, "platform", "darwin")
    assert md._fetch_hstech_bars_via_ak("20260101", "20260110") is None
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(md, "sys", md.sys)
    assert md._fetch_hstech_bars_via_ak("20260101", "20260110") is None  # akshare import missing

    df = pd.DataFrame({
        "date": ["2026-01-05", "2026-01-06"],
        "open": [1.0, 2.0], "high": [2.0, 3.0], "low": [0.5, 1.5],
        "close": [1.5, 2.5], "volume": [100, 200], "amount": [300, 400],
    })
    ak = type("AK", (), {"stock_hk_index_daily_sina": staticmethod(lambda symbol: df)})()
    monkeypatch.setattr(md, "sys", __import__("sys"))
    monkeypatch.setattr(md, "akshare", None) if hasattr(md, "akshare") else None
    import akshare  # noqa: F401

    # simulate import failure path by removing module attr
    monkeypatch.delattr(md, "akshare", raising=False)
    assert md._fetch_hstech_bars_via_ak("20260101", "20260110") is None


def test_sync_macro_daily_full_skip(monkeypatch) -> None:
    monkeypatch.setattr(md, "get_today_run", lambda job: {"success": True})
    out = md.sync_macro_daily_full()
    assert out["skipped"] is True


def test_sync_macro_daily_full_no_key(monkeypatch) -> None:
    monkeypatch.setattr(md, "get_today_run", lambda job: None)
    monkeypatch.setattr(md, "get_settings", lambda: type("S", (), {"tu_share_api_key": ""})())
    out = md.sync_macro_daily_full()
    assert out["ok"] is False and "API_KEY" in out["error"]


def test_sync_macro_daily_full_success(monkeypatch) -> None:
    monkeypatch.setattr(md, "get_today_run", lambda job: None)
    monkeypatch.setattr(md, "get_settings", lambda: type("S", (), {"tu_share_api_key": "K"})())
    monkeypatch.setattr(md, "_tushare_pro", lambda: object())
    monkeypatch.setattr(md, "get_last_trade_date", lambda sid: None)
    monkeypatch.setattr(md, "_today_yyyymmdd", lambda: "20260807")
    monkeypatch.setattr(md, "_date_to_yyyymmdd", lambda d: "20260807")

    def fake_upsert(df, series_id, source, underlying_ts_code):
        return 1 if series_id in (md.SID_IXIC, md.SID_DJI) else 0

    monkeypatch.setattr(md, "upsert_from_dataframe", fake_upsert)
    monkeypatch.setattr(md, "_paged_index_global", lambda pro, code, s, e: pd.DataFrame({"trade_date": ["2026-08-07"]}))
    monkeypatch.setattr(md, "_normalize_fx_daily_df", lambda df: pd.DataFrame({"trade_date": ["2026-08-07"]}))
    monkeypatch.setattr(md, "resolve_sgx_a50_main", lambda pro: "A50M")
    monkeypatch.setattr(md, "resolve_main_fut_by_prefix", lambda pro, ex, prefix: None)
    monkeypatch.setattr(md, "resolve_ine_sc_main", lambda pro: None)
    monkeypatch.setattr(md, "_paged_fut_daily", lambda pro, code, s, e: pd.DataFrame({"trade_date": ["2026-08-07"]}))
    monkeypatch.setattr(md, "_fetch_hstech_bars_via_ak", lambda s, e: None)
    monkeypatch.setattr(md, "_fetch_hstech_bars_via_yf", lambda s, e: None)
    monkeypatch.setattr(md, "insert_record", lambda **kw: None)
    out = md.sync_macro_daily_full()
    assert out["ok"] is True
    assert out["updated"] >= 2


def test_sync_macro_daily_full_resume_and_failure(monkeypatch) -> None:
    monkeypatch.setattr(md, "get_today_run", lambda job: {"success": False, "last_ts_code": md.SID_DJI})
    monkeypatch.setattr(md, "get_settings", lambda: type("S", (), {"tu_share_api_key": "K"})())
    monkeypatch.setattr(md, "_tushare_pro", lambda: object())
    monkeypatch.setattr(md, "get_last_trade_date", lambda sid: None)
    monkeypatch.setattr(md, "_today_yyyymmdd", lambda: "20260807")
    monkeypatch.setattr(md, "_date_to_yyyymmdd", lambda d: "20260807")
    monkeypatch.setattr(md, "_paged_index_global", lambda pro, code, s, e: (_ for _ in ()).throw(RuntimeError("provider down")))
    monkeypatch.setattr(md, "insert_record", lambda **kw: None)
    out = md.sync_macro_daily_full()
    assert out["ok"] is False
    assert out["series"] == md.SID_SPX  # resumed after DJI → SPX fails first
    assert out["last_ts_code"] is None  # nothing succeeded after resume point
