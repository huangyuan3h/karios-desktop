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
