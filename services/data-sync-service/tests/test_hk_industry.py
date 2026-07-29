"""Tests for HK industry sync (Xueqiu mbu → stock_basic.industry)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

from data_sync_service.service import hk_industry


# ----- _truncate_mbu pure-function tests -----

def test_truncate_mbu_basic_chinese() -> None:
    text = "主要於中国及其他国家或地区研发及销售智能手机、IoT及生活消费产品、提供互联网服务及从事投资控股业务。"
    out = hk_industry._truncate_mbu(text)
    assert out is not None
    # Stops at first 。
    assert "。" not in out
    # Within length budget
    assert len(out) <= hk_industry.INDUSTRY_MAX_LEN
    assert out == "主要於中国及其他国家或地区研发及销售智能手机、I"


def test_truncate_mbu_removes_period_english() -> None:
    out = hk_industry._truncate_mbu("A tech retail company. It also does other stuff.")
    assert out is not None
    # First sentence only
    assert out == "A tech retail company"


def test_truncate_mbu_collapses_whitespace() -> None:
    out = hk_industry._truncate_mbu("  multi\n\nspace   text  ")
    assert out == "multi space text"


def test_truncate_mbu_handles_placeholder() -> None:
    for placeholder in (None, "", "None", "—", "-", "暂无", "N/A"):
        assert hk_industry._truncate_mbu(placeholder) is None


def test_truncate_mbu_no_separator_long_text_truncates_only() -> None:
    long = "A" * 100
    out = hk_industry._truncate_mbu(long)
    assert out is not None
    assert len(out) == hk_industry.INDUSTRY_MAX_LEN


# ----- fetch_xueqiu_mbu tests -----

def test_fetch_xueqiu_mbu_returns_truncated_label(monkeypatch: pytest.MonkeyPatch) -> None:
    df = pd.DataFrame(
        {
            "item": ["mbu", "comcnname"],
            "value": ["本集团主要从事金融业。", "腾讯控股"],
        }
    )
    fake_ak = SimpleNamespace(stock_individual_basic_info_hk_xq=lambda **_kw: df)
    monkeypatch.setitem(__import__("sys").modules, "akshare", fake_ak)

    out = hk_industry.fetch_xueqiu_mbu("00700.HK", sleep_s=0.0)
    assert out == "本集团主要从事金融业"


def test_fetch_xueqiu_mbu_strips_leading_zero_in_ticker(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_kwargs: dict = {}

    def fake_fetch(**kwargs):
        captured_kwargs.update(kwargs)
        return pd.DataFrame({"item": ["mbu"], "value": ["本集团主要从事金融业"]})

    fake_ak = SimpleNamespace(stock_individual_basic_info_hk_xq=fake_fetch)
    monkeypatch.setitem(__import__("sys").modules, "akshare", fake_ak)

    hk_industry.fetch_xueqiu_mbu("01810.HK", sleep_s=0.0)
    assert captured_kwargs["symbol"] == "1810"


def test_fetch_xueqiu_mbu_returns_none_for_non_hk() -> None:
    assert hk_industry.fetch_xueqiu_mbu("002415.SZ") is None


def test_fetch_xueqiu_mbu_returns_none_when_akshare_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """If akshare is not installed, fetch_xueqiu_mbu returns None gracefully."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "akshare":
            raise ImportError("no akshare")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert hk_industry.fetch_xueqiu_mbu("00700.HK", sleep_s=0.0) is None


def test_fetch_xueqiu_mbu_returns_none_on_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(**_kw):
        raise RuntimeError("xueqiu down")

    fake_ak = SimpleNamespace(stock_individual_basic_info_hk_xq=boom)
    monkeypatch.setitem(__import__("sys").modules, "akshare", fake_ak)
    assert hk_industry.fetch_xueqiu_mbu("00700.HK", sleep_s=0.0) is None


def test_fetch_xueqiu_mbu_retries_when_all_values_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """Xueqiu soft rate-limits by returning rows of all None; we must retry."""
    calls = {"n": 0}
    none_df = pd.DataFrame({"item": ["mbu"], "value": [None]})
    real_df = pd.DataFrame({"item": ["mbu"], "value": ["本集团主要从事金融业"]})

    def flaky(**_kw):
        calls["n"] += 1
        return none_df if calls["n"] <= 1 else real_df

    fake_ak = SimpleNamespace(stock_individual_basic_info_hk_xq=flaky)
    monkeypatch.setitem(__import__("sys").modules, "akshare", fake_ak)

    out = hk_industry.fetch_xueqiu_mbu("00700.HK", sleep_s=0.0, retries=2)
    assert out == "本集团主要从事金融业"
    assert calls["n"] == 2


def test_fetch_xueqiu_mbu_gives_up_after_all_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    """If every retry returns all-None, return None rather than infinite loop."""
    calls = {"n": 0}
    none_df = pd.DataFrame({"item": ["mbu"], "value": [None]})

    def always_none(**_kw):
        calls["n"] += 1
        return none_df

    fake_ak = SimpleNamespace(stock_individual_basic_info_hk_xq=always_none)
    monkeypatch.setitem(__import__("sys").modules, "akshare", fake_ak)

    out = hk_industry.fetch_xueqiu_mbu("00700.HK", sleep_s=0.0, retries=2)
    assert out is None
    assert calls["n"] == 3  # initial + 2 retries


# ----- _iter_missing_hk_codes tests -----

def _patch_get_connection(monkeypatch, conn_factory):
    """Patch get_connection in the data_sync_service.db module (lazy import target)."""
    from data_sync_service import db as db_mod
    monkeypatch.setattr(db_mod, "get_connection", conn_factory)


def test_iter_missing_hk_codes_returns_only_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hk_industry, "ensure_stock_basic", lambda: None)
    monkeypatch.setattr(
        hk_industry,
        "fetch_ts_codes_by_market",
        lambda market: ["00700.HK", "01810.HK", "09988.HK"],
    )

    class _Cur:
        def execute(self, *_args, **_kw):
            self._rows = [("01810.HK",), ("09988.HK",)]

        def fetchall(self):
            return self._rows

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class _Conn:
        def cursor(self):
            return _Cur()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    _patch_get_connection(monkeypatch, lambda: _Conn())

    missing = hk_industry._iter_missing_hk_codes(limit=None)
    assert missing == ["01810.HK", "09988.HK"]


def test_iter_missing_hk_codes_respects_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hk_industry, "ensure_stock_basic", lambda: None)
    monkeypatch.setattr(hk_industry, "fetch_ts_codes_by_market", lambda market: ["x"] * 100)

    class _Cur:
        def __init__(self):
            self._rows = [(f"00{i:03d}.HK",) for i in range(20)]

        def execute(self, *_args, **_kw):
            pass

        def fetchall(self):
            return self._rows

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class _Conn:
        def cursor(self):
            return _Cur()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    _patch_get_connection(monkeypatch, lambda: _Conn())

    missing = hk_industry._iter_missing_hk_codes(limit=5)
    assert missing == ["00000.HK", "00001.HK", "00002.HK", "00003.HK", "00004.HK"]


# ----- sync_hk_industry tests -----

def test_sync_hk_industry_upserts_resolved_labels(monkeypatch: pytest.MonkeyPatch) -> None:
    """When symbols are provided, fetch each mbu and update_industry is called."""
    captured: dict = {}

    def fake_update(mapping):
        captured["mapping"] = mapping
        return len(mapping)

    def fake_fetch(code, *, sleep_s=hk_industry.DEFAULT_SLEEP_S):
        return {"00700.HK": "本集团主要从事金融业", "01810.HK": None}.get(code)

    monkeypatch.setattr(hk_industry, "fetch_xueqiu_mbu", fake_fetch)
    monkeypatch.setattr(hk_industry, "update_industry", fake_update)
    monkeypatch.setattr(hk_industry, "insert_record", lambda **_kw: None)

    result = hk_industry.sync_hk_industry(symbols=["00700.HK", "01810.HK"])

    assert result["ok"] is True
    assert result["requested"] == 2
    assert result["resolved"] == 1
    assert result["updated"] == 1
    assert captured["mapping"] == {"00700.HK": "本集团主要从事金融业"}


def test_sync_hk_industry_returns_error_when_no_labels_resolved(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hk_industry, "fetch_xueqiu_mbu", lambda *_a, **_kw: None)
    monkeypatch.setattr(hk_industry, "insert_record", lambda **_kw: None)

    result = hk_industry.sync_hk_industry(symbols=["00700.HK"])
    assert result["ok"] is False
    assert "no labels resolved" in result["error"]


def test_sync_hk_industry_skips_when_no_codes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hk_industry, "_iter_missing_hk_codes", lambda *, limit: [])
    monkeypatch.setattr(hk_industry, "insert_record", lambda **_kw: None)

    result = hk_industry.sync_hk_industry()
    assert result["ok"] is True
    assert result["skipped"] is True


# ----- get_hk_industry_status tests -----

def test_get_hk_industry_status_returns_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hk_industry, "ensure_stock_basic", lambda: None)

    class _Cur:
        def execute(self, *_args, **_kw):
            pass

        def fetchone(self):
            return (3000, 2500)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class _Conn:
        def cursor(self):
            return _Cur()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    _patch_get_connection(monkeypatch, lambda: _Conn())

    status = hk_industry.get_hk_industry_status()
    assert status["totalHk"] == 3000
    assert status["mappedHk"] == 2500
    assert status["missingHk"] == 500
    assert status["coveragePct"] == pytest.approx(83.33, abs=0.01)


# ----- db.update_industry tests -----

def test_update_industry_writes_only_industry_column(monkeypatch: pytest.MonkeyPatch) -> None:
    """update_industry should issue per-row UPDATE statements, not full-row UPSERT."""
    from data_sync_service.db import stock_basic

    captured: list = []

    class _Cur:
        def executemany(self, sql, rows):
            captured.append((sql, list(rows)))

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class _Conn:
        def cursor(self):
            return _Cur()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def commit(self):
            pass

    monkeypatch.setattr(stock_basic, "ensure_table", lambda: None)
    monkeypatch.setattr(stock_basic, "get_connection", lambda: _Conn())

    n = stock_basic.update_industry(
        {"00700.HK": "本集团主要从事金融业", "01810.HK": "小米", "09988.HK": ""}
    )
    assert n == 2
    sql, rows = captured[0]
    assert "UPDATE" in sql
    assert "industry = %s" in sql
    assert "WHERE ts_code = %s" in sql
    assert rows == [("本集团主要从事金融业", "00700.HK"), ("小米", "01810.HK")]


def test_update_industry_skips_empty_mapping() -> None:
    from data_sync_service.db import stock_basic

    assert stock_basic.update_industry({}) == 0


# ----- keep_industry in upsert_from_dataframe -----

def test_upsert_from_dataframe_keep_industry_uses_coalesce_sql(monkeypatch: pytest.MonkeyPatch) -> None:
    """hk_basic sync must use the COALESCE-preserving SQL so it doesn't blank industry."""
    import pandas as pd

    from data_sync_service.db import stock_basic

    captured: list = []

    class _Cur:
        def executemany(self, sql, rows):
            captured.append(sql)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class _Conn:
        def cursor(self):
            return _Cur()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def commit(self):
            pass

    monkeypatch.setattr(stock_basic, "ensure_table", lambda: None)
    monkeypatch.setattr(stock_basic, "get_connection", lambda: _Conn())

    df = pd.DataFrame(
        [
            {
                "ts_code": "01810.HK",
                "symbol": "01810",
                "name": "小米集团-W",
                "industry": None,
                "market": "HK",
                "list_date": "20180709",
                "delist_date": None,
            }
        ]
    )

    stock_basic.upsert_from_dataframe(df, keep_industry=True)
    assert "COALESCE" in captured[0]
    assert "EXCLUDED.industry" in captured[0]