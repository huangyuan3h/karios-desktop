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
    monkeypatch.setattr(hk_industry, "_iter_all_hk_codes", lambda: [])
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


# ----- East Money primary fetch path -----


class _FakeEMResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


def _fake_em_payload(*, page_number: int, page_size: int, total: int) -> dict:
    """Build a single East Money RPT_HKF10_INFO_ORGPROFILE response payload.

    Codes are offset by `(page_number - 1) * page_size` so each page has unique codes.
    """
    rows: list[dict] = []
    start = (page_number - 1) * page_size + 1
    for i in range(start, start + total):
        rows.append({"SECUCODE": f"{i:05d}.HK", "BELONG_INDUSTRY": f"行业{i}"})
    return {"success": True, "result": {"data": rows, "pages": 1, "count": total}}


def test_fetch_eastmoney_hk_industry_map_paginates(monkeypatch: pytest.MonkeyPatch) -> None:
    """EM fetcher must paginate 500/page and stop when a page is short."""
    calls: list[int] = []

    def fake_fetch_page(page_number: int, *, page_size: int = 500):
        assert page_size == 500
        calls.append(page_number)
        if page_number < 3:
            return _fake_em_payload(
                page_number=page_number, page_size=500, total=500
            )["result"]["data"]
        # Last page returns 123 rows — must stop.
        return _fake_em_payload(
            page_number=page_number, page_size=500, total=123
        )["result"]["data"]

    monkeypatch.setattr(hk_industry, "_fetch_em_page", fake_fetch_page)

    result, stats = hk_industry.fetch_eastmoney_hk_industry_map()

    assert calls == [1, 2, 3]
    assert len(result) == 500 + 500 + 123
    assert result["00001.HK"] == "行业1"
    assert result["00500.HK"] == "行业500"
    assert result["00501.HK"] == "行业501"
    assert stats["emPages"] == 3
    assert stats["emResolved"] == 500 + 500 + 123


def test_fetch_eastmoney_hk_industry_map_skips_empty_industries(monkeypatch: pytest.MonkeyPatch) -> None:
    """Row with empty / missing BELONG_INDUSTRY must be dropped, not saved as empty string."""
    rows = [
        {"SECUCODE": "00001.HK", "BELONG_INDUSTRY": "银行"},
        {"SECUCODE": "00002.HK", "BELONG_INDUSTRY": ""},
        {"SECUCODE": "00003.HK", "BELONG_INDUSTRY": None},
        {"SECUCODE": "00004.HK"},  # missing key entirely
        {"SECUCODE": "00005.HK", "BELONG_INDUSTRY": "  银行  "},
    ]

    monkeypatch.setattr(
        hk_industry,
        "_fetch_em_page",
        lambda page_number, *, page_size=500: rows,
    )

    result, stats = hk_industry.fetch_eastmoney_hk_industry_map()

    assert set(result.keys()) == {"00001.HK", "00005.HK"}
    assert result["00005.HK"] == "银行"  # stripped
    assert stats["emEmpty"] == 3
    assert stats["emResolved"] == 2


def test_fetch_eastmoney_hk_industry_map_ignores_non_hk_codes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Rows whose SECUCODE doesn't end with .HK must be ignored."""
    rows = [
        {"SECUCODE": "00001.HK", "BELONG_INDUSTRY": "银行"},
        {"SECUCODE": "00002.SZ", "BELONG_INDUSTRY": "银行"},
        {"SECUCODE": "", "BELONG_INDUSTRY": "银行"},
    ]

    monkeypatch.setattr(
        hk_industry,
        "_fetch_em_page",
        lambda page_number, *, page_size=500: rows,
    )

    result, _stats = hk_industry.fetch_eastmoney_hk_industry_map()
    assert result == {"00001.HK": "银行"}


def test_sync_hk_industry_uses_em_first(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default (no symbols) path must read EM and write to stock_basic.industry."""
    monkeypatch.setattr(hk_industry, "_iter_all_hk_codes", lambda: ["00001.HK", "00002.HK"])
    monkeypatch.setattr(
        hk_industry,
        "fetch_eastmoney_hk_industry_map",
        lambda *, page_size=500, max_pages=30, sleep_s=0.0: (
            {"00001.HK": "银行", "00002.HK": "科技"},
            {"emPages": 1, "emRows": 2, "emEmpty": 0, "emResolved": 2},
        ),
    )

    def boom(*_a, **_kw):
        raise AssertionError("Xueqiu fallback should not be called when EM covers all")

    monkeypatch.setattr(hk_industry, "_resolve_via_xueqiu_only", boom)

    captured: dict = {}

    def fake_update(mapping):
        captured["mapping"] = mapping
        return len(mapping)

    monkeypatch.setattr(hk_industry, "update_industry", fake_update)
    monkeypatch.setattr(hk_industry, "insert_record", lambda **_kw: None)

    result = hk_industry.sync_hk_industry()

    assert result["ok"] is True
    assert result["requested"] == 2
    assert result["resolved"] == 2
    assert result["updated"] == 2
    assert result["emResolved"] == 2
    assert result["xueqiuResolved"] == 0
    assert result["emPages"] == 1
    assert captured["mapping"] == {"00001.HK": "银行", "00002.HK": "科技"}


def test_sync_hk_industry_em_fallback_to_xueqiu(monkeypatch: pytest.MonkeyPatch) -> None:
    """Codes not in EM map must fall back to Xueqiu per-stock."""
    monkeypatch.setattr(
        hk_industry,
        "_iter_all_hk_codes",
        lambda: ["00001.HK", "00099.HK", "00W01.HK"],
    )
    monkeypatch.setattr(
        hk_industry,
        "fetch_eastmoney_hk_industry_map",
        lambda *, page_size=500, max_pages=30, sleep_s=0.0: (
            {"00001.HK": "银行"},  # only one code has EM coverage
            {"emPages": 1, "emRows": 1, "emEmpty": 0, "emResolved": 1},
        ),
    )

    xueqiu_calls: list[str] = []

    def fake_xueqiu(codes, *, sleep_s=hk_industry.DEFAULT_SLEEP_S):
        # Only the two missing-in-EM codes should be passed in.
        xueqiu_calls.extend(codes)
        if "00099.HK" in codes:
            return {"00099.HK": "通讯"}
        return {}

    monkeypatch.setattr(hk_industry, "_resolve_via_xueqiu_only", fake_xueqiu)

    captured: dict = {}
    monkeypatch.setattr(
        hk_industry,
        "update_industry",
        lambda mapping: captured.setdefault("mapping", mapping) or len(mapping),
    )
    monkeypatch.setattr(hk_industry, "insert_record", lambda **_kw: None)

    result = hk_industry.sync_hk_industry()

    assert result["ok"] is True
    assert result["requested"] == 3
    assert result["resolved"] == 2
    assert result["emResolved"] == 1
    assert result["xueqiuResolved"] == 1
    assert sorted(xueqiu_calls) == ["00099.HK", "00W01.HK"]
    assert captured["mapping"] == {"00001.HK": "银行", "00099.HK": "通讯"}


def test_sync_hk_industry_symbols_path_uses_xueqiu(monkeypatch: pytest.MonkeyPatch) -> None:
    """Explicit symbols must bypass the EM fetch and use Xueqiu only (manual override)."""
    monkeypatch.setattr(
        hk_industry,
        "_iter_all_hk_codes",
        lambda: (_ for _ in ()).throw(AssertionError("EM path should not run when symbols given")),
    )

    monkeypatch.setattr(
        hk_industry,
        "_resolve_via_xueqiu_only",
        lambda codes, *, sleep_s=hk_industry.DEFAULT_SLEEP_S: {"00700.HK": "互联网"},
    )

    captured: dict = {}
    monkeypatch.setattr(
        hk_industry,
        "update_industry",
        lambda mapping: captured.setdefault("mapping", mapping) or len(mapping),
    )
    monkeypatch.setattr(hk_industry, "insert_record", lambda **_kw: None)

    result = hk_industry.sync_hk_industry(symbols=["00700.HK"])

    assert result["ok"] is True
    assert result["requested"] == 1
    assert result["resolved"] == 1
    assert result["emResolved"] == 0
    assert captured["mapping"] == {"00700.HK": "互联网"}


def test_sync_hk_industry_em_total_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """If EM returns empty and Xueqiu returns empty, fail with no_labels error."""
    monkeypatch.setattr(hk_industry, "_iter_all_hk_codes", lambda: ["00700.HK", "01810.HK"])
    monkeypatch.setattr(
        hk_industry,
        "fetch_eastmoney_hk_industry_map",
        lambda *, page_size=500, max_pages=30, sleep_s=0.0: ({}, {"emPages": 0, "emResolved": 0}),
    )
    monkeypatch.setattr(
        hk_industry,
        "_resolve_via_xueqiu_only",
        lambda codes, *, sleep_s=hk_industry.DEFAULT_SLEEP_S: {},
    )
    monkeypatch.setattr(hk_industry, "insert_record", lambda **_kw: None)

    result = hk_industry.sync_hk_industry()

    assert result["ok"] is False
    assert "no labels" in result["error"]
    assert result["resolved"] == 0
    assert result["updated"] == 0


def test_fetch_em_page_uses_proxy_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """_fetch_em_page must pass proxies={'http': None, 'https': None} to bypass HTTP_PROXY."""
    import requests

    captured: dict = {}

    def fake_get(url, *, params, headers, timeout, proxies):
        captured["url"] = url
        captured["params"] = params
        captured["headers"] = headers
        captured["timeout"] = timeout
        captured["proxies"] = proxies
        return _FakeEMResponse(
            {"success": True, "result": {"data": [], "pages": 0, "count": 0}}
        )

    monkeypatch.setattr(requests, "get", fake_get)

    hk_industry._fetch_em_page(1)

    assert captured["proxies"] == {"http": None, "https": None}
    assert captured["params"]["reportName"] == hk_industry.EM_HK_REPORT_NAME
    assert captured["params"]["pageNumber"] == "1"
    assert captured["params"]["pageSize"] == "500"
    assert captured["headers"]["Referer"] == "https://data.eastmoney.com/"