"""top_inst_flow network layer + util coverage (EM HTTP, retry, tushare, payloads)."""

from __future__ import annotations

import json

import pytest

from data_sync_service.service import top_inst_flow as tif


class _FakeTime:
    sleep_calls: list[float] = []

    def sleep(self, s: float) -> None:
        self.sleep_calls.append(s)


def _monkey_no_sleep(monkeypatch) -> _FakeTime:
    fake = _FakeTime()
    fake.sleep_calls = []
    monkeypatch.setattr(tif, "time", fake)
    return fake


def _em_page(pages: int, rows: list[dict]) -> dict:
    return {"success": True, "result": {"data": rows, "pages": pages, "count": len(rows)}}


class _Resp:
    def __init__(self, payload) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode()


class _Ctx:
    def __init__(self, resp) -> None:
        self._resp = resp

    def __enter__(self):
        return self._resp

    def __exit__(self, *a):
        return None


def _patch_urlopen(monkeypatch, payload) -> None:
    monkeypatch.setattr(tif.urllib.request, "urlopen", lambda req, timeout=25: _Ctx(_Resp(payload)))


# ---- _with_retry -----------------------------------------------------------

def test_with_retry_succeeds_first_try(monkeypatch) -> None:
    _monkey_no_sleep(monkeypatch)
    out = tif._with_retry(lambda: "ok")
    assert out == "ok"


def test_with_retry_retries_then_succeeds(monkeypatch) -> None:
    fake = _monkey_no_sleep(monkeypatch)
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("boom")
        return "recovered"

    assert tif._with_retry(fn) == "recovered"
    assert calls["n"] == 3
    assert len(fake.sleep_calls) == 2


def test_with_retry_raises_after_all_tries(monkeypatch) -> None:
    _monkey_no_sleep(monkeypatch)
    with pytest.raises(RuntimeError, match="dead"):
        tif._with_retry(lambda: (_ for _ in ()).throw(RuntimeError("dead")), tries=2)


def test_with_retry_clamps_tries(monkeypatch) -> None:
    fake = _monkey_no_sleep(monkeypatch)
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        raise ValueError("x")

    with pytest.raises(ValueError):
        tif._with_retry(fn, tries=0)
    assert calls["n"] == 1
    calls["n"] = 0
    with pytest.raises(ValueError):
        tif._with_retry(fn, tries=10)
    assert calls["n"] == 5  # clamped to 5
    assert fake.sleep_calls and fake.sleep_calls[0] == fake.sleep_calls[0]  # floats, no NaN


# ---- _em_request -----------------------------------------------------------

def test_em_request_ok(monkeypatch) -> None:
    _patch_urlopen(monkeypatch, {"success": True, "result": {"data": [{"a": 1}]}})
    j = tif._em_request({"reportName": "X"})
    assert j["result"]["data"][0]["a"] == 1


def test_em_request_not_dict(monkeypatch) -> None:
    _patch_urlopen(monkeypatch, [1, 2, 3])
    assert tif._em_request({}) == {}


def test_em_request_error_9201_empty(monkeypatch) -> None:
    _patch_urlopen(monkeypatch, {"success": False, "code": 9201, "message": "no data"})
    j = tif._em_request({})
    assert j["result"]["data"] == []
    assert j["success"] is True


def test_em_request_raises_on_error(monkeypatch) -> None:
    _patch_urlopen(monkeypatch, {"success": False, "code": 500, "message": "server down"})
    with pytest.raises(RuntimeError, match="eastmoney_error: server down"):
        tif._em_request({})


# ---- _em_fetch_pages -------------------------------------------------------

def test_em_fetch_pages_single_page(monkeypatch) -> None:
    _monkey_no_sleep(monkeypatch)
    monkeypatch.setattr(
        tif, "_em_request", lambda params: _em_page(1, [{"SECURITY_CODE": "600000"}, {"SECURITY_CODE": "600001"}])
    )
    rows = tif._em_fetch_pages(report_name="R", filter_expr="(TRADE_DATE='2026-08-07')")
    assert len(rows) == 2


def test_em_fetch_pages_multiple_pages(monkeypatch) -> None:
    _monkey_no_sleep(monkeypatch)
    calls: list[int] = []

    def fake(params):
        page = int(params["pageNumber"])
        calls.append(page)
        if page == 1:
            return _em_page(2, [{"SECURITY_CODE": "600000"}])
        return _em_page(2, [{"SECURITY_CODE": "600001"}])

    monkeypatch.setattr(tif, "_em_request", fake)
    rows = tif._em_fetch_pages(report_name="R", filter_expr="f")
    assert len(rows) == 2
    assert calls == [1, 2]


def test_em_fetch_pages_empty_data_breaks(monkeypatch) -> None:
    _monkey_no_sleep(monkeypatch)
    monkeypatch.setattr(tif, "_em_request", lambda params: _em_page(5, []))
    assert tif._em_fetch_pages(report_name="R", filter_expr="f") == []


def test_em_fetch_pages_bad_pages_field(monkeypatch) -> None:
    _monkey_no_sleep(monkeypatch)
    monkeypatch.setattr(tif, "_em_request", lambda params: {"success": True, "result": {"data": [{"a": 1}], "pages": "abc"}})
    rows = tif._em_fetch_pages(report_name="R", filter_expr="f")
    assert len(rows) == 1


def test_em_fetch_pages_respects_max_pages(monkeypatch) -> None:
    _monkey_no_sleep(monkeypatch)
    original = tif.EM_MAX_PAGES
    monkeypatch.setattr(tif, "EM_MAX_PAGES", 2)
    monkeypatch.setattr(tif, "_em_request", lambda params: _em_page(99, [{"a": 1}]))
    rows = tif._em_fetch_pages(report_name="R", filter_expr="f")
    assert len(rows) == 2
    monkeypatch.setattr(tif, "EM_MAX_PAGES", original)


def test_em_fetch_pages_non_dict_rows_skipped(monkeypatch) -> None:
    _monkey_no_sleep(monkeypatch)
    monkeypatch.setattr(tif, "_em_request", lambda params: _em_page(1, [{"a": 1}, "junk", None]))
    rows = tif._em_fetch_pages(report_name="R", filter_expr="f")
    assert len(rows) == 1


# ---- EM public fetchers ----------------------------------------------------

def test_fetch_em_lhb_tickers_on_date(monkeypatch) -> None:
    _monkey_no_sleep(monkeypatch)
    monkeypatch.setattr(
        tif,
        "_em_request",
        lambda params: _em_page(
            1,
            [{"SECURITY_CODE": "600000"}, {"SECURITY_CODE": "abc"}, {"SECURITY_CODE": None}, {"SECURITY_CODE": " 000001 "}],
        ),
    )
    out = tif.fetch_em_lhb_tickers_on_date("2026-08-07")
    assert out == {"600000", "000001"}


def test_fetch_em_org_trades_on_date(monkeypatch) -> None:
    _monkey_no_sleep(monkeypatch)
    monkeypatch.setattr(
        tif,
        "_em_request",
        lambda params: _em_page(1, [{"SECURITY_CODE": "600000", "NET_BUY_AMT": 1.0}, {"SECURITY_CODE": ""}]),
    )
    out = tif.fetch_em_org_trades_on_date("2026-08-07")
    assert set(out) == {"600000"}
    assert out["600000"]["NET_BUY_AMT"] == 1.0


def test_seat_rows_from_report_invalid_ticker(monkeypatch) -> None:
    _monkey_no_sleep(monkeypatch)
    monkeypatch.setattr(tif, "_em_request", lambda params: _em_page(1, []))
    assert tif._seat_rows_from_report(report_name="R", ts_code="bad", trade_date_iso="2026-08-07", side="buy") == []


def test_seat_rows_from_report_parses_and_filters(monkeypatch) -> None:
    _monkey_no_sleep(monkeypatch)
    monkeypatch.setattr(
        tif,
        "_em_request",
        lambda params: _em_page(
            1,
            [
                {"OPERATEDEPT_NAME": "机构专用", "BUY": "1.5", "SELL": "0.5", "NET": "1.0", "EXPLANATION": "三日涨幅偏离"},
                {"OPERATEDEPT_NAME_ABBR": "拉萨团结路", "BUY": "bad", "SELL": None, "NET": "x", "EXPLAIN": ""},
                {"BUY": "3"},  # no name -> skipped
            ],
        ),
    )
    rows = tif._seat_rows_from_report(report_name="R", ts_code="600000.SH", trade_date_iso="2026-08-07", side="buy")
    assert len(rows) == 2
    assert rows[0]["exalter"] == "机构专用"
    assert rows[0]["buy"] == 1.5
    assert rows[0]["sell"] == 0.5
    assert rows[0]["net_buy"] == 1.0
    assert rows[0]["side"] == "buy"
    assert rows[0]["reason"] == "三日涨幅偏离"
    assert rows[1]["buy"] is None and rows[1]["net_buy"] is None
    assert rows[1]["reason"] is None


def test_seat_rows_from_report_swallows_http_error(monkeypatch) -> None:
    _monkey_no_sleep(monkeypatch)

    def boom(params):
        raise RuntimeError("net down")

    monkeypatch.setattr(tif, "_em_request", boom)
    assert tif._seat_rows_from_report(report_name="R", ts_code="600000.SH", trade_date_iso="2026-08-07", side="buy") == []


def test_fetch_em_lhb_buy_seats(monkeypatch) -> None:
    _monkey_no_sleep(monkeypatch)
    monkeypatch.setattr(
        tif,
        "_seat_rows_from_report",
        lambda **kw: [
            {"exalter": "机构专用", "buy": 5.0, "sell": None, "net_buy": 5.0, "side": "buy", "reason": None},
            {"exalter": "拉萨", "buy": 3.0},
        ],
    )
    out = tif.fetch_em_lhb_buy_seats(ts_code="600000.SH", trade_date_iso="2026-08-07")
    assert out == [{"exalter": "机构专用", "buy": 5.0}, {"exalter": "拉萨", "buy": 3.0}]


def test_fetch_em_inst_seat_rows_filters(monkeypatch) -> None:
    _monkey_no_sleep(monkeypatch)
    monkeypatch.setattr(
        tif,
        "_seat_rows_from_report",
        lambda **kw: [
            {"exalter": "机构专用", "side": kw["side"]},
            {"exalter": "拉萨团结路", "side": kw["side"]},
        ],
    )
    out = tif.fetch_em_inst_seat_rows(ts_code="600000.SH", trade_date_iso="2026-08-07")
    assert [r["exalter"] for r in out] == ["机构专用", "机构专用"]


def test_fetch_em_seat_bundle_ok(monkeypatch) -> None:
    monkeypatch.setattr(tif, "fetch_em_lhb_buy_seats", lambda **kw: [{"exalter": "a"}])
    monkeypatch.setattr(tif, "fetch_em_inst_seat_rows", lambda **kw: [{"exalter": "机构专用"}])
    b = tif.fetch_em_seat_bundle(ts_code="600000.SH", trade_date_iso="2026-08-07")
    assert b.error is None
    assert len(b.buy_seats) == 1 and len(b.inst_seats) == 1


def test_fetch_em_seat_bundle_error(monkeypatch) -> None:
    def boom(**kw):
        raise RuntimeError("kaboom")

    monkeypatch.setattr(tif, "fetch_em_lhb_buy_seats", boom)
    b = tif.fetch_em_seat_bundle(ts_code="600000.SH", trade_date_iso="2026-08-07")
    assert b.error and "kaboom" in b.error


def test_fetch_em_seat_bundles_parallel_empty(monkeypatch) -> None:
    assert tif.fetch_em_seat_bundles_parallel([], trade_date_iso="2026-08-07") == {}


def test_fetch_em_seat_bundles_parallel_isolates_worker_error(monkeypatch) -> None:
    def boom(*, ts_code: str, trade_date_iso: str) -> tif.EastMoneySeatBundle:
        raise RuntimeError(f"fail {ts_code}")

    monkeypatch.setattr(tif, "fetch_em_seat_bundle", boom)
    out = tif.fetch_em_seat_bundles_parallel(["600000.SH"], trade_date_iso="2026-08-07")
    assert out["600000.SH"].error and "fail" in out["600000.SH"].error


# ---- tushare layer ---------------------------------------------------------

def _fake_pro():
    class _Pro:
        def top_list(self, trade_date: str):
            return [{"ts_code": "600000.SH"}, {"code": "000001.SZ"}]

        def top_inst(self, trade_date: str):
            return [
                {"ts_code": "600000.SH", "exalter": "机构专用", "buy": 100.0, "net": 95.0, "side": "0", "上榜理由": "日换手率达20%"},
                {"ts_code": "600000.SH", "exalter": "拉萨团结路", "side": "1", "net": -5.0},
                {"ts_code": "bad", "exalter": "机构专用", "buy": 1.0},
            ]

    return _Pro()


def test_fetch_tushare_top_inst_on_date(monkeypatch) -> None:
    monkeypatch.setenv("TUSHARE_TOKEN", "secret-token")

    class _FakeTs:
        @staticmethod
        def pro_api(token=None):
            assert token == "secret-token"
            return _fake_pro()

    monkeypatch.setitem(__import__("sys").modules, "tushare", _FakeTs)
    out = tif.fetch_tushare_top_inst_on_date("2026-08-07")
    assert out.source == "tushare"
    assert out.lhb_tickers == {"600000", "000001"}
    assert set(out.org_by_ticker) == {"600000"}
    assert out.org_by_ticker["600000"]["NET_BUY_AMT"] == 95.0
    assert out.org_by_ticker["600000"]["EXPLANATION"] == "日换手率达20%"
    assert out.lhb_count == 2 and out.org_trade_count == 1


def test_fetch_tushare_top_inst_on_date_no_token(monkeypatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)

    class _FakeTs:
        @staticmethod
        def pro_api(token=None):
            assert token is None
            return _fake_pro()

    monkeypatch.setitem(__import__("sys").modules, "tushare", _FakeTs)
    out = tif.fetch_tushare_top_inst_on_date("2026-08-07")
    assert out.lhb_tickers == {"600000", "000001"}


def test_fetch_tushare_import_failure(monkeypatch) -> None:
    monkeypatch.delitem(__import__("sys").modules, "tushare", raising=False)
    real_import = __import__

    def fake_import(name, *args, **kw):
        if name == "tushare":
            raise ImportError("no tushare installed")
        return real_import(name, *args, **kw)

    monkeypatch.setattr(__import__("sys").modules["builtins"], "__import__", fake_import)
    with pytest.raises(RuntimeError, match="tushare_import_failed"):
        tif.fetch_tushare_top_inst_on_date("2026-08-07")


def test_fetch_eastmoney_top_inst_on_date(monkeypatch) -> None:
    monkeypatch.setattr(tif, "fetch_em_lhb_tickers_on_date", lambda td: {"600000"})
    monkeypatch.setattr(tif, "fetch_em_org_trades_on_date", lambda td: {"600000": {"NET_BUY_AMT": 1.0}})
    out = tif.fetch_eastmoney_top_inst_on_date("2026-08-07")
    assert out.source == "eastmoney"
    assert out.lhb_tickers == {"600000"}
    assert out.lhb_count == 1 and out.org_trade_count == 1


# ---- provider orchestration ------------------------------------------------

def test_configured_providers_default(monkeypatch) -> None:
    monkeypatch.delenv("TOP_INST_PROVIDER", raising=False)
    assert tif._configured_top_inst_providers() == ["tushare", "eastmoney"]


def test_configured_providers_filters_unknown(monkeypatch) -> None:
    monkeypatch.setenv("TOP_INST_PROVIDER", "tushare, bogus, TUSHARE, eastmoney")
    assert tif._configured_top_inst_providers() == ["tushare", "eastmoney"]


def test_configured_providers_all_unknown_fallback(monkeypatch) -> None:
    monkeypatch.setenv("TOP_INST_PROVIDER", "bogus, nope")
    assert tif._configured_top_inst_providers() == ["eastmoney"]


def test_fetch_top_inst_provider_result_tushare_first(monkeypatch) -> None:
    monkeypatch.setattr(tif, "_configured_top_inst_providers", lambda: ["tushare", "eastmoney"])
    result = tif.TopInstProviderResult(source="tushare", lhb_tickers={"1"})
    monkeypatch.setattr(tif, "fetch_tushare_top_inst_on_date", lambda td: result)
    out, errors = tif.fetch_top_inst_provider_result("2026-08-07")
    assert out is result and errors == []


def test_fetch_top_inst_provider_result_fallback(monkeypatch) -> None:
    monkeypatch.setattr(tif, "_configured_top_inst_providers", lambda: ["tushare", "eastmoney"])

    def boom(td):
        raise RuntimeError("quota exceeded")

    result = tif.TopInstProviderResult(source="eastmoney", lhb_tickers={"1"})
    monkeypatch.setattr(tif, "fetch_tushare_top_inst_on_date", boom)
    monkeypatch.setattr(tif, "fetch_eastmoney_top_inst_on_date", lambda td: result)
    out, errors = tif.fetch_top_inst_provider_result("2026-08-07")
    assert out is result
    assert errors == ["tushare: quota exceeded"]


def test_fetch_top_inst_provider_result_all_fail(monkeypatch) -> None:
    monkeypatch.setattr(tif, "_configured_top_inst_providers", lambda: ["tushare"])

    def boom(td):
        raise RuntimeError("quota exceeded")

    monkeypatch.setattr(tif, "fetch_tushare_top_inst_on_date", boom)
    with pytest.raises(RuntimeError, match="tushare: quota exceeded"):
        tif.fetch_top_inst_provider_result("2026-08-07")


# ---- payload builders ------------------------------------------------------

def test_build_top_buy_seats_payload(monkeypatch) -> None:
    seats = [
        {"exalter": "拉萨团结路", "buy": 9.0},
        {"exalter": "", "buy": 0.1},
        "junk",
        {"exalter": "机构专用", "buy": 0.5},
        {"exalter": "财通证券", "buy": 1.0},
    ]
    out = tif.build_top_buy_seats_payload(seats, limit=3)
    assert [s["name"] for s in out] == ["拉萨团结路", "财通证券", "机构专用"]
    assert out[0]["isLhasa"] is True and out[0]["isInst"] is False
    assert out[1]["buyAmt"] == 1.0
    assert out[2]["isInst"] is True
    assert tif.build_top_buy_seats_payload(None) == []
    assert tif.build_top_buy_seats_payload([]) == []


def test_format_seats_summary(monkeypatch) -> None:
    seats = [
        {"name": "拉萨团结路", "isLhasa": True, "isInst": False},
        {"name": "机构专用", "isLhasa": False, "isInst": True},
        {"name": "某营业部", "isLhasa": False, "isInst": False},
    ]
    assert tif._format_seats_summary(seats) == " | 买: 拉萨, 机构, 某营业部"
    assert tif._format_seats_summary([]) == ""
    assert tif._format_seats_summary(None) == ""


def test_build_inst_flow_payload_incomplete_yi(monkeypatch) -> None:
    summary = {"trade_date": "2026-08-07", "on_board": True, "inst_net_buy_yi": None, "seat_label": ""}
    out = tif.build_inst_flow_payload(summary, buy_seats=[{"exalter": "a", "buy": 1.0}])
    assert out["display"] == "上榜(数据不完整)"
    assert out["topBuySeats"][0]["name"] == "a"


def test_build_inst_flow_payload_bad_yi(monkeypatch) -> None:
    summary = {"trade_date": "2026-08-07", "on_board": True, "inst_net_buy_yi": "abc", "seat_label": "机构主买"}
    out = tif.build_inst_flow_payload(summary)
    assert out["display"] == "上榜(数据不完整)"


def test_build_inst_flow_payload_full(monkeypatch) -> None:
    summary = {
        "trade_date": "2026-08-07",
        "on_board": True,
        "inst_net_buy_yi": -0.25,
        "seat_label": "机构净卖",
        "lhasa_dominant": False,
    }
    out = tif.build_inst_flow_payload(
        summary, buy_seats=[{"exalter": "拉萨团结路", "buy": 1.0}, {"exalter": "机构专用", "buy": 2.0}]
    )
    assert out["instNetBuyYi"] == -0.25
    assert out["label"] == "机构净卖"
    assert out["display"] == "-0.2亿 (机构净卖) | 买: 机构, 拉萨"


# ---- util helpers ----------------------------------------------------------

def test_parse_cal_date_variants(monkeypatch) -> None:
    from datetime import date

    assert tif._parse_cal_date("20260807") == date(2026, 8, 7)
    assert tif._parse_cal_date("2026-08-07") == date(2026, 8, 7)
    assert tif._parse_cal_date(" 20260807 ") == date(2026, 8, 7)
    with pytest.raises(ValueError):
        tif._parse_cal_date("garbage")


def test_yyyymmdd_to_iso_variants(monkeypatch) -> None:
    assert tif._yyyymmdd_to_iso("20260807") == "2026-08-07"
    assert tif._yyyymmdd_to_iso("2026-08-07") == "2026-08-07"
    assert tif._yyyymmdd_to_iso(None) == "None"


def test_symbol_to_ts_code_variants(monkeypatch) -> None:
    assert tif._symbol_to_ts_code("CN:600000") == "600000.SH"
    assert tif._symbol_to_ts_code("CN:000001") == "000001.SZ"
    assert tif._symbol_to_ts_code("HK:00700") is None
    assert tif._symbol_to_ts_code("CN:12345") is None
    assert tif._symbol_to_ts_code("CN:abc123") is None


def test_ts_code_to_ticker_variants(monkeypatch) -> None:
    assert tif._ts_code_to_ticker("600000.SH") == "600000"
    assert tif._ts_code_to_ticker("bad") is None
    assert tif._ts_code_to_ticker("") is None
    assert tif._ts_code_to_ticker("12345.SH") is None
    assert tif._ts_code_to_ticker(None) is None


def test_ticker_to_ts_code_variants(monkeypatch) -> None:
    assert tif._ticker_to_ts_code("600000") == "600000.SH"
    assert tif._ticker_to_ts_code("000001") == "000001.SZ"
    assert tif._ticker_to_ts_code("") == ".SZ"


def test_side_label_variants(monkeypatch) -> None:
    assert tif._side_label("0") == "buy"
    assert tif._side_label("买入") == "buy"
    assert tif._side_label("BUY") == "buy"
    assert tif._side_label("1") == "sell"
    assert tif._side_label("卖出") == "sell"
    assert tif._side_label(None) == "buy"
    assert tif._side_label("中间值") == "中间值"


def test_tushare_trade_date_variants(monkeypatch) -> None:
    assert tif._tushare_trade_date("2026-08-07") == "20260807"
    assert tif._tushare_trade_date("20260807") == "20260807"
    assert tif._tushare_trade_date(None) == ""


def test_df_records_variants(monkeypatch) -> None:
    assert tif._df_records(None) == []

    class FakeDf:
        def __init__(self, records):
            self._records = records

        def to_dict(self, orient):
            return self._records

    assert tif._df_records(FakeDf([{"a": 1}])) == [{"a": 1}]
    assert tif._df_records([{"a": 1}, "junk"]) == [{"a": 1}]
    assert tif._df_records("plain-string") == []


def test_latest_cn_trade_date_no_open_dates(monkeypatch) -> None:
    monkeypatch.setattr(tif, "get_open_dates", lambda **kw: [])
    assert tif._latest_cn_trade_date_yyyymmdd() is None


def test_missing_summary_codes(monkeypatch) -> None:
    monkeypatch.setattr(tif, "fetch_summaries_for_codes", lambda codes, trade_date: {"600000.SH"})
    assert tif._missing_summary_codes(["600000.SH", "000001.SZ"], trade_date_iso="2026-08-07") == ["000001.SZ"]


def test_sync_no_trade_date(monkeypatch) -> None:
    monkeypatch.setattr(tif, "ensure_table", lambda: None)
    monkeypatch.setattr(tif, "_latest_cn_trade_date_yyyymmdd", lambda: None)
    out = tif.sync_top_inst_watchlist()
    assert out["ok"] is False
    assert out["error"] == "no_trade_date"
    assert out["jobType"] == tif.JOB_TYPE


def test_watchlist_ts_codes_mixed_symbols(monkeypatch) -> None:
    monkeypatch.setattr(
        tif,
        "list_registry",
        lambda: [
            {"symbol": "CN:600000"},
            {"symbol": "CN:600000"},
            {"symbol": "CN:000001"},
            {"symbol": "HK:00700"},
            {"symbol": "CN:bad!"},
            {"symbol": ""},
        ],
    )
    assert tif._watchlist_ts_codes() == ["600000.SH", "000001.SZ"]


def test_sync_eastmoney_bundle_success_path(monkeypatch) -> None:
    monkeypatch.setattr(tif, "ensure_table", lambda: None)
    monkeypatch.setattr(tif, "_latest_cn_trade_date_yyyymmdd", lambda: "20260807")
    monkeypatch.setattr(tif, "is_trading_day", lambda exchange, cal_date: True)
    monkeypatch.setattr(tif, "_watchlist_ts_codes", lambda: ["600000.SH"])
    monkeypatch.setattr(tif, "get_today_run", lambda job: None)
    monkeypatch.setattr(
        tif,
        "fetch_top_inst_provider_result",
        lambda td: (
            tif.TopInstProviderResult(
                source="eastmoney",
                lhb_tickers={"600000"},
                org_by_ticker={"600000": {"NET_BUY_AMT": 200000000.0, "EXPLANATION": "r"}},
                lhb_count=1,
                org_trade_count=1,
            ),
            [],
        ),
    )
    monkeypatch.setattr(
        tif,
        "fetch_em_seat_bundles_parallel",
        lambda ts_codes, trade_date_iso: {
            "600000.SH": tif.EastMoneySeatBundle(
                buy_seats=[{"exalter": "拉萨团结路", "buy": 1.0}],
                inst_seats=[
                    {"exalter": "机构专用", "buy": 1.0, "sell": 0.0, "net_buy": 1.0, "side": "buy", "reason": None}
                ],
            )
        },
    )
    monkeypatch.setattr(tif, "upsert_daily_rows", lambda rows: len(rows))
    monkeypatch.setattr(tif, "upsert_summary_rows", lambda rows: len(rows))
    monkeypatch.setattr(tif, "insert_record", lambda **kw: None)

    out = tif.sync_top_inst_watchlist()
    assert out["ok"] is True
    assert out["source"] == "eastmoney"
    assert out["onBoardCount"] == 1
    assert out["seatFetchFailures"] == 0
    assert out["dailyRows"] == 1
