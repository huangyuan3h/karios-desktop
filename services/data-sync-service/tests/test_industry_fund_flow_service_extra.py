"""service/industry_fund_flow.py coverage."""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import Mock

import pytest

from data_sync_service.service import industry_fund_flow as iff


class TestParse:
    def test_parse_money(self) -> None:
        assert iff._parse_money_to_cny(None) == 0.0
        assert iff._parse_money_to_cny(5) == 5.0
        assert iff._parse_money_to_cny(float("nan")) == 0.0
        assert iff._parse_money_to_cny("") == 0.0
        assert iff._parse_money_to_cny("—") == 0.0
        assert iff._parse_money_to_cny("N/A") == 0.0
        assert iff._parse_money_to_cny("1.5亿") == 1.5e8
        assert iff._parse_money_to_cny("3,200万") == 3.2e7
        assert iff._parse_money_to_cny("-123.4") == -123.4
        assert iff._parse_money_to_cny("+5") == 5.0
        assert iff._parse_money_to_cny("abc") == 0.0

    def test_stable_code(self) -> None:
        assert iff._now_iso().endswith("+00:00")
        assert iff._stable_industry_code("") == ""
        c = iff._stable_industry_code("半导体")
        assert len(c) == 12 and c == iff._stable_industry_code("半导体")

    def test_with_retry(self, monkeypatch) -> None:
        calls = []
        flaky = Mock(side_effect=[ValueError("x"), 42])
        monkeypatch.setattr(iff.time, "sleep", lambda s: calls.append(s))
        assert iff._with_retry(flaky, tries=3) == 42
        assert len(calls) == 1
        always = Mock(side_effect=ValueError("boom"))
        with pytest.raises(ValueError):
            iff._with_retry(always, tries=2, base_sleep_s=0.01)


class TestDataApi:
    def test_getbkzj_ok(self, monkeypatch) -> None:
        class Resp:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return None

            def read(self):
                return json.dumps({"data": {"diff": [{"f14": "半导体", "f62": "1亿"}]}}).encode()

        monkeypatch.setattr(iff.urllib.request, "urlopen", lambda req, timeout: Resp())
        out = iff._dataapi_getbkzj("f62", "m:90 t:2")
        assert out[0]["f14"] == "半导体"

    def test_getbkzj_bad(self, monkeypatch) -> None:
        class Resp:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return None

            def read(self):
                return b"not json"

        monkeypatch.setattr(iff.urllib.request, "urlopen", lambda req, timeout: Resp())
        with pytest.raises(json.JSONDecodeError):
            iff._dataapi_getbkzj("k", "c")
        monkeypatch.setattr(iff.urllib.request, "urlopen", lambda req, timeout: (_ for _ in ()).throw(OSError("conn")))
        with pytest.raises(OSError):
            iff._dataapi_getbkzj("k", "c")


class TestDayKline:
    def test_ok(self, monkeypatch) -> None:
        class Resp:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return None

            def read(self):
                return json.dumps({"data": {"klines": ["2026-08-07,1.2亿,3,4", "2026-08-06,5000万,1,2"]}}).encode()

        monkeypatch.setattr(iff.urllib.request, "urlopen", lambda req, timeout: Resp())
        out = iff._eastmoney_board_fund_flow_daykline(secid="90.BK0475")
        assert out[0]["date"] == "2026-08-07"
        assert out[0]["net_inflow"] == pytest.approx(1.2e8)

    def test_bad_klines(self, monkeypatch) -> None:
        class Resp:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return None

            def read(self):
                return json.dumps({"data": {"klines": ["", "a", "2026-08-07,5,6", "2026-08-06"]}}).encode()

        monkeypatch.setattr(iff.urllib.request, "urlopen", lambda req, timeout: Resp())
        out = iff._eastmoney_board_fund_flow_daykline(secid="90.x")
        assert len(out) == 1 and out[0]["net_inflow"] == 5.0

    def test_no_data(self, monkeypatch) -> None:
        class Resp:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return None

            def read(self):
                return json.dumps({"data": None}).encode()

        monkeypatch.setattr(iff.urllib.request, "urlopen", lambda req, timeout: Resp())
        assert iff._eastmoney_board_fund_flow_daykline(secid="x") == []


class TestAkHist:
    def test_darwin(self, monkeypatch) -> None:
        monkeypatch.setattr(iff.sys, "platform", "darwin")

        with pytest.raises(RuntimeError, match="darwin"):
            iff._try_akshare_hist("半导体", days=5)

    def test_import_fail(self, monkeypatch) -> None:
        monkeypatch.setattr(iff.sys, "platform", "linux")
        monkeypatch.setitem(__import__("sys").modules, "akshare", None)
        with pytest.raises(RuntimeError, match="AkShare is required"):
            iff._try_akshare_hist("半导体", days=5)

    def test_missing_attr(self, monkeypatch) -> None:
        monkeypatch.setattr(iff.sys, "platform", "linux")
        ak = Mock()
        del ak.stock_sector_fund_flow_hist
        monkeypatch.setitem(__import__("sys").modules, "akshare", ak)
        with pytest.raises(RuntimeError, match="upgrade"):
            iff._try_akshare_hist("半导体", days=5)

    def test_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(iff.sys, "platform", "linux")
        df = Mock()
        df.to_dict.return_value = [
            {"日期": "2026-08-07", "主力净流入-净额": "1亿"},
            {"date": "2026-08-06", "主力净流入": "2000万"},
            {"date": "", "净流入": "0"},
            {"date": "2026-08-05"},
        ]
        ak = Mock()
        ak.stock_sector_fund_flow_hist.return_value = df
        monkeypatch.setitem(__import__("sys").modules, "akshare", ak)
        out = iff._try_akshare_hist("半导体", days=2)
        assert len(out) == 2
        assert out[0]["date"] == "2026-08-06" and out[0]["net_inflow"] == 2e7


class TestEod:
    def test_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(iff, "_with_retry", lambda fn, tries: [{"行业名称": "半导体", "行业代码": "BK0475", "今日主力净流入-净额": "5亿"}])
        monkeypatch.setattr(iff, "classify_sw_l1_industry", lambda name, row: {"is_allowed": True, "industry_name": "电子", "taxonomy": "SW", "industry_level": 1})
        out = iff.fetch_cn_industry_fund_flow_eod(date(2026, 8, 7))
        assert out[0]["industry_name"] == "电子"
        assert out[0]["industry_code"] == "BK0475"
        assert out[0]["net_inflow"] == pytest.approx(5e8)

    def test_fallback_code_and_fields(self, monkeypatch) -> None:
        monkeypatch.setattr(iff, "_with_retry", lambda fn, tries: [
            {"板块": "通信设备", "f12": "BK123", "f62": "1.5亿"},
            {"名称": "银行", "今日主力净流入": "-2亿"},
            {"行业名称": "非允许", "代码": ""},
        ])
        monkeypatch.setattr(iff, "classify_sw_l1_industry", lambda name, row: {"is_allowed": name != "非允许", "industry_name": "" if name == "银行" else name, "taxonomy": "SW", "industry_level": 1})
        out = iff.fetch_cn_industry_fund_flow_eod(date(2026, 8, 7))
        assert out[0]["industry_code"] == "BK123" and out[0]["net_inflow"] == 1.5e8
        assert len(out) == 1

    def test_fetch_error(self, monkeypatch) -> None:
        monkeypatch.setattr(iff, "_with_retry", lambda fn, tries: (_ for _ in ()).throw(RuntimeError("x")))
        assert iff.fetch_cn_industry_fund_flow_eod(date(2026, 8, 7)) == []


class TestHist:
    def test_with_code(self, monkeypatch) -> None:
        monkeypatch.setattr(iff, "_with_retry", lambda fn, tries: [{"date": "2026-08-07", "net_inflow": 1.0}, {"date": "2026-08-06", "net_inflow": 2.0}])
        out = iff.fetch_cn_industry_fund_flow_hist("半导体", industry_code="BK0475", days=10)
        assert len(out) == 2
        out2 = iff.fetch_cn_industry_fund_flow_hist("半导体", industry_code="90.90.xxx", days=10)
        assert len(out2) == 2

    def test_code_fallback_error(self, monkeypatch) -> None:
        monkeypatch.setattr(iff, "_with_retry", lambda fn, tries: (_ for _ in ()).throw(RuntimeError("x")))
        monkeypatch.setattr(iff, "_try_akshare_hist", lambda name, days: [{"date": "d", "net_inflow": 3.0}])
        out = iff.fetch_cn_industry_fund_flow_hist("半导体", industry_code="BK0475", days=10)
        assert out[0]["net_inflow"] == 3.0
        monkeypatch.setattr(iff, "_try_akshare_hist", lambda name, days: (_ for _ in ()).throw(RuntimeError("akfail")))
        with pytest.raises(RuntimeError):
            iff.fetch_cn_industry_fund_flow_hist("半导体", industry_code="BK0475", days=10)

    def test_no_code_no_name(self, monkeypatch) -> None:
        assert iff.fetch_cn_industry_fund_flow_hist("", days=10) == []

    def test_hist_rows_for_top_row(self, monkeypatch) -> None:
        monkeypatch.setattr(iff, "fetch_cn_industry_fund_flow_hist", lambda name, industry_code, days: [{"date": "d", "net_inflow": 7.0, "raw": {"k": 1}}])
        out = iff._hist_rows_for_top_row({"industry_name": "半导体", "industry_code": "BK1", "taxonomy": "SW", "industry_level": 2, "source": "src"}, days=5, updated_at="now")
        assert out[0]["industry_code"] == "BK1"
        assert out[0]["net_inflow"] == 7.0
        monkeypatch.setattr(iff, "fetch_cn_industry_fund_flow_hist", lambda name, industry_code, days: [])
        assert iff._hist_rows_for_top_row({}, days=5, updated_at="now") == []


class TestResolveAsOf:
    def test_trading_day(self, monkeypatch) -> None:
        monkeypatch.setattr(iff, "is_cn_trading_day", lambda d: True)
        assert iff._resolve_sync_as_of(today=date(2026, 8, 7), force=False) == (date(2026, 8, 7), None)

    def test_no_calendar(self, monkeypatch) -> None:
        monkeypatch.setattr(iff, "is_cn_trading_day", lambda d: False)
        monkeypatch.setattr(iff, "last_open_date_on_or_before", lambda d: None)
        as_of, skip = iff._resolve_sync_as_of(today=date(2026, 8, 8), force=False)
        assert as_of is None and skip["ok"] is False

    def test_skip_catchup(self, monkeypatch) -> None:
        monkeypatch.setattr(iff, "is_cn_trading_day", lambda d: False)
        monkeypatch.setattr(iff, "last_open_date_on_or_before", lambda d: date(2026, 8, 7))
        monkeypatch.setattr(iff, "get_latest_date", lambda: "2026-08-07")
        as_of, skip = iff._resolve_sync_as_of(today=date(2026, 8, 8), force=False)
        assert as_of is None and skip["ok"] is True and skip["skipped"] is True
        as_of, skip = iff._resolve_sync_as_of(today=date(2026, 8, 8), force=True)
        assert as_of == date(2026, 8, 7)
        monkeypatch.setattr(iff, "get_latest_date", lambda: "2026-08-01")
        as_of, skip = iff._resolve_sync_as_of(today=date(2026, 8, 8), force=False)
        assert as_of == date(2026, 8, 7)


class TestSync:
    def test_sync(self, monkeypatch) -> None:
        monkeypatch.setattr(iff, "shanghai_today", lambda: date(2026, 8, 7))
        monkeypatch.setattr(iff, "_resolve_sync_as_of", lambda **kw: (date(2026, 8, 7), None))
        monkeypatch.setattr(iff, "fetch_cn_industry_fund_flow_eod", lambda as_of: [
            {"date": "2026-08-07", "industry_code": "BK1", "industry_name": "电子", "net_inflow": 1.0, "taxonomy": "SW", "industry_level": 1, "source": "s"},
            {"date": "2026-08-07", "industry_code": "BK2", "industry_name": "银行", "net_inflow": -1.0, "taxonomy": "SW", "industry_level": 1, "source": "s"},
            {"date": "2026-08-07", "industry_code": "BK3", "industry_name": "非SW", "net_inflow": 5.0, "taxonomy": "SW", "industry_level": 1, "source": "s"},
        ])
        monkeypatch.setattr(iff, "row_is_sw_l1", lambda it: it["industry_name"] != "非SW")
        monkeypatch.setattr(iff, "_now_iso", lambda: "now")
        monkeypatch.setattr(iff, "upsert_daily_rows", lambda rows: None)
        monkeypatch.setattr(iff, "_hist_rows_for_top_row", lambda r, **kw: [{"date": "d", "industry_code": r["industry_code"], "industry_name": r["industry_name"], "net_inflow": 2.0, "updated_at": "now"}])
        out = iff.sync_cn_industry_fund_flow(days=10, top_n=10, force=True)
        assert out["ok"] is True
        assert out["rows"] == 2
        assert out["filteredRows"] == 1
        assert out["histRows"] == 2
        assert out["histFailures"] == 0

    def test_sync_catchup(self, monkeypatch) -> None:
        monkeypatch.setattr(iff, "shanghai_today", lambda: date(2026, 8, 8))
        monkeypatch.setattr(iff, "_resolve_sync_as_of", lambda **kw: (date(2026, 8, 7), None))
        monkeypatch.setattr(iff, "fetch_cn_industry_fund_flow_eod", lambda as_of: [])
        monkeypatch.setattr(iff, "row_is_sw_l1", lambda it: True)
        monkeypatch.setattr(iff, "_now_iso", lambda: "now")
        monkeypatch.setattr(iff, "upsert_daily_rows", lambda rows: None)
        out = iff.sync_cn_industry_fund_flow(force=True)
        assert out["ok"] is True and out["catchup"] is True

    def test_sync_hist_failure(self, monkeypatch) -> None:
        monkeypatch.setattr(iff, "shanghai_today", lambda: date(2026, 8, 7))
        monkeypatch.setattr(iff, "_resolve_sync_as_of", lambda **kw: (date(2026, 8, 7), None))
        monkeypatch.setattr(iff, "fetch_cn_industry_fund_flow_eod", lambda as_of: [{"date": "d", "industry_code": "BK1", "industry_name": "电子", "net_inflow": 1.0, "taxonomy": "SW", "industry_level": 1, "source": "s"}])
        monkeypatch.setattr(iff, "row_is_sw_l1", lambda it: True)
        monkeypatch.setattr(iff, "_now_iso", lambda: "now")
        monkeypatch.setattr(iff, "upsert_daily_rows", lambda rows: None)
        monkeypatch.setattr(iff, "_hist_rows_for_top_row", lambda r, **kw: (_ for _ in ()).throw(RuntimeError("boom")))
        out = iff.sync_cn_industry_fund_flow(force=True)
        assert out["histFailures"] == 1

    def test_sync_skip(self, monkeypatch) -> None:
        monkeypatch.setattr(iff, "shanghai_today", lambda: date(2026, 8, 8))
        monkeypatch.setattr(iff, "_resolve_sync_as_of", lambda **kw: (None, {"ok": True, "skipped": True}))
        out = iff.sync_cn_industry_fund_flow(force=False)
        assert out["skipped"] is True


class TestGet:
    def test_get(self, monkeypatch) -> None:
        monkeypatch.setattr(iff, "get_latest_date", lambda: "2026-08-07")
        monkeypatch.setattr(iff, "resolve_effective_as_of", lambda d: d)
        monkeypatch.setattr(iff, "trade_dates_upto", lambda *a, **k: ["2026-08-07"])
        monkeypatch.setattr(iff, "get_top_rows", lambda d, n: [{"industry_code": "BK1", "industry_name": "电子", "net_inflow": 1.0, "taxonomy": "SW", "industry_level": 1, "source": "s"}])
        monkeypatch.setattr(iff, "get_rows_for_dates", lambda dates: [{"date": "2026-08-07", "industry_name": "电子", "net_inflow": 2.0}])
        monkeypatch.setattr(iff, "series_map_from_rows", lambda rows, dates: {"电子": [{"date": "2026-08-07", "net_inflow": 2.0}]})
        out = iff.get_cn_industry_fund_flow(days=10, top_n=30)
        assert out["top"][0]["industryCode"] == "BK1"
        assert out["top"][0]["sum10d"] == 2.0

    def test_get_empty(self, monkeypatch) -> None:
        monkeypatch.setattr(iff, "get_latest_date", lambda: "")
        monkeypatch.setattr(iff, "resolve_effective_as_of", lambda d: "")
        assert iff.get_cn_industry_fund_flow() == {"asOfDate": "", "days": 10, "topN": 30, "dates": [], "top": []}
        monkeypatch.setattr(iff, "resolve_effective_as_of", lambda d: "2026-08-07")
        out = iff.get_cn_industry_fund_flow(as_of_date="2026-08-07")
        assert out["asOfDate"] == "2026-08-07"
