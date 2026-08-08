"""service/eastmoney_industry.py coverage."""

from __future__ import annotations

import json
from unittest.mock import Mock

import pytest

from data_sync_service.service import eastmoney_industry as ei


class TestSecid:
    def test_now_iso(self) -> None:
        assert ei._now_iso().endswith("+00:00")

    def test_ts_code_to_secid(self) -> None:
        assert ei._ts_code_to_secid("600519.SH") == "1.600519"
        assert ei._ts_code_to_secid("000001.SZ") == "0.000001"
        assert ei._ts_code_to_secid("600519.sz") == "0.600519"
        assert ei._ts_code_to_secid("bad") is None
        assert ei._ts_code_to_secid("600519") is None
        assert ei._ts_code_to_secid("60051.SH") is None
        assert ei._ts_code_to_secid("abc.SH") is None

    def test_symbol_to_ts_code(self, monkeypatch) -> None:
        monkeypatch.setattr("data_sync_service.service.market_quotes.normalize_market_symbol", lambda s: f"CN:{s}" if s.startswith("6") else s)
        assert ei._symbol_to_ts_code("600519") == "600519.SH"
        monkeypatch.setattr("data_sync_service.service.market_quotes.normalize_market_symbol", lambda s: "X")
        assert ei._symbol_to_ts_code("600519") is None
        monkeypatch.setattr("data_sync_service.service.market_quotes.normalize_market_symbol", lambda s: "CN:abc")
        assert ei._symbol_to_ts_code("600519") is None
        monkeypatch.setattr("data_sync_service.service.market_quotes.normalize_market_symbol", lambda s: "CN:12345")
        assert ei._symbol_to_ts_code("600519") is None
        monkeypatch.setattr("data_sync_service.service.market_quotes.normalize_market_symbol", lambda s: "CN:000001")
        assert ei._symbol_to_ts_code("600519") == "000001.SZ"


class TestEm2016:
    def test_em2016_to_board_name(self) -> None:
        assert ei._em2016_to_board_name("医药生物-化学制药-化学制剂") == "化学制药"
        assert ei._em2016_to_board_name("食品饮料") == "食品饮料"
        assert ei._em2016_to_board_name("") is None
        assert ei._em2016_to_board_name("  ") is None
        assert ei._em2016_to_board_name("A-B") == "B"


class TestPush2:
    def test_push2_get_label(self, monkeypatch) -> None:
        class Resp:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return None

            def read(self):
                return json.dumps({"data": {"f127": "白酒"}}).encode()

        monkeypatch.setattr(ei.urllib.request, "urlopen", lambda req, timeout: Resp())
        assert ei._push2_get_label("https://x", "600519.SH") == "白酒"
        monkeypatch.setattr(ei.urllib.request, "urlopen", lambda req, timeout: (_ for _ in ()).throw(OSError("down")))
        with pytest.raises(OSError):
            ei._push2_get_label("https://x", "600519.SH")

    def test_push2_get_label_bad(self, monkeypatch) -> None:
        class Resp:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return None

            def read(self):
                return json.dumps({"data": None}).encode()

        monkeypatch.setattr(ei.urllib.request, "urlopen", lambda req, timeout: Resp())
        assert ei._push2_get_label("https://x", "600519.SH") is None

    def test_fetch_push2(self, monkeypatch) -> None:
        monkeypatch.setattr(ei, "_push2_get_label", lambda url, code: "半导体")
        assert ei._fetch_em_industry_push2("600519.SH") == "半导体"
        monkeypatch.setattr(ei, "_push2_get_label", lambda url, code: (_ for _ in ()).throw(RuntimeError("x")))
        assert ei._fetch_em_industry_push2("600519.SH") is None
        assert ei._fetch_em_industry_push2("bad") is None
        assert ei._fetch_em_industry_push2delay("bad") is None
        assert ei._fetch_em_industry_push2delay("600519.SH") is None
        monkeypatch.setattr(ei, "_push2_get_label", lambda url, code: "银行")
        assert ei._fetch_em_industry_push2delay("600519.SH") == "银行"


class TestEmweb:
    def test_fetch_emweb(self, monkeypatch) -> None:
        class Resp:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return None

            def read(self):
                return json.dumps({"jbzl": [{"EM2016": "医药生物-化学制药"}]}).encode()

        monkeypatch.setattr(ei.urllib.request, "urlopen", lambda req, timeout: Resp())
        assert ei._fetch_em_industry_emweb("600519.SH") == "化学制药"

    def test_fetch_emweb_paths(self, monkeypatch) -> None:
        monkeypatch.setattr(ei.urllib.request, "urlopen", lambda req, timeout: (_ for _ in ()).throw(OSError("down")))
        assert ei._fetch_em_industry_emweb("600519.SH") is None
        assert ei._fetch_em_industry_emweb("bad") is None
        assert ei._fetch_em_industry_emweb("60051.SH") is None

    def test_fetch_emweb_bad_json(self, monkeypatch) -> None:
        class Resp:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return None

            def read(self):
                return b"not json"

        monkeypatch.setattr(ei.urllib.request, "urlopen", lambda req, timeout: Resp())
        assert ei._fetch_em_industry_emweb("600519.SH") is None

    def test_fetch_emweb_bad_payload(self, monkeypatch) -> None:
        class Resp:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return None

            def read(self):
                return json.dumps({"jbzl": []}).encode()

        monkeypatch.setattr(ei.urllib.request, "urlopen", lambda req, timeout: Resp())
        assert ei._fetch_em_industry_emweb("600519.SH") is None

    def test_fetch_emweb_non_dict_row(self, monkeypatch) -> None:
        class Resp:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return None

            def read(self):
                return json.dumps({"jbzl": ["not-a-dict"]}).encode()

        monkeypatch.setattr(ei.urllib.request, "urlopen", lambda req, timeout: Resp())
        assert ei._fetch_em_industry_emweb("600519.SH") is None

    def test_fetch_chain(self, monkeypatch) -> None:
        monkeypatch.setattr(ei, "_fetch_em_industry_push2", lambda c: None)
        monkeypatch.setattr(ei, "_fetch_em_industry_push2delay", lambda c: None)
        monkeypatch.setattr(ei, "_fetch_em_industry_emweb", lambda c: "白酒")
        assert ei._fetch_em_industry_for_ts_code("600519.SH") == "白酒"
        monkeypatch.setattr(ei, "_fetch_em_industry_emweb", lambda c: None)
        assert ei._fetch_em_industry_for_ts_code("600519.SH") is None


class TestFetchCodes:
    def test_fetch(self, monkeypatch) -> None:
        monkeypatch.setattr(ei, "_fetch_em_industry_for_ts_code", lambda c: "白酒" if c == "600519.SH" else None)
        monkeypatch.setattr(ei.time, "sleep", lambda s: None)
        out = ei.fetch_em_industries_for_ts_codes(["600519.SH", "000001.SZ", "  ", None])
        assert out == {"600519.SH": "白酒"}
        monkeypatch.setattr(ei, "_fetch_em_industry_for_ts_code", lambda c: (_ for _ in ()).throw(RuntimeError("x")))
        assert ei.fetch_em_industries_for_ts_codes(["600519.SH"]) == {}


class TestListCodes:
    def test_list_cn_ts_codes(self, monkeypatch) -> None:
        class Cur:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return None

            def execute(self, sql, params):
                return self

            def fetchall(self):
                return [("600519.SH",), ("000001.SZ",), (None,), ("",)]

        class Conn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return None

            def cursor(self):
                return Cur()

        monkeypatch.setattr(ei, "ensure_stock_basic", lambda: None)
        monkeypatch.setattr("data_sync_service.db.get_connection", lambda: Conn())
        assert ei._list_cn_ts_codes() == ["600519.SH", "000001.SZ"]
        assert ei._list_cn_ts_codes(limit=0) == ["600519.SH", "000001.SZ"]
        assert ei._list_cn_ts_codes(limit=1) == ["600519.SH", "000001.SZ"]


class TestCoverage:
    def test_result_with_coverage(self, monkeypatch) -> None:
        monkeypatch.setattr(ei, "coverage_stats", lambda: {"totalCnStocks": 100, "emMapped": 50, "missingCount": 50})
        monkeypatch.setattr(ei, "count_rows", lambda: 50)
        out = ei._result_with_coverage(ok=True)
        assert out["coveragePct"] == 50.0 and out["totalInDb"] == 50
        monkeypatch.setattr(ei, "coverage_stats", lambda: {"totalCnStocks": 0, "emMapped": 0, "missingCount": 0})
        assert ei._result_with_coverage(ok=True)["coveragePct"] == 0.0

    def test_resume(self, monkeypatch) -> None:
        monkeypatch.setattr(ei, "get_today_run", lambda j: None)
        assert ei._resume_after_ts_code() is None
        monkeypatch.setattr(ei, "get_today_run", lambda j: {"success": False, "last_ts_code": "600519.SH"})
        assert ei._resume_after_ts_code() == "600519.SH"
        monkeypatch.setattr(ei, "get_today_run", lambda j: {"success": True})
        assert ei._resume_after_ts_code() is None


class TestSyncIncremental:
    def test_skip_no_codes(self, monkeypatch) -> None:
        monkeypatch.setattr(ei, "_resume_after_ts_code", lambda: None)
        monkeypatch.setattr(ei, "list_missing_cn_ts_codes", lambda **kw: [])
        monkeypatch.setattr(ei, "_now_iso", lambda: "now")
        monkeypatch.setattr(ei, "coverage_stats", lambda: {"totalCnStocks": 0, "emMapped": 0, "missingCount": 0})
        monkeypatch.setattr(ei, "count_rows", lambda: 0)
        out = ei.sync_eastmoney_industry_incremental()
        assert out["skipped"] is True and out["message"] == "no codes to sync"

    def test_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(ei, "_resume_after_ts_code", lambda: None)
        monkeypatch.setattr(ei, "list_missing_cn_ts_codes", lambda **kw: ["600519.SH", "000001.SZ"])
        monkeypatch.setattr(ei, "fetch_em_industries_for_ts_codes", lambda codes, sleep_s: {"600519.SH": "白酒"})
        seen = {}
        monkeypatch.setattr(ei, "upsert_rows", lambda rows: (seen.update(rows=rows) or 1))
        monkeypatch.setattr(ei, "insert_record", lambda **kw: None)
        monkeypatch.setattr(ei, "_now_iso", lambda: "now")
        monkeypatch.setattr(ei, "coverage_stats", lambda: {"totalCnStocks": 2, "emMapped": 1, "missingCount": 1})
        monkeypatch.setattr(ei, "count_rows", lambda: 1)
        out = ei.sync_eastmoney_industry_incremental()
        assert out["ok"] is True
        assert out["requested"] == 2 and out["resolved"] == 1
        assert seen["rows"][0]["industry_name"] == "白酒"

    def test_empty_batch_failed(self, monkeypatch) -> None:
        monkeypatch.setattr(ei, "_resume_after_ts_code", lambda: None)
        monkeypatch.setattr(ei, "list_missing_cn_ts_codes", lambda **kw: ["600519.SH"])
        monkeypatch.setattr(ei, "fetch_em_industries_for_ts_codes", lambda codes, sleep_s: {})
        seen = {}
        monkeypatch.setattr(ei, "upsert_rows", lambda rows: 0)
        monkeypatch.setattr(ei, "insert_record", lambda **kw: seen.update(kw))
        monkeypatch.setattr(ei, "_now_iso", lambda: "now")
        monkeypatch.setattr(ei, "coverage_stats", lambda: {"totalCnStocks": 2, "emMapped": 0, "missingCount": 2})
        monkeypatch.setattr(ei, "count_rows", lambda: 0)
        out = ei.sync_eastmoney_industry_incremental()
        assert out["ok"] is False
        assert seen["success"] is False
        assert "no industry resolved" in seen["error_message"]

    def test_exception(self, monkeypatch) -> None:
        monkeypatch.setattr(ei, "_resume_after_ts_code", lambda: None)
        monkeypatch.setattr(ei, "list_missing_cn_ts_codes", lambda **kw: ["600519.SH"])
        monkeypatch.setattr(ei, "fetch_em_industries_for_ts_codes", lambda codes, sleep_s: (_ for _ in ()).throw(RuntimeError("boom")))
        monkeypatch.setattr(ei, "insert_record", lambda **kw: None)
        monkeypatch.setattr(ei, "_now_iso", lambda: "now")
        monkeypatch.setattr(ei, "coverage_stats", lambda: {"totalCnStocks": 2, "emMapped": 0, "missingCount": 2})
        monkeypatch.setattr(ei, "count_rows", lambda: 0)
        out = ei.sync_eastmoney_industry_incremental()
        assert out["ok"] is False and out["error"] == "boom"

    def test_stale_mode_second_batch_break(self, monkeypatch) -> None:
        calls = {"n": 0}
        def stale(**kw):
            calls["n"] += 1
            return ["600519.SH"] if calls["n"] == 1 else []

        monkeypatch.setattr(ei, "_resume_after_ts_code", lambda: None)
        monkeypatch.setattr(ei, "list_stale_cn_ts_codes", stale)
        monkeypatch.setattr(ei, "fetch_em_industries_for_ts_codes", lambda codes, sleep_s: {"600519.SH": "白酒"})
        monkeypatch.setattr(ei, "upsert_rows", lambda rows: 1)
        monkeypatch.setattr(ei, "insert_record", lambda **kw: None)
        monkeypatch.setattr(ei, "_now_iso", lambda: "now")
        monkeypatch.setattr(ei, "coverage_stats", lambda: {"totalCnStocks": 2, "emMapped": 1, "missingCount": 1})
        monkeypatch.setattr(ei, "count_rows", lambda: 1)
        out = ei.sync_eastmoney_industry_incremental(mode="stale", max_batches=2)
        assert out["ok"] is True and out["batchesRun"] == 1
        assert calls["n"] == 2


class TestSyncFull:
    def test_with_symbols(self, monkeypatch) -> None:
        monkeypatch.setattr(ei, "_symbol_to_ts_code", lambda s: "600519.SH" if s == "600519" else None)
        monkeypatch.setattr(ei, "fetch_em_industries_for_ts_codes", lambda codes, sleep_s: {"600519.SH": "白酒"})
        seen = {}
        monkeypatch.setattr(ei, "upsert_rows", lambda rows: (seen.update(rows=rows) or 1))
        monkeypatch.setattr(ei, "_now_iso", lambda: "now")
        monkeypatch.setattr(ei, "coverage_stats", lambda: {"totalCnStocks": 1, "emMapped": 1, "missingCount": 0})
        monkeypatch.setattr(ei, "count_rows", lambda: 1)
        out = ei.sync_eastmoney_industry(symbols=["600519", "bad"])
        assert out["ok"] is True and out["requested"] == 1

    def test_no_symbols(self, monkeypatch) -> None:
        monkeypatch.setattr(ei, "_list_cn_ts_codes", lambda **kw: ["600519.SH"])
        monkeypatch.setattr(ei, "fetch_em_industries_for_ts_codes", lambda codes, sleep_s: {})
        monkeypatch.setattr(ei, "upsert_rows", lambda rows: 0)
        monkeypatch.setattr(ei, "_now_iso", lambda: "now")
        monkeypatch.setattr(ei, "coverage_stats", lambda: {"totalCnStocks": 1, "emMapped": 0, "missingCount": 1})
        monkeypatch.setattr(ei, "count_rows", lambda: 0)
        out = ei.sync_eastmoney_industry()
        assert out["ok"] is True and out["resolved"] == 0

    def test_no_ts_codes(self, monkeypatch) -> None:
        monkeypatch.setattr(ei, "_list_cn_ts_codes", lambda **kw: [])
        monkeypatch.setattr(ei, "_now_iso", lambda: "now")
        monkeypatch.setattr(ei, "coverage_stats", lambda: {"totalCnStocks": 0, "emMapped": 0, "missingCount": 0})
        monkeypatch.setattr(ei, "count_rows", lambda: 0)
        out = ei.sync_eastmoney_industry()
        assert out["ok"] is False and out["error"] == "no_ts_codes"


class TestLookup:
    def test_lookup(self, monkeypatch) -> None:
        monkeypatch.setattr(ei, "lookup_by_ts_codes", lambda codes: {"600519.SH": "白酒"})
        assert ei.lookup_em_industries_for_ts_codes(["600519.SH", " "]) == {"600519.SH": "白酒"}
        assert ei.lookup_em_industries_for_ts_codes([]) == {}
        assert ei.lookup_em_industries_for_ts_codes([" "]) == {}

    def test_sync_missing(self, monkeypatch) -> None:
        monkeypatch.setattr(ei, "lookup_by_ts_codes", lambda codes: {"600519.SH": "白酒"})
        monkeypatch.setattr(ei, "fetch_em_industries_for_ts_codes", lambda codes, sleep_s: {"000001.SZ": "银行"})
        seen = {}
        monkeypatch.setattr(ei, "upsert_rows", lambda rows: (seen.update(rows=rows) or 1))
        monkeypatch.setattr(ei, "_now_iso", lambda: "now")
        ei._sync_missing_em_industries(["600519.SH", "000001.SZ"])
        assert seen["rows"][0]["ts_code"] == "000001.SZ"
        ei._sync_missing_em_industries([])
        monkeypatch.setattr(ei, "lookup_by_ts_codes", lambda codes: {"600519.SH": "白酒", "000001.SZ": "银行"})
        ei._sync_missing_em_industries(["600519.SH"])
        monkeypatch.setattr(ei, "lookup_by_ts_codes", lambda codes: {})
        monkeypatch.setattr(ei, "fetch_em_industries_for_ts_codes", lambda codes, sleep_s: {})
        ei._sync_missing_em_industries(["600519.SH"])

    def test_ensure_deprecated(self, monkeypatch) -> None:
        seen = []
        monkeypatch.setattr(ei, "_sync_missing_em_industries", lambda codes: seen.append(codes))
        with pytest.warns(DeprecationWarning):
            ei.ensure_em_industries_for_ts_codes(["600519.SH"])
        assert seen == [["600519.SH"]]

    def test_sync_status(self, monkeypatch) -> None:
        monkeypatch.setattr(ei, "coverage_stats", lambda: {"totalCnStocks": 100, "emMapped": 80, "missingCount": 20})
        monkeypatch.setattr(ei, "count_rows", lambda: 80)
        monkeypatch.setattr(ei, "get_today_run", lambda j: {"success": True})
        out = ei.get_eastmoney_industry_sync_status()
        assert out["coveragePct"] == 80.0 and out["ok"] is True
