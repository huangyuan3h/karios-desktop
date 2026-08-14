"""market_sentiment.py coverage: panic rules, capitulation, FTD, breadth fetchers, compute+sync."""

from __future__ import annotations

import sys
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from data_sync_service.service import market_sentiment as ms


class _Cur:
    def __init__(self, rows=None):
        self._rows = rows or []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def execute(self, sql, params=None):
        return self

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Conn:
    def __init__(self, rows=None):
        self._rows = rows or []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def cursor(self):
        return _Cur(self._rows)


class TestPanicRules:
    def test_breadth_panic_active(self) -> None:
        assert ms.breadth_panic_active(3000) is True
        assert ms.breadth_panic_active(2999) is False

    def test_breadth_panic_rule(self) -> None:
        assert "广度恐慌" in ms.breadth_panic_rule(3200)

    def test_apply_risk_mode(self) -> None:
        rules = []
        assert ms.apply_breadth_panic_risk_mode("hot", 100, rules) == "hot"
        assert ms.apply_breadth_panic_risk_mode("hot", 4000, rules) == "extreme_caution"
        assert len(rules) == 1
        ms.apply_breadth_panic_risk_mode("hot", 4000, rules)
        assert len(rules) == 1

    def test_apply_index_signals(self) -> None:
        sigs = [{"name": "上证指数", "signal": "green", "rules": []}, {"name": "恒生指数", "signal": "green", "rules": []}]
        out = ms.apply_breadth_panic_index_signals(sigs, 100)
        assert out[0]["signal"] == "green"
        out2 = ms.apply_breadth_panic_index_signals(sigs, 4000)
        assert out2[0]["signal"] == "red"
        assert any("breadth_panic override" in x for x in out2[0]["rules"])
        assert out2[1]["signal"] == "green"
        out3 = ms.apply_breadth_panic_index_signals(sigs, 4000)
        assert len(out3[0]["rules"]) == 1

    def test_apply_sentiment_items(self) -> None:
        items = [{"riskMode": "hot", "rules": []}]
        assert ms.apply_breadth_panic_sentiment_items(items, 100) == items
        out = ms.apply_breadth_panic_sentiment_items(items, 4000)
        assert out[0]["riskMode"] == "extreme_caution"
        assert len(out[0]["rules"]) == 1
        assert ms.apply_breadth_panic_sentiment_items([], 4000) == []


class TestCapitulation:
    def test_all_conditions(self, monkeypatch) -> None:
        from data_sync_service.db import etf_fund_flow as eff
        from data_sync_service.db import macro_daily as md
        from data_sync_service.service import macro_daily as smd

        monkeypatch.setattr(md, "get_latest_row", lambda sid: {"close": 25.0})
        monkeypatch.setattr(smd, "SID_510300_PUT_IV", "M510300")
        monkeypatch.setattr(eff, "get_last_trade_date", lambda code: date(2026, 8, 7))
        monkeypatch.setattr(eff, "fetch_row", lambda code, d: {"main_net_inflow": 3e9, "super_large_net_inflow": 0.0})
        out = ms.check_capitulation_bottom(down=4000, as_of=date(2026, 8, 7))
        assert out["triggered"] is True
        assert out["raw"]["ivPct"] == 25.0
        assert out["raw"]["mainFlowYi"] == 30.0

    def test_breadth_fails(self, monkeypatch) -> None:
        from data_sync_service.db import etf_fund_flow as eff
        from data_sync_service.db import macro_daily as md
        from data_sync_service.service import macro_daily as smd

        monkeypatch.setattr(md, "get_latest_row", lambda sid: {"close": 25.0})
        monkeypatch.setattr(smd, "SID_510300_PUT_IV", "M510300")
        monkeypatch.setattr(eff, "get_last_trade_date", lambda code: date(2026, 8, 7))
        monkeypatch.setattr(eff, "fetch_row", lambda code, d: {"main_net_inflow": 3e9, "super_large_net_inflow": 0.0})
        out = ms.check_capitulation_bottom(down=1000, as_of=date(2026, 8, 7))
        assert out["triggered"] is False
        assert out["raw"]["down"] == 1000

    def test_iv_fails(self, monkeypatch) -> None:
        from data_sync_service.db import etf_fund_flow as eff
        from data_sync_service.db import macro_daily as md
        from data_sync_service.service import macro_daily as smd

        monkeypatch.setattr(md, "get_latest_row", lambda sid: {"close": 5.0})
        monkeypatch.setattr(smd, "SID_510300_PUT_IV", "M510300")
        monkeypatch.setattr(eff, "get_last_trade_date", lambda code: None)
        out = ms.check_capitulation_bottom(down=4000, as_of=date(2026, 8, 7))
        assert out["triggered"] is False
        assert out["raw"]["mainFlowYi"] is None

    def test_flow_fails(self, monkeypatch) -> None:
        from data_sync_service.db import macro_daily as md
        from data_sync_service.service import macro_daily as smd

        monkeypatch.setattr(md, "get_latest_row", lambda sid: {"close": 25.0})
        monkeypatch.setattr(smd, "SID_510300_PUT_IV", "M510300")
        out = ms.check_capitulation_bottom(down=4000, as_of=date(2026, 8, 7))
        assert out["triggered"] is False

    def test_errors_silent(self, monkeypatch) -> None:
        from data_sync_service.db import macro_daily as md

        monkeypatch.setattr(md, "get_latest_row", lambda sid: (_ for _ in ()).throw(RuntimeError("down")))
        out = ms.check_capitulation_bottom(down=1000, as_of=date(2026, 8, 7))
        assert out["triggered"] is False


class TestFollowThroughDay:
    def test_all(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "list_days", lambda **kw: [{"riskMode": "capitulation_v_bottom"}])
        out = ms.check_follow_through_day(
            as_of=date(2026, 8, 7), index_chg_max_pct=2.0,
            today_turnover_cny=2e12, prev_turnover_cny=1e12,
        )
        assert out["triggered"] is True

    def test_missing_cap(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "list_days", lambda **kw: [{"riskMode": "normal"}])
        out = ms.check_follow_through_day(
            as_of=date(2026, 8, 7), index_chg_max_pct=2.0,
            today_turnover_cny=2e12, prev_turnover_cny=1e12,
        )
        assert out["triggered"] is False

    def test_low_chg(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "list_days", lambda **kw: [{"riskMode": "capitulation_v_bottom"}])
        out = ms.check_follow_through_day(
            as_of=date(2026, 8, 7), index_chg_max_pct=1.0,
            today_turnover_cny=2e12, prev_turnover_cny=1e12,
        )
        assert out["triggered"] is False

    def test_turnover_down(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "list_days", lambda **kw: [{"riskMode": "capitulation_v_bottom"}])
        out = ms.check_follow_through_day(
            as_of=date(2026, 8, 7), index_chg_max_pct=2.0,
            today_turnover_cny=5e11, prev_turnover_cny=1e12,
        )
        assert out["triggered"] is False

    def test_capitulation_in_lookback_errors(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "list_days", lambda **kw: (_ for _ in ()).throw(RuntimeError("down")))
        assert ms._capitulation_in_lookback(date(2026, 8, 7)) is False

    def test_compute_index_max_chg_pct(self, monkeypatch) -> None:
        from data_sync_service.db import index_daily as idm

        monkeypatch.setattr(idm, "fetch_last_closes_upto", lambda code, d, days: [("d1", 100.0), ("d2", 103.0)])
        assert ms._compute_index_max_chg_pct(date(2026, 8, 7)) == pytest.approx(3.0)

    def test_compute_index_max_chg_pct_errors(self, monkeypatch) -> None:
        from data_sync_service.db import index_daily as idm

        monkeypatch.setattr(idm, "fetch_last_closes_upto", lambda code, d, days: (_ for _ in ()).throw(RuntimeError("down")))
        assert ms._compute_index_max_chg_pct(date(2026, 8, 7)) is None

    def test_read_prev_day_turnover(self, monkeypatch) -> None:
        from data_sync_service.service import trade_calendar_utils as tcu

        monkeypatch.setattr(tcu, "previous_open_date", lambda d: date(2026, 8, 6))
        monkeypatch.setattr(ms, "list_days", lambda **kw: [{"marketTurnoverCny": 1.5e12}])
        assert ms._read_prev_day_turnover(date(2026, 8, 7)) == 1.5e12

    def test_read_prev_day_turnover_empty(self, monkeypatch) -> None:
        from data_sync_service.service import trade_calendar_utils as tcu

        monkeypatch.setattr(tcu, "previous_open_date", lambda d: None)
        assert ms._read_prev_day_turnover(date(2026, 8, 7)) is None


class TestParsing:
    def test_parse_money_to_cny(self) -> None:
        assert ms._parse_money_to_cny(None) == 0.0
        assert ms._parse_money_to_cny(123.0) == 123.0
        assert ms._parse_money_to_cny(float("nan")) == 0.0
        assert ms._parse_money_to_cny("1.5亿") == pytest.approx(1.5e8)
        assert ms._parse_money_to_cny("3,200万") == pytest.approx(3.2e7)
        assert ms._parse_money_to_cny("—") == 0.0
        assert ms._parse_money_to_cny("abc") == 0.0
        assert ms._parse_money_to_cny("+12.5") == 12.5

    def test_finite_float(self) -> None:
        assert ms._finite_float("1.5", 0.0) == 1.5
        assert ms._finite_float("x", 9.0) == 9.0
        assert ms._finite_float(float("inf"), 1.0) == 1.0

    def test_try_float(self) -> None:
        assert ms._try_float("2.5") == 2.5
        assert ms._try_float("x") is None
        assert ms._try_float(float("nan")) is None

    def test_realtime_pct_chg(self) -> None:
        assert ms._realtime_pct_chg({"pct_chg": "1.5"}) == 1.5
        assert ms._realtime_pct_chg({"price": "10.5", "pre_close": "10.0"}) == pytest.approx(5.0)
        assert ms._realtime_pct_chg({"price": "10.5"}) is None

    def test_limit_pct_for(self) -> None:
        assert ms._limit_pct_for("600000.SH", None) == 10.0
        assert ms._limit_pct_for("600000.SH", "ST某某") == 5.0
        assert ms._limit_pct_for("300001.SZ", None) == 20.0
        assert ms._limit_pct_for("688001.SH", None) == 20.0
        assert ms._limit_pct_for("831000.BJ", None) == 30.0
        assert ms._limit_pct_for("301001.SZ", None) == 20.0

    def test_to_records(self, monkeypatch) -> None:
        import pandas as pd

        df = pd.DataFrame([{"a": 1}])
        assert ms._to_records(df) == [{"a": 1}]
        with pytest.raises(RuntimeError):
            ms._to_records("nope")

    def test_safe_trade_date(self) -> None:
        assert ms._safe_trade_date(date(2026, 8, 7)) == "20260807"

    def test_with_retry_success(self, monkeypatch) -> None:
        calls = {"n": 0}

        def fn():
            calls["n"] += 1
            if calls["n"] < 3:
                raise RuntimeError("x")
            return "ok"

        assert ms._with_retry(fn, tries=3, base_sleep_s=0.0) == "ok"

    def test_with_retry_fail(self, monkeypatch) -> None:
        monkeypatch.setattr(ms.time, "sleep", lambda s: None)

        def fn():
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            ms._with_retry(fn, tries=2, base_sleep_s=0.0)

    def test_akshare_missing(self, monkeypatch) -> None:
        monkeypatch.setitem(sys.modules, "akshare", None)
        with pytest.raises(RuntimeError, match="AkShare is required"):
            ms._akshare()

    def test_tushare_pro_no_key(self, monkeypatch) -> None:
        from data_sync_service import config

        monkeypatch.setattr(config, "get_settings", lambda: type("S", (), {"tu_share_api_key": ""})())
        with pytest.raises(RuntimeError, match="TU_SHARE_API_KEY"):
            ms._tushare_pro()

    def test_tushare_pro_ok(self, monkeypatch) -> None:
        from data_sync_service import config

        monkeypatch.setattr(config, "get_settings", lambda: type("S", (), {"tu_share_api_key": "k"})())
        pro = ms._tushare_pro()
        assert pro is not None


class TestBreadthEod:
    def _fake_pro(self, monkeypatch, pages):
        class FakeDf:
            def __init__(self, rows):
                self._rows = rows

            @property
            def empty(self):
                return not self._rows

            def to_dict(self, orient):
                return self._rows

        calls = {"n": 0}

        def daily(**kw):
            n = calls["n"]
            calls["n"] += 1
            if n >= len(pages):
                return FakeDf([])
            return FakeDf(pages[n])

        FakePro = type("FakePro", (), {"daily": staticmethod(daily)})
        monkeypatch.setattr(ms, "_tushare_pro", lambda: FakePro())
        return calls

    def test_eod_paged(self, monkeypatch) -> None:
        page1 = [{"ts_code": "600000.SH", "pct_chg": 1.0, "vol": 100.0, "amount": 5.0},
                 {"ts_code": "000001.SZ", "pct_chg": -2.0, "vol": 200.0, "amount": 6.0}] * 2500
        page2 = [{"ts_code": "300001.SZ", "pct_chg": 0.0, "vol": 300.0, "amount": 7.0}]
        self._fake_pro(monkeypatch, [page1, page2])
        out = ms.fetch_cn_market_breadth_eod(date(2026, 8, 7))
        assert out["up_count"] == 2500
        assert out["down_count"] == 2500
        assert out["flat_count"] == 1
        assert out["total_turnover_cny"] == pytest.approx((5.0 * 2500 + 6.0 * 2500 + 7.0) * 1000.0)

    def test_eod_single_page(self, monkeypatch) -> None:
        self._fake_pro(monkeypatch, [[{"ts_code": "600000.SH", "pct_chg": 1.0, "vol": 100.0, "amount": 5.0}]])
        out = ms.fetch_cn_market_breadth_eod(date(2026, 8, 7))
        assert out["up_count"] == 1 and out["down_count"] == 0
        assert out["up_down_ratio"] == 1.0

    def test_eod_no_rows(self, monkeypatch) -> None:
        self._fake_pro(monkeypatch, [])
        out = ms.fetch_cn_market_breadth_eod(date(2026, 8, 7))
        assert out["total_count"] == 0 and out["up_down_ratio"] == 0.0

    def test_eod_bad_pct(self, monkeypatch) -> None:
        self._fake_pro(monkeypatch, [[{"ts_code": "600000.SH", "pct_chg": "x", "vol": "y", "amount": "z"}]])
        out = ms.fetch_cn_market_breadth_eod(date(2026, 8, 7))
        assert out["flat_count"] == 1


class TestBreadthIntraday:
    def test_empty_codes(self, monkeypatch) -> None:
        ms._INTRADAY_BREADTH_CACHE.clear()
        monkeypatch.setattr(ms, "ensure_stock_basic", lambda: None)
        monkeypatch.setattr(ms, "fetch_stock_ts_codes", lambda: [])
        out = ms.fetch_cn_market_breadth_intraday(date(2026, 8, 7))
        assert out["total_count"] == 0
        assert ms._INTRADAY_BREADTH_CACHE.get("2026-08-07") is out

    def test_cached(self, monkeypatch) -> None:
        ms._INTRADAY_BREADTH_CACHE["2026-08-07"] = {"cached": True}
        assert ms.fetch_cn_market_breadth_intraday(date(2026, 8, 7))["cached"] is True
        ms._INTRADAY_BREADTH_CACHE.clear()

    def test_fetch_parts(self, monkeypatch) -> None:
        ms._INTRADAY_BREADTH_CACHE.clear()
        codes = [f"600{i:04d}.SH" for i in range(120)]
        monkeypatch.setattr(ms, "ensure_stock_basic", lambda: None)
        monkeypatch.setattr(ms, "fetch_stock_ts_codes", lambda: codes)

        def quotes(part):
            return {"ok": True, "items": [{"ts_code": c, "pct_chg": 1.0, "volume": 100.0, "amount": 200.0} for c in part]}

        monkeypatch.setattr(ms, "fetch_realtime_quotes", quotes)
        out = ms.fetch_cn_market_breadth_intraday(date(2026, 8, 7))
        assert out["up_count"] == 120
        assert out["raw"]["matched"] == 120
        assert out["raw"]["batches"] == 3
        ms._INTRADAY_BREADTH_CACHE.clear()

    def test_fetch_part_failure(self, monkeypatch) -> None:
        ms._INTRADAY_BREADTH_CACHE.clear()
        monkeypatch.setattr(ms, "ensure_stock_basic", lambda: None)
        monkeypatch.setattr(ms, "fetch_stock_ts_codes", lambda: ["600000.SH"])
        monkeypatch.setattr(ms, "fetch_realtime_quotes", lambda part: {"ok": False, "error": "quota"})
        out = ms.fetch_cn_market_breadth_intraday(date(2026, 8, 7))
        assert out["total_count"] == 0
        assert "quota" in out["raw"]["errors"]
        ms._INTRADAY_BREADTH_CACHE.clear()


class TestTushareHelpers:
    def _fake_pro(self, monkeypatch, daily_df=None, limit=None, exc=None):
        class FakeDf:
            def __init__(self, rows):
                self._rows = rows

            @property
            def empty(self):
                return not self._rows

            def to_dict(self, orient):
                return self._rows

        class FakeDfFull:
            def __init__(self, rows):
                self._rows = rows or []

            @property
            def empty(self):
                return not self._rows

            def to_dict(self, orient):
                return self._rows

        class FakePro:
            def __init__(self):
                self._d = daily_df

            def daily(self, **kw):
                if limit is not None:
                    limit(**kw)
                if exc:
                    raise exc
                return FakeDfFull(self._d or [])

            def limit_list_d(self, **kw):
                return FakeDfFull(limit(**kw) if limit else [])

        monkeypatch.setattr(ms, "_tushare_pro", lambda: FakePro())

    def test_daily_pct_chg_map(self, monkeypatch) -> None:
        self._fake_pro(monkeypatch, daily_df=[{"ts_code": "600000.SH", "pct_chg": 1.2}, {"ts_code": "bad", "pct_chg": "x"}])
        out = ms._tushare_daily_pct_chg_map(date(2026, 8, 7))
        assert out == {"600000.SH": 1.2}

    def test_yesterday_limitup_codes(self, monkeypatch) -> None:
        seen = {}

        def limit(**kw):
            seen.update(kw)
            return [{"ts_code": "600000.SH"}, {"ts_code": "000001.SZ"}]

        self._fake_pro(monkeypatch, limit=limit)
        y, codes = ms._tushare_yesterday_limitup_codes(date(2026, 8, 7))
        assert len(codes) == 2
        assert y is not None

    def test_yesterday_limitup_codes_none(self, monkeypatch) -> None:
        def limit(**kw):
            return []

        self._fake_pro(monkeypatch, limit=limit)
        y, codes = ms._tushare_yesterday_limitup_codes(date(2026, 8, 7))
        assert codes == []

    def test_premium_tushare(self, monkeypatch) -> None:
        def limit(**kw):
            return [{"ts_code": "600000.SH"}, {"ts_code": "000001.SZ"}]

        self._fake_pro(monkeypatch, limit=limit, daily_df=[{"ts_code": "600000.SH", "pct_chg": 2.0}])
        out = ms.fetch_cn_yesterday_limitup_premium_tushare(date(2026, 8, 7))
        assert out["premium"] == 2.0
        assert out["count"] == 2
        assert out["raw"]["matched"] == 1

    def test_premium_tushare_no_codes(self, monkeypatch) -> None:
        self._fake_pro(monkeypatch, limit=lambda **kw: [])
        out = ms.fetch_cn_yesterday_limitup_premium_tushare(date(2026, 8, 7))
        assert out["premium"] == 0.0 and out["raw"]["y"] is None


class TestLimitupPool:
    def test_close_limit_up_pool(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "_daily_rows_for_date", lambda d: [
            ("600000.SH", 10.0, 11.0, 11.0, 10.0, "平安银行"),
            ("300001.SZ", 10.0, 12.0, 12.0, 20.0, "创业板股"),
            ("000002.SZ", 10.0, 10.1, 10.05, 0.5, "未涨停"),
            ("600100.SH", None, None, None, None, None),
            ("bad", "x", "y", "z", "w", None),
        ])
        codes = ms._close_limit_up_pool_codes(date(2026, 8, 7))
        assert "600000.SH" in codes and "300001.SZ" in codes and "000002.SZ" not in codes

    def test_failed_limitup_rate_from_db(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "_daily_rows_for_date", lambda d: [
            ("600000.SH", 10.0, 11.0, 11.0, 10.0, None),
            ("300001.SZ", 10.0, 12.0, 10.5, 5.0, None),
            ("000002.SZ", 10.0, 10.1, 10.05, 0.5, None),
        ])
        rate, ever, close = ms._failed_limitup_rate_from_db(date(2026, 8, 7))
        assert ever == 2 and close == 1
        assert rate == 50.0

    def test_avg_pct_chg_from_db(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "ensure_daily", lambda: None)
        monkeypatch.setattr(ms, "get_connection", lambda: _Conn([("600000.SH", 2.0), ("300001.SZ", 4.0), ("bad", "x")]))
        avg, n = ms._avg_pct_chg_from_db(date(2026, 8, 7), ["600000.SH", "300001.SZ"])
        assert avg == pytest.approx(3.0) and n == 2
        assert ms._avg_pct_chg_from_db(date(2026, 8, 7), []) == (0.0, 0)

    def test_avg_pct_chg_from_realtime(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "fetch_realtime_quotes_batched", lambda codes: [
            {"pct_chg": "1.0"}, {"pct_chg": "3.0"},
        ])
        avg, n = ms._avg_pct_chg_from_realtime(["600000.SH"])
        assert avg == pytest.approx(2.0) and n == 2
        assert ms._avg_pct_chg_from_realtime([]) == (0.0, 0)

    def test_prev_open_date_calendar(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "is_trading_day", lambda exc, d: True)
        monkeypatch.setattr(ms, "get_open_dates", lambda **kw: [date(2026, 8, 6), date(2026, 8, 7)])
        assert ms._prev_open_date("SSE", date(2026, 8, 7)) == date(2026, 8, 6)

    def test_prev_open_date_db_fallback(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "is_trading_day", lambda exc, d: None)
        monkeypatch.setattr(ms, "ensure_daily", lambda: None)
        monkeypatch.setattr(ms, "get_connection", lambda: _Conn([(date(2026, 8, 6),)]))
        assert ms._prev_open_date("SSE", date(2026, 8, 7)) == date(2026, 8, 6)

    def test_daily_rows_for_date(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "ensure_daily", lambda: None)
        monkeypatch.setattr(ms, "ensure_stock_basic", lambda: None)
        monkeypatch.setattr(ms, "get_connection", lambda: _Conn([("600000.SH", 1.0, 2.0, 3.0, 4.0, "名")]))
        out = ms._daily_rows_for_date(date(2026, 8, 7))
        assert out == [("600000.SH", 1.0, 2.0, 3.0, 4.0, "名")]


class TestCompute:
    def _patch_compute_deps(self, monkeypatch, breadth=None, premium=1.0, failed_rate=20.0, down=500, turnover=2e12, ratio=1.6):
        monkeypatch.setattr(ms, "fetch_cn_market_breadth_eod", lambda dt: breadth if breadth is not None else {
            "date": dt.isoformat(), "up_count": 2000, "down_count": down, "flat_count": 100,
            "total_count": 2600, "up_down_ratio": ratio, "total_turnover_cny": turnover, "total_volume": 1e10,
        })
        monkeypatch.setattr(ms, "fetch_cn_market_breadth_intraday", lambda dt: {})
        monkeypatch.setattr(ms, "_prev_open_date", lambda exc, d: date(2026, 8, 6))
        monkeypatch.setattr(ms, "_close_limit_up_pool_codes", lambda d: ["600000.SH"])
        monkeypatch.setattr(ms, "_avg_pct_chg_from_db", lambda d, pool: (premium, 1 if pool else 0))
        monkeypatch.setattr(ms, "_avg_pct_chg_from_realtime", lambda pool: (premium, 1))
        monkeypatch.setattr(ms, "_failed_limitup_rate_from_db", lambda d: (failed_rate, 10, 5))
        monkeypatch.setattr(ms, "_compute_index_max_chg_pct", lambda d: 0.5)
        monkeypatch.setattr(ms, "_read_prev_day_turnover", lambda d: 1e12)
        monkeypatch.setattr(ms, "check_capitulation_bottom", lambda **kw: {"triggered": False, "rule": "", "raw": {}})
        monkeypatch.setattr(ms, "check_follow_through_day", lambda **kw: {"triggered": False, "rule": "", "raw": {}})
        monkeypatch.setattr(ms, "list_days", lambda **kw: [])

    def test_euphoric(self, monkeypatch) -> None:
        self._patch_compute_deps(monkeypatch, premium=3.5, turnover=2.6e12, ratio=2.2, failed_rate=20.0)
        out = ms.compute_cn_sentiment_for_date("2026-08-07")
        assert out["riskMode"] == "euphoric"

    def test_hot(self, monkeypatch) -> None:
        self._patch_compute_deps(monkeypatch, premium=1.0, turnover=1.9e12, ratio=1.6, failed_rate=20.0)
        out = ms.compute_cn_sentiment_for_date("2026-08-07")
        assert out["riskMode"] == "hot"

    def test_no_new_positions(self, monkeypatch) -> None:
        self._patch_compute_deps(monkeypatch, premium=-1.0, failed_rate=80.0)
        out = ms.compute_cn_sentiment_for_date("2026-08-07")
        assert out["riskMode"] == "no_new_positions"

    def test_caution_failed_rate(self, monkeypatch) -> None:
        self._patch_compute_deps(monkeypatch, premium=1.0, failed_rate=80.0, turnover=1e12)
        out = ms.compute_cn_sentiment_for_date("2026-08-07")
        assert out["riskMode"] == "caution"
        assert any("炸板率≥70%" in r for r in out["rules"])

    def test_caution_premium(self, monkeypatch) -> None:
        self._patch_compute_deps(monkeypatch, premium=-1.0, failed_rate=20.0)
        out = ms.compute_cn_sentiment_for_date("2026-08-07")
        assert out["riskMode"] == "caution"

    def test_bullish_override(self, monkeypatch) -> None:
        self._patch_compute_deps(monkeypatch, premium=0.5, failed_rate=80.0, turnover=2.2e12, ratio=1.5)
        out = ms.compute_cn_sentiment_for_date("2026-08-07")
        assert out["riskMode"] == "normal"
        assert any("多头覆盖" in r for r in out["rules"])

    def test_breadth_panic(self, monkeypatch) -> None:
        self._patch_compute_deps(monkeypatch, down=4000, ratio=0.3)
        out = ms.compute_cn_sentiment_for_date("2026-08-07")
        assert out["riskMode"] == "extreme_caution"

    def test_capitulation_override(self, monkeypatch) -> None:
        self._patch_compute_deps(monkeypatch, down=4000)
        monkeypatch.setattr(ms, "check_capitulation_bottom", lambda **kw: {"triggered": True, "rule": "cap rule", "raw": {"x": 1}})
        out = ms.compute_cn_sentiment_for_date("2026-08-07")
        assert out["riskMode"] == "capitulation_v_bottom"
        assert out["raw"]["capitulation"] == {"x": 1}

    def test_ftd_override(self, monkeypatch) -> None:
        self._patch_compute_deps(monkeypatch)
        monkeypatch.setattr(ms, "check_follow_through_day", lambda **kw: {"triggered": True, "rule": "ftd rule", "raw": {}})
        out = ms.compute_cn_sentiment_for_date("2026-08-07")
        assert out["riskMode"] == "confirmed_uptrend"

    def test_breadth_error(self, monkeypatch) -> None:
        self._patch_compute_deps(monkeypatch)
        monkeypatch.setattr(ms, "fetch_cn_market_breadth_eod", lambda dt: (_ for _ in ()).throw(RuntimeError("down")))
        monkeypatch.setattr(ms, "fetch_cn_market_breadth_intraday", lambda dt: (_ for _ in ()).throw(RuntimeError("down")))
        out = ms.compute_cn_sentiment_for_date("2026-08-07")
        assert out["riskMode"] == "caution"
        assert "breadthError" in out["raw"]

    def test_premium_failure_path(self, monkeypatch) -> None:
        self._patch_compute_deps(monkeypatch)
        monkeypatch.setattr(ms, "_prev_open_date", lambda exc, d: (_ for _ in ()).throw(RuntimeError("cal")))
        out = ms.compute_cn_sentiment_for_date("2026-08-07")
        assert "yesterdayLimitUpPremiumError" in out["raw"]

    def test_failed_rate_error(self, monkeypatch) -> None:
        self._patch_compute_deps(monkeypatch)
        monkeypatch.setattr(ms, "_failed_limitup_rate_from_db", lambda d: (_ for _ in ()).throw(RuntimeError("x")))
        out = ms.compute_cn_sentiment_for_date("2026-08-07")
        assert "failedLimitUpRateError" in out["raw"]

    def test_intraday_fallback(self, monkeypatch) -> None:
        self._patch_compute_deps(monkeypatch, breadth={"date": "2026-08-08", "total_count": 0, "total_turnover_cny": 0.0})
        monkeypatch.setattr(ms, "fetch_cn_market_breadth_intraday", lambda dt: {
            "date": dt.isoformat(), "up_count": 1, "down_count": 2, "flat_count": 0,
            "total_count": 3, "up_down_ratio": 0.5, "total_turnover_cny": 1e12, "total_volume": 1.0,
        })
        monkeypatch.setattr(ms, "_read_prev_day_turnover", lambda d: None)
        monkeypatch.setattr(ms, "list_days", lambda **kw: [{"riskMode": "capitulation_v_bottom"}])
        today = datetime.now(tz=ZoneInfo("Asia/Shanghai")).date().isoformat()
        out = ms.compute_cn_sentiment_for_date(today)
        assert out["down"] == 2


class TestRowAndDates:
    def test_row_from_compute(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "now_iso", lambda: "2026-08-07T10:00:00+00:00")
        out = ms._sentiment_row_from_compute({"date": "2026-08-07", "asOfDate": "2026-08-07", "up": 1, "down": 2,
                                              "flat": 3, "ratio": 0.5, "marketTurnoverCny": 1e12, "marketVolume": 2e10,
                                              "premium": 1.0, "failedRate": 2.0, "riskMode": "hot", "rules": ["a"],
                                              "updatedAt": "t", "raw": {"b": 1}}, "2026-08-07")
        assert out["total_count"] == 6
        assert out["risk_mode"] == "hot"

    def test_item_from_compute(self) -> None:
        out = ms._sentiment_item_from_compute({"up": 1, "ratio": 0.5}, "2026-08-07", rules_list=["r"])
        assert out["upCount"] == 1 and out["rules"] == ["r"]

    def test_persist(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "compute_cn_sentiment_for_date", lambda d: {"riskMode": "hot"})
        monkeypatch.setattr(ms, "upsert_daily_rows", lambda rows: None)
        monkeypatch.setattr(ms, "list_days", lambda **kw: [{"date": "2026-08-07", "riskMode": "hot"}])
        assert ms._persist_sentiment_for_date("2026-08-07")["riskMode"] == "hot"

    def test_persist_no_cached(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "compute_cn_sentiment_for_date", lambda d: {"riskMode": "hot"})
        monkeypatch.setattr(ms, "upsert_daily_rows", lambda rows: None)
        monkeypatch.setattr(ms, "list_days", lambda **kw: [])
        out = ms._persist_sentiment_for_date("2026-08-07")
        assert out["riskMode"] == "hot"

    def test_get_cn_sentiment(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "get_latest_date", lambda: "2026-08-07")
        monkeypatch.setattr(ms, "list_days", lambda **kw: [{"date": "2026-08-07"}])
        out = ms.get_cn_sentiment(days=5)
        assert out["asOfDate"] == "2026-08-07" and len(out["items"]) == 1

    def test_get_cn_sentiment_empty(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "get_latest_date", lambda: "")
        out = ms.get_cn_sentiment()
        assert out["items"] == []


class TestSyncDates:
    def _patch(self, monkeypatch, latest_db="2026-08-07", is_trading=True, cached=None, open_dates=None):
        monkeypatch.setattr(ms, "last_open_date_on_or_before", lambda d: date(2026, 8, 7))
        monkeypatch.setattr(ms, "get_latest_date", lambda: latest_db)
        monkeypatch.setattr(ms, "is_cn_trading_day", lambda d: is_trading)
        monkeypatch.setattr(ms, "list_days", lambda **kw: cached if cached is not None else [{"date": "2026-08-07"}])
        monkeypatch.setattr(ms, "get_open_dates", lambda **kw: open_dates if open_dates is not None else [date(2026, 8, 6), date(2026, 8, 7)])

    def test_up_to_date_force(self, monkeypatch) -> None:
        self._patch(monkeypatch)
        dates, skip = ms._resolve_sentiment_sync_dates(request_date=date(2026, 8, 7), force=True)
        assert dates == [date(2026, 8, 7)] and skip is None

    def test_already_synced(self, monkeypatch) -> None:
        self._patch(monkeypatch, cached=[{"date": "2026-08-07"}])
        dates, skip = ms._resolve_sentiment_sync_dates(request_date=date(2026, 8, 7), force=False)
        assert dates == [] and skip["skipped"] is True
        assert skip["reason"] == "already_synced"

    def test_trading_day_needs_sync(self, monkeypatch) -> None:
        self._patch(monkeypatch, cached=[])
        dates, skip = ms._resolve_sentiment_sync_dates(request_date=date(2026, 8, 7), force=False)
        assert dates == [date(2026, 8, 7)] and skip is None

    def test_not_trading_day(self, monkeypatch) -> None:
        self._patch(monkeypatch, is_trading=False)
        dates, skip = ms._resolve_sentiment_sync_dates(request_date=date(2026, 8, 8), force=False)
        assert dates == [] and skip["reason"] == "not_trading_day"

    def test_stale_db_dates_from_calendar(self, monkeypatch) -> None:
        self._patch(monkeypatch, latest_db="2026-08-05")
        dates, skip = ms._resolve_sentiment_sync_dates(request_date=date(2026, 8, 7), force=False)
        assert dates == [date(2026, 8, 6), date(2026, 8, 7)]

    def test_no_calendar(self, monkeypatch) -> None:
        self._patch(monkeypatch)
        monkeypatch.setattr(ms, "last_open_date_on_or_before", lambda d: None)
        dates, skip = ms._resolve_sentiment_sync_dates(request_date=date(2026, 8, 7), force=False)
        assert dates == [] and skip["ok"] is False

    def test_stale_db_empty_calendar(self, monkeypatch) -> None:
        self._patch(monkeypatch, latest_db="", open_dates=[])
        dates, skip = ms._resolve_sentiment_sync_dates(request_date=date(2026, 8, 7), force=False)
        assert dates == [date(2026, 8, 7)]


class TestSync:
    def test_skip_out(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "_resolve_sentiment_sync_dates", lambda **kw: ([], {"ok": True, "skipped": True}))
        out = ms.sync_cn_sentiment(date_str="2026-08-07", force=False)
        assert out["skipped"] is True

    def test_no_dates(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "_resolve_sentiment_sync_dates", lambda **kw: ([], None))
        monkeypatch.setattr(ms, "last_open_date_on_or_before", lambda d: date(2026, 8, 7))
        monkeypatch.setattr(ms, "list_days", lambda **kw: [{"date": "2026-08-07"}])
        out = ms.sync_cn_sentiment(date_str="2026-08-07", force=False)
        assert out["ok"] is True

    def test_sync_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "_resolve_sentiment_sync_dates", lambda **kw: ([date(2026, 8, 7)], None))
        monkeypatch.setattr(ms, "_persist_sentiment_for_date", lambda d: {"date": d, "riskMode": "hot"})
        monkeypatch.setattr(ms, "list_days", lambda **kw: [{"date": "2026-08-07", "riskMode": "hot"}])
        monkeypatch.setattr(ms, "is_cn_trading_day", lambda d: True)
        out = ms.sync_cn_sentiment(date_str="2026-08-07", force=False)
        assert out["ok"] is True and out["asOfDate"] == "2026-08-07"

    def test_sync_catchup(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "_resolve_sentiment_sync_dates", lambda **kw: ([date(2026, 8, 6), date(2026, 8, 7)], None))
        monkeypatch.setattr(ms, "_persist_sentiment_for_date", lambda d: {"date": d})
        monkeypatch.setattr(ms, "list_days", lambda **kw: [{"date": d} for d in ("2026-08-06", "2026-08-07")])
        monkeypatch.setattr(ms, "is_cn_trading_day", lambda d: False)
        out = ms.sync_cn_sentiment(date_str="2026-08-07", force=False)
        assert out["catchup"] is True
        assert out["syncedDates"] == ["2026-08-06", "2026-08-07"]

    def test_sync_error_with_cached(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "_resolve_sentiment_sync_dates", lambda **kw: ([date(2026, 8, 7)], None))

        def persist(d):
            raise RuntimeError("compute boom")

        monkeypatch.setattr(ms, "_persist_sentiment_for_date", persist)
        monkeypatch.setattr(ms, "list_days", lambda **kw: [{"date": "2026-08-07"}])
        monkeypatch.setattr(ms, "is_cn_trading_day", lambda d: True)
        out = ms.sync_cn_sentiment(date_str="2026-08-07", force=False)
        assert out["ok"] is False
        assert out["errors"] == [{"date": "2026-08-07", "error": "compute boom"}]

    def test_sync_error_no_synced(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "_resolve_sentiment_sync_dates", lambda **kw: ([date(2026, 8, 7)], None))

        def persist(d):
            raise RuntimeError("boom")

        monkeypatch.setattr(ms, "_persist_sentiment_for_date", persist)
        monkeypatch.setattr(ms, "list_days", lambda **kw: [])
        out = ms.sync_cn_sentiment(date_str="2026-08-07", force=False)
        assert out["ok"] is False and out["reason"] == "compute_failed"

    def test_bad_date_str(self, monkeypatch) -> None:
        monkeypatch.setattr(ms, "shanghai_today", lambda: date(2026, 8, 7))
        monkeypatch.setattr(ms, "_resolve_sentiment_sync_dates", lambda **kw: ([], {"ok": True, "skipped": True}))
        out = ms.sync_cn_sentiment(date_str="garbage", force=False)
        assert out["skipped"] is True
