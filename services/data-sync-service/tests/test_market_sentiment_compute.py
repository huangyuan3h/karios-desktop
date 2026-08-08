"""compute_cn_sentiment_for_date driver tests (mocked sub-fetches)."""

from __future__ import annotations

import datetime

from data_sync_service.service import market_sentiment as ms


def _base_mocks(monkeypatch, **overrides) -> None:
    def mk(name, fn):
        monkeypatch.setattr(ms, name, fn)
    monkeypatch.setattr(ms, "fetch_cn_market_breadth_eod", lambda dt: {
        "up_count": 3000, "down_count": 2000, "flat_count": 100,
        "total_count": 5100, "up_down_ratio": 1.5,
        "total_turnover_cny": 1.9e12, "total_volume": 1e9,
    })

    mk("_prev_open_date", lambda ex, d: datetime.date(2026, 7, 31))
    mk("_close_limit_up_pool_codes", lambda d: ["600000.SH", "000001.SZ"])
    mk("_avg_pct_chg_from_db", lambda dt, pool: (0.6, 2))
    mk("_failed_limitup_rate_from_db", lambda dt: (40.0, 10, 6))
    mk("_compute_index_max_chg_pct", lambda d: 1.5)
    mk("_read_prev_day_turnover", lambda d: 1.5e12)
    mk("check_capitulation_bottom", lambda *, down, as_of: {"triggered": False, "rule": "", "raw": {}})
    mk("check_follow_through_day", lambda **kw: {"triggered": False, "rule": "", "raw": {}})
    mk("apply_breadth_panic_risk_mode", lambda mode, down, rules: mode)
    for k, v in overrides.items():
        mk(k, v)


def test_compute_hot_mode(monkeypatch) -> None:
    _base_mocks(monkeypatch)
    out = ms.compute_cn_sentiment_for_date("2026-08-04")
    assert out["riskMode"] == "hot"
    assert out["premium"] == 0.6
    assert out["failedRate"] == 40.0
    assert out["ratio"] == 1.5
    assert out["up"] == 3000


def test_compute_no_new_positions_when_premium_negative_failed_high(monkeypatch) -> None:
    _base_mocks(monkeypatch, _avg_pct_chg_from_db=lambda dt, pool: (-0.5, 2),
                _failed_limitup_rate_from_db=lambda dt: (75.0, 10, 2))
    out = ms.compute_cn_sentiment_for_date("2026-08-04")
    assert out["riskMode"] == "no_new_positions"


def test_compute_caution_when_failed_rate_high(monkeypatch) -> None:
    _base_mocks(
        monkeypatch,
        _avg_pct_chg_from_db=lambda dt, pool: (0.2, 2),
        _failed_limitup_rate_from_db=lambda dt: (80.0, 10, 2),
        # ratio 1.0 < 1.2 → bullish_override off → failed>=70 ⇒ caution
        fetch_cn_market_breadth_eod=lambda dt: {
            "up_count": 2500, "down_count": 2500, "flat_count": 100,
            "total_count": 5100, "up_down_ratio": 1.0,
            "total_turnover_cny": 1.9e12, "total_volume": 1e9,
        },
    )
    out = ms.compute_cn_sentiment_for_date("2026-08-04")
    assert out["riskMode"] == "caution"


def test_compute_caution_when_breadth_fails(monkeypatch) -> None:
    _base_mocks(monkeypatch,
                fetch_cn_market_breadth_eod=lambda dt: (_ for _ in ()).throw(RuntimeError("x")))
    out = ms.compute_cn_sentiment_for_date("2026-08-04")
    assert out["riskMode"] == "caution"  # errors force caution
    assert "breadth_failed" in out["rules"][0] if out["rules"] else True
    assert out["up"] == 0


def test_compute_euphoric_requires_all_flags(monkeypatch) -> None:
    _base_mocks(
        monkeypatch,
        fetch_cn_market_breadth_eod=lambda dt: {
            "up_count": 4000, "down_count": 1000, "flat_count": 0,
            "total_count": 5000, "up_down_ratio": 2.5,
            "total_turnover_cny": 2.6e12, "total_volume": 1e9,
        },
        _avg_pct_chg_from_db=lambda dt, pool: (3.5, 2),
        _failed_limitup_rate_from_db=lambda dt: (20.0, 10, 8),
    )
    out = ms.compute_cn_sentiment_for_date("2026-08-04")
    assert out["riskMode"] == "euphoric"


def test_compute_ftd_overrides_to_confirmed_uptrend(monkeypatch) -> None:
    _base_mocks(monkeypatch, check_follow_through_day=lambda **kw: {
        "triggered": True, "rule": "ftd", "raw": {"ok": 1}
    })
    out = ms.compute_cn_sentiment_for_date("2026-08-04")
    assert out["riskMode"] == "confirmed_uptrend"
    assert out["raw"]["ftd"] == {"ok": 1}


def test_limit_pct_for() -> None:
    assert ms._limit_pct_for("600000.SH", None) == 10.0
    assert ms._limit_pct_for("000001.SZ", None) == 10.0
    assert ms._limit_pct_for("300750.SZ", None) == 20.0
    assert ms._limit_pct_for("688235.SH", None) == 20.0
    assert ms._limit_pct_for("600000.SH", "ST某某") == 5.0
    assert ms._limit_pct_for("920001.BJ", None) == 30.0


def test_tushare_daily_pct_chg_map(monkeypatch) -> None:
    import pandas as pd

    monkeypatch.setattr(ms, "_tushare_pro", lambda: object())
    monkeypatch.setattr(ms, "_safe_trade_date", lambda d: "20260701")

    def fake_daily(**kw):
        return pd.DataFrame({"ts_code": ["A.SH", "B.SZ", "C.SZ"], "pct_chg": [1.0, "bad", None]})

    monkeypatch.setattr(ms, "_with_retry", lambda fn, **kw: fake_daily())
    out = ms._tushare_daily_pct_chg_map(__import__("datetime").date(2026, 7, 1))
    assert out == {"A.SH": 1.0}


def test_tushare_daily_pct_chg_map_none(monkeypatch) -> None:
    monkeypatch.setattr(ms, "_tushare_pro", lambda: object())
    monkeypatch.setattr(ms, "_safe_trade_date", lambda d: "20260701")
    monkeypatch.setattr(ms, "_with_retry", lambda fn, **kw: None)
    assert ms._tushare_daily_pct_chg_map(__import__("datetime").date(2026, 7, 1)) == {}


def test_close_limit_up_pool_detects_10pct(monkeypatch) -> None:
    rows = [
        ("600001.SH", 10.0, 11.0, 11.0, 10.0, "普通股"),    # 10% limit → hit exactly
        ("600002.SH", 10.0, 10.5, 10.2, 2.0, None),          # below limit → not in pool
        ("300001.SZ", 10.0, 12.0, 12.0, 20.0, None),         # 20% limit board → hit
    ]
    monkeypatch.setattr(ms, "_daily_rows_for_date", lambda d: rows)
    out = ms._close_limit_up_pool_codes(__import__("datetime").date(2026, 7, 1))
    assert "600001.SH" in out
    assert "300001.SZ" in out
    assert "600002.SH" not in out


def test_close_limit_up_pool_pct_fallback(monkeypatch) -> None:
    rows = [
        ("600003.SH", 10.0, None, 10.5, None, None),  # below limit → not in pool
        ("600004.SH", 10.0, 10.95, 10.95, 9.8, None),  # pct 9.8 >= 10-0.2 → pool via fallback
    ]
    monkeypatch.setattr(ms, "_daily_rows_for_date", lambda d: rows)
    out = ms._close_limit_up_pool_codes(__import__("datetime").date(2026, 7, 1))
    assert "600004.SH" in out
    assert "600003.SH" not in out


def test_prev_open_date_uses_calendar(monkeypatch) -> None:
    import datetime as dt

    monkeypatch.setattr(ms, "is_trading_day", lambda ex, d: True)
    monkeypatch.setattr(
        ms, "get_open_dates", lambda exchange, start_date, end_date: [dt.date(2026, 7, 31), dt.date(2026, 8, 3)]
    )
    assert ms._prev_open_date("SSE", dt.date(2026, 8, 4)) == dt.date(2026, 8, 3)


def test_prev_open_date_falls_back_to_daily(monkeypatch) -> None:
    import datetime as dt

    monkeypatch.setattr(ms, "is_trading_day", lambda ex, d: None)  # calendar missing
    monkeypatch.setattr(ms, "ensure_daily", lambda: None)

    class _FakeCur:
        def execute(self, sql, params):
            pass

        def fetchone(self):
            return (dt.date(2026, 7, 31),)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

    class _FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

        def cursor(self):
            return _FakeCur()

    monkeypatch.setattr(ms, "get_connection", lambda: _FakeConn())
    assert ms._prev_open_date("SSE", dt.date(2026, 8, 4)) == dt.date(2026, 7, 31)


def test_tushare_yesterday_limitup_codes_finds_y(monkeypatch) -> None:
    import pandas as pd

    class _FakePro:
        def limit_list_d(self, **kw):
            return None  # v1 signature missing → try next kwargs

    monkeypatch.setattr(ms, "_tushare_pro", lambda: _FakePro())
    monkeypatch.setattr(ms, "_safe_trade_date", lambda d: d.strftime("%Y%m%d"))

    def fake_retry(fn, **kw):
        return fn()

    monkeypatch.setattr(ms, "_with_retry", fake_retry)

    # limit_list_d returns None for all kwargs → fallthrough to limit_list missing → []
    y, codes = ms._tushare_yesterday_limitup_codes(__import__("datetime").date(2026, 8, 4))
    assert codes == []


def test_fetch_cn_yesterday_limitup_premium_tushare_no_codes(monkeypatch) -> None:
    monkeypatch.setattr(ms, "_tushare_yesterday_limitup_codes", lambda d: (None, []))
    out = ms.fetch_cn_yesterday_limitup_premium_tushare(__import__("datetime").date(2026, 8, 4))
    assert out["premium"] == 0.0
    assert out["count"] == 0


def test_fetch_cn_yesterday_limitup_premium_tushare_computes(monkeypatch) -> None:
    import datetime as dt

    monkeypatch.setattr(
        ms, "_tushare_yesterday_limitup_codes",
        lambda d: (dt.date(2026, 7, 31), ["A.SH", "B.SH", "C.SH"]),
    )
    monkeypatch.setattr(ms, "_tushare_daily_pct_chg_map", lambda d: {"A.SH": 2.0, "B.SH": 4.0})
    out = ms.fetch_cn_yesterday_limitup_premium_tushare(dt.date(2026, 8, 4))
    assert out["premium"] == 3.0  # (2+4)/2
    assert out["count"] == 3
    assert out["raw"]["matched"] == 2


def test_fetch_cn_yesterday_limitup_premium_darwin_uses_tushare(monkeypatch) -> None:
    """On darwin the AkShare path is skipped entirely."""
    monkeypatch.setattr(ms.sys, "platform", "darwin")
    monkeypatch.setattr(ms, "fetch_cn_yesterday_limitup_premium_tushare", lambda d: {"premium": 1.0})
    out = ms.fetch_cn_yesterday_limitup_premium(__import__("datetime").date(2026, 8, 4))
    assert out["premium"] == 1.0


def test_fetch_cn_failed_limitup_rate_darwin_raises_akshare_disabled(monkeypatch) -> None:
    monkeypatch.setattr(ms.sys, "platform", "darwin")
    out = ms.fetch_cn_failed_limitup_rate(__import__("datetime").date(2026, 8, 4))
    assert out["failed_rate"] == 0.0
    assert "akshare_disabled_on_darwin" in out["raw"].get("akshareError", "")
"""market_sentiment wave-3: tushare/akshare fetchers + compute driver."""

import datetime
import math
import sys

import pandas as pd

from data_sync_service.service import market_sentiment as ms


def test_finite_and_try_float() -> None:
    assert ms._finite_float(1.5) == 1.5
    assert ms._finite_float("x", 7.0) == 7.0
    assert ms._finite_float(math.inf) == 0.0
    assert ms._try_float("3.0") == 3.0
    assert ms._try_float(math.nan) is None
    assert ms._try_float("bad") is None


def test_realtime_pct_chg() -> None:
    assert ms._realtime_pct_chg({"pct_chg": "1.2"}) == 1.2
    assert ms._realtime_pct_chg({"price": 11.0, "pre_close": 10.0}) == 10.0
    assert ms._realtime_pct_chg({"price": 10.0}) is None
    assert ms._realtime_pct_chg({}) is None


def test_with_retry_success_and_retry(monkeypatch) -> None:
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("boom")
        return "ok"

    monkeypatch.setattr(ms, "time", type("T", (), {"sleep": staticmethod(lambda s: None)})())
    assert ms._with_retry(flaky, tries=3) == "ok"
    assert calls["n"] == 3

    def always_fail():
        raise RuntimeError("boom2")

    try:
        ms._with_retry(always_fail, tries=3)
        assert False
    except RuntimeError:
        pass


def test_tushare_yesterday_limitup_codes(monkeypatch) -> None:
    class _Pro:
        def __init__(self):
            self.calls = []

        def limit_list_d(self, **kw):
            self.calls.append(kw)
            raise TypeError("no such signature")

        def limit_list(self, **kw):
            self.calls.append(kw)
            if kw.get("trade_date") == "20260805":
                return pd.DataFrame({"ts_code": ["600000.SH", "000001.SZ"], "name": ["a", "b"]})
            return pd.DataFrame()

    pro = _Pro()
    monkeypatch.setattr(ms, "_tushare_pro", lambda: pro)
    monkeypatch.setattr(ms, "time", type("T", (), {"sleep": staticmethod(lambda s: None)})())
    y, codes = ms._tushare_yesterday_limitup_codes(datetime.date(2026, 8, 7))
    assert codes == ["600000.SH", "000001.SZ"]
    assert y == datetime.date(2026, 8, 5)


def test_tushare_yesterday_limitup_codes_none(monkeypatch) -> None:
    monkeypatch.setattr(ms, "_tushare_pro", lambda: type("P", (), {"limit_list_d": lambda **kw: None})())
    monkeypatch.setattr(ms, "time", type("T", (), {"sleep": staticmethod(lambda s: None)})())
    y, codes = ms._tushare_yesterday_limitup_codes(datetime.date(2026, 8, 7))
    assert y is None and codes == []


def test_fetch_premium_tushare(monkeypatch) -> None:
    monkeypatch.setattr(ms, "_tushare_yesterday_limitup_codes", lambda as_of: (datetime.date(2026, 8, 5), ["600000.SH", "000001.SZ"]))
    monkeypatch.setattr(ms, "_tushare_daily_pct_chg_map", lambda as_of: {"600000.SH": 5.0, "000001.SZ": 3.0, "000002.SZ": 9.0})
    out = ms.fetch_cn_yesterday_limitup_premium_tushare(datetime.date(2026, 8, 7))
    assert out["premium"] == 4.0
    assert out["count"] == 2
    assert out["raw"]["matched"] == 2

    monkeypatch.setattr(ms, "_tushare_yesterday_limitup_codes", lambda as_of: (None, []))
    out2 = ms.fetch_cn_yesterday_limitup_premium_tushare(datetime.date(2026, 8, 7))
    assert out2["premium"] == 0.0 and out2["count"] == 0


def test_fetch_cn_a_spot_change_pct(monkeypatch) -> None:
    df = pd.DataFrame({"代码": ["600000", "000001"], "涨跌幅": ["5.0%", "3.5"]})
    ak = type("AK", (), {"stock_zh_a_spot_em": staticmethod(lambda: df)})()
    monkeypatch.setattr(ms, "_akshare", lambda: ak)
    out = ms._fetch_cn_a_spot_change_pct()
    assert out == {"600000": 5.0, "000001": 3.5}


def test_fetch_cn_a_spot_change_pct_fallback_and_fail(monkeypatch) -> None:
    df = pd.DataFrame({"code": ["600000"], "change_pct": [2.0]})
    ak = type("AK", (), {
        "stock_zh_a_spot_em": staticmethod(lambda: (_ for _ in ()).throw(RuntimeError("blocked"))),
        "stock_zh_a_spot": staticmethod(lambda: df),
    })()
    monkeypatch.setattr(ms, "_akshare", lambda: ak)
    monkeypatch.setattr(ms, "time", type("T", (), {"sleep": staticmethod(lambda s: None)})())
    assert ms._fetch_cn_a_spot_change_pct() == {"600000": 2.0}

    ak2 = type("AK", (), {})()
    monkeypatch.setattr(ms, "_akshare", lambda: ak2)
    try:
        ms._fetch_cn_a_spot_change_pct()
        assert False
    except RuntimeError:
        pass


def test_fetch_premium_akshare_path(monkeypatch) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    df = pd.DataFrame({"股票代码": ["600000", "000001"], "名称": ["a", "b"]})
    ak = type("AK", (), {"stock_zt_pool_em": staticmethod(lambda date: df)})()
    monkeypatch.setattr(ms, "_akshare", lambda: ak)
    monkeypatch.setattr(ms, "_fetch_cn_a_spot_change_pct", lambda: {"600000": 4.0, "000001": 2.0})
    monkeypatch.setattr(ms, "time", type("T", (), {"sleep": staticmethod(lambda s: None)})())
    out = ms.fetch_cn_yesterday_limitup_premium(datetime.date(2026, 8, 7))
    assert out["raw"]["source"] == "akshare"
    assert out["premium"] == 3.0
    monkeypatch.undo()


def test_fetch_premium_akshare_fails_falls_back(monkeypatch) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(ms, "_akshare", lambda: type("AK", (), {})())
    monkeypatch.setattr(ms, "fetch_cn_yesterday_limitup_premium_tushare", lambda as_of: {"date": "x", "premium": 1.0, "count": 1, "raw": {}})
    out = ms.fetch_cn_yesterday_limitup_premium(datetime.date(2026, 8, 7))
    assert out["premium"] == 1.0
    assert "akshareError" in out["raw"]

    def boom(as_of):
        raise RuntimeError("tushare also down")

    monkeypatch.setattr(ms, "fetch_cn_yesterday_limitup_premium_tushare", boom)
    out2 = ms.fetch_cn_yesterday_limitup_premium(datetime.date(2026, 8, 7))
    assert out2["premium"] == 0.0
    assert out2["raw"]["source"] == "fallback"
    monkeypatch.undo()


def test_fetch_premium_darwin_uses_tushare(monkeypatch) -> None:
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(ms, "fetch_cn_yesterday_limitup_premium_tushare", lambda as_of: {"date": "x", "premium": 2.5, "count": 1, "raw": {"source": "tushare"}})
    out = ms.fetch_cn_yesterday_limitup_premium(datetime.date(2026, 8, 7))
    assert out["premium"] == 2.5
    monkeypatch.undo()


def test_failed_limitup_rate_zbgc(monkeypatch) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    df_close = pd.DataFrame({"代码": ["600000", "000001", "000002"]})
    df_failed = pd.DataFrame({"代码": ["600003", "000004"]})

    class _AK:
        def stock_zt_pool_em(self, date):
            return df_close

        def stock_zt_pool_zbgc_em(self, date):
            return df_failed

    monkeypatch.setattr(ms, "_akshare", lambda: _AK())
    out = ms.fetch_cn_failed_limitup_rate(datetime.date(2026, 8, 7))
    assert out["raw"]["method"] == "zbgc_over_zbgc_plus_close"
    assert out["failed_rate"] == 40.0  # 2 / (2+3)
    assert out["ever_count"] == 5
    monkeypatch.undo()


def test_failed_limitup_rate_strong_fallback(monkeypatch) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    df_close = pd.DataFrame({"代码": ["600000", "000001"]})
    df_ever = pd.DataFrame({"代码": ["600000", "000001", "000002", "000003"]})

    class _AK:
        def stock_zt_pool_em(self, date):
            return df_close

        def stock_zt_pool_zbgc_em(self, date):
            raise RuntimeError("no zbgc")

        def stock_zt_pool_strong_em(self, date):
            return df_ever

    monkeypatch.setattr(ms, "_akshare", lambda: _AK())
    out = ms.fetch_cn_failed_limitup_rate(datetime.date(2026, 8, 7))
    assert out["raw"]["method"] == "fallback_strong_minus_close"
    assert out["failed_rate"] == 50.0  # (4-2)/4
    monkeypatch.undo()


def test_failed_limitup_rate_fallback_default(monkeypatch) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(ms, "_akshare", lambda: type("AK", (), {})())
    out = ms.fetch_cn_failed_limitup_rate(datetime.date(2026, 8, 7))
    assert out["failed_rate"] == 0.0
    assert out["raw"]["source"] == "fallback"
    monkeypatch.undo()
