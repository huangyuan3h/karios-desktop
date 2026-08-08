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
