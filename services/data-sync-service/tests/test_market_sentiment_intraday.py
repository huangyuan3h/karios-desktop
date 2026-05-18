from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


def test_fetch_cn_market_breadth_intraday_aggregates(monkeypatch) -> None:
    import data_sync_service.service.market_sentiment as ms  # type: ignore[import-not-found]

    monkeypatch.setattr(ms, "fetch_ts_codes", lambda: ["000001.SZ", "000002.SZ"])
    monkeypatch.setattr(
        ms,
        "fetch_realtime_quotes",
        lambda codes: {
            "ok": True,
            "items": [
                {"ts_code": "000001.SZ", "price": "10.12", "pre_close": "10.00", "volume": "100", "amount": "1200"},
                {"ts_code": "000002.SZ", "price": "9.94", "pre_close": "10.00", "volume": "200", "amount": "1800"},
            ],
        },
    )

    out = ms.fetch_cn_market_breadth_intraday(date(2026, 2, 24))
    assert out["up_count"] == 1
    assert out["down_count"] == 1
    assert out["flat_count"] == 0
    assert out["total_count"] == 2
    assert out["total_turnover_cny"] == 3000.0
    assert out["total_volume"] == 300.0
    assert out["raw"]["source"] == "tushare.realtime_quote"


def test_avg_pct_chg_from_realtime_fallback(monkeypatch) -> None:
    import data_sync_service.service.market_sentiment as ms  # type: ignore[import-not-found]

    monkeypatch.setattr(
        ms,
        "fetch_realtime_quotes",
        lambda codes: {
            "ok": True,
            "items": [
                {"ts_code": "000001.SZ", "price": "10.10", "pre_close": "10.00"},
                {"ts_code": "000002.SZ", "price": "9.80", "pre_close": "10.00"},
            ],
        },
    )

    avg, matched = ms._avg_pct_chg_from_realtime(["000001.SZ", "000002.SZ"])
    assert matched == 2
    assert round(avg, 2) == -0.5


def test_compute_sentiment_uses_intraday_when_eod_empty(monkeypatch) -> None:
    import data_sync_service.service.market_sentiment as ms  # type: ignore[import-not-found]

    today_cn = datetime.now(tz=ZoneInfo("Asia/Shanghai")).date()

    monkeypatch.setattr(
        ms,
        "fetch_cn_market_breadth_eod",
        lambda d: {
            "date": d.isoformat(),
            "up_count": 0,
            "down_count": 0,
            "flat_count": 0,
            "total_count": 0,
            "up_down_ratio": 0.0,
            "total_turnover_cny": 0.0,
            "total_volume": 0.0,
            "raw": {"source": "tushare.daily"},
        },
    )
    monkeypatch.setattr(
        ms,
        "fetch_cn_market_breadth_intraday",
        lambda d: {
            "date": d.isoformat(),
            "up_count": 10,
            "down_count": 5,
            "flat_count": 1,
            "total_count": 16,
            "up_down_ratio": 2.0,
            "total_turnover_cny": 1000.0,
            "total_volume": 2000.0,
            "raw": {"source": "tushare.realtime_quote"},
        },
    )
    monkeypatch.setattr(ms, "_prev_open_date", lambda exchange, d0: d0 - timedelta(days=1))
    monkeypatch.setattr(ms, "_close_limit_up_pool_codes", lambda d0: ["000001.SZ"])
    monkeypatch.setattr(ms, "_avg_pct_chg_from_db", lambda d0, pool: (0.0, 0))
    monkeypatch.setattr(ms, "_avg_pct_chg_from_realtime", lambda pool: (1.5, 1))
    monkeypatch.setattr(ms, "_failed_limitup_rate_from_db", lambda d0: (0.0, 0, 0))

    out = ms.compute_cn_sentiment_for_date(today_cn.isoformat())
    assert out["up"] == 10
    assert out["down"] == 5
    assert out["marketTurnoverCny"] == 1000.0
    assert out["raw"]["breadth"]["raw"]["source"] == "tushare.realtime_quote"
