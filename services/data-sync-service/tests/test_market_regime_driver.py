"""market_regime: technical helpers + context drivers."""

from __future__ import annotations

import datetime

from data_sync_service.service import market_regime as mr


def test_ema_short_input() -> None:
    assert mr._ema([1.0, 2.0], 5) == []


def test_ema_produces_series() -> None:
    out = mr._ema([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], 3)
    assert len(out) == 4  # 6 - 3 + 1


def test_macd_histogram_short_input() -> None:
    assert mr._macd_histogram([1.0] * 30, slow=26, signal=9) == []


def test_macd_histogram_ok() -> None:
    closes = [10.0 + i * 0.1 for i in range(60)]
    hist = mr._macd_histogram(closes)
    assert isinstance(hist, list)
    assert len(hist) > 0


def test_get_trade_minutes_buckets() -> None:
    assert mr._get_trade_minutes(datetime.datetime(2026, 8, 4, 9, 0)) == 0
    assert mr._get_trade_minutes(datetime.datetime(2026, 8, 4, 10, 0)) == 30
    assert mr._get_trade_minutes(datetime.datetime(2026, 8, 4, 11, 30)) == 120
    assert mr._get_trade_minutes(datetime.datetime(2026, 8, 4, 12, 0)) == 120
    assert mr._get_trade_minutes(datetime.datetime(2026, 8, 4, 14, 0)) == 180
    assert mr._get_trade_minutes(datetime.datetime(2026, 8, 4, 15, 0)) == 240
    assert mr._get_trade_minutes(datetime.datetime(2026, 8, 4, 20, 0)) == 240


def test_estimate_full_day_volume() -> None:
    assert mr._estimate_full_day_volume(1000.0, 0) is None
    assert mr._estimate_full_day_volume(1000.0, 120) == 2000.0


def test_trade_date_from_trade_time() -> None:
    assert mr._trade_date_from_trade_time(None) is None
    assert mr._trade_date_from_trade_time("2026-08-04T10:00:00+08:00") == "2026-08-04"
    assert mr._trade_date_from_trade_time("garbage") is None


def test_hsi_series_stale() -> None:
    import datetime as dt

    today = dt.date.today().isoformat()
    assert mr._hsi_series_stale([(today, 100.0)]) is False
    assert mr._hsi_series_stale([("2020-01-01", 100.0)]) is True
    assert mr._hsi_series_stale([]) is True


def test_merge_on_demand_into_series() -> None:
    series = [("2026-08-01", 100.0), ("2026-08-03", 102.0)]
    merged = mr._merge_on_demand_into_series(
        series, {"asOfDate": "2026-08-04", "close": 104.0}
    )
    assert dict(merged)["2026-08-04"] == 104.0
    # same-day replacement
    replaced = mr._merge_on_demand_into_series(series, {"asOfDate": "2026-08-03", "close": 103.0})
    assert dict(replaced)["2026-08-03"] == 103.0
    # stale asOf → unchanged
    assert mr._merge_on_demand_into_series(series, {"asOfDate": "2020-01-01", "close": 1.0}) == series
    # missing close → unchanged
    assert mr._merge_on_demand_into_series(series, {"asOfDate": "2026-08-05"}) == series


def test_hsi_source_label() -> None:
    assert mr._hsi_source_label(used_realtime=False, on_demand_src=None) == "db.macro_daily"
    assert mr._hsi_source_label(used_realtime=True, on_demand_src=None) == "tushare.realtime_quote"
    assert mr._hsi_source_label(used_realtime=False, on_demand_src="manual") == "manual"


def test_safe_float_and_realtime_pct() -> None:
    assert mr._safe_float(None) is None
    assert mr._safe_float("12.5") == 12.5
    assert mr._safe_float("bad") is None

    assert mr._realtime_pct_or_price({"pct_chg": 1.5, "price": 10.0}) == (1.5, 10.0)
    assert mr._realtime_pct_or_price({"price": 10.0, "pre_close": 8.0}) == (25.0, 10.0)
    assert mr._realtime_pct_or_price({}) == (None, None)



def _ohlcv_rows(n: int, close: float) -> list[list[float]]:
    return [[i, 1.0, 2.0, 0.5, close] for i in range(n)]


def test_compute_breadth_with_realtime(monkeypatch) -> None:
    monkeypatch.setattr(mr, "ensure_stock_basic", lambda: None)
    monkeypatch.setattr(mr, "fetch_stock_ts_codes", lambda: ["600000.SH", "600001.SH", "000001.SZ", "000002.SZ"])
    monkeypatch.setattr(
        mr, "fetch_last_ohlcv_batch",
        lambda ts_codes, days=30, as_of=None, **kwargs: {
            "600000.SH": _ohlcv_rows(30, 10.0),
            "600001.SH": _ohlcv_rows(30, 10.0),
            "000001.SZ": _ohlcv_rows(30, 10.0),
            "000002.SZ": _ohlcv_rows(30, 10.0),
        },
    )
    monkeypatch.setattr(mr, "_is_shanghai_sync_window", lambda: True)
    monkeypatch.setattr(
        mr, "fetch_realtime_quotes_batched",
        lambda codes: [
            {"ts_code": "600000.SH", "price": 12.0, "pct_chg": 1.0},   # above ma20
            {"ts_code": "600001.SH", "price": 9.0, "pct_chg": -1.0},   # below ma20
            {"ts_code": "", "price": 99.0},                            # skipped no code
        ],
    )
    monkeypatch.setattr(mr, "_realtime_pct_or_price", lambda it: (it.get("price"), float(it["price"])))
    out = mr._compute_breadth_above_ma20_ratio(as_of_date=None)
    assert out["total"] == 4
    assert out["above_count"] == 1  # 600000 realtime above; others fall back to close == ma20
    assert out["ratio"] == 0.25


def test_compute_breadth_no_codes_and_eod_only(monkeypatch) -> None:
    monkeypatch.setattr(mr, "ensure_stock_basic", lambda: None)
    monkeypatch.setattr(mr, "fetch_stock_ts_codes", lambda: [])
    assert mr._compute_breadth_above_ma20_ratio(as_of_date="2026-08-07") == {
        "ratio": 0.0, "total": 0, "above_count": 0
    }

    monkeypatch.setattr(mr, "fetch_stock_ts_codes", lambda: ["600000.SH"])
    monkeypatch.setattr(
        mr, "fetch_last_ohlcv_batch",
        lambda ts_codes, days=30, as_of=None, **kwargs: {"600000.SH": _ohlcv_rows(30, 10.0)},
    )
    # as_of_date set → realtime window branch skipped, uses last close (10 == ma20, not above)
    out = mr._compute_breadth_above_ma20_ratio(as_of_date="2026-08-07")
    assert out["above_count"] == 0


def test_compute_breadth_short_history(monkeypatch) -> None:
    monkeypatch.setattr(mr, "ensure_stock_basic", lambda: None)
    monkeypatch.setattr(mr, "fetch_stock_ts_codes", lambda: ["600000.SH"])
    monkeypatch.setattr(
        mr, "fetch_last_ohlcv_batch",
        lambda ts_codes, days=30, as_of=None, **kwargs: {"600000.SH": _ohlcv_rows(10, 10.0)},
    )
    out = mr._compute_breadth_above_ma20_ratio(as_of_date="2026-08-07")
    assert out["total"] == 0


def test_compute_liquidity_mainline(monkeypatch) -> None:
    monkeypatch.setattr(mr, "fetch_cn_market_breadth_eod", lambda dt: {"total_turnover_cny": 1.6e12})
    monkeypatch.setattr(mr, "get_rows_by_date", lambda ds: [{"net_inflow": 6e9}, {"net_inflow": 1e9}])
    out = mr._compute_market_liquidity_and_mainline(as_of_date="2026-08-07", breadth_ratio=0.5)
    assert out["turnover_above_1_5T"] is True
    assert out["mainline_inflow_above_5B"] is True
    assert out["total_turnover_cny"] == 1.6e12
    assert out["max_industry_inflow"] == 6e9


def test_compute_liquidity_intraday_fallback(monkeypatch) -> None:
    monkeypatch.setattr(mr, "fetch_cn_market_breadth_eod", lambda dt: {})
    monkeypatch.setattr(mr, "get_rows_by_date", lambda ds: [])
    today = mr.datetime.now(tz=mr.ZoneInfo("Asia/Shanghai")).date().isoformat()

    monkeypatch.setattr(mr, "fetch_cn_market_breadth_intraday", lambda dt: {"total_turnover_cny": 1.2e12})
    out = mr._compute_market_liquidity_and_mainline(as_of_date=today, breadth_ratio=0.5)
    assert out["total_turnover_cny"] == 1.2e12
    assert out["turnover_above_1_5T"] is False

    # bad as_of_date → treated as today; eod/intraday raising → zeros
    monkeypatch.setattr(mr, "fetch_cn_market_breadth_eod", lambda dt: (_ for _ in ()).throw(RuntimeError("x")))
    monkeypatch.setattr(mr, "fetch_cn_market_breadth_intraday", lambda dt: (_ for _ in ()).throw(RuntimeError("x")))
    out2 = mr._compute_market_liquidity_and_mainline(as_of_date="not-a-date", breadth_ratio=0.5)
    assert out2["total_turnover_cny"] == 0.0


def test_quote_error_message() -> None:
    assert mr._quote_error_message(None) == "invalid_quote_response"
    assert mr._quote_error_message({}) is None
    assert mr._quote_error_message({"error": "  boom  "}) == "boom"
    assert mr._quote_error_message({"error": ""}) is None


def test_fetch_realtime_quote_map_batch_ok(monkeypatch) -> None:
    monkeypatch.setattr(
        mr, "fetch_realtime_quotes",
        lambda codes: {"ok": True, "items": [{"ts_code": "600000.SH", "price": 12.0}]},
    )
    quotes, errors = mr._fetch_realtime_quote_map(["600000.SH"])
    assert quotes["600000.SH"]["price"] == 12.0
    assert errors == {}


def test_fetch_realtime_quote_map_partial_fallback(monkeypatch) -> None:
    def fake_fetch(codes: list[str]) -> dict:
        return {"ok": True, "items": [{"ts_code": codes[0], "price": 1.0}]}

    monkeypatch.setattr(mr, "fetch_realtime_quotes", fake_fetch)
    monkeypatch.setattr(mr, "time", type("T", (), {"sleep": staticmethod(lambda s: None)})())
    quotes, errors = mr._fetch_realtime_quote_map(["600000.SH", "600001.SH"])
    assert set(quotes) == {"600000.SH", "600001.SH"}
    assert errors == {}


def test_fetch_realtime_quote_map_batch_failed(monkeypatch) -> None:
    def fake_fetch(codes: list[str]) -> dict:
        if codes == ["600000.SH", "600001.SH"]:
            return {"ok": False, "error": "batch boom"}
        if codes == ["600000.SH"]:
            return {"ok": True, "items": [{"ts_code": "600000.SH", "price": 1.0}]}
        return {"ok": True, "items": []}

    monkeypatch.setattr(mr, "fetch_realtime_quotes", fake_fetch)
    monkeypatch.setattr(mr, "time", type("T", (), {"sleep": staticmethod(lambda s: None)})())
    quotes, errors = mr._fetch_realtime_quote_map(["600000.SH", "600001.SH"])
    assert set(quotes) == {"600000.SH"}
    assert errors["600001.SH"] == "batch boom"  # per-code ok but empty; batch error reported


def test_fetch_realtime_quote_map_empty_codes() -> None:
    quotes, errors = mr._fetch_realtime_quote_map([])
    assert quotes == {} and errors == {}


def test_macd_histogram_short_and_normal() -> None:
    assert mr._macd_histogram([1.0, 2.0, 3.0]) == []
    closes = [float(i) for i in range(1, 120)]
    hist = mr._macd_histogram(closes)
    assert len(hist) == 86
