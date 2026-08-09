"""fetch_cn_market_breadth_eod / intraday driver tests (mocked tushare)."""

from __future__ import annotations

import pandas as pd

from data_sync_service.service import market_sentiment as ms


class _FakePro:
    def __init__(self, df) -> None:
        self._df = df

    def daily(self, **kw):
        if self._df is None:
            return None
        df = self._df
        self._df = None
        return df


def _daily_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "600000.SH", "000002.SZ"],
            "pct_chg": [1.5, -0.5, 0.0],
            "vol": [1000.0, 2000.0, 3000.0],
            "amount": [100.0, 200.0, 300.0],
        }
    )


def test_fetch_eod_counts_up_down_flat(monkeypatch) -> None:
    pro = _FakePro(_daily_df())
    monkeypatch.setattr(ms, "_tushare_pro", lambda: pro)
    monkeypatch.setattr(ms, "_safe_trade_date", lambda d: "20260701")
    out = ms.fetch_cn_market_breadth_eod(__import__("datetime").date(2026, 7, 1))
    assert out["up_count"] == 1
    assert out["down_count"] == 1
    assert out["flat_count"] == 1
    assert out["total_count"] == 3
    assert out["up_down_ratio"] == 1.0
    # amount in thousand RMB → CNY
    assert abs(out["total_turnover_cny"] - (100 + 200 + 300) * 1000.0) < 1.0
    assert out["raw"]["source"] == "tushare.daily"


def test_fetch_eod_empty_provider(monkeypatch) -> None:
    monkeypatch.setattr(ms, "_tushare_pro", lambda: _FakePro(None))
    monkeypatch.setattr(ms, "_safe_trade_date", lambda d: "20260701")
    out = ms.fetch_cn_market_breadth_eod(__import__("datetime").date(2026, 7, 1))
    assert out["total_count"] == 0
    assert out["up_down_ratio"] == 0.0


def test_fetch_eod_bad_pct_values_counted_as_flat(monkeypatch) -> None:
    df = pd.DataFrame(
        {"ts_code": ["A", "B"], "pct_chg": ["bad", None], "vol": ["x", "y"], "amount": ["z", None]}
    )
    monkeypatch.setattr(ms, "_tushare_pro", lambda: _FakePro(df))
    monkeypatch.setattr(ms, "_safe_trade_date", lambda d: "20260701")
    out = ms.fetch_cn_market_breadth_eod(__import__("datetime").date(2026, 7, 1))
    assert out["flat_count"] == 2
    assert out["total_turnover_cny"] == 0.0


def test_with_retry_retries_then_raises(monkeypatch) -> None:
    import time

    monkeypatch.setattr(time, "sleep", lambda *a, **k: None)
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        raise RuntimeError("boom")

    import pytest

    with pytest.raises(RuntimeError):
        ms._with_retry(flaky, tries=2, base_sleep_s=0.0)
    assert calls["n"] == 2


def test_with_retry_succeeds_second_try(monkeypatch) -> None:
    import time

    monkeypatch.setattr(time, "sleep", lambda *a, **k: None)
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        return "ok"

    assert ms._with_retry(flaky, tries=3, base_sleep_s=0.0) == "ok"
    assert calls["n"] == 2


def test_parse_money_to_cny() -> None:
    assert ms._parse_money_to_cny(123) == 123.0
    assert ms._parse_money_to_cny("1.2万") == 12000.0
    assert ms._parse_money_to_cny("3亿") == 300000000.0
    assert ms._parse_money_to_cny("bad") == 0.0


def test_fetch_intraday_empty_universe(monkeypatch) -> None:
    ms._INTRADAY_BREADTH_CACHE.clear()
    monkeypatch.setattr(ms, "fetch_stock_ts_codes", lambda: [])
    out = ms.fetch_cn_market_breadth_intraday(__import__("datetime").date(2026, 7, 1))
    assert out["total_count"] == 0
    assert out["raw"]["requested"] == 0


def test_fetch_intraday_counts_from_realtime(monkeypatch) -> None:
    import datetime

    d = datetime.date(2026, 7, 1)
    ms._INTRADAY_BREADTH_CACHE.clear()
    monkeypatch.setattr(
        ms, "fetch_stock_ts_codes", lambda: [f"00000{i}.SZ" for i in range(3)]
    )
    monkeypatch.setattr(
        ms,
        "fetch_realtime_quotes",
        lambda part: {
            "ok": True,
            "items": [
                {"pct_chg": 1.0, "volume": 100.0, "amount": 200.0},
                {"pct_chg": -1.0, "volume": 100.0, "amount": 200.0},
                {"pct_chg": 0.0, "volume": 100.0, "amount": 200.0},
                {"volume": None, "amount": None},  # pct missing → no count
            ],
        },
    )
    out = ms.fetch_cn_market_breadth_intraday(d)
    assert out["up_count"] == 1
    assert out["down_count"] == 1
    assert out["flat_count"] == 1
    assert out["total_count"] == 3
    assert out["raw"]["source"] == "tushare.realtime_quote"


def test_fetch_intraday_uses_cache(monkeypatch) -> None:
    import datetime

    d = datetime.date(2026, 7, 1)
    monkeypatch.setattr(
        ms, "fetch_stock_ts_codes", lambda: ["000001.SZ"]
    )
    calls = {"n": 0}

    def fake_realtime(part):
        calls["n"] += 1
        return {"ok": True, "items": [{"pct_chg": 1.0, "volume": 1.0, "amount": 1.0}]}

    monkeypatch.setattr(ms, "fetch_realtime_quotes", fake_realtime)
    ms._INTRADAY_BREADTH_CACHE.clear()
    first = ms.fetch_cn_market_breadth_intraday(d)
    second = ms.fetch_cn_market_breadth_intraday(d)
    assert first["up_count"] == 1
    assert second is first  # cache hit
    assert calls["n"] == 1
