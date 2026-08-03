from datetime import date
from unittest.mock import patch

import data_sync_service.service.market_sentiment as ms


def test_intraday_breadth_cache_skips_second_quote_fetch() -> None:
    ms._INTRADAY_BREADTH_CACHE.clear()
    calls = {"n": 0}

    def _quotes(_part):
        calls["n"] += 1
        return {"ok": True, "items": [{"pct_chg": "1", "volume": "100", "amount": "1000"}]}

    with patch.object(ms, "fetch_stock_ts_codes", return_value=["000001.SZ"] * 60):
        with patch.object(ms, "fetch_realtime_quotes", side_effect=_quotes):
            d = date(2026, 6, 18)
            first = ms.fetch_cn_market_breadth_intraday(d)
            second = ms.fetch_cn_market_breadth_intraday(d)
    assert first["date"] == "2026-06-18"
    assert second["date"] == "2026-06-18"
    assert calls["n"] == 2
