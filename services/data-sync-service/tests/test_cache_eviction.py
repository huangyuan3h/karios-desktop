from __future__ import annotations

from cachetools import TTLCache

from data_sync_service.service import market_sentiment as ms
from data_sync_service.service import trendok as trendok_mod


def test_intraday_breadth_cache_is_bounded_ttl_cache() -> None:
    assert isinstance(ms._INTRADAY_BREADTH_CACHE, TTLCache)
    assert ms._INTRADAY_BREADTH_CACHE.maxsize == 64


def test_trendok_cache_is_bounded_ttl_cache() -> None:
    assert isinstance(trendok_mod._trendok_cache, TTLCache)
    assert trendok_mod._trendok_cache.maxsize == 128


def test_trendok_cache_evicts_when_maxsize_exceeded() -> None:
    cache = TTLCache(maxsize=2, ttl=60.0)
    cache["a"] = [1]
    cache["b"] = [2]
    cache["c"] = [3]
    assert len(cache) <= 2
