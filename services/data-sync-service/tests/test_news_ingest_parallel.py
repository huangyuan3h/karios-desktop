import pytest

pytestmark = pytest.mark.requires_postgres

from unittest.mock import patch

import data_sync_service.service.news as news_mod


def test_fetch_all_sources_runs_parallel() -> None:
    inflight = {"n": 0, "max": 0}

    def _fetch_rss(_url: str):
        inflight["n"] += 1
        inflight["max"] = max(inflight["max"], inflight["n"])
        try:
            import time

            time.sleep(0.05)
            return [{"id": "1", "title": "t", "link": "l", "summary": "s", "published_at": "p"}]
        finally:
            inflight["n"] -= 1

    sources = [{"id": f"s{i}", "url": f"http://x/{i}"} for i in range(6)]

    with patch.object(news_mod, "fetch_sources", return_value=sources):
        with patch.object(news_mod, "fetch_rss_feed", side_effect=_fetch_rss):
            with patch.object(news_mod, "upsert_item"):
                with patch.object(news_mod, "update_source_last_fetch"):
                    with patch.object(news_mod, "delete_old_items"):
                        news_mod.fetch_all_sources()

    assert inflight["max"] > 1
