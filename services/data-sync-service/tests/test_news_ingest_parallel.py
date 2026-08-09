
from unittest.mock import patch

import data_sync_service.service.alpha_radar_ingest as alpha_ingest
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


def test_quote_url_encodes_non_ascii_path() -> None:
    """Chinese RSSHub routes must be percent-encoded before urllib — the
    raw URL raises "'ascii' codec can't encode character"."""
    url = "http://127.0.0.1:1200/eastmoney/search/铜?key=值"
    quoted = alpha_ingest._quote_url(url)
    assert quoted == "http://127.0.0.1:1200/eastmoney/search/%E9%93%9C?key=%E5%80%BC"


def test_quote_url_preserves_already_encoded_url() -> None:
    url = "https://example.com/a%20b/%E9%93%9C?x=1&y=2"
    assert alpha_ingest._quote_url(url) == url


def test_quote_url_leaves_ascii_url_untouched() -> None:
    url = "https://stratechery.com/feed/"
    assert alpha_ingest._quote_url(url) == url
