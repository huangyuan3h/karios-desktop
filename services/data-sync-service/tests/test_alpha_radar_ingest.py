"""alpha_radar_ingest: config, RSS fetch, drivers."""

from __future__ import annotations

import urllib.request

from data_sync_service.service import alpha_radar_ingest as ai


def test_config_getters(monkeypatch) -> None:
    monkeypatch.setenv("JINA_API_KEY", "  abc  ")
    assert ai.jina_api_key() == "abc"

    monkeypatch.setenv("ALPHA_RADAR_ENRICH_FULLTEXT", "1")
    assert ai.enrich_fulltext_enabled() is True
    monkeypatch.setenv("ALPHA_RADAR_ENRICH_FULLTEXT", "no")
    assert ai.enrich_fulltext_enabled() is False

    monkeypatch.setenv("ALPHA_RADAR_MAX_ITEMS_PER_SOURCE", "9")
    assert ai.max_items_per_source() == 9
    monkeypatch.setenv("ALPHA_RADAR_MAX_ITEMS_PER_SOURCE", "999")
    assert ai.max_items_per_source() == 50
    monkeypatch.setenv("ALPHA_RADAR_MAX_ITEMS_PER_SOURCE", "x")
    assert ai.max_items_per_source() == 5

    monkeypatch.setenv("ALPHA_RADAR_FULLTEXT_MAX_PER_SOURCE", "4")
    assert ai.fulltext_max_per_priority_source() == 4

    monkeypatch.setenv("ALPHA_RADAR_RSS_TIMEOUT", "30")
    assert ai.rss_timeout_seconds() == 30
    monkeypatch.setenv("ALPHA_RADAR_RSS_TIMEOUT", "500")
    assert ai.rss_timeout_seconds() == 120
    monkeypatch.setenv("ALPHA_RADAR_RSS_TIMEOUT", "bad")
    assert ai.rss_timeout_seconds() == 60


def test_proxy_url(monkeypatch) -> None:
    monkeypatch.delenv("https_proxy", raising=False)
    monkeypatch.delenv("HTTPS_PROXY", raising=False)
    monkeypatch.delenv("http_proxy", raising=False)
    monkeypatch.delenv("HTTP_PROXY", raising=False)
    assert ai._proxy_url() is None
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy:7890")
    assert ai._proxy_url() == "http://proxy:7890"


def test_build_opener_uses_proxy(monkeypatch) -> None:
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:7890")
    opener = ai._build_opener()
    assert opener is not None
    monkeypatch.delenv("HTTPS_PROXY")


def test_opener_cached(monkeypatch) -> None:
    ai._OPENER = None
    a = ai._opener()
    b = ai._opener()
    assert a is b
    ai._OPENER = None


def test_rsshub_base_and_chinese_url(monkeypatch) -> None:
    monkeypatch.delenv("ALPHA_RADAR_RSSHUB_BASE_URL", raising=False)
    monkeypatch.delenv("ALPHA_RADAR_RSS_GOV_NDRC", raising=False)
    assert ai.rsshub_base_url() == "http://127.0.0.1:1200"
    assert ai._chinese_source_url("ndrc", "ALPHA_RADAR_RSS_GOV_NDRC") == "http://127.0.0.1:1200/ndrc"
    monkeypatch.setenv("ALPHA_RADAR_RSS_GOV_NDRC", "http://override:8888/x")
    assert ai._chinese_source_url("ndrc", "ALPHA_RADAR_RSS_GOV_NDRC") == "http://override:8888/x"


def test_add_default_sources_seeds(monkeypatch) -> None:
    created: list[tuple] = []
    monkeypatch.setattr(ai, "ensure_tables", lambda: None)

    def fake_create(**kw):
        created.append((kw["source_id"], kw["category"]))

    monkeypatch.setattr(ai, "create_source", fake_create)
    ai.add_default_sources()
    ids = [c[0] for c in created]
    assert "eastmoney" in ids or len(created) > 0


def test_urlopen_uses_opener(monkeypatch) -> None:
    class _Resp:
        pass

    monkeypatch.setattr(ai, "_opener", lambda: type("O", (), {"open": lambda self, req, timeout: _Resp()})())
    req = urllib.request.Request("http://example.com")
    assert ai._urlopen(req, timeout=10) is not None


import hashlib  # noqa: E402


class _FakeResp:
    def __init__(self, data: bytes, code: int = 200):
        self._data = data
        self.code = code
        self.fp = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return self._data

    def __call__(self, req, timeout):
        return self


class _Err:
    def __init__(self, code: int, body: str = "boom"):
        self.code = code
        self.fp = None
        self.body = body

    def read(self):
        return self.body.encode()

    def __str__(self):
        return f"HTTP {self.code}"


def _http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("http://x/", code, "err", {}, None)


def _fake_entries():
    parsed = type(
        "P",
        (),
        {
            "entries": [
                {
                    "title": "标题A",
                    "link": "http://a.example/x/1",
                    "summary": "<p>sum one</p>",
                    "content": [],
                    "published_parsed": None,
                },
                {
                    "title": "no link",
                    "link": "",
                    "summary": "",
                    "content": [],
                },
            ]
        },
    )()
    return parsed


def test_strip_html() -> None:
    assert ai._strip_html("<p>a<b>b</b>  c</p>") == "a b c"
    assert ai._strip_html("plain") == "plain"


def test_entry_summary_from_content_blocks() -> None:
    class E:
        def get(self, k, d=None):
            return {
                "summary": "s",
                "description": "d",
                "content": [{"value": "v1"}, {"value": "  "}, "raw"],
                "content_encoded": "<i>enc</i>",
            }.get(k, d)

    out = ai._entry_summary(E())
    assert "s" in out and "v1" in out and "enc" in out
    assert ai._entry_summary(type("X", (), {"get": lambda self, k, d=None: None})()) is None


def test_quote_url() -> None:
    assert ai._quote_url("http://h/eastmoney/search/铜") == "http://h/eastmoney/search/%E9%93%9C"
    assert ai._quote_url("http://h/a?x=1") == "http://h/a?x=1"
    assert ai._quote_url("http://h/%E9%93%9C") == "http://h/%E9%93%9C"


def test_fetch_rss_feed(monkeypatch) -> None:
    items = []
    for _, entry in enumerate(_fake_entries().entries):
        items.append(entry)

    def fake_feedparser_parse(raw):
        return _fake_entries()

    monkeypatch.setattr(ai, "feedparser", type("F", (), {"parse": staticmethod(fake_feedparser_parse)})())
    monkeypatch.setattr(ai, "_urlopen", _FakeResp(b"<rss/>"))
    out = ai.fetch_rss_feed("http://h/feed")
    assert len(out) == 1
    assert out[0]["id"] == hashlib.md5(b"http://a.example/x/1").hexdigest()[:16]
    assert out[0]["title"] == "标题A"
    assert out[0]["summary"] == "sum one"


def test_fetch_rss_feed_http_error(monkeypatch) -> None:
    monkeypatch.setattr(ai, "feedparser", type("F", (), {"parse": lambda raw: None})())
    monkeypatch.setattr(ai, "_urlopen", lambda req, timeout: (_ for _ in ()).throw(_http_error(500)))
    try:
        ai.fetch_rss_feed("http://h/feed")
        raise AssertionError()
    except RuntimeError as exc:
        assert "HTTP 500" in str(exc)


def test_is_youtube_url() -> None:
    assert ai._is_youtube_url("https://www.youtube.com/watch?v=x")
    assert ai._is_youtube_url("http://youtube.com/x")
    assert not ai._is_youtube_url("https://example.com/youtube")


def test_fetch_jina_markdown_retries(monkeypatch) -> None:
    calls = {"n": 0}

    def fake_urlopen(req, timeout):
        calls["n"] += 1
        raise _http_error(429)

    monkeypatch.setattr(ai, "jina_api_key", lambda: "")
    monkeypatch.setattr(ai, "_proxy_url", lambda: "http://proxy")
    monkeypatch.setattr(ai, "_urlopen", fake_urlopen)
    monkeypatch.setattr(ai, "time", type("T", (), {"sleep": staticmethod(lambda s: None)})())
    assert ai.fetch_jina_markdown("http://x/y") is None
    assert calls["n"] == 2  # retries=2 → 2 total attempts


def test_fetch_jina_markdown_ok(monkeypatch) -> None:
    monkeypatch.setattr(ai, "jina_api_key", lambda: "")
    monkeypatch.setattr(ai, "_proxy_url", lambda: None)
    monkeypatch.setattr(ai, "_urlopen", _FakeResp(b"  md body  "))
    out = ai.fetch_jina_markdown("http://x/y")
    assert out == "md body"


def test_fetch_jina_markdown_breaks_on_4xx(monkeypatch) -> None:
    calls = {"n": 0}

    def fake_urlopen(req, timeout):
        calls["n"] += 1
        raise _http_error(403)

    monkeypatch.setattr(ai, "jina_api_key", lambda: "")
    monkeypatch.setattr(ai, "_proxy_url", lambda: "http://proxy")
    monkeypatch.setattr(ai, "_urlopen", fake_urlopen)
    assert ai.fetch_jina_markdown("http://x/y") is None
    assert calls["n"] == 1


def test_fetch_one_alpha_source_full_flow(monkeypatch) -> None:
    from threading import Semaphore

    monkeypatch.setattr(ai, "fetch_rss_feed", lambda url: [
        {"id": "i1", "title": "t1", "link": "http://a/1", "summary": "short", "published_at": None},
        {"id": "i2", "title": "t2", "link": "http://a/2", "summary": "x" * 500, "published_at": None},
    ])
    monkeypatch.setattr(ai, "max_items_per_source", lambda: 50)
    monkeypatch.setattr(ai, "filter_feed_items", lambda items, source_id: (items, {"filteredOut": 0}))
    monkeypatch.setattr(ai, "fulltext_max_per_priority_source", lambda: 2)
    monkeypatch.setattr(ai, "_needs_jina_fallback", lambda s, min_chars=280: len(s) < 100)
    monkeypatch.setattr(ai, "fetch_jina_markdown", lambda url: "FULL TEXT")
    monkeypatch.setattr(ai, "upsert_document", lambda **kw: {"_inserted": True})
    monkeypatch.setattr(ai, "update_source_last_fetch", lambda sid, at: None)

    out = ai._fetch_one_alpha_source(
        {"id": "stratechery", "url": "http://h/feed", "category": "priority"},
        fetched_at="2026-08-08T00:00:00+00:00",
        use_fulltext=True,
        apply_filter=True,
        force_reprocess=False,
        jina_semaphore=Semaphore(2),
    )
    assert out["stored"] == 2
    assert out["ingest_new"] == 2
    assert out["fulltext_attempted"] == 1  # only the short-summary item
    assert out["fulltext_ok"] == 1
    assert out["count"] == 2


def test_fetch_one_alpha_source_requeued_and_error(monkeypatch) -> None:
    def fake_upsert(**kw):
        return {"_requeued": True}

    monkeypatch.setattr(ai, "fetch_rss_feed", lambda url: [
        {"id": "i1", "title": "t1", "link": "http://a/1", "summary": None, "published_at": None},
    ])
    monkeypatch.setattr(ai, "max_items_per_source", lambda: 50)
    monkeypatch.setattr(ai, "filter_feed_items", lambda items, source_id: (items, {"filteredOut": 0}))
    monkeypatch.setattr(ai, "upsert_document", fake_upsert)
    monkeypatch.setattr(ai, "update_source_last_fetch", lambda sid, at: None)
    out = ai._fetch_one_alpha_source(
        {"id": "stratechery", "url": "http://h/feed", "category": "priority"},
        fetched_at="2026-08-08T00:00:00+00:00",
        use_fulltext=False,
        apply_filter=False,
        force_reprocess=False,
        jina_semaphore=None,
    )
    assert out["ingest_requeued"] == 1

    monkeypatch.setattr(ai, "fetch_rss_feed", lambda url: (_ for _ in ()).throw(RuntimeError("feed down")))
    out2 = ai._fetch_one_alpha_source(
        {"id": "stratechery", "url": "http://h/feed", "category": "priority"},
        fetched_at="x", use_fulltext=False, apply_filter=True,
        force_reprocess=False, jina_semaphore=None,
    )
    assert out2["error"] == "feed down"
    assert out2["count"] == -1


def test_fetch_all_sources_aggregates(monkeypatch) -> None:

    monkeypatch.setattr(ai, "ensure_tables", lambda: None)
    monkeypatch.setattr(ai, "add_default_sources", lambda: None)
    monkeypatch.setattr(ai, "enrich_fulltext_enabled", lambda: True)
    monkeypatch.setattr(ai, "fetch_sources", lambda enabled_only: [{"id": "s1"}, {"id": "s2"}])
    monkeypatch.setattr(ai, "fetch_jina_markdown", lambda url: None)

    def fake_one(source, **kw):
        return {
            "source_id": source["id"],
            "count": 3 if source["id"] == "s1" else -1,
            "error": None if source["id"] == "s1" else "boom",
            "fetched": 3,
            "filtered_out": 1,
            "stored": 2,
            "ingest_new": 1,
            "ingest_requeued": 1,
            "ingest_unchanged": 0,
            "fulltext_attempted": 1,
            "fulltext_ok": 0,
        }

    monkeypatch.setattr(ai, "_fetch_one_alpha_source", fake_one)
    out = ai.fetch_all_sources()
    assert out["results"] == {"s1": 3, "s2": -1}
    assert out["sourceErrors"]["s2"] == "boom"
    assert out["fullTextMode"] == "priority"
    assert out["ingestStats"]["new"] == 2
    assert out["ingestStats"]["requeued"] == 2
    assert out["ingestStats"]["fetched"] == 6
