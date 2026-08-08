"""news_enrich driver coverage: _call_llm, enrich_batch, cycle, filtering."""

from __future__ import annotations

import json
import types
import urllib.error
from unittest.mock import patch

import pytest

from data_sync_service.service import news_enrich as ne


def _settings(**kw) -> types.SimpleNamespace:
    d = {"ai_service_base_url": "http://ai:4310", "karios_api_keys": ()}
    d.update(kw)
    return types.SimpleNamespace(**d)


def _patch_settings(monkeypatch, **kw) -> None:
    monkeypatch.setattr(ne, "get_settings", lambda: _settings(**kw))


def _items(n: int = 2) -> list[dict]:
    return [{"id": f"i{i}", "title": "美联储宣布利率决议", "summary": "s"} for i in range(n)]


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode()


class _Ctx:
    def __init__(self, resp) -> None:
        self._resp = resp

    def __enter__(self):
        return self._resp

    def __exit__(self, *a):
        return None


def _patch_urlopen(monkeypatch, payload: dict | None = None, exc: Exception | None = None) -> None:
    def fake(req, timeout):
        if exc is not None:
            raise exc
        return _Ctx(_Resp(payload or {"choices": [{"message": {"content": "[]"}}]}))

    monkeypatch.setattr(ne.urllib.request, "urlopen", fake)


# ---- _call_llm -------------------------------------------------------------

def test_call_llm_success(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    _patch_urlopen(monkeypatch, {"choices": [{"message": {"content": '[{"id":"i1"}]'}}]})
    out = ne._call_llm("prompt")
    assert out == '[{"id":"i1"}]'


def test_call_llm_http_error(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    _patch_urlopen(monkeypatch, exc=urllib.error.HTTPError("u", 500, "boom", {}, None))
    with pytest.raises(RuntimeError, match="LLM call failed"):
        ne._call_llm("p")


def test_call_llm_url_error(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    _patch_urlopen(monkeypatch, exc=urllib.error.URLError("no route"))
    with pytest.raises(RuntimeError, match="LLM call failed"):
        ne._call_llm("p")


def test_call_llm_timeout(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    _patch_urlopen(monkeypatch, exc=TimeoutError("hung"))
    with pytest.raises(RuntimeError, match="LLM call failed"):
        ne._call_llm("p")


def test_call_llm_bad_json_body(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    _patch_urlopen(monkeypatch, {"weird": True})
    assert ne._call_llm("p") == ""  # missing choices chain -> "" safely


def test_call_llm_sends_api_key(monkeypatch) -> None:
    _patch_settings(monkeypatch, karios_api_keys=("sekrit",))
    captured: list = []

    def fake(req, timeout):
        captured.append(req)
        return _Ctx(_Resp({"choices": [{"message": {"content": "ok"}}]}))

    monkeypatch.setattr(ne.urllib.request, "urlopen", fake)
    ne._call_llm("p")
    assert captured[0].get_header("Authorization") == "Bearer sekrit"
    assert captured[0].get_header("Content-type") == "application/json"  # urllib capitalizes header key


# ---- enrich_batch ----------------------------------------------------------

def test_enrich_batch_empty() -> None:
    assert ne.enrich_batch([]) == {"enriched": 0, "failed": 0, "filtered": 0}


def test_enrich_batch_llm_failure_marks_all_failed(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    monkeypatch.setattr(ne, "_call_llm", lambda p: (_ for _ in ()).throw(RuntimeError("net down")))
    calls: list[dict] = []
    monkeypatch.setattr(ne, "update_item_enrichment", lambda **kw: calls.append(kw))
    out = ne.enrich_batch(_items(3))
    assert out == {"enriched": 0, "failed": 3, "filtered": 0}
    assert all(c["enrichment_status"] == "failed" for c in calls)
    assert len(calls) == 3


def test_enrich_batch_success(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    raw = json.dumps(
        [
            {"tickers": ["600519"], "sectors": ["白酒"], "eventType": "earnings", "importance": 3,
             "aiSummary": "茅台业绩超预期", "actionability": "actionable"},
            {"tickers": [], "sectors": [], "eventType": "macro", "importance": 2,
             "aiSummary": "央行降准", "actionability": "informational"},
        ]
    )
    monkeypatch.setattr(ne, "_call_llm", lambda p: raw)
    writes: list[dict] = []
    monkeypatch.setattr(ne, "update_item_enrichment", lambda **kw: writes.append(kw) or True)
    out = ne.enrich_batch(_items(2))
    assert out["enriched"] == 2 and out["failed"] == 0
    assert writes[0]["importance"] == 3
    assert writes[0]["tickers"] == ["600519"]
    assert writes[0]["enrichment_status"] == "done"


def test_enrich_batch_single_item_padded(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    monkeypatch.setattr(ne, "_call_llm", lambda p: '[{"tickers": []}]')
    monkeypatch.setattr(ne, "update_item_enrichment", lambda **kw: True)
    out = ne.enrich_batch(_items(1))
    assert out["enriched"] == 1 and out["failed"] == 0


def test_enrich_batch_validation_error_isolated(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    monkeypatch.setattr(ne, "_call_llm", lambda p: '[{"tickers": []},{"tickers": []}]')

    def boom(entry):
        if entry.get("id") == "i0":
            raise ValueError("bad entry")
        return {
            "tickers": [], "sectors": [], "event_type": "other", "importance": 2,
            "relevance_score": 30, "ai_summary": "x", "actionability": "informational",
        }

    monkeypatch.setattr(ne, "_validate_entry", boom)
    monkeypatch.setattr(ne, "update_item_enrichment", lambda **kw: True)
    out = ne.enrich_batch(_items(2))
    assert out["enriched"] == 1
    assert out["failed"] == 1


def test_enrich_batch_update_false_counts_failed(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    monkeypatch.setattr(ne, "_call_llm", lambda p: '[{"tickers": []}]')
    monkeypatch.setattr(ne, "update_item_enrichment", lambda **kw: False)
    out = ne.enrich_batch(_items(1))
    assert out["enriched"] == 0 and out["failed"] == 1


# ---- _filter_pending -------------------------------------------------------

def test_filter_pending_mixed(monkeypatch) -> None:
    kept_calls: list[dict] = []
    monkeypatch.setattr(ne, "update_item_enrichment", lambda **kw: kept_calls.append(kw))
    items = [
        {"id": "a", "title": "美联储加息", "summary": ""},  # passes
        {"id": "b", "title": "本周回顾", "summary": ""},  # noise -> filtered
        {"id": "c", "title": "娱乐新闻", "summary": ""},  # no include -> filtered
    ]
    kept, filtered = ne._filter_pending(items)
    assert [k["id"] for k in kept] == ["a"]
    assert filtered == 2
    assert kept_calls[0]["importance"] == 0
    assert kept_calls[0]["enrichment_status"] == "done"
    assert kept_calls[0]["enrichment_model"] == "prefilter"


def test_filter_pending_all_kept(monkeypatch) -> None:
    monkeypatch.setattr(ne, "update_item_enrichment", lambda **kw: None)
    items = [{"id": "a", "title": "TSMC 财报", "summary": ""}, {"id": "b", "title": "央行降准", "summary": ""}]
    kept, filtered = ne._filter_pending(items)
    assert len(kept) == 2 and filtered == 0


# ---- run_enrichment_cycle --------------------------------------------------

def test_cycle_no_pending(monkeypatch) -> None:
    monkeypatch.setattr(ne, "fetch_pending_enrichment", lambda limit: [])
    out = ne.run_enrichment_cycle()
    assert out["batchesProcessed"] == 0
    assert out["totalFiltered"] == 0


def test_cycle_all_prefiltered(monkeypatch) -> None:
    pending = iter([[{"id": "a", "title": "本周回顾", "summary": ""}], []])
    monkeypatch.setattr(ne, "fetch_pending_enrichment", lambda limit: next(pending))
    monkeypatch.setattr(ne, "update_item_enrichment", lambda **kw: None)
    out = ne.run_enrichment_cycle(max_batches=3)
    assert out["batchesProcessed"] == 1
    assert out["totalFiltered"] == 1
    assert out["totalEnriched"] == 0


def test_cycle_multiple_batches(monkeypatch) -> None:
    pending = iter([
        [{"id": f"i{i}", "title": "美联储利率决议", "summary": ""} for i in range(5)],
        [],
    ])
    monkeypatch.setattr(ne, "fetch_pending_enrichment", lambda limit: next(pending))
    monkeypatch.setattr(
        ne, "enrich_batch", lambda items: {"enriched": len(items), "failed": 0, "filtered": 0}
    )
    monkeypatch.setattr(ne, "update_item_enrichment", lambda **kw: None)
    out = ne.run_enrichment_cycle(max_batches=10)
    assert out["batchesProcessed"] == 1  # one fetch returns all; next fetch empty -> break
    assert out["totalEnriched"] == 5


def test_cycle_enrich_batch_raises_marks_failed(monkeypatch) -> None:
    pending = iter([[{"id": "i0", "title": "美联储利率决议", "summary": ""}], []])
    monkeypatch.setattr(ne, "fetch_pending_enrichment", lambda limit: next(pending))

    def boom(items):
        raise RuntimeError("crash")

    monkeypatch.setattr(ne, "enrich_batch", boom)
    calls: list[dict] = []
    monkeypatch.setattr(ne, "update_item_enrichment", lambda **kw: calls.append(kw) or None)
    out = ne.run_enrichment_cycle(max_batches=2)
    assert out["totalFailed"] == 1
    assert calls[0]["enrichment_status"] == "failed"


def test_cycle_respects_max_batches(monkeypatch) -> None:
    monkeypatch.setattr(ne, "fetch_pending_enrichment", lambda limit: [{"id": "a", "title": "美联储利率决议", "summary": ""}])
    monkeypatch.setattr(
        ne, "enrich_batch", lambda items: {"enriched": 1, "failed": 0, "filtered": 0}
    )
    monkeypatch.setattr(ne, "update_item_enrichment", lambda **kw: None)
    out = ne.run_enrichment_cycle(max_batches=0)
    assert out["batchesProcessed"] == 0
    assert out["totalEnriched"] == 0


# ---- watchlist / relevance -------------------------------------------------

def test_get_watchlist_symbols_cached(monkeypatch) -> None:
    ne._WATCHLIST_CACHE = ["CN:600000"]
    assert ne._get_watchlist_symbols() == ["CN:600000"]
    ne._WATCHLIST_CACHE = None


def test_get_watchlist_symbols_db_error(monkeypatch) -> None:
    ne._WATCHLIST_CACHE = None
    import data_sync_service.db as dbmod

    def boom():
        raise RuntimeError("db down")

    monkeypatch.setattr(dbmod, "get_connection", boom)
    assert ne._get_watchlist_symbols() == []
    assert ne._WATCHLIST_CACHE == []
    ne._WATCHLIST_CACHE = None


def test_compute_relevance_with_watchlist(monkeypatch) -> None:
    monkeypatch.setattr(ne, "_get_watchlist_symbols", lambda: ["CN:600519", "HK:00700"])
    assert ne._compute_relevance(importance=3, tickers=["600519"]) == 45 + 30
    assert ne._compute_relevance(importance=3, tickers=["600519", "00700"]) == 100  # cap 100
    assert ne._compute_relevance(importance=5, tickers=[]) == 75
    monkeypatch.setattr(ne, "_get_watchlist_symbols", lambda: [])
    assert ne._compute_relevance(importance=4, tickers=["600519"]) == 60
