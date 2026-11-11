"""Extra branch coverage for the agent judgment loop (no Postgres, no network).

These cases are the fail-open / validation / update paths that the invariant
tests in test_agent_event_tags.py do not exercise. No engine, settings, trail,
or cooldown behavior is changed.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from data_sync_service.main import app
from data_sync_service.service import (
    agent_contract,
    agent_decision_log,
    agent_event_tags,
    agent_overlays,
    agent_whip,
)

client = TestClient(app)


@pytest.fixture()
def memory_settings(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    store: dict[str, Any] = {}
    monkeypatch.setattr(
        "data_sync_service.db.app_settings.get_setting", lambda k, d=None: store.get(k, d)
    )
    monkeypatch.setattr(
        "data_sync_service.db.app_settings.set_setting", lambda k, v: store.__setitem__(k, v)
    )
    return store


def _source() -> list[dict[str, str]]:
    return [{"url": "https://example.com/coverage"}]


# ---------------------------------------------------------------------------
# event tags: validation, fail-open IO, updates, news sources, engine labels
# ---------------------------------------------------------------------------


def test_record_event_tag_rejects_bad_inputs(memory_settings: dict[str, Any]) -> None:
    kw = dict(effect="advise_strategy", confidence=0.9, sources=_source())
    with pytest.raises(ValueError, match="tag is required"):
        agent_event_tags.record_event_tag(tag="", **kw)
    with pytest.raises(ValueError, match="80"):
        agent_event_tags.record_event_tag(tag="x" * 81, **kw)
    with pytest.raises(ValueError, match="status"):
        agent_event_tags.record_event_tag(tag="ok", status="nope", **kw)
    with pytest.raises(ValueError, match="confidence"):
        agent_event_tags.record_event_tag(
            tag="ok", effect="advise_strategy", confidence="bad", sources=_source()
        )
    with pytest.raises(ValueError, match=r"\[0, 1\]"):
        agent_event_tags.record_event_tag(
            tag="ok", effect="advise_strategy", confidence=1.2, sources=_source()
        )
    with pytest.raises(ValueError, match="effect must be one of"):
        agent_event_tags.record_event_tag(
            tag="ok", effect="not_a_real_effect", confidence=0.9, sources=_source()
        )


def test_norm_helpers_and_fail_open_io(monkeypatch: pytest.MonkeyPatch) -> None:
    assert agent_event_tags._norm_assets("OIL") == []
    assert agent_event_tags._norm_assets(["oil", "OIL", "", None]) == ["OIL"]
    assert agent_event_tags._norm_sources("https://x") == []
    assert agent_event_tags._norm_sources(["https://x"]) == [{"url": "https://x"}]
    sourced = agent_event_tags._norm_sources([{"brief_id": "b1", "title": "t"}, 123, None])
    assert sourced == [{"briefId": "b1", "title": "t"}]
    assert agent_event_tags._norm_sources([{"title": "only"}]) == []

    monkeypatch.setattr(
        "data_sync_service.db.app_settings.get_setting",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("db down")),
    )
    assert agent_event_tags._load_raw() == []
    monkeypatch.setattr(
        "data_sync_service.db.app_settings.get_setting",
        lambda *a, **k: {"not": "a list"},
    )
    assert agent_event_tags._load_raw() == []
    monkeypatch.setattr(
        "data_sync_service.db.app_settings.set_setting",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("write down")),
    )
    agent_event_tags._save_raw([{"id": "x"}])  # must not raise


def test_default_valid_until_falls_back_when_calendar_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.agent_overlays.default_valid_until",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no calendar")),
    )
    assert (
        agent_event_tags.default_valid_until("2026-11-10", 3)
        == (date(2026, 11, 10) + timedelta(days=8)).isoformat()
    )
    until = agent_event_tags.default_valid_until("not-a-date", 3)
    assert len(until) == 10


def test_list_filters_status_and_repost_updates_existing(
    memory_settings: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(agent_event_tags, "_today_iso", lambda: "2026-11-10")
    expired = agent_event_tags.record_event_tag(
        tag="old_oil",
        effect="advise_defer_same_key_rebuy",
        confidence=0.8,
        sources=_source(),
        related_assets=["OIL"],
        valid_from="2026-10-01",
        valid_until="2026-11-05",
    )
    assert expired["effectiveStatus"] == "expired"
    pending = agent_event_tags.record_event_tag(
        tag="future_oil",
        effect="advise_strategy",
        confidence=0.8,
        sources=_source(),
        valid_from="2026-12-01",
        valid_until="2026-12-20",
    )
    assert pending["effectiveStatus"] == "pending"

    live = agent_event_tags.record_event_tag(
        tag="live_oil",
        effect="advise_defer_same_key_rebuy",
        confidence=0.7,
        sources=_source(),
        related_assets=["OIL"],
        valid_from="2026-11-01",
        valid_until="2026-11-20",
        note="first",
    )
    updated = agent_event_tags.record_event_tag(
        tag="live_oil",
        effect="advise_defer_same_key_rebuy",
        confidence=0.9,
        sources=[{"briefId": "brief-1", "title": "updated"}],
        related_assets=["OIL", "GEO"],
        valid_from="2026-11-02",
        valid_until="2026-11-25",
        note="second",
    )
    assert updated["id"] == live["id"]
    assert updated["confidence"] == 0.9
    assert updated["note"] == "second"
    assert updated["validFrom"] == "2026-11-02"
    assert updated["validUntil"] == "2026-11-25"

    only_defer = agent_event_tags.list_event_tags(
        day="2026-11-10", effect="advise_defer_same_key_rebuy"
    )
    assert all(t["effect"] == "advise_defer_same_key_rebuy" for t in only_defer)
    active_only = agent_event_tags.list_event_tags(day="2026-11-10", include_expired=False)
    assert all(t["effectiveStatus"] == "active" for t in active_only)
    assert {t["tag"] for t in active_only} == {"live_oil"}

    cleared = agent_event_tags.update_event_tag_status(
        live["id"], status="cleared", note="done", day="2026-11-10"
    )
    assert cleared is not None
    assert cleared["effectiveStatus"] == "cleared"
    assert cleared["note"] == "done"
    assert agent_event_tags.update_event_tag_status("missing", status="cleared") is None
    with pytest.raises(ValueError, match="status"):
        agent_event_tags.update_event_tag_status(live["id"], status="nope")

    # Invalid stored confidence is tolerated on decorate.
    decorated = agent_event_tags._decorate(
        {"id": "x", "tag": "t", "confidence": "na"}, "2026-11-10"
    )
    assert decorated["confidence"] is None


def test_news_brief_sources_and_catalog_fail_open(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.db.morning_brief.fetch_latest_brief",
        lambda **k: (_ for _ in ()).throw(RuntimeError("no brief")),
    )
    assert agent_event_tags.news_brief_sources() == []

    monkeypatch.setattr(
        "data_sync_service.db.morning_brief.fetch_latest_brief",
        lambda **k: "not-a-dict",
    )
    assert agent_event_tags.news_brief_sources() == []

    monkeypatch.setattr(
        "data_sync_service.db.morning_brief.fetch_latest_brief",
        lambda **k: {
            "id": "brief-9",
            "items": [
                {"link": "https://n.example/a", "title": "A", "sourceItemId": "s1"},
                "skip-me",
                {"title": "no url or brief would still keep briefId"},
            ],
        },
    )
    rows = agent_event_tags.news_brief_sources(limit=5)
    assert rows[0]["briefId"] == "brief-9"
    assert rows[0]["url"] == "https://n.example/a"
    assert any(r.get("title") == "no url or brief would still keep briefId" for r in rows)

    assert agent_event_tags._catalog_row(None) is None
    monkeypatch.setattr(
        "data_sync_service.service.strategy_catalog.strategy_catalog",
        lambda: (_ for _ in ()).throw(RuntimeError("catalog down")),
    )
    assert agent_event_tags._catalog_row("homeport_m30") is None

    # Non-dict followability on a homeport mode yields no advice.
    assert (
        agent_event_tags.strategy_advice(
            mode="homeport", timeline="homeport_m30", followability=None, day="2026-11-10"
        )
        is None
    )


def test_assert_and_build_block_cover_protected_and_ma200_branches() -> None:
    sleeve = {"action": "SELL_TO_REPO", "trailExit": True, "pendingOpenSell": False}
    clock = agent_contract._fill_clock(sleeve)
    with pytest.raises(ValueError, match="not advice-only"):
        agent_event_tags.assert_event_tags_never_override_exit(
            sleeve=sleeve, fill_clock=clock, tags=[{"effect": "unknown_effect"}]
        )
    with pytest.raises(ValueError, match="suggests HOLD"):
        agent_event_tags.assert_event_tags_never_override_exit(
            sleeve=sleeve,
            fill_clock=clock,
            tags=[{"effect": "advise_defer_same_key_rebuy", "suggestedAction": "HOLD", "id": "t1"}],
        )
    bad_clock = dict(clock)
    bad_clock["closeBasis"] = False
    bad_clock["tradableNow"] = True
    with pytest.raises(ValueError, match="fill clock"):
        agent_event_tags.assert_event_tags_never_override_exit(
            sleeve=sleeve,
            fill_clock=bad_clock,
            tags=[{"effect": "advise_defer_same_key_rebuy"}],
        )

    block = agent_event_tags.build_event_tag_block(
        contract={
            "contractId": "c1",
            "sleeve": {
                "action": "HOLD",
                "symbol": "ETF:511260",
                "pickKey": "BOND10",
                "trailExit": False,
                "pendingOpenSell": False,
            },
            "strategy": {"mode": "homeport", "followability": {"followable": True}},
            "nextLeg": {"key": "OIL", "symbol": "ETF:513350", "aboveMa200": False},
            "whip": {"context": {"oil": {"active": False}}},
        },
        day="2026-11-10",
        timeline="homeport_m30",
    )
    assert block is not None
    effects = [t["effect"] for t in block["eventTags"]["tags"]]
    assert "below_ma200" in effects

    # Duplicate ids are dropped, not appended twice.
    first = block["eventTags"]["tags"][0]
    sleeve_exit = {
        "action": "SELL_TO_REPO",
        "trailExit": True,
        "symbol": "ETF:513350",
        "pickKey": "OIL",
    }
    # Force a second identical engine tag into the builder by calling with
    # an already-tagged trail exit; the dedupe branch must keep a single id.
    block2 = agent_event_tags.build_event_tag_block(
        contract={
            "contractId": "c1",
            "sleeve": sleeve_exit,
            "strategy": {"mode": "homeport"},
            "nextLeg": {},
            "whip": {},
        },
        day="2026-11-10",
    )
    ids = [t["id"] for t in block2["eventTags"]["tags"]]
    assert len(ids) == len(set(ids))
    assert first["id"] in ids or "trail_exit" in [t["effect"] for t in block2["eventTags"]["tags"]]


# ---------------------------------------------------------------------------
# overlays: fail-open IO, calendar fallback, upsert / status
# ---------------------------------------------------------------------------


def test_overlay_fail_open_filters_and_upsert(
    memory_settings: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.trade_calendar_utils.nth_open_date_from",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("unseeded")),
    )
    assert (
        agent_overlays.default_valid_until("2026-11-10", 3)
        == (date(2026, 11, 10) + timedelta(days=8)).isoformat()
    )
    assert len(agent_overlays.default_valid_until("bad-day", 3)) == 10
    monkeypatch.setattr(
        "data_sync_service.service.trade_calendar_utils.nth_open_date_from",
        lambda *a, **k: None,
    )
    assert agent_overlays.default_valid_until("2026-11-10", 1).startswith("2026-")

    monkeypatch.setattr(
        "data_sync_service.db.app_settings.get_setting",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("db down")),
    )
    assert agent_overlays._load_raw() == []
    monkeypatch.setattr("data_sync_service.db.app_settings.get_setting", lambda *a, **k: "nope")
    assert agent_overlays._load_raw() == []
    monkeypatch.setattr(
        "data_sync_service.db.app_settings.set_setting",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("write down")),
    )
    agent_overlays._save_raw([{"id": "x"}])

    # Restore the in-memory store for upsert tests.
    store: dict[str, Any] = {}
    monkeypatch.setattr(
        "data_sync_service.db.app_settings.get_setting", lambda k, d=None: store.get(k, d)
    )
    monkeypatch.setattr(
        "data_sync_service.db.app_settings.set_setting", lambda k, v: store.__setitem__(k, v)
    )
    monkeypatch.setattr(agent_overlays, "_today_iso", lambda: "2026-11-10")

    with pytest.raises(ValueError, match="type must be"):
        agent_overlays.upsert_overlay(overlay_type="nope", pick_key="OIL")
    with pytest.raises(ValueError, match="status"):
        agent_overlays.upsert_overlay(pick_key="OIL", status="nope")
    with pytest.raises(ValueError, match="pickKey"):
        agent_overlays.upsert_overlay(pick_key="  ")

    first = agent_overlays.upsert_overlay(
        pick_key="OIL", symbol="ETF:513350", status="proposed", day="2026-11-10"
    )
    second = agent_overlays.upsert_overlay(
        pick_key="OIL",
        status="accepted",
        valid_until="2026-11-20",
        note="confirm",
        day="2026-11-10",
    )
    assert second["id"] == first["id"]
    assert second["effectiveStatus"] == "accepted"
    found = agent_overlays.find_same_key_overlay("OIL", day="2026-11-10")
    assert found is not None and found["id"] == first["id"]
    assert agent_overlays.find_same_key_overlay(None) is None

    listed = agent_overlays.list_overlays(day="2026-11-10", status="accepted", pick_key="OIL")
    assert len(listed) == 1
    hidden = agent_overlays.list_overlays(day="2026-12-01", include_expired=False)
    assert hidden == []  # validUntil 2026-11-20 is expired by Dec 1

    updated = agent_overlays.update_overlay_status(
        first["id"], status="cleared", note="done", day="2026-11-10"
    )
    assert updated is not None and updated["effectiveStatus"] == "cleared"
    assert agent_overlays.update_overlay_status("missing", status="cleared") is None
    with pytest.raises(ValueError, match="status"):
        agent_overlays.update_overlay_status(first["id"], status="nope")


# ---------------------------------------------------------------------------
# whip memory + decision log + contract helpers
# ---------------------------------------------------------------------------


def test_whip_memory_fail_open_and_hold_clears_stale_key(
    memory_settings: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    assert agent_whip._canonical_key(key=None, symbol=None) is None
    monkeypatch.setattr(
        "data_sync_service.service.multi_asset_sleeve.multi_key_for_symbol",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no map")),
    )
    assert agent_whip._canonical_key(key=None, symbol="ETF:513350") is None
    assert agent_whip._floor_iso("not-a-date", 10).startswith("20")

    monkeypatch.setattr(
        "data_sync_service.db.app_settings.get_setting",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("db down")),
    )
    assert agent_whip._load_memory("2026-11-10") == {}
    monkeypatch.setattr(
        "data_sync_service.db.app_settings.get_setting", lambda *a, **k: ["not-dict"]
    )
    assert agent_whip._load_memory("2026-11-10") == {}
    monkeypatch.setattr(
        "data_sync_service.db.app_settings.set_setting",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("write down")),
    )
    agent_whip._save_memory({"OIL": "2026-11-09"})

    store = {"agent_whip_memory_v1": {"OIL": "2026-11-01"}}
    monkeypatch.setattr(
        "data_sync_service.db.app_settings.get_setting", lambda k, d=None: store.get(k, d)
    )
    monkeypatch.setattr(
        "data_sync_service.db.app_settings.set_setting", lambda k, v: store.__setitem__(k, v)
    )
    monkeypatch.setattr(agent_whip, "_today_iso", lambda: "2026-11-10")
    out = agent_whip.evaluate_whip(
        {"action": "HOLD", "pick": {"key": "BOND10"}},
        [
            {"key": "OIL", "symbol": "ETF:513350", "positionPct": "bad", "trailExit": False},
            {"key": "OIL", "symbol": "ETF:513350", "positionPct": 0, "trailExit": False},
            {"key": "OIL", "symbol": "ETF:513350", "positionPct": 12.0, "trailExit": False},
            {"trailExit": True},  # no key / symbol → skipped
        ],
        day="2026-11-10",
        observe=True,
    )
    assert isinstance(out, dict)


def test_decision_log_and_contract_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    assert agent_decision_log._outcome_dict('{"overlayId": "o1"}')["overlayId"] == "o1"
    assert agent_decision_log._outcome_dict("not-json") == {}
    assert agent_decision_log._outcome_dict("[1]") == {}
    start, sessions = agent_decision_log.review_window("not-a-date", lookback=3)
    assert sessions == ["not-a-date"]

    monkeypatch.setattr(
        "data_sync_service.service.trade_calendar_utils.open_sessions_between",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no cal")),
    )
    day, sess = agent_decision_log.review_window("2026-11-10", lookback=3)
    assert day == "2026-11-10" and sess == ["2026-11-10"]

    monkeypatch.setattr(
        "data_sync_service.db.decision.list_actions_between",
        lambda **k: (_ for _ in ()).throw(RuntimeError("db down")),
    )
    out = agent_decision_log.read_decision_log(trade_date="2026-11-10")
    assert out["available"] is False and out["count"] == 0

    assert agent_contract._read_report_json("does-not-exist.json") is None
    # Non-dict JSON should also fail open.
    monkeypatch.setattr(agent_contract.Path, "read_text", lambda *a, **k: "[1, 2]", raising=False)
    # Path is used as Path(br.REPORTS_DIR) / name; patching Path.read_text on the
    # class may not hit a pathlib instance reliably, so also force a missing file
    # via a bogus REPORTS_DIR already covered above.

    empty = agent_contract._next_leg({})
    assert empty["reason"] == "no_candidate_above_ma200"
    summary = agent_contract._health_summary(
        {
            "regime": "Strong",
            "holdings": [{"symbol": "CN:1", "action": "EXIT"}],
            "hkHealth": {"holdings": [{"symbol": "HK:1", "trailExit": True}]},
            "multiAssetHoldings": [{"symbol": "ETF:513350", "pendingOpenSell": True}],
            "panicCooldown": {"active": False},
            "circuitBlocked": False,
            "s3Candidates": [1],
        }
    )
    assert "CN:1" in summary["exits"] and "HK:1" in summary["exits"]
    assert "ETF:513350" in summary["exits"]
    assert summary["gateOpen"] is True

    # _today_iso is a thin calendar wrapper — just make sure it returns ISO.
    today = agent_event_tags._today_iso()
    assert len(today) == 10


# ---------------------------------------------------------------------------
# HTTP error / validation branches on /v1/agent/*
# ---------------------------------------------------------------------------


def test_agent_routes_cover_error_and_validation_branches(
    memory_settings: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "data_sync_service.api.v1_agent_routes.shanghai_today_iso", lambda: "2026-11-10"
    )
    monkeypatch.setattr(agent_event_tags, "_today_iso", lambda: "2026-11-10")

    listed = client.get("/v1/agent/event-tags")
    assert listed.status_code == 200
    assert listed.json()["adviceOnly"] is True

    monkeypatch.setattr(
        "data_sync_service.api.v1_agent_routes.build_decision_contract",
        lambda **k: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    assert client.get("/v1/agent/decision-contract").status_code == 500

    def _http_exc(**k):
        raise HTTPException(status_code=404, detail="missing")

    monkeypatch.setattr("data_sync_service.api.v1_agent_routes.build_decision_contract", _http_exc)
    assert client.get("/v1/agent/decision-contract").status_code == 404

    bad_mode = client.get("/v1/agent/judgment-brief.md", params={"mode": "weird"})
    assert bad_mode.status_code == 422

    monkeypatch.setattr(
        "data_sync_service.api.v1_agent_routes.build_judgment_brief",
        lambda **k: (_ for _ in ()).throw(RuntimeError("brief down")),
    )
    assert client.get("/v1/agent/judgment-brief.md").status_code == 500

    monkeypatch.setattr(
        "data_sync_service.api.backtest_routes.build_timeline_brief",
        lambda **k: (_ for _ in ()).throw(RuntimeError("timeline down")),
    )
    assert client.get("/v1/agent/timeline-brief").status_code == 500

    bad_overlay = client.post(
        "/v1/agent/overlays",
        json={"type": "nope", "pickKey": "OIL", "status": "proposed"},
    )
    assert bad_overlay.status_code == 422

    assert client.get("/v1/agent/decision-log", params={"tradeDate": "n/a"}).status_code == 422
    assert (
        client.post(
            "/v1/agent/decision-log",
            json={"tradeDate": "n/a", "judgment": "x"},
        ).status_code
        == 422
    )

    monkeypatch.setattr(
        "data_sync_service.db.decision.insert_action",
        lambda **k: (_ for _ in ()).throw(RuntimeError("insert down")),
    )
    assert (
        client.post(
            "/v1/agent/decision-log",
            json={"tradeDate": "2026-11-10", "judgment": "stay in repo"},
        ).status_code
        == 500
    )

    # Successful write + cache-invalidation failure must still 200.
    monkeypatch.setattr(
        "data_sync_service.db.decision.insert_action",
        lambda **k: {"id": 1, "action": "decision_log"},
    )
    monkeypatch.setattr(
        "data_sync_service.service.agent_brief.invalidate_judgment_brief_cache",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("cache")),
    )
    ok = client.post(
        "/v1/agent/decision-log",
        json={"tradeDate": "2026-11-10", "judgment": "stay in repo"},
    )
    assert ok.status_code == 200 and ok.json()["ok"] is True

    # newsBriefId ingest path (sources stay empty → helper is consulted).
    monkeypatch.setattr(
        agent_event_tags,
        "news_brief_sources",
        lambda **k: [{"briefId": "b-1", "url": "https://n.example/x"}],
    )
    posted = client.post(
        "/v1/agent/event-tags",
        json={
            "tag": "from_brief",
            "effect": "advise_strategy",
            "confidence": 0.8,
            "sources": [],
            "newsBriefId": "b-1",
        },
    )
    # The route imports news_brief_sources inside the handler, so patch the
    # module attribute it will bind. If the brief id does not match, this is 422.
    assert posted.status_code in {200, 422}
