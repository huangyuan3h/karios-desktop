"""Decision agent loop (TIP-015) API tests."""

from __future__ import annotations

from datetime import date, timedelta

from fastapi.testclient import TestClient

from data_sync_service.main import app
from data_sync_service.service.decision import (
    build_daily_snapshot,
    search_archive_by_symbol,
)

client = TestClient(app)


def test_session_crud_roundtrip() -> None:
    create = client.post(
        "/api/decision/sessions",
        json={"title": "t", "system_prompt": "contract-v7.8"},
    )
    assert create.status_code == 200
    session = create.json()["session"]
    sid = session["id"]
    assert session["title"] == "t"
    assert session["system_prompt"] == "contract-v7.8"

    msg = client.post(
        f"/api/decision/sessions/{sid}/messages",
        json={"role": "user", "content": "hello", "context_snapshot": {"layer1": {"P0": 1}}},
    )
    assert msg.status_code == 200
    assert msg.json()["message"]["role"] == "user"

    listing = client.get(f"/api/decision/sessions/{sid}/messages")
    assert listing.status_code == 200
    assert len(listing.json()["messages"]) == 1

    renamed = client.patch(f"/api/decision/sessions/{sid}", json={"title": "renamed"})
    assert renamed.json()["session"]["title"] == "renamed"

    sessions = client.get("/api/decision/sessions")
    assert len(sessions.json()["sessions"]) >= 1
    found = next(s for s in sessions.json()["sessions"] if s["id"] == sid)
    assert found["title"] == "renamed"
    assert found["messageCount"] == 1


def test_sessions_list_shape() -> None:
    resp = client.get("/api/decision/sessions")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    for s in payload["sessions"]:
        assert {"id", "title", "createdAt", "lastActiveAt", "messageCount"} <= set(s)


def test_messages_require_valid_session() -> None:
    resp = client.get("/api/decision/sessions/999999999/messages")
    assert resp.status_code == 200
    assert resp.json()["messages"] == []


def test_snapshots_list_empty() -> None:
    resp = client.get("/api/decision/snapshots")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


def test_snapshot_build_and_search_roundtrip() -> None:
    session = client.post("/api/decision/sessions", json={"title": "m3-snap"}).json()["session"]
    client.post(
        f"/api/decision/sessions/{session['id']}/messages",
        json={"role": "user", "content": "分析 CN:600519.SH 的走势与研报"},
    )
    rec = build_daily_snapshot()
    assert rec["status"] == "open"
    assert rec["exchangeCount"] >= 1

    hits = search_archive_by_symbol("600519")
    assert any("600519" in (m or "") for h in hits for m in h["matches"])

    detail = client.get(f"/api/decision/snapshots/{rec['snapshotDate']}")
    assert detail.status_code == 200
    body = detail.json()
    assert body["ok"] is True
    assert len(body["snapshot"]["exchanges"]) >= 1

    search = client.get("/api/decision/archive/search", params={"symbol": "600519"})
    assert search.status_code == 200
    assert len(search.json()["hits"]) >= 1

    invalid = client.get("/api/decision/snapshots/not-a-date")
    assert invalid.status_code == 200
    assert invalid.json()["ok"] is False


def test_delete_message_endpoint() -> None:
    session = client.post("/api/decision/sessions", json={"title": "del"}).json()["session"]
    msg = client.post(
        f"/api/decision/sessions/{session['id']}/messages",
        json={"role": "user", "content": "delete me"},
    ).json()["message"]
    resp = client.delete(f"/api/decision/sessions/{session['id']}/messages/{msg['id']}")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    listing = client.get(f"/api/decision/sessions/{session['id']}/messages")
    assert listing.json()["messages"] == []
    missing = client.delete(f"/api/decision/sessions/{session['id']}/messages/999999999")
    assert missing.json()["ok"] is False


def test_actions_crud_and_tracking(monkeypatch) -> None:
    from data_sync_service.db.decision import (
        list_actions,
        update_action_status,
        upsert_actions,
    )
    from data_sync_service.service.decision import match_executions

    n = upsert_actions(
        [
            {
                "session_id": 1,
                "message_id": 999901,
                "symbol": "CN:600519.SH",
                "action": "BUY",
                "rationale": "test",
                "confidence": 0.8,
                "source": "decision_agent",
            }
        ]
    )
    assert n == 1
    actions = list_actions(days=7)
    assert any(a["symbol"] == "CN:600519.SH" and a["status"] == "proposed" for a in actions)
    target = next(a for a in actions if a["message_id"] == 999901)

    monkeypatch.setattr(
        "data_sync_service.db.execution_journal.list_changes",
        lambda **kwargs: [
            {
                "id": "chg-1",
                "symbol": "CN:600519.SH",
                "field": "action",
                "new_value": "BUY",
                "source": "alpha_radar",
            }
        ],
    )
    result = match_executions()
    assert result["matched"] >= 1
    refreshed = list_actions(days=7)
    target2 = next(a for a in refreshed if a["id"] == target["id"])
    assert target2["status"] == "executed"
    assert target2["matchedChangeId"] == "chg-1"

    assert update_action_status(target["id"], status="executed", outcome={"pct1": 1.5}) is True


def test_analysis_endpoint_shape() -> None:
    resp = client.get("/api/decision/analysis")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert "firedBySource" in payload
    assert "firedTotal" in payload
    assert "paper" in payload
    assert set(payload["paper"]) >= {
        "total",
        "closed",
        "wins",
        "losses",
        "winRate",
        "avgPnlPct",
        "byMarket",
    }
    assert isinstance(payload["sessions"], list)
