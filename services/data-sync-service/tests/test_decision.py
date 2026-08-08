"""Decision agent loop (TIP-015) API tests."""

from __future__ import annotations

import json
from datetime import date, timedelta

from fastapi.testclient import TestClient

from data_sync_service.main import app
from data_sync_service.service.decision import (
    build_daily_snapshot,
    search_archive_by_symbol,
)

client = TestClient(app)


def test_session_crud_roundtrip() -> None:
    from data_sync_service.db import get_connection

    create = client.post(
        "/api/decision/sessions",
        json={"title": "t", "system_prompt": "contract-v7.8"},
    )
    assert create.status_code == 200
    session = create.json()["session"]
    sid = session["id"]
    try:
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
    finally:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM decision_sessions WHERE id = %s", (sid,))
            conn.commit()


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
    from datetime import datetime, timezone

    from data_sync_service.db import get_connection

    session = client.post("/api/decision/sessions", json={"title": "m3-snap"}).json()["session"]
    msg = client.post(
        f"/api/decision/sessions/{session['id']}/messages",
        json={"role": "user", "content": "分析 CN:600519.SH 的走势与研报"},
    ).json()["message"]
    # Pin the message into the snapshot window: _messages_on queries the
    # UTC calendar day, so created_at must fall inside UTC today regardless
    # of the Shanghai/UTC date boundary (the historical flake).
    today = datetime.now(timezone.utc).date()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE decision_messages SET created_at = %s WHERE id = %s",
                (f"{today.isoformat()}T10:00:00+00:00", msg["id"]),
            )
            cur.execute(
                "SELECT snapshot_date, active_layer_ref, agent_exchanges, outcome, status FROM decision_snapshots WHERE snapshot_date = %s",
                (today,),
            )
            before = cur.fetchone()
        conn.commit()
    try:
        rec = build_daily_snapshot(snapshot_date=today)
        assert rec["status"] == "open"
        assert rec["exchangeCount"] >= 1

        # Assertions must run BEFORE the finally cleanup restores/deletes the
        # daily snapshot row (with no pre-existing snapshot the row is gone
        # after cleanup and the detail endpoint would return empty).
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
    finally:
        # Restore the real daily snapshot (build_daily_snapshot upserts the
        # current date and would otherwise clobber today's archived row) and
        # remove the test session (CASCADE removes its messages).
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM decision_snapshots WHERE snapshot_date = %s",
                    (today,),
                )
                if before:
                    cur.execute(
                        """
                        INSERT INTO decision_snapshots
                            (snapshot_date, active_layer_ref, agent_exchanges, outcome, status)
                        VALUES (%s, %s::jsonb, %s::jsonb, %s::jsonb, %s)
                        """,
                        (
                            before[0],
                            json.dumps(before[1]) if before[1] is not None else None,
                            json.dumps(before[2]) if before[2] is not None else None,
                            json.dumps(before[3]) if before[3] is not None else None,
                            before[4],
                        ),
                    )
                cur.execute(
                    "DELETE FROM decision_sessions WHERE id = %s",
                    (session["id"],),
                )
            conn.commit()


def test_delete_message_endpoint() -> None:
    from data_sync_service.db import get_connection

    session = client.post("/api/decision/sessions", json={"title": "del"}).json()["session"]
    try:
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
    finally:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM decision_sessions WHERE id = %s", (session["id"],))
            conn.commit()


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
                "newValue": "BUY",
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


def _fake_conn(rows_seq):
    class _FakeCursor:
        def __init__(self, seq):
            self._seq = list(seq)
            self._i = 0
            self._rows = []

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params=None):
            self._rows = self._seq[min(self._i, len(self._seq) - 1)]
            self._i += 1
            return None

        def fetchall(self):
            return self._rows

    class _FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def cursor(self):
            return _FakeCursor(rows_seq)

    return _FakeConn()


def test_apply_daily_outcomes_attaches_paper_and_fired(monkeypatch) -> None:
    from data_sync_service.service import decision as svc

    upserted: list[dict] = []
    monkeypatch.setattr(
        svc,
        "list_snapshots",
        lambda limit=30: [{"id": 1, "snapshotDate": date(2026, 8, 7), "status": "open"}],
    )
    monkeypatch.setattr(
        "data_sync_service.db.execution_journal.list_changes",
        lambda trade_date=None, limit=200: [
            {"id": "c1", "symbol": "CN:600519.SH", "field": "action",
             "newValue": "BUY", "source": "alpha_radar"},
            {"id": "c2", "symbol": "CN:600000.SH", "field": "hardStop",
             "newValue": "33.5", "source": "TV"},
        ],
    )
    monkeypatch.setattr(
        "data_sync_service.db.paper_trading.list_paper_trades",
        lambda limit=100: [
            {"id": "p1", "symbol": "CN:600519.SH", "side": "BUY", "status": "closed",
             "entryDate": "2026-08-07", "pnlPct": 3.2},
            {"id": "p2", "symbol": "CN:000001.SZ", "side": "BUY", "status": "open",
             "entryDate": "2026-08-01", "pnlPct": -1.1},
        ],
    )
    monkeypatch.setattr(svc, "upsert_snapshot", lambda **kw: upserted.append(kw) or {"ok": True})

    result = svc.apply_daily_outcomes(days=5)
    assert result["ok"] is True
    assert result["updated"] == ["2026-08-07"]
    assert len(upserted) == 1
    rec = upserted[0]
    assert rec["status"] == "reviewed"
    assert [f["symbol"] for f in rec["outcome"]["fired"]] == ["CN:600519.SH"]
    assert rec["outcome"]["fired"][0]["newValue"] == "BUY"
    assert [p["symbol"] for p in rec["outcome"]["paper"]] == ["CN:600519.SH"]
    assert rec["outcome"]["paper"][0]["pnlPct"] == 3.2


def test_apply_daily_outcomes_skips_empty_days(monkeypatch) -> None:
    from data_sync_service.service import decision as svc

    upserted: list[dict] = []
    monkeypatch.setattr(
        svc,
        "list_snapshots",
        lambda limit=30: [{"id": 1, "snapshotDate": date(2026, 8, 7), "status": "open"}],
    )
    monkeypatch.setattr(
        "data_sync_service.db.execution_journal.list_changes",
        lambda trade_date=None, limit=200: [],
    )
    monkeypatch.setattr(
        "data_sync_service.db.paper_trading.list_paper_trades",
        lambda limit=100: [],
    )
    monkeypatch.setattr(svc, "upsert_snapshot", lambda **kw: upserted.append(kw) or {"ok": True})

    result = svc.apply_daily_outcomes(days=5)
    assert result["updated"] == []
    assert upserted == []


def test_apply_daily_outcomes_handles_paper_read_failure(monkeypatch) -> None:
    from data_sync_service.service import decision as svc

    upserted: list[dict] = []
    monkeypatch.setattr(
        svc,
        "list_snapshots",
        lambda limit=30: [{"id": 1, "snapshotDate": date(2026, 8, 7), "status": "open"}],
    )
    monkeypatch.setattr(
        "data_sync_service.db.execution_journal.list_changes",
        lambda trade_date=None, limit=200: [],
    )
    monkeypatch.setattr(
        "data_sync_service.db.paper_trading.list_paper_trades",
        lambda limit=100: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(svc, "upsert_snapshot", lambda **kw: upserted.append(kw) or {"ok": True})

    result = svc.apply_daily_outcomes(days=5)
    assert result["ok"] is True
    assert result["updated"] == []


def test_analysis_stats_win_rate_and_failures(monkeypatch) -> None:
    from data_sync_service.service import decision as svc

    trades = [
        {"symbol": "CN:600519.SH", "status": "closed", "pnlPct": 4.0},
        {"symbol": "CN:600519.SH", "status": "closed", "pnlPct": -2.0},
        {"symbol": "CN:000001.SZ", "status": "closed", "pnlPct": 1.5},
        {"symbol": "CN:000002.SZ", "status": "open", "pnlPct": 0.5},
    ]
    monkeypatch.setattr(
        "data_sync_service.db.paper_trading.list_paper_trades",
        lambda limit=500: trades,
    )
    monkeypatch.setattr(
        "data_sync_service.db.paper_trading.count_by_market_since",
        lambda since: {"CN": {"total": 3, "wins": 2, "losses": 1, "winRate": 0.667}},
    )
    monkeypatch.setattr(
        "data_sync_service.db.execution_journal.count_changes_by_source",
        lambda **kw: {"alpha_radar": 2},
    )
    stats = svc.analysis_stats(fired_days=30, paper_limit=500)
    assert stats["paper"]["total"] == 4
    assert stats["paper"]["open"] == 1
    assert stats["paper"]["closed"] == 3
    assert stats["paper"]["wins"] == 2
    assert stats["paper"]["losses"] == 1
    assert stats["paper"]["winRate"] == 0.667
    assert stats["paper"]["avgPnlPct"] == 1.17  # (4.0 - 2.0 + 1.5) / 3
    assert stats["paper"]["byMarket"] == {"CN": {"total": 3, "wins": 2, "losses": 1, "winRate": 0.667}}

    monkeypatch.setattr(
        "data_sync_service.db.paper_trading.list_paper_trades",
        lambda limit=500: (_ for _ in ()).throw(RuntimeError("db down")),
    )
    stats2 = svc.analysis_stats(fired_days=30)
    assert stats2["paper"]["total"] == 0
    assert stats2["paper"]["winRate"] is None

    monkeypatch.setattr(
        "data_sync_service.db.paper_trading.list_paper_trades",
        lambda limit=500: trades,
    )
    monkeypatch.setattr(
        "data_sync_service.db.paper_trading.count_by_market_since",
        lambda since: (_ for _ in ()).throw(RuntimeError("stats down")),
    )
    stats3 = svc.analysis_stats(fired_days=30)
    assert stats3["paper"]["byMarket"] == {}


def test_build_daily_snapshot_truncates_exchanges(monkeypatch) -> None:
    from data_sync_service.service import decision as svc

    messages = []
    for i in range(25):
        messages.append(
            (f"session-{i}", "user", f"message {i} {'X' * 300}", f"2026-08-07T0{i % 10}:00:00+00:00")
        )
    monkeypatch.setattr(svc, "get_connection", lambda: _fake_conn([messages]))
    monkeypatch.setattr(
        svc,
        "_watchlist_ref",
        lambda: {"watchlistSymbols": ["CN:600519.SH"], "bySource": {"TV": 1}, "count": 1},
    )
    captured: dict = {}

    def fake_upsert(**kw):
        captured.update(kw)
        return {"id": 1, "snapshotDate": kw["snapshot_date"], "status": "open", "createdAt": "2026-08-07T08:00:00+00:00"}

    monkeypatch.setattr(svc, "upsert_snapshot", fake_upsert)

    rec = svc.build_daily_snapshot(snapshot_date=date(2026, 8, 7))
    assert rec["exchangeCount"] == 20
    assert rec["status"] == "open"
    assert captured["status"] == "open"
    assert captured["active_layer_ref"]["count"] == 1
    assert captured["active_layer_ref"]["snapshotAt"].endswith("+00:00")


def test_watchlist_ref_fails_open(monkeypatch) -> None:
    from data_sync_service.service import decision as svc

    monkeypatch.setattr(
        "data_sync_service.db.watchlist_automation.list_registry",
        lambda: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    ref = svc._watchlist_ref()
    assert ref == {"watchlistSymbols": [], "bySource": {}, "count": 0}


def test_extract_pending_actions(monkeypatch) -> None:
    import json
    import urllib.request

    from data_sync_service.service import decision as svc

    class _FakeResp:
        def __init__(self, payload):
            self._payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return json.dumps(self._payload).encode()

    calls: list[list[dict]] = []
    monkeypatch.setattr(svc, "get_connection", lambda: _fake_conn([[(
        11, "s-1", "## 操作建议\nBUY CN:600519.SH", "2026-08-07T08:00:00+00:00",
    ), (
        12, "s-2", "## 操作建议\nEXIT CN:000001.SZ", "2026-08-07T09:00:00+00:00",
    )]]))

    def fake_urlopen(req, timeout=30):
        if getattr(fake_urlopen, "_first", True):
            fake_urlopen._first = False
            return _FakeResp({"actions": [
                {"symbol": "CN:600519.SH", "action": "BUY", "rationale": "r", "confidence": 0.9},
            ]})
        raise RuntimeError("ai down")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(
        "data_sync_service.db.decision.upsert_actions",
        lambda recs: calls.append(recs) or len(recs),
    )
    monkeypatch.setattr(
        "data_sync_service.config.get_settings",
        lambda: type("S", (), {"ai_service_base_url": "http://ai"})(),
    )

    result = svc.extract_pending_actions(hours=48)
    assert result["ok"] is True
    assert result["processed"] == [11]
    assert result["extracted"] == 1
    assert calls[0][0]["action"] == "BUY"
    assert calls[0][0]["source"] == "decision_agent"


def test_extract_pending_actions_empty_messages(monkeypatch) -> None:
    from data_sync_service.service import decision as svc

    monkeypatch.setattr(svc, "get_connection", lambda: _fake_conn([[]]))
    monkeypatch.setattr(
        "data_sync_service.db.decision.upsert_actions",
        lambda recs: len(recs),
    )
    monkeypatch.setattr(
        "data_sync_service.config.get_settings",
        lambda: type("S", (), {"ai_service_base_url": "http://ai"})(),
    )

    result = svc.extract_pending_actions(hours=48)
    assert result == {"ok": True, "processed": [], "extracted": 0}


def test_track_action_outcomes(monkeypatch) -> None:
    from data_sync_service.service import decision as svc

    actions = [
        {
            "id": 1,
            "symbol": "CN:600519.SH",
            "status": "executed",
            "outcome": None,
            "createdAt": "2026-08-01T08:00:00+00:00",
        }
    ]
    bars = [
        {"ts_code": "600519.SH", "trade_date": "2026-07-30", "close": 100.0},
        {"ts_code": "600519.SH", "trade_date": "2026-07-31", "close": 105.0},
        {"ts_code": "600519.SH", "trade_date": "2026-08-01", "close": 110.0},
        {"ts_code": "600519.SH", "trade_date": "2026-08-04", "close": 115.0},
        {"ts_code": "600519.SH", "trade_date": "2026-08-05", "close": 121.0},
    ]
    monkeypatch.setattr(
        "data_sync_service.db.decision.list_actions",
        lambda days=14, limit=300: actions,
    )
    monkeypatch.setattr(
        "data_sync_service.db.daily.fetch_daily_for_codes",
        lambda codes, start, end: bars,
    )
    updates: list[dict] = []
    monkeypatch.setattr(
        "data_sync_service.db.decision.update_action_status",
        lambda action_id, status, matched_change_id=None, outcome=None: updates.append(
            {"id": action_id, "status": status, "outcome": outcome}
        )
        or True,
    )

    result = svc.track_action_outcomes(horizon_days=5)
    assert result == {"ok": True, "tracked": 1}
    assert updates[0]["outcome"] == {"pct1": 4.55, "pct3": None, "pct5": None}


def test_track_action_outcomes_skips_unpriced(monkeypatch) -> None:
    from data_sync_service.service import decision as svc

    actions = [
        {
            "id": 1,
            "symbol": "CN:600519.SH",
            "status": "executed",
            "outcome": None,
            "createdAt": "2026-08-01T08:00:00+00:00",
        },
        {
            "id": 2,
            "symbol": "CN:600519.SH",
            "status": "executed",
            "outcome": None,
            "createdAt": "2026-08-06T08:00:00+00:00",
        },
    ]
    monkeypatch.setattr(
        "data_sync_service.db.decision.list_actions",
        lambda days=14, limit=300: actions,
    )
    monkeypatch.setattr(
        "data_sync_service.db.daily.fetch_daily_for_codes",
        lambda codes, start, end: [],  # no bars at all
    )
    tracked: list[int] = []
    monkeypatch.setattr(
        "data_sync_service.db.decision.update_action_status",
        lambda action_id, status, matched_change_id=None, outcome=None: tracked.append(action_id)
        or True,
    )

    result = svc.track_action_outcomes(horizon_days=5)
    assert result["tracked"] == 0
    assert tracked == []


def test_search_archive_case_insensitive(monkeypatch) -> None:
    from data_sync_service.service import decision as svc

    snapshots = [
        {
            "snapshotDate": date(2026, 8, 7),
            "status": "reviewed",
            "agentExchanges": [
                {"role": "user", "content": "分析 CN:600519.SH 的走势与研报"},
                {"role": "assistant", "content": "BUY 建议 茅台"},
            ],
            "outcome": {"fired": [], "paper": []},
        },
        {
            "snapshotDate": date(2026, 8, 6),
            "status": "open",
            "agentExchanges": [{"role": "user", "content": "与茅台无关的话题"}],
            "outcome": None,
        },
    ]
    monkeypatch.setattr(
        "data_sync_service.service.decision.list_snapshots",
        lambda limit=60: snapshots,
    )

    hits = svc.search_archive_by_symbol("cn:600519.sh")
    assert len(hits) == 1
    assert hits[0]["date"] == "2026-08-07"
    assert len(hits[0]["matches"]) == 1

    assert svc.search_archive_by_symbol("") == []
    assert svc.search_archive_by_symbol("   ") == []
    no_hits = svc.search_archive_by_symbol("CN:000001.SZ")
    assert no_hits == []


def test_extract_pending_actions_skips_empty_actions(monkeypatch) -> None:
    import json
    import urllib.request

    from data_sync_service.service import decision as svc

    def fake_urlopen(req, timeout=30):
        return type("R", (), {
            "__enter__": lambda s: s,
            "__exit__": lambda *a: False,
            "read": lambda: json.dumps({"actions": []}).encode(),
        })()

    monkeypatch.setattr(svc, "get_connection", lambda: _fake_conn([[(
        13, "s-1", "## 操作建议\n无操作", "2026-08-07T08:00:00+00:00",
    )]]))
    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(
        "data_sync_service.config.get_settings",
        lambda: type("S", (), {"ai_service_base_url": "http://ai"})(),
    )

    result = svc.extract_pending_actions(hours=48)
    assert result == {"ok": True, "processed": [], "extracted": 0}


def test_track_action_outcomes_no_actions(monkeypatch) -> None:
    from data_sync_service.service import decision as svc

    monkeypatch.setattr(
        "data_sync_service.db.decision.list_actions",
        lambda days=14, limit=300: [],
    )
    monkeypatch.setattr(
        "data_sync_service.db.daily.fetch_daily_for_codes",
        lambda codes, start, end: [],
    )

    result = svc.track_action_outcomes(horizon_days=5)
    assert result == {"ok": True, "tracked": 0}


def test_track_action_outcomes_zero_close_skipped(monkeypatch) -> None:
    from data_sync_service.service import decision as svc

    monkeypatch.setattr(
        "data_sync_service.db.decision.list_actions",
        lambda days=14, limit=300: [{
            "id": 3,
            "symbol": "CN:600519.SH",
            "status": "executed",
            "outcome": None,
            "createdAt": "2026-08-01T08:00:00+00:00",
        }],
    )
    monkeypatch.setattr(
        "data_sync_service.db.daily.fetch_daily_for_codes",
        lambda codes, start, end: [{"ts_code": "600519.SH", "trade_date": "2026-08-01", "close": 0.0}],
    )
    tracked: list[int] = []
    monkeypatch.setattr(
        "data_sync_service.db.decision.update_action_status",
        lambda action_id, status, matched_change_id=None, outcome=None: tracked.append(action_id)
        or True,
    )

    result = svc.track_action_outcomes(horizon_days=5)
    assert result["tracked"] == 0
    assert tracked == []


def test_search_archive_respects_limit(monkeypatch) -> None:
    from data_sync_service.service import decision as svc

    snapshots = []
    for i in range(25):
        snapshots.append({
            "snapshotDate": date(2026, 8, 7 - (i % 7)),
            "status": "open",
            "agentExchanges": [{"role": "user", "content": f"CN:600519.SH day {i}"}],
            "outcome": None,
        })
    monkeypatch.setattr(
        "data_sync_service.service.decision.list_snapshots",
        lambda limit=60: snapshots,
    )

    hits = svc.search_archive_by_symbol("600519", limit=5)
    assert len(hits) == 5
