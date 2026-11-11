"""Incremental agent watchlist API tests (no Postgres, no network).

``POST /watchlist/registry`` is the desktop-UI full replace: it deletes every
symbol that is not in the posted list. The agent API must never be able to wipe
unrelated rows, so the safety test here is "upsert one symbol keeps the rest".

The DB helpers are monkeypatched with an in-memory store; ``upsert_registry``
(the full-replace path) is patched to raise, proving the write path never
touches it.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from data_sync_service.db import watchlist_automation as registry_db
from data_sync_service.main import app
from data_sync_service.service import agent_brief
from data_sync_service.service.market_quotes import normalize_market_symbol

client = TestClient(app)

AS_OF = "2026-11-11"
UPDATED_AT = "2026-11-10T15:00:00+08:00"


def _row(
    symbol: str,
    *,
    name: str,
    position_pct: float | None = None,
    cost_price: float | None = None,
    source: str = "manual",
    added_at: str = "2026-09-01T09:30:00+08:00",
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "name": name,
        "source": source,
        "addedAt": added_at,
        "positionPct": position_pct,
        "costPrice": cost_price,
        "maxPrice": None,
        "entryDate": "2026-09-01" if position_pct else None,
        "color": None,
    }


def _seed() -> dict[str, dict[str, Any]]:
    rows = [
        _row("ETF:511260", name="10年国债ETF", position_pct=29.07, cost_price=134.46),
        _row("ETF:513350", name="富国油气QDII", position_pct=0.0),
        _row("CN:990001", name="测试标的"),
    ]
    return {row["symbol"]: row for row in rows}


def _full_replace_boom(items: list[dict[str, Any]]) -> int:
    raise AssertionError("upsert_registry (full replace) must not be used by the agent API")


@pytest.fixture()
def store(monkeypatch: pytest.MonkeyPatch) -> dict[str, dict[str, Any]]:
    rows = _seed()

    def _list() -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in sorted(
                rows.values(), key=lambda r: (str(r.get("addedAt") or ""), r["symbol"])
            )
        ]

    def _upsert_items(items: list[dict[str, Any]]) -> int:
        count = 0
        for item in items:
            symbol = normalize_market_symbol(str(item.get("symbol") or ""))
            if not symbol:
                continue
            merged = dict(rows.get(symbol) or {})
            merged.update({k: v for k, v in item.items() if v is not None or k not in merged})
            merged["symbol"] = symbol
            rows[symbol] = merged
            count += 1
        return count

    def _delete(symbols: list[str]) -> int:
        deleted = 0
        for symbol in symbols:
            key = normalize_market_symbol(str(symbol or ""))
            if key in rows:
                del rows[key]
                deleted += 1
        return deleted

    monkeypatch.setattr(registry_db, "list_registry", _list)
    monkeypatch.setattr(registry_db, "upsert_registry_rows", _upsert_items)
    monkeypatch.setattr(registry_db, "delete_registry_symbols", _delete)
    monkeypatch.setattr(registry_db, "latest_registry_updated_at", lambda: UPDATED_AT)
    monkeypatch.setattr(registry_db, "upsert_registry", _full_replace_boom)
    monkeypatch.setattr("data_sync_service.api.v1_agent_routes.shanghai_today_iso", lambda: AS_OF)
    agent_brief.reset_judgment_brief_cache()
    yield rows
    agent_brief.reset_judgment_brief_cache()


def _post(payload: dict[str, Any]) -> Any:
    return client.post("/v1/agent/watchlist", json=payload)


def _symbols(items: list[dict[str, Any]]) -> list[str]:
    return sorted(item["symbol"] for item in items)


# ---------------------------------------------------------------------------
# GET
# ---------------------------------------------------------------------------


def test_get_returns_items_positions_and_never_places_orders(store: dict[str, Any]) -> None:
    resp = client.get("/v1/agent/watchlist")
    assert resp.status_code == 200
    body = resp.json()

    assert body["ok"] is True
    assert body["placesOrders"] is False
    assert body["count"] == 3
    assert _symbols(body["items"]) == ["CN:990001", "ETF:511260", "ETF:513350"]
    # Only positionPct > 0 is an open position (0 is listed but not held).
    assert [p["symbol"] for p in body["positions"]] == ["ETF:511260"]
    assert body["positions"][0]["positionPct"] == 29.07
    assert body["positions"][0]["costPrice"] == 134.46
    assert body["writeApi"] == "POST /v1/agent/watchlist"
    assert "full replace" in body["fullReplaceWarning"]
    assert body["asOfDate"] == AS_OF
    assert body["updatedAt"] == UPDATED_AT


def test_get_markdown_formats(store: dict[str, Any]) -> None:
    md = client.get("/v1/agent/watchlist", params={"format": "md"})
    assert md.status_code == 200
    assert md.headers["content-type"].startswith("text/markdown")
    assert "| symbol | name | positionPct | costPrice | source |" in md.text
    assert "ETF:511260" in md.text
    assert "29.07" in md.text

    route_md = client.get("/v1/agent/watchlist.md")
    assert route_md.status_code == 200
    assert route_md.text == md.text

    bad = client.get("/v1/agent/watchlist", params={"format": "xml"})
    assert bad.status_code == 422


def test_get_omits_updated_at_when_unavailable(
    store: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(registry_db, "latest_registry_updated_at", lambda: None)
    body = client.get("/v1/agent/watchlist").json()
    assert "updatedAt" not in body


# ---------------------------------------------------------------------------
# critical safety: one upsert never deletes unrelated rows
# ---------------------------------------------------------------------------


def test_upsert_one_new_symbol_does_not_delete_existing_rows(store: dict[str, Any]) -> None:
    before = set(store)
    resp = _post(
        {
            "reason": "14:30 confirmed add",
            "ops": [
                {
                    "op": "upsert",
                    "symbol": "ETF:518880",
                    "name": "黄金ETF",
                    "positionPct": 10.0,
                    "costPrice": 7.11,
                }
            ],
        }
    )
    assert resp.status_code == 200
    body = resp.json()

    assert body["placesOrders"] is False
    assert body["count"] == len(before) + 1
    assert set(store) == before | {"ETF:518880"}
    # Nothing else was touched.
    assert store["ETF:511260"]["positionPct"] == 29.07
    assert store["ETF:513350"]["positionPct"] == 0.0
    assert body["applied"][0]["before"] == {}
    assert body["applied"][0]["after"]["symbol"] == "ETF:518880"
    assert body["applied"][0]["after"]["source"] == "agent"
    assert body["applied"][0]["after"]["addedAt"]
    assert [p["symbol"] for p in body["positions"]] == ["ETF:511260", "ETF:518880"]


def test_upsert_merges_existing_row_and_keeps_omitted_fields(store: dict[str, Any]) -> None:
    original = dict(store["ETF:511260"])
    resp = _post(
        {
            "reason": "mark avg cost down",
            "ops": [{"op": "upsert", "symbol": "ETF:511260", "costPrice": 133.0}],
        }
    )
    assert resp.status_code == 200
    after = store["ETF:511260"]

    assert after["costPrice"] == 133.0
    assert after["positionPct"] == original["positionPct"]
    assert after["name"] == original["name"]
    assert after["source"] == "manual"
    assert after["addedAt"] == original["addedAt"]
    assert after["entryDate"] == original["entryDate"]
    assert resp.json()["applied"][0]["before"]["costPrice"] == 134.46
    assert resp.json()["applied"][0]["after"]["costPrice"] == 133.0


# ---------------------------------------------------------------------------
# set_position
# ---------------------------------------------------------------------------


def test_set_position_to_zero_keeps_the_row(store: dict[str, Any]) -> None:
    resp = _post(
        {
            "reason": "position closed, keep symbol for review",
            "ops": [{"op": "set_position", "symbol": "ETF:511260", "positionPct": 0}],
        }
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "ETF:511260" in store
    assert store["ETF:511260"]["positionPct"] == 0
    assert body["positions"] == []
    assert body["applied"][0]["applied"] is True


def test_set_position_on_missing_symbol_is_404(store: dict[str, Any]) -> None:
    before = {k: dict(v) for k, v in store.items()}
    resp = _post(
        {
            "reason": "typo symbol",
            "ops": [{"op": "set_position", "symbol": "ETF:000000", "positionPct": 5}],
        }
    )
    assert resp.status_code == 404
    assert "not in watchlist" in resp.json()["detail"]
    assert store == before


def test_set_position_clamps_and_rounds(store: dict[str, Any]) -> None:
    resp = _post(
        {
            "reason": "clamp",
            "ops": [{"op": "set_position", "symbol": "ETF:513350", "positionPct": 129.876}],
        }
    )
    assert resp.status_code == 200
    assert store["ETF:513350"]["positionPct"] == 100.0
    assert resp.json()["applied"][0]["after"]["positionPct"] == 100.0

    resp = _post(
        {
            "reason": "clamp low",
            "ops": [{"op": "set_position", "symbol": "ETF:513350", "positionPct": -3}],
        }
    )
    assert resp.status_code == 200
    assert store["ETF:513350"]["positionPct"] == 0.0


# ---------------------------------------------------------------------------
# remove
# ---------------------------------------------------------------------------


def test_remove_holding_requires_force(store: dict[str, Any]) -> None:
    resp = _post(
        {"reason": "drop holding by accident", "ops": [{"op": "remove", "symbol": "ETF:511260"}]}
    )
    assert resp.status_code == 422
    assert "force" in resp.json()["detail"]
    assert "ETF:511260" in store

    resp = _post(
        {
            "reason": "confirmed exit",
            "ops": [{"op": "remove", "symbol": "ETF:511260", "force": True}],
        }
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "ETF:511260" not in store
    assert body["applied"][0]["applied"] is True
    assert body["applied"][0]["after"] == {}
    assert body["count"] == 2


def test_remove_missing_symbol_is_idempotent(store: dict[str, Any]) -> None:
    before = {k: dict(v) for k, v in store.items()}
    resp = _post({"reason": "already gone", "ops": [{"op": "remove", "symbol": "ETF:512000"}]})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["applied"] == [
        {
            "op": "remove",
            "symbol": "ETF:512000",
            "applied": False,
            "before": {},
            "after": {},
        }
    ]
    assert store == before


# ---------------------------------------------------------------------------
# dryRun
# ---------------------------------------------------------------------------


def test_dry_run_previews_without_writing(store: dict[str, Any]) -> None:
    before = {k: dict(v) for k, v in store.items()}
    resp = _post(
        {
            "reason": "preview",
            "dryRun": True,
            "ops": [
                {"op": "upsert", "symbol": "ETF:518880", "positionPct": 12.5},
                {"op": "set_position", "symbol": "ETF:513350", "positionPct": 3.25},
                {"op": "remove", "symbol": "CN:990001"},
            ],
        }
    )
    assert resp.status_code == 200
    body = resp.json()

    assert body["dryRun"] is True
    assert body["placesOrders"] is False
    assert store == before  # nothing written
    assert _symbols(body["items"]) == ["ETF:511260", "ETF:513350", "ETF:518880"]
    by_symbol = {row["symbol"]: row for row in body["applied"]}
    assert by_symbol["ETF:518880"]["after"]["positionPct"] == 12.5
    assert by_symbol["ETF:513350"]["before"]["positionPct"] == 0.0
    assert by_symbol["ETF:513350"]["after"]["positionPct"] == 3.25
    assert by_symbol["CN:990001"]["applied"] is True
    assert by_symbol["CN:990001"]["after"] == {}
    assert sorted(p["symbol"] for p in body["positions"]) == [
        "ETF:511260",
        "ETF:513350",
        "ETF:518880",
    ]


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "payload",
    [
        {"ops": [{"op": "upsert", "symbol": "ETF:518880"}]},  # reason missing
        {"reason": "", "ops": [{"op": "upsert", "symbol": "ETF:518880"}]},
        {"reason": "   ", "ops": [{"op": "upsert", "symbol": "ETF:518880"}]},
        {"reason": "x", "ops": []},
        {"reason": "x"},
        {"reason": "x", "ops": [{"op": "replace_all", "symbol": "ETF:518880"}]},
        {"reason": "x", "ops": [{"op": "upsert"}]},  # symbol missing
        {"reason": "x", "ops": [{"op": "upsert", "symbol": "ETF:518880", "position_pct": 1}]},
        {"reason": "x", "ops": [{"op": "upsert", "symbol": "ETF:518880", "positionPct": None}]},
        {"reason": "x", "ops": [{"op": "set_position", "symbol": "ETF:511260"}]},
        {
            "reason": "x",
            "ops": [{"op": "upsert", "symbol": f"ETF:{i:06d}"} for i in range(51)],
        },
    ],
)
def test_validation_errors_are_422(store: dict[str, Any], payload: dict[str, Any]) -> None:
    before = {k: dict(v) for k, v in store.items()}
    resp = _post(payload)
    assert resp.status_code == 422
    assert store == before


def test_validation_does_not_partially_apply(store: dict[str, Any]) -> None:
    before = {k: dict(v) for k, v in store.items()}
    resp = _post(
        {
            "reason": "mixed batch",
            "ops": [
                {"op": "upsert", "symbol": "ETF:518880", "positionPct": 5},
                {"op": "set_position", "symbol": "ETF:000000", "positionPct": 5},
            ],
        }
    )
    assert resp.status_code == 404
    assert store == before


# ---------------------------------------------------------------------------
# full-replace path is unreachable + cache eviction is fail-open
# ---------------------------------------------------------------------------


def test_post_never_calls_full_replace_upsert(store: dict[str, Any]) -> None:
    resp = _post(
        {
            "reason": "explicitly assert no full replace",
            "ops": [{"op": "upsert", "symbol": "ETF:518880", "positionPct": 1}],
        }
    )
    # registry_db.upsert_registry is patched to raise for the whole fixture.
    assert resp.status_code == 200
    assert store["ETF:511260"]["positionPct"] == 29.07


def test_cache_invalidation_failure_does_not_fail_the_write(
    store: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    def _boom(reason: str = "") -> None:
        raise RuntimeError("cache store unavailable")

    monkeypatch.setattr(agent_brief, "invalidate_judgment_brief_cache", _boom)
    resp = _post(
        {
            "reason": "cache failure is fail-open",
            "ops": [{"op": "upsert", "symbol": "ETF:518880", "positionPct": 1}],
        }
    )
    assert resp.status_code == 200
    assert "ETF:518880" in store
