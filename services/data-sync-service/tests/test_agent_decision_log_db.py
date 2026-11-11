"""DB integration tests for the readable decision log (Step 3, 2026-11-11).

``POST /v1/agent/decision-log`` writes a TIP-015 ``decision_actions`` row;
``GET /v1/agent/decision-log`` and the judgment brief read it back. Rows are
written with a unique ``source`` and deleted in teardown so the real decision
log is never polluted (see AGENTS.md DB test discipline).
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from fastapi.testclient import TestClient

from data_sync_service.db import decision as decision_db
from data_sync_service.main import app
from data_sync_service.service import agent_brief, agent_decision_log

pytestmark = pytest.mark.requires_postgres

client = TestClient(app)

_SOURCES: set[str] = set()


@pytest.fixture(autouse=True)
def _ensure_and_clean() -> Any:
    decision_db.ensure_table()
    yield
    if _SOURCES:
        from data_sync_service.db import get_connection

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {decision_db.ACTIONS_TABLE} WHERE source = ANY(%s)",
                    (sorted(_SOURCES),),
                )
            conn.commit()
        _SOURCES.clear()


def _source() -> str:
    src = f"test-decision-log-{uuid.uuid4().hex[:8]}"
    _SOURCES.add(src)
    return src


def _symbol() -> str:
    return f"CN:99{uuid.uuid4().hex[:6]}".upper()


def test_get_decision_log_reads_row_written_by_post() -> None:
    source = _source()
    symbol = _symbol()
    posted = client.post(
        "/v1/agent/decision-log",
        json={
            "tradeDate": "2026-11-10",
            "judgment": "破线卖油 → 逆回购；暂缓同键回买",
            "contractSnapshotId": "dc:2026-11-10:homeport_m30:SELL_TO_REPO:OIL",
            "adoptedOverlay": True,
            "overlayId": "ov-test",
            "symbol": symbol,
            "source": source,
        },
    )
    assert posted.status_code == 200
    assert posted.json()["ok"] is True

    resp = client.get(
        "/v1/agent/decision-log",
        params={
            "tradeDate": "2026-11-10",
            "lookback": 1,
            "symbol": symbol,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["available"] is True
    assert body["tradeDate"] == "2026-11-10"
    assert body["count"] == 1
    item = body["items"][0]
    assert item["tradeDate"] == "2026-11-10"
    assert item["symbol"] == symbol
    assert item["judgment"] == "破线卖油 → 逆回购；暂缓同键回买"
    assert item["action"] == "hold_same_key_rebuy"
    assert item["adoptedOverlay"] is True
    assert item["contractSnapshotId"] == "dc:2026-11-10:homeport_m30:SELL_TO_REPO:OIL"
    assert item["source"] == source


def test_brief_decision_log_block_includes_written_row() -> None:
    source = _source()
    symbol = _symbol()
    decision_db.insert_action(
        symbol=symbol,
        action="decision_log",
        rationale="14:30 确认：持有 BOND10，不追高",
        status="logged",
        source=source,
        snapshot_date=decision_log_date("2026-11-10"),
        outcome={
            "contractSnapshotId": "dc:2026-11-10:homeport_m30:HOLD:BOND10",
            "adoptedOverlay": False,
        },
    )

    block = agent_brief._decision_log_block("2026-11-10")
    assert block["available"] is True
    mine = [r for r in block["items"] if r.get("symbol") == symbol]
    assert mine and mine[0]["contractSnapshotId"].startswith("dc:2026-11-10")
    assert mine[0]["adoptedOverlay"] is False


def test_read_decision_log_filters_by_symbol_and_is_compact() -> None:
    source = _source()
    symbol = _symbol()
    decision_db.insert_action(
        symbol=symbol,
        action="decision_log",
        rationale="unit",
        source=source,
        snapshot_date=decision_log_date("2026-11-10"),
        outcome=None,
    )
    out = agent_decision_log.read_decision_log(trade_date="2026-11-10", lookback=1, symbol=symbol)
    assert out["available"] is True
    assert out["lookbackSessions"] == 1
    assert out["items"][0]["symbol"] == symbol
    assert "outcome" not in out["items"][0]


def decision_log_date(iso: str) -> Any:
    from datetime import date

    return date.fromisoformat(iso)
