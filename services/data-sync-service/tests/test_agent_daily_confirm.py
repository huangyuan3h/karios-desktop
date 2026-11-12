"""14:30 advice / confirmation / next-session deviation persistence (Step 5).

Pure reconcile tests run without a DB; the record/read-back tests use the
TIP-015 ``decision_actions`` table with a unique ``source`` and delete only
their own rows in teardown (AGENTS.md DB test discipline).
"""

from __future__ import annotations

import inspect
import uuid
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from data_sync_service.db import decision as decision_db
from data_sync_service.main import app
from data_sync_service.service import agent_brief, agent_daily_confirm, agent_decision_log, harbor

client = TestClient(app)
SHANGHAI = ZoneInfo("Asia/Shanghai")
_SOURCES: set[str] = set()


@pytest.fixture
def _db_clean() -> Any:
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
    src = f"test-daily-confirm-{uuid.uuid4().hex[:8]}"
    _SOURCES.add(src)
    return src


def _advice(*, no_change: bool = False, symbol: str = "ETF:513350") -> dict[str, Any]:
    if no_change:
        return {
            "tradeDate": "2026-11-10",
            "mode": "homeport",
            "strategyLabel": "母港 M30",
            "dailyAdjust": True,
            "noChange": True,
            "mustAct": False,
            "mustWatch": False,
            "adjustments": [],
            "lines": ["今日无需调整"],
            "summary": "今日无需调整",
            "riskGates": {},
            "warnings": [],
        }
    return {
        "tradeDate": "2026-11-10",
        "mode": "homeport",
        "strategyLabel": "母港 M30",
        "dailyAdjust": True,
        "noChange": False,
        "mustAct": True,
        "mustWatch": False,
        "adjustments": [
            {
                "symbol": symbol,
                "side": "SELL",
                "to": "逆回购",
                "reason": "峰值回撤 8.6%",
                "pendingOpenSell": True,
            }
        ],
        "lines": [f"卖出：{symbol} → 逆回购（峰值回撤 8.6%）"],
        "summary": f"卖出：{symbol} → 逆回购",
        "riskGates": {},
        "warnings": [],
    }


def _snapshot(
    *,
    advice: dict[str, Any],
    status: str = "confirmed",
    lines: list[Any] | None = None,
) -> dict[str, Any]:
    return {
        "advice": advice,
        "decision": {"status": status, "decisionLines": lines or []},
    }


# ---------------------------------------------------------------------------
# Pure reconcile comparison
# ---------------------------------------------------------------------------


def test_reconcile_no_change_and_unchanged_book_is_agreement() -> None:
    out = agent_daily_confirm.reconcile_snapshot(
        _snapshot(advice=_advice(no_change=True)),
        next_session="2026-11-11",
        registry=[{"symbol": "ETF:513350", "positionPct": 63.5, "entryDate": "2026-10-01"}],
        trades=[],
    )
    assert out["status"] == "done"
    assert out["deviations"] == []
    assert out["asOf"] == "2026-11-11"


def test_reconcile_intended_sell_not_filled_is_a_clear_deviation() -> None:
    out = agent_daily_confirm.reconcile_snapshot(
        _snapshot(advice=_advice()),
        next_session="2026-11-11",
        registry=[{"symbol": "ETF:513350", "positionPct": 63.5}],
        trades=[],
    )
    assert out["status"] == "done"
    assert len(out["deviations"]) == 1
    dev = out["deviations"][0]
    assert dev["type"] == "not_filled"
    assert "未成交" in dev["copy"]
    assert "仍持有" in dev["copy"]


def test_reconcile_matching_sell_trade_agrees() -> None:
    out = agent_daily_confirm.reconcile_snapshot(
        _snapshot(advice=_advice()),
        next_session="2026-11-11",
        registry=[],
        trades=[
            {
                "symbol": "ETF:513350",
                "side": "SELL",
                "tradeDate": "2026-11-11",
                "price": 1.2,
                "positionPct": 63.5,
            }
        ],
    )
    assert out["status"] == "done"
    assert out["deviations"] == []
    assert out["fills"][0]["symbol"] == "ETF:513350"


def test_reconcile_position_drop_without_trade_agrees_but_flags_missing() -> None:
    out = agent_daily_confirm.reconcile_snapshot(
        _snapshot(advice=_advice()),
        next_session="2026-11-11",
        registry=[],
        trades=[],
    )
    assert out["status"] == "done"
    assert out["deviations"] == []
    assert any("无 SELL 成交记录" in reason for reason in out["missing"])


def test_reconcile_amended_lines_are_the_target() -> None:
    """His amended lines win: the original sell is no longer the target."""
    out = agent_daily_confirm.reconcile_snapshot(
        _snapshot(
            advice=_advice(),
            status="amended",
            lines=["持有 ETF:513350 不动", "买入 ETF:511260"],
        ),
        next_session="2026-11-11",
        registry=[{"symbol": "ETF:513350", "positionPct": 63.5}],
        trades=[
            {"symbol": "ETF:513350", "side": "SELL", "tradeDate": "2026-11-11"},
        ],
    )
    assert out["status"] == "done"
    # The amended target is a BUY of 511260; the 513350 sell is unexpected.
    assert out["deviations"]
    assert any(d["type"] == "not_filled" and d["symbol"] == "ETF:511260" for d in out["deviations"])
    assert any(
        d["type"] == "unexpected_fill" and d["symbol"] == "ETF:513350" for d in out["deviations"]
    )


def test_reconcile_wrong_direction_is_a_deviation() -> None:
    out = agent_daily_confirm.reconcile_snapshot(
        _snapshot(advice=_advice()),
        next_session="2026-11-11",
        registry=[],
        trades=[
            {"symbol": "ETF:513350", "side": "BUY", "tradeDate": "2026-11-11"},
        ],
    )
    assert out["status"] == "done"
    assert any(d["type"] == "wrong_direction" for d in out["deviations"])
    assert any("方向" in d["copy"] for d in out["deviations"])


def test_reconcile_unparseable_amendment_degrades_not_agrees() -> None:
    out = agent_daily_confirm.reconcile_snapshot(
        _snapshot(advice=_advice(), status="amended", lines=["再看看"]),
        next_session="2026-11-11",
        registry=[],
        trades=[],
    )
    assert out["status"] == "degraded"
    assert out["deviations"] == []


def test_reconcile_missing_inputs_degrade_and_keep_no_fake_agreement() -> None:
    out = agent_daily_confirm.reconcile_snapshot(
        _snapshot(advice=_advice()),
        next_session="2026-11-11",
        registry=None,
        trades=None,
        errors=["registry down"],
    )
    assert out["status"] == "degraded"
    assert any("registry down" in reason for reason in out["missing"])
    assert any("注册表不可用" in reason for reason in out["missing"])
    assert any("成交流水不可用" in reason for reason in out["missing"])


# ---------------------------------------------------------------------------
# Advice + decision persistence (DB, unique source)
# ---------------------------------------------------------------------------


def _post_confirm(source: str, **body: Any) -> dict[str, Any]:
    payload = {
        "tradeDate": "2026-11-10",
        "mode": "homeport",
        "source": source,
        **body,
    }
    resp = client.post("/v1/agent/daily-confirm", json=payload)
    assert resp.status_code == 200, resp.text
    return resp.json()


def _count_rows(source: str) -> int:
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {decision_db.ACTIONS_TABLE} WHERE source = %s",
                (source,),
            )
            row = cur.fetchone()
    return int(row[0]) if row else 0


@pytest.mark.requires_postgres
def test_record_daily_confirm_uses_server_advice_and_updates_in_place(
    _db_clean: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.daily_adjust.compose_daily_adjust",
        lambda mode, trade_date=None: _advice(),
    )
    source = _source()
    first = _post_confirm(
        source,
        decision="confirmed",
        contractSnapshotId="dc:2026-11-10:homeport_m30:SELL_TO_REPO:OIL",
    )
    item = first["item"]
    assert item["source"] == source
    assert item["outcome"]["kind"] == "daily_confirm"
    assert item["outcome"]["advice"]["available"] is True
    assert item["outcome"]["advice"]["lines"][0].startswith("卖出：ETF:513350")
    assert item["outcome"]["decision"]["status"] == "confirmed"
    assert item["outcome"]["reconcile"]["status"] == "pending"

    second = _post_confirm(
        source,
        decision="amended",
        note="改为明日再卖",
        decisionLines=["卖出 ETF:513350 → 逆回购（明日）"],
    )
    updated = second["item"]
    assert updated["id"] == item["id"]
    assert _count_rows(source) == 1
    assert updated["outcome"]["decision"]["status"] == "amended"
    assert updated["outcome"]["decision"]["note"] == "改为明日再卖"
    assert updated["outcome"]["decision"]["decisionLines"] == ["卖出 ETF:513350 → 逆回购（明日）"]
    assert updated["outcome"]["advice"]["lines"] == _advice()["lines"]


@pytest.mark.requires_postgres
def test_record_daily_confirm_fail_open_on_composer_error(
    _db_clean: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boom(mode: str, trade_date: str | None = None) -> dict[str, Any]:
        raise RuntimeError("composer down")

    monkeypatch.setattr("data_sync_service.service.daily_adjust.compose_daily_adjust", _boom)
    source = _source()
    out = _post_confirm(source, decision="confirmed", note="照常执行")
    item = out["item"]
    assert item["outcome"]["advice"]["available"] is False
    assert "composer down" in item["outcome"]["adviceError"]
    assert item["outcome"]["decision"]["status"] == "confirmed"
    assert item["outcome"]["decision"]["note"] == "照常执行"
    assert _count_rows(source) == 1


@pytest.mark.requires_postgres
def test_get_log_exposes_advice_decision_and_reconcile(
    _db_clean: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.daily_adjust.compose_daily_adjust",
        lambda mode, trade_date=None: _advice(no_change=True),
    )
    source = _source()
    _post_confirm(source, decision="confirmed", note="同意")

    out = agent_decision_log.read_decision_log(trade_date="2026-11-10", lookback=1, limit=200)
    mine = [r for r in out["items"] if r.get("source") == source]
    assert len(mine) == 1
    row = mine[0]
    assert row["kind"] == "daily_confirm"
    assert row["mode"] == "homeport"
    assert row["advice"]["noChange"] is True
    assert row["advice"]["lines"] == ["今日无需调整"]
    assert row["decision"]["status"] == "confirmed"
    assert row["decision"]["note"] == "同意"
    assert row["reconcile"]["status"] == "pending"

    md = "\n".join(agent_brief._render_decision_log({"decisionLog": {**out}}, compact=True))
    assert "14:30 建议（homeport）" in md
    assert "今日无需调整" in md
    assert "决定: confirmed" in md
    assert "次日对账: pending" in md


def test_old_free_text_decision_rows_still_serialize() -> None:
    compact = agent_decision_log.compact_decision_row(
        {
            "id": 1,
            "symbol": "CN:600000",
            "action": "decision_log",
            "rationale": "旧格式",
            "status": "logged",
            "source": "agent",
            "snapshotDate": "2026-11-10",
            "outcome": {"adoptedOverlay": True},
            "createdAt": "2026-11-10T06:30:00+00:00",
        }
    )
    assert compact["judgment"] == "旧格式"
    assert compact["adoptedOverlay"] is True
    assert "kind" not in compact


@pytest.mark.requires_postgres
def test_reconcile_pending_writes_back_same_row(
    _db_clean: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.daily_adjust.compose_daily_adjust",
        lambda mode, trade_date=None: _advice(),
    )
    source = _source()
    _post_confirm(source, decision="confirmed")

    monkeypatch.setattr(
        agent_daily_confirm,
        "_load_evidence",
        lambda next_session: ([], [], []),
    )
    now = datetime(2026, 11, 30, 19, 0, tzinfo=SHANGHAI)
    result = agent_daily_confirm.reconcile_pending(trade_date="2026-11-30", source=source, now=now)
    assert result["ok"] is True
    assert result["checked"] == 1
    assert result["counts"]["done"] == 1
    assert result["items"][0]["updated"] is True
    assert result["items"][0]["reconcile"]["status"] == "done"
    assert _count_rows(source) == 1

    # Already done → not selected again; no duplicate confirm rows.
    again = agent_daily_confirm.reconcile_pending(trade_date="2026-11-30", source=source, now=now)
    assert again["checked"] == 0
    assert _count_rows(source) == 1


@pytest.mark.requires_postgres
def test_reconcile_pending_degrades_on_missing_evidence_and_keeps_row(
    _db_clean: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.daily_adjust.compose_daily_adjust",
        lambda mode, trade_date=None: _advice(no_change=True),
    )
    source = _source()
    _post_confirm(source, decision="confirmed")

    monkeypatch.setattr(
        agent_daily_confirm,
        "_load_evidence",
        lambda next_session: (None, None, ["registry down"]),
    )
    now = datetime(2026, 11, 30, 19, 0, tzinfo=SHANGHAI)
    result = agent_daily_confirm.reconcile_pending(trade_date="2026-11-30", source=source, now=now)
    assert result["counts"]["degraded"] == 1
    assert result["items"][0]["reconcile"]["status"] == "degraded"
    assert _count_rows(source) == 1


@pytest.mark.requires_postgres
def test_reconcile_pending_waits_before_next_session_close(
    _db_clean: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.daily_adjust.compose_daily_adjust",
        lambda mode, trade_date=None: _advice(no_change=True),
    )
    source = _source()
    _post_confirm(source, decision="confirmed")
    now = datetime(2026, 11, 11, 10, 0, tzinfo=SHANGHAI)  # during the next session
    result = agent_daily_confirm.reconcile_pending(trade_date="2026-11-11", source=source, now=now)
    assert result["counts"]["pending"] == 1
    assert result["items"][0]["reconcile"]["status"] == "pending"


def test_frozen_constants_unchanged() -> None:
    assert harbor.TRAIL_PCT == 8.0
    assert inspect.signature(harbor.parking_replay).parameters["cooldown_days"].default == 0
