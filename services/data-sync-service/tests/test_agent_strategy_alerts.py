"""After-close strategy alert tests (advice only, no DB/network, no writes on GET).

Covers the Step-6 contract:
- a new decay emits ``newToday``; a standing cold/revived state does not,
- a revival emits as the sibling of decay,
- an excess classification change emits once and is idempotent on re-run,
- incomparable units are skipped (never compared),
- GET reads the snapshot and never writes one,
- the brief version is bumped and carries the compact alert block,
- frozen ``TRAIL_PCT`` / parking cooldown stay untouched.
"""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from data_sync_service.api import backtest_routes as br
from data_sync_service.main import app
from data_sync_service.service import agent_brief, harbor
from data_sync_service.service import agent_strategy_alerts as alerts

client = TestClient(app)

AS_OF = "2026-11-12"
PREV = "2026-11-11"


def _factor(
    fid: str,
    *,
    name: str | None = None,
    status: str = "cold",
    position: int = 0,
    unit: str = "点",
    net: float = -10.0,
    percentile: float | None = None,
    trades: int = 40,
) -> dict[str, Any]:
    return {
        "id": fid,
        "name": name or fid,
        "family": "fixture",
        "source": "fixture",
        "doc": "fixture",
        "unit": unit,
        "status": status,
        "badge": {"cold": "冷库", "watch": "观察", "revived": "复活"}[status],
        "position": position,
        "percentile": percentile,
        "confirmations": 1,
        "days_since_change": 1,
        "history": [
            {
                "window": "holdout",
                "date": "2026-09-30",
                "percentile": percentile,
                "net": net,
                "trades": trades,
                "revived": False,
            }
        ],
    }


def _vault(**overrides: dict[str, Any]) -> dict[str, Any]:
    base = {
        "m30": _factor("m30", name="母港M30 (参考)", status="watch", unit="%", net=-5.0, trades=2),
        "starship_b": _factor("starship_b", name="星舰B默认", unit="点", net=-12.0),
        "harbor": _factor("harbor", name="港湾 (参考)", status="revived", unit="点", net=11.0),
    }
    base.update(overrides)
    rows = list(base.values())
    summary = {
        "n_factors": len(rows),
        "n_cold": sum(1 for r in rows if r["status"] == "cold"),
        "n_watch": sum(1 for r in rows if r["status"] == "watch"),
        "n_revived": sum(1 for r in rows if r["status"] == "revived"),
    }
    return {"meta": {"generated_at": AS_OF}, "summary": summary, "factors": rows}


def _states(vault: dict[str, Any]) -> dict[str, str]:
    return {r["id"]: r["status"] for r in vault["factors"]}


def _write_reports(
    tmp_path: Path,
    *,
    as_of: str = AS_OF,
    vault: dict[str, Any] | None = None,
    history: list[dict[str, Any]] | None = None,
    transitions: list[dict[str, Any]] | None = None,
    sgap_revival_pos: int = 0,
    sgap_generated_at: str | None = None,
) -> Path:
    reports = tmp_path / "reports"
    reports.mkdir(exist_ok=True)
    payload = vault if vault is not None else _vault()
    payload = json.loads(json.dumps(payload))
    payload.setdefault("meta", {})["generated_at"] = as_of
    (reports / "factor_vault.json").write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8"
    )
    hist = history if history is not None else [{"date": as_of, "states": _states(payload)}]
    (reports / "factor_vault_history.json").write_text(
        json.dumps({"history": hist}, ensure_ascii=False), encoding="utf-8"
    )
    (reports / "factor_vault_transitions.json").write_text(
        json.dumps({"transitions": transitions or []}, ensure_ascii=False), encoding="utf-8"
    )
    (reports / "sgap_decay.json").write_text(
        json.dumps(
            {
                "meta": {"generated_at": sgap_generated_at or as_of},
                "summary": {
                    "date_end": "2026-09-23",
                    "revival_pos": sgap_revival_pos,
                    "latest_r40_percentile": 2.2,
                },
            }
        ),
        encoding="utf-8",
    )
    return reports


# ---------------------------------------------------------------------------
# decay / revival
# ---------------------------------------------------------------------------


def test_new_decay_emits_new_today(tmp_path: Path) -> None:
    vault = _vault(starship_b=_factor("starship_b", name="星舰B默认", status="cold"))
    prev_vault = _vault(starship_b=_factor("starship_b", name="星舰B默认", status="revived"))
    reports = _write_reports(
        tmp_path,
        vault=vault,
        history=[
            {"date": PREV, "states": _states(prev_vault)},
            {"date": AS_OF, "states": _states(vault)},
        ],
    )

    out = alerts.build_strategy_alerts(as_of=AS_OF, reports_dir=reports)

    assert out["available"] is True
    assert out["quiet"] is False
    decay = [e for e in out["events"] if e["kind"] == "decay" and e["factorId"] == "starship_b"]
    assert len(decay) == 1
    assert decay[0]["newToday"] is True
    assert decay[0]["from"] == "revived"
    assert decay[0]["to"] == "cold"
    assert "不自动改设置" in decay[0]["copy"]
    assert "不下单" in decay[0]["copy"]
    assert out["copyLines"] == [e["copy"] for e in out["events"]]


def test_standing_cold_state_is_not_rebroadcast(tmp_path: Path) -> None:
    standing = {
        "starship_b": _factor("starship_b", status="cold"),
        "starship_a25": _factor("starship_a25", status="revived", position=10),
    }
    vault_prev = _vault(**standing)
    reports_prev = _write_reports(
        tmp_path,
        as_of=PREV,
        vault=vault_prev,
        history=[{"date": PREV, "states": _states(vault_prev)}],
    )
    first = alerts.run_strategy_alerts(as_of=PREV, reports_dir=reports_prev)
    assert first["persisted"] is True
    assert first["quiet"] is True  # 点 vs baseline % -> skipped, not an event

    vault_today = _vault(**standing)
    reports_today = _write_reports(
        tmp_path,
        as_of=AS_OF,
        vault=vault_today,
        history=[
            {"date": PREV, "states": _states(vault_today)},
            {"date": AS_OF, "states": _states(vault_today)},
        ],
    )
    out = alerts.build_strategy_alerts(as_of=AS_OF, reports_dir=reports_today)

    assert out["available"] is True
    assert out["events"] == []
    assert out["newToday"] == []
    assert out["quiet"] is True
    # Standing lights are still visible as current state, not as daily alerts.
    lights = {row["id"]: row for row in out["currentState"]["standingLights"]}
    assert lights["starship_b"]["status"] == "cold"
    assert "不是今日新提醒" in lights["starship_b"]["note"]
    assert lights["starship_a25"]["status"] == "revived"
    assert "不是今日新提醒" not in lights["starship_a25"]["note"]
    assert "复活灯" in lights["starship_a25"]["note"]


def test_new_revival_emits_new_today(tmp_path: Path) -> None:
    prev_vault = _vault(starship_b=_factor("starship_b", name="星舰B默认", status="cold"))
    vault = _vault(
        starship_b=_factor("starship_b", name="星舰B默认", status="revived", position=10)
    )
    reports = _write_reports(
        tmp_path,
        vault=vault,
        history=[
            {"date": PREV, "states": _states(prev_vault)},
            {"date": AS_OF, "states": _states(vault)},
        ],
    )

    out = alerts.build_strategy_alerts(as_of=AS_OF, reports_dir=reports)

    revival = [e for e in out["events"] if e["kind"] == "revival" and e["factorId"] == "starship_b"]
    assert len(revival) == 1
    assert revival[0]["from"] == "cold"
    assert revival[0]["to"] == "revived"
    assert revival[0]["toPosition"] == 10
    assert "复活提醒" in revival[0]["copy"]
    assert "不自动改设置" in revival[0]["copy"]
    assert out["currentState"]["policyAction"] == "advise_follow"


# ---------------------------------------------------------------------------
# excess vs baseline
# ---------------------------------------------------------------------------


def test_excess_classification_change_emits_once_and_is_idempotent(tmp_path: Path) -> None:
    behind_vault = _vault(h15=_factor("h15", name="H15 小市值+ROE", unit="%", net=-8.0))
    reports_prev = _write_reports(
        tmp_path,
        as_of=PREV,
        vault=behind_vault,
        history=[{"date": PREV, "states": _states(behind_vault)}],
    )
    first = alerts.run_strategy_alerts(as_of=PREV, reports_dir=reports_prev)
    assert first["persisted"] is True
    first_event = [e for e in first["events"] if e["factorId"] == "h15"]
    assert len(first_event) == 1
    assert first_event[0]["kind"] == "excess_change"
    assert first_event[0]["fromClass"] is None
    assert first_event[0]["toClass"] == "behind"

    ahead_vault = _vault(h15=_factor("h15", name="H15 小市值+ROE", unit="%", net=2.0))
    reports_today = _write_reports(
        tmp_path,
        as_of=AS_OF,
        vault=ahead_vault,
        history=[
            {"date": PREV, "states": _states(ahead_vault)},
            {"date": AS_OF, "states": _states(ahead_vault)},
        ],
    )
    second = alerts.run_strategy_alerts(as_of=AS_OF, reports_dir=reports_today)
    change = [e for e in second["events"] if e["factorId"] == "h15"]
    assert len(change) == 1
    assert change[0]["fromClass"] == "behind"
    assert change[0]["toClass"] == "ahead"
    assert change[0]["evidence"]["unit"] == "%"
    assert change[0]["evidence"]["baselineNet"] == -5.0
    assert "不自动改设置" in change[0]["copy"]

    again = alerts.run_strategy_alerts(as_of=AS_OF, reports_dir=reports_today)
    keys_before = [e["eventKey"] for e in second["events"]]
    keys_after = [e["eventKey"] for e in again["events"]]
    assert keys_before == keys_after
    assert len(keys_after) == len(set(keys_after))

    persisted = json.loads(
        alerts.snapshot_path(AS_OF, reports_today).read_text(encoding="utf-8")
    )

    def _keys(rows: list[dict[str, Any]]) -> list[str]:
        return [row["eventKey"] for row in rows]

    assert _keys(persisted["events"]) == keys_after
    assert persisted["previousAsOf"] == PREV


def test_incomparable_units_are_skipped(tmp_path: Path) -> None:
    vault = _vault(
        starship_b=_factor("starship_b", name="星舰B默认", unit="点", net=100.0),
    )
    reports = _write_reports(
        tmp_path,
        vault=vault,
        history=[{"date": AS_OF, "states": _states(vault)}],
    )

    out = alerts.build_strategy_alerts(as_of=AS_OF, reports_dir=reports)

    skipped = {row["factorId"]: row for row in out["skippedIncomparable"]}
    assert skipped["starship_b"]["reason"] == "unit_mismatch"
    assert skipped["starship_b"]["unit"] == "点"
    assert skipped["starship_b"]["baselineUnit"] == "%"
    assert not any(e["factorId"] == "starship_b" for e in out["events"])
    assert not any(e["factorId"] == "harbor" for e in out["events"])
    # Frozen fixture data is deliberately stale: degraded, with a reason, but
    # still available and quiet (no invented events).
    assert out["degraded"] is True
    assert out["freshness"]["stale"] is True
    assert out["freshness"]["reason"]
    assert out["quiet"] is True


# ---------------------------------------------------------------------------
# GET is read-only; brief version + block
# ---------------------------------------------------------------------------


def test_get_reads_snapshot_and_never_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reports = _write_reports(tmp_path, vault=_vault(), history=[{"date": AS_OF, "states": _states(_vault())}])
    monkeypatch.setattr(br, "REPORTS_DIR", reports)
    path = alerts.snapshot_path(AS_OF, reports)
    assert not path.exists()

    resp = client.get(f"/v1/agent/strategy-alerts?asOf={AS_OF}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is False
    assert body["quiet"] is True
    assert body["events"] == []
    assert body["reason"]
    assert not path.exists()  # GET must not run the writer

    bad = client.get("/v1/agent/strategy-alerts?asOf=2026-13-99")
    assert bad.status_code == 422

    run = client.post(f"/v1/agent/strategy-alerts/run?asOf={AS_OF}")
    assert run.status_code == 200
    assert run.json()["persisted"] is True
    assert path.exists()

    before = path.read_bytes()
    mtime = path.stat().st_mtime_ns
    read = client.get(f"/v1/agent/strategy-alerts?asOf={AS_OF}")
    assert read.status_code == 200
    assert read.json()["available"] is True
    assert read.json()["asOf"] == AS_OF
    assert path.read_bytes() == before
    assert path.stat().st_mtime_ns == mtime


def test_brief_version_and_compact_block(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reports = _write_reports(tmp_path)
    monkeypatch.setattr(br, "REPORTS_DIR", reports)

    assert agent_brief.BRIEF_VERSION == "2026-11-12.2"

    block = agent_brief._strategy_alerts_block(day=AS_OF)
    assert block["available"] is False
    assert block["quiet"] is True
    assert block["snapshotMissing"] is True
    # The brief path reads only; it must not create the snapshot directory.
    assert not alerts.alerts_dir(reports).exists()

    alerts.run_strategy_alerts(as_of=AS_OF, reports_dir=reports)
    block = agent_brief._strategy_alerts_block(day=AS_OF)
    assert block["available"] is True
    assert block["asOf"] == AS_OF
    assert set(block) >= {"available", "quiet", "newToday", "asOf", "freshness", "degraded"}
    assert block["stale"] == block["freshness"]["stale"]
    assert block["baseline"] == "homeport_m30"
    assert block["baselineLabel"] == "母港 M30"


def test_render_alerts_surfaces_new_strategy_events_front_loaded() -> None:
    copy = "衰减提醒（仅告知，不自动改设置、不下单）：星舰B默认 从 复活 衰减到 冷库"
    brief = {
        "contract": {},
        "strategyAlerts": {
            "available": True,
            "degraded": False,
            "quiet": False,
            "freshness": {"stale": False},
            "newToday": [{"eventKey": "e1", "kind": "decay", "copy": copy}],
        },
    }

    text = "\n".join(agent_brief._render_alerts(brief))

    assert "盘后策略提醒" in text
    assert copy in text
    # Front-loaded: the section is above the contract section in the markdown.
    markdown = agent_brief.render_judgment_brief_markdown(brief)
    assert markdown.index("盘后策略提醒") < markdown.index("## 1. 契约 contract")


def test_frozen_constants_unchanged() -> None:
    assert harbor.TRAIL_PCT == 8.0
    assert inspect.signature(harbor.parking_replay).parameters["cooldown_days"].default == 0
