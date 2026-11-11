"""Factor-system contract tests (advice only, fail-open, no DB/network).

Covers the three policy branches the brief requires:
- cold -> wait in 母港 M30
- decay -> advise_exit
- revived + live preconditions unmet -> advise_follow but live weight 0
Also covers the API routes, compact/full shapes, and freshness honesty.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from data_sync_service.api import backtest_routes as br
from data_sync_service.main import app
from data_sync_service.service import agent_brief, factor_system

client = TestClient(app)


def _factor(
    fid: str,
    *,
    name: str | None = None,
    status: str = "cold",
    position: int = 0,
    percentile: float | None = 10.0,
    holdout_net: float | None = -19.8,
    valid_net: float | None = -6.4,
) -> dict[str, Any]:
    badge = {"cold": "冷库", "watch": "观察", "revived": "复活"}[status]
    return {
        "id": fid,
        "name": name or fid,
        "family": "starship" if fid.startswith("starship") else "reference",
        "status": status,
        "badge": badge,
        "position": position,
        "percentile": percentile,
        "history": [
            {"window": "holdout", "net": holdout_net},
            {"window": "valid", "net": valid_net},
        ],
    }


def _vault(**overrides: dict[str, Any]) -> dict[str, Any]:
    base = {
        "starship_b": _factor("starship_b", name="星舰B默认"),
        "starship_a25": _factor("starship_a25", name="H2-a25"),
        "starship_b3": _factor("starship_b3", name="纯B3组合"),
        "starship_h2": _factor("starship_h2", name="星舰H2 100%"),
        "starship_cap50": _factor(
            "starship_cap50", name="容量版星舰50M", percentile=19.6, valid_net=-6.4
        ),
        "h11": _factor("h11", name="H11 S-gap liquid"),
        "h08": _factor("h08", name="H08 跳空追涨"),
        "h13": _factor("h13", name="H13 大阴"),
        "harbor": _factor(
            "harbor", name="港湾 (参考)", status="revived", position=40, percentile=99.9
        ),
        "m30": _factor("m30", name="母港M30 (参考)", status="watch", percentile=None),
        "bleg": _factor("bleg", name="B腿 (参考)", status="watch", percentile=None),
    }
    base.update(overrides)
    rows = list(base.values())
    summary = {
        "n_factors": len(rows),
        "n_cold": sum(1 for r in rows if r["status"] == "cold"),
        "n_watch": sum(1 for r in rows if r["status"] == "watch"),
        "n_revived": sum(1 for r in rows if r["status"] == "revived"),
    }
    return {
        "meta": {"generated_at": "2026-10-03"},
        "summary": summary,
        "factors": rows,
    }


def _write_reports(
    tmp_path: Path,
    *,
    vault: dict[str, Any] | None = None,
    history: list[dict[str, Any]] | None = None,
    transitions: list[dict[str, Any]] | None = None,
) -> Path:
    reports = tmp_path / "reports"
    reports.mkdir()
    (reports / "factor_vault.json").write_text(
        json.dumps(vault if vault is not None else _vault()), encoding="utf-8"
    )
    states = {r["id"]: r["status"] for r in (vault or _vault())["factors"]}
    hist = history if history is not None else [{"date": "2026-10-03", "states": states}]
    (reports / "factor_vault_history.json").write_text(
        json.dumps({"history": hist}), encoding="utf-8"
    )
    (reports / "factor_vault_transitions.json").write_text(
        json.dumps({"transitions": transitions or []}), encoding="utf-8"
    )
    (reports / "sgap_decay.json").write_text(
        json.dumps(
            {
                "summary": {
                    "date_end": "2026-09-23",
                    "revival_pos": 0,
                    "latest_r40_percentile": 2.2,
                }
            }
        ),
        encoding="utf-8",
    )
    return reports


def test_cold_starship_waits_in_m30_and_reference_is_not_an_order(tmp_path: Path) -> None:
    reports = _write_reports(tmp_path)
    out = factor_system.build_factor_system(reports_dir=reports, now="2026-11-11")

    assert out["available"] is True
    policy = out["policy"]
    assert policy["action"] == "wait_in_baseline"
    assert policy["baseline"] == "homeport_m30"
    assert policy["suggestedLiveWeight"] == 0
    assert policy["liveBlocked"] is True
    assert out["adviceOnly"] is True
    assert out["summary"] == {"nFactors": 11, "nCold": 8, "nWatch": 2, "nRevived": 1}

    focus = {row["id"]: row for row in out["focus"]}
    assert focus["harbor"]["role"] == "reference"
    assert focus["harbor"]["actionable"] is False
    assert "不是下单指令" in focus["harbor"]["note"]
    assert focus["starship_b"]["role"] == "satellite"
    assert focus["starship_b"]["status"] == "cold"


def test_decay_to_cold_advises_exit(tmp_path: Path) -> None:
    vault = _vault(starship_b=_factor("starship_b", name="星舰B默认", status="cold"))
    reports = _write_reports(
        tmp_path,
        vault=vault,
        history=[
            {
                "date": "2026-11-10",
                "states": {r["id"]: r["status"] for r in vault["factors"]}
                | {"starship_b": "revived"},
            }
        ],
    )
    out = factor_system.build_factor_system(
        reports_dir=reports,
        now="2026-11-11",
        prev_factors={"starship_b": {"status": "revived", "position": 10}},
    )
    policy = out["policy"]
    assert policy["action"] == "advise_exit"
    assert policy["suggestedWeight"] == 0
    assert policy["suggestedLiveWeight"] == 0
    assert policy["decayedFactors"][0]["id"] == "starship_b"
    assert "M30" in policy["reason"]


def test_sgap_revival_pos_step_down_advises_exit(tmp_path: Path) -> None:
    reports = _write_reports(tmp_path)
    (reports / "sgap_decay.json").write_text(
        json.dumps(
            {
                "summary": {"date_end": "2026-09-23", "revival_pos": 0},
                "rolling": [
                    {"i": 1, "revival_pos": 10},
                    {"i": 2, "revival_pos": 0},
                ],
            }
        ),
        encoding="utf-8",
    )
    out = factor_system.build_factor_system(reports_dir=reports, now="2026-11-11")
    assert out["policy"]["action"] == "advise_exit"
    assert out["policy"]["decayedFactors"][0]["id"] == "sgap"


def test_position_step_down_advises_exit(tmp_path: Path) -> None:
    vault = _vault(
        starship_b=_factor(
            "starship_b", name="星舰B默认", status="revived", position=10, percentile=80.0
        )
    )
    reports = _write_reports(tmp_path, vault=vault)
    out = factor_system.build_factor_system(
        reports_dir=reports,
        now="2026-11-11",
        prev_factors={"starship_b": {"status": "revived", "position": 20}},
    )
    assert out["policy"]["action"] == "advise_exit"
    assert out["policy"]["decayedFactors"][0]["fromPosition"] == 20
    assert out["policy"]["decayedFactors"][0]["toPosition"] == 10


def test_revived_but_live_blocked_follows_with_zero_live_weight(tmp_path: Path) -> None:
    vault = _vault(
        starship_b=_factor(
            "starship_b", name="星舰B默认", status="revived", position=10, percentile=80.0
        )
    )
    reports = _write_reports(tmp_path, vault=vault)
    out = factor_system.build_factor_system(
        reports_dir=reports,
        now="2026-11-11",
        preconditions_override={
            "paperFills": 4,
            "holdoutPct": -19.8,
            "filteredValidPct": -6.4,
        },
    )
    policy = out["policy"]
    assert policy["action"] == "advise_follow"
    assert policy["suggestedWeight"] == 10
    assert policy["suggestedLiveWeight"] == 0
    assert policy["liveBlocked"] is True
    assert "paper 4/20" in policy["liveBlockedReason"]
    assert policy["preconditions"]["allPass"] is False


def test_revived_and_preconditions_pass_follow_at_rule_weight(tmp_path: Path) -> None:
    vault = _vault(
        starship_b=_factor(
            "starship_b", name="星舰B默认", status="revived", position=20, percentile=90.0
        )
    )
    reports = _write_reports(tmp_path, vault=vault)
    out = factor_system.build_factor_system(
        reports_dir=reports,
        now="2026-11-11",
        preconditions_override={
            "paperFills": 20,
            "holdoutPct": -5.0,
            "filteredValidPct": 3.0,
        },
    )
    policy = out["policy"]
    assert policy["action"] == "advise_follow"
    assert policy["suggestedWeight"] == 20
    assert policy["suggestedLiveWeight"] == 20
    assert policy["liveBlocked"] is False
    assert policy["suggestedLiveWeight"] <= factor_system.MAX_LIVE_WEIGHT


def test_revived_unconfirmed_stays_wait(tmp_path: Path) -> None:
    vault = _vault(
        starship_b=_factor(
            "starship_b", name="星舰B默认", status="revived", position=0, percentile=80.0
        )
    )
    reports = _write_reports(tmp_path, vault=vault)
    out = factor_system.build_factor_system(reports_dir=reports, now="2026-11-11")
    assert out["policy"]["action"] == "wait_in_baseline"
    assert out["policy"]["suggestedLiveWeight"] == 0


def test_missing_or_corrupt_vault_fails_open(tmp_path: Path) -> None:
    missing = factor_system.build_factor_system(reports_dir=tmp_path / "nope", now="2026-11-11")
    assert missing["available"] is False
    assert "factor_vault.json" in missing["error"]
    assert missing["policy"]["action"] == "wait_in_baseline"
    assert missing["adviceOnly"] is True

    corrupt_dir = tmp_path / "corrupt"
    corrupt_dir.mkdir()
    (corrupt_dir / "factor_vault.json").write_text("{oops", encoding="utf-8")
    corrupt = factor_system.build_factor_system(reports_dir=corrupt_dir, now="2026-11-11")
    assert corrupt["available"] is False
    assert corrupt["policy"]["action"] == "wait_in_baseline"


def test_freshness_reports_frozen_blotter(tmp_path: Path) -> None:
    reports = _write_reports(tmp_path)
    out = factor_system.build_factor_system(reports_dir=reports, now="2026-11-11")
    fresh = out["freshness"]
    assert fresh["sourceGeneratedAt"] == "2026-10-03"
    assert fresh["tradeBlotterLastEntry"] == "2026-09-23"
    assert fresh["stale"] is True
    assert "交易流水" in fresh["reason"]


def test_compact_shape_keeps_status_only(tmp_path: Path) -> None:
    reports = _write_reports(tmp_path)
    full = factor_system.build_factor_system(reports_dir=reports, now="2026-11-11")
    compact = factor_system.compact_factor_system(full)
    assert compact["available"] is True
    assert set(compact["summary"]) == {"nFactors", "nCold", "nWatch", "nRevived"}
    assert compact["policy"]["action"] == "wait_in_baseline"
    assert compact["freshness"]["stale"] is True
    assert compact["liveBlocked"] is True
    assert "focus" not in compact


def test_agent_brief_block_and_version(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reports = _write_reports(tmp_path)
    monkeypatch.setattr(br, "REPORTS_DIR", reports)
    assert agent_brief.BRIEF_VERSION == "2026-11-11.4"
    full = agent_brief._factor_system_block(day="2026-11-11", compact=False)
    compact = agent_brief._factor_system_block(day="2026-11-11", compact=True)
    assert full["available"] is True
    assert "focus" in full
    assert "focus" not in compact
    assert compact["policy"]["action"] == "wait_in_baseline"


def test_factor_system_routes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reports = _write_reports(tmp_path)
    monkeypatch.setattr(br, "REPORTS_DIR", reports)

    resp = client.get("/v1/agent/factor-system")
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is True
    assert body["policy"]["action"] == "wait_in_baseline"
    assert body["adviceOnly"] is True

    md = client.get("/v1/agent/factor-system.md")
    assert md.status_code == 200
    assert md.headers["content-type"].startswith("text/markdown")
    assert "# 因子系统 factor system" in md.text

    via_query = client.get("/v1/agent/factor-system?format=md")
    assert via_query.status_code == 200
    assert via_query.text.startswith("# 因子系统 factor system")

    bad = client.get("/v1/agent/factor-system?format=xml")
    assert bad.status_code == 422

    monkeypatch.setattr(br, "REPORTS_DIR", tmp_path / "missing")
    degraded = client.get("/v1/agent/factor-system")
    assert degraded.status_code == 200
    assert degraded.json()["available"] is False
    assert degraded.json()["policy"]["action"] == "wait_in_baseline"
