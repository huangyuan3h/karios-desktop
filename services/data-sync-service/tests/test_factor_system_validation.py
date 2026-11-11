"""Walk-forward factor-system validation tests (pure, tiny fixtures)."""

from __future__ import annotations

import json
from pathlib import Path

from data_sync_service.service import factor_system_validation as val


def _trades(contribs: list[float], dates: list[str] | None = None) -> list[dict]:
    if dates is None:
        dates = [f"2026-03-{(i % 28) + 1:02d}" for i in range(len(contribs))]
    return [
        {"entry": dates[i], "contrib": contribs[i], "ret": contribs[i] / 25 + 0.003228}
        for i in range(len(contribs))
    ]


def test_normalize_reconstructs_return_and_sorts() -> None:
    raw = [
        {"entry": "2026-03-02", "contrib": 2.5},
        {"entryDate": "2026-03-01", "contribPct": -1.0},
        {"entry": "2026-03-03", "contrib": "nope"},
        "junk",
    ]
    rows = val.normalize_trades(raw)
    assert [r["entry"] for r in rows] == ["2026-03-01", "2026-03-02"]
    assert rows[1]["ret"] == 2.5 / 25 + val.SAT_COST_BP / 10_000


def test_assemble_arms_weights_and_windows() -> None:
    trades = _trades(
        [10.0, -5.0, 20.0, -20.0],
        ["2024-08-02", "2026-03-03", "2026-08-11", "2026-08-12"],
    )
    positions = [
        {"i": 0, "position": 0},
        {"i": 1, "position": 10},
        {"i": 2, "position": 20},
        {"i": 3, "position": 20},
    ]
    arms = val.assemble_arms(trades, positions)
    gated = arms["gatedK2"]
    assert gated["unit"] == "satellite points"
    assert gated["total"] == -2.5
    assert gated["byWindow"]["OOS2"] == 0.0
    assert gated["byWindow"]["valid"] == -2.5
    assert gated["byWindow"]["holdout"] == 0.0
    assert gated["exposurePct"] == 62.5
    assert gated["counts"] == {"nOn10": 1, "nOn20": 2, "nOff": 1}
    assert [c["to"] for c in gated["positionChanges"]] == [0, 10, 20]
    assert arms["alwaysOnStarship"]["total"] == 5.0
    assert arms["alwaysOnStarship"]["byWindow"]["holdout"] == 0.0


def test_k2_positions_warmup_two_step_and_cap() -> None:
    contribs = [10.0] * 80
    states = val.k2_positions(contribs, trades=_trades(contribs))
    assert all(s["position"] == 0 for s in states[:40])
    assert 10 in {s["position"] for s in states}
    assert 20 in {s["position"] for s in states}
    assert max(s["position"] for s in states) <= val.SATELLITE_CAP_PCT
    assert states[-1]["position"] == 20


def test_k2_positions_step_down_and_single_trade_breaker() -> None:
    contribs = [10.0] * 80 + [-30.0] * 40
    states = val.k2_positions(contribs, trades=_trades(contribs))
    assert states[79]["position"] == 20
    assert states[-1]["position"] == 0
    assert any(s["breaker"] == "single_trade_-8" for s in states[80:])

    # A single -8% trade forces the next position to 0.
    contribs2 = [10.0] * 80
    loss_contrib = 25 * (-0.08 - 0.003228)
    contribs2.append(loss_contrib)
    contribs2.extend([10.0] * 5)
    states2 = val.k2_positions(contribs2, trades=_trades(contribs2))
    assert states2[81]["position"] == 0
    assert states2[81]["breaker"] == "single_trade_-8"


def test_run_walk_forward_without_m30_is_honest() -> None:
    trades = _trades([1.0] * 60)
    result = val.run_walk_forward(trades, m30_nav_path=None, as_of="2026-11-11")
    assert result["available"] is True
    assert result["adviceOnly"] is True
    assert result["preregistered"]["retuned"] is False
    assert set(result["arms"]) == {"gatedK2", "alwaysOnStarship", "waitInM30Baseline"}
    baseline = result["arms"]["waitInM30Baseline"]
    assert baseline["available"] is False
    assert "REPO/0%" in baseline["caveat"]
    assert result["summary"]["nTrades"] == 60
    diag = result["diagnostics"]["gatedK2NoBreaker"]
    assert diag["total"] >= result["arms"]["gatedK2"]["total"]
    assert "not a retuned arm" in diag["note"]


def test_run_walk_forward_reads_h2d_m30_windows(tmp_path: Path) -> None:
    nav = {
        "windows": {
            "OOS2": {"cal": ["2024-08-01", "2025-08-01"], "m30_h2": [1.0, 1.4]},
            "train": {"cal": ["2025-08-01", "2026-01-30"], "m30_h2": [1.0, 1.2]},
            "valid": {"cal": ["2026-03-02", "2026-08-07"], "m30_h2": [1.0, 1.3]},
            "holdout": {"cal": ["2026-08-10", "2026-09-30"], "m30_h2": [1.0, 0.9]},
            "long": {"cal": ["2021-08-02", "2026-08-07"], "m30_h2": [1.0, 2.5]},
        }
    }
    path = tmp_path / "h2d_nav.json"
    path.write_text(json.dumps(nav), encoding="utf-8")
    result = val.run_walk_forward(_trades([1.0] * 50), m30_nav_path=path)
    baseline = result["arms"]["waitInM30Baseline"]
    assert baseline["available"] is True
    assert baseline["byWindow"]["OOS2"] == 40.0
    assert baseline["byWindow"]["holdout"] == -10.0
    assert baseline["total"] == 125.0
    assert "NAV %" in baseline["caveat"]


def test_render_validation_markdown_has_table_and_caveat() -> None:
    result = val.run_walk_forward(_trades([1.0] * 50), m30_nav_path=None)
    text = val.render_validation_markdown(result)
    assert "# 因子系统 walk-forward 验证" in text
    assert "| K2 gated |" in text
    assert "基线臂 caveat" in text
    assert "TRAIL_PCT" in text
