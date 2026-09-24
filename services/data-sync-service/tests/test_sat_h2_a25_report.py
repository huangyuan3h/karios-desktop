from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from eval_sat_idle_parking import _true_cash_share
from eval_sat_parking_frontier import _a25_verdict, _grid_for_mode

from data_sync_service.service.state_bucket_track import (
    A25_B3_WEIGHT,
    A25_SLEEVE_WEIGHT,
)


def test_h2_a25_verdict_preserves_failed_k3() -> None:
    results: dict[str, dict] = {}
    values = {
        "OOS2": (238.4, -5.0, 6.32, 236.5, -5.6, 6.41),
        "train": (55.9, -7.5, 4.15, 49.6, -8.3, 3.84),
        "valid": (3.2, -10.8, 0.46, 3.0, -10.1, 0.42),
        "long": (738.5, -8.4, 3.5, 654.3, -6.8, 3.67),
    }
    for window, (total, mdd, sharpe, b3_total, b3_mdd, b3_sharpe) in values.items():
        results[window] = {
            "h2_a25": {"total_pct": total, "max_dd": mdd, "sharpe": sharpe},
            "pure_B3": {"total_pct": b3_total, "max_dd": b3_mdd, "sharpe": b3_sharpe},
        }

    verdict = _a25_verdict(results, "h2_a25")

    assert verdict["status"] == "REJECT"
    assert verdict["pass"] is False
    assert verdict["deltas_3w"] == [1.9, 6.3, 0.2]
    assert verdict["long_delta_mdd"] == -1.6
    assert verdict["checks"]["K1_three_window_total_ge_0"] is True
    assert verdict["checks"]["K3_long_mdd_ge_minus_1"] is False


def test_h2_a25_report_uses_timeline_cash_share() -> None:
    sat = {
        "rows": [
            {"cashShare": 1.0},
            {"cashShare": 0.5},
            {"cashShare": 0.75},
        ],
        "blotter": [],
    }
    assert _true_cash_share(sat, from_rows=True) == [1.0, 0.5, 0.75]


def test_h2_a25_grid_uses_explicit_canonical_weights() -> None:
    grid = dict((label, (sleeve, b3, repo)) for label, sleeve, b3, repo in _grid_for_mode("h2"))
    assert grid["h2_a25"] == (A25_SLEEVE_WEIGHT, A25_B3_WEIGHT, 0.0)
    assert "a25_0pt" in dict((label, (sleeve, b3, repo)) for label, sleeve, b3, repo in _grid_for_mode("legacy-canonical"))
