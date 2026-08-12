"""api/backtest_routes.py coverage."""

from __future__ import annotations

from unittest.mock import Mock

import pytest
from fastapi import HTTPException

from data_sync_service.api import backtest_routes as br


class FakeSummary:
    annual_net_pnl_pct = 5.0
    best_benchmark = ""
    excess_vs_best_benchmark_pct = 0.0

    def to_dict(self):
        return {"pnl": 1.0}


class TestRun:
    def test_run(self, monkeypatch) -> None:
        monkeypatch.setattr(br, "simulate", lambda config: Mock(summary=FakeSummary()))
        monkeypatch.setattr(br, "load_benchmarks", lambda start, end: [])
        out = br.backtest_run(start="2026-01-01", end="2026-06-01", score_threshold=80.0, max_hold_days=5, stop_loss_pct=-5.0, target_pnl_pct=10.0, score_floor=30.0, market="CN", gates="full", trailing_stop_pct=0.0, position_pct=0.05, max_positions=10, rs_rank_min=0.0, diverging_scale=0.0, drawdown_circuit_pct=0.0, panic_cooldown_days=0, slippage_pct=0.0, trend_score_min=0.0, exclude_boards="")
        assert out["ok"] is True and out["summary"] == {"pnl": 1.0}

    def test_run_bad_window(self) -> None:
        with pytest.raises(HTTPException) as exc:
            br.backtest_run(start="", end="2026-01-01")
        assert exc.value.status_code == 422
        with pytest.raises(HTTPException) as exc:
            br.backtest_run(start="2026-06-01", end="2026-01-01")
        assert exc.value.status_code == 422

    def test_run_config_error(self, monkeypatch) -> None:
        monkeypatch.setattr(br.BacktestConfig, "__init__", lambda self, **kw: (_ for _ in ()).throw(ValueError("bad config")))
        with pytest.raises(HTTPException) as exc:
            br.backtest_run(start="2026-01-01", end="2026-06-01")
        assert exc.value.status_code == 422

    def test_run_simulate_error(self, monkeypatch) -> None:
        monkeypatch.setattr(br, "simulate", lambda config: (_ for _ in ()).throw(RuntimeError("boom")))
        with pytest.raises(HTTPException) as exc:
            br.backtest_run(start="2026-01-01", end="2026-06-01", score_threshold=85.0, max_hold_days=5, stop_loss_pct=-5.0, target_pnl_pct=10.0, score_floor=30.0, market="CN", gates="full", trailing_stop_pct=0.0, position_pct=0.05, max_positions=10, rs_rank_min=0.0, diverging_scale=0.0, drawdown_circuit_pct=0.0, panic_cooldown_days=0, slippage_pct=0.0, trend_score_min=0.0, exclude_boards="")
        assert exc.value.status_code == 500
        assert "backtest failed" in exc.value.detail


class TestSensitivity:
    def test_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(br, "default_sensitivity_grid", lambda s, e: [{"a": 1}, {"b": 2}])
        monkeypatch.setattr(br, "load_benchmarks", lambda start, end: [])
        monkeypatch.setattr(br, "run_sensitivity", lambda grid: [FakeSummary(), FakeSummary()])
        out = br.backtest_sensitivity(start="2026-01-01", end="2026-06-01")
        assert out["ok"] is True and out["configs"] == 2 and len(out["results"]) == 2

    def test_error(self, monkeypatch) -> None:
        monkeypatch.setattr(br, "default_sensitivity_grid", lambda s, e: (_ for _ in ()).throw(RuntimeError("boom")))
        with pytest.raises(HTTPException) as exc:
            br.backtest_sensitivity(start="2026-01-01", end="2026-06-01")
        assert exc.value.status_code == 500

    def test_bad_window(self) -> None:
        with pytest.raises(HTTPException) as exc:
            br.backtest_sensitivity(start="", end="")
        assert exc.value.status_code == 422


class TestReport:
    def test_missing(self, monkeypatch) -> None:
        monkeypatch.setattr(br, "LATEST_REPORT", Mock(exists=lambda: False))
        with pytest.raises(HTTPException) as exc:
            br.backtest_latest_report()
        assert exc.value.status_code == 404

    def test_ok(self, monkeypatch) -> None:
        path = Mock(exists=lambda: True, read_text=lambda encoding: '{"report": 1}')
        monkeypatch.setattr(br, "LATEST_REPORT", path)
        assert br.backtest_latest_report() == {"ok": True, "report": {"report": 1}}

    def test_unreadable(self, monkeypatch) -> None:
        path = Mock(exists=lambda: True, read_text=lambda encoding: "not json")
        monkeypatch.setattr(br, "LATEST_REPORT", path)
        with pytest.raises(HTTPException) as exc:
            br.backtest_latest_report()
        assert exc.value.status_code == 500


class TestExitAttribution:
    def test_ok(self, monkeypatch) -> None:
        monkeypatch.setattr("data_sync_service.service.exit_attribution.analyze_exit_attribution", lambda **kw: {"byReason": []})
        out = br.backtest_exit_attribution(days=5, limit=500)
        assert out["ok"] is True

    def test_error_field(self, monkeypatch) -> None:
        monkeypatch.setattr("data_sync_service.service.exit_attribution.analyze_exit_attribution", lambda **kw: {"error": "no trades"})
        with pytest.raises(HTTPException) as exc:
            br.backtest_exit_attribution()
        assert exc.value.status_code == 500

    def test_exception(self, monkeypatch) -> None:
        monkeypatch.setattr("data_sync_service.service.exit_attribution.analyze_exit_attribution", lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")))
        with pytest.raises(HTTPException) as exc:
            br.backtest_exit_attribution()
        assert exc.value.status_code == 500


class TestWeeklyReview:
    def test_ok(self, monkeypatch) -> None:
        monkeypatch.setattr("data_sync_service.service.weekly_review.build_weekly_review", lambda end_date: {"ok": True})
        assert br.weekly_review(end="2026-08-07")["ok"] is True

    def test_value_error(self, monkeypatch) -> None:
        monkeypatch.setattr("data_sync_service.service.weekly_review.build_weekly_review", lambda end_date: (_ for _ in ()).throw(ValueError("bad date")))
        with pytest.raises(HTTPException) as exc:
            br.weekly_review(end="bad")
        assert exc.value.status_code == 422

    def test_exception(self, monkeypatch) -> None:
        monkeypatch.setattr("data_sync_service.service.weekly_review.build_weekly_review", lambda end_date: (_ for _ in ()).throw(RuntimeError("boom")))
        with pytest.raises(HTTPException) as exc:
            br.weekly_review(end="2026-08-07")
        assert exc.value.status_code == 500


class TestCorrelation:
    def test_ok(self, monkeypatch) -> None:
        monkeypatch.setattr("data_sync_service.db.watchlist_automation.list_registry", lambda: [
            {"symbol": "CN:600519", "positionPct": 10.0},
            {"symbol": "HK:00700", "positionPct": "5"},
            {"symbol": "CN:bad", "positionPct": "x"},
        ])
        monkeypatch.setattr("data_sync_service.service.paper_trading._resolve_ts_code", lambda sym: ("CN:600519", "600519.SH"))
        monkeypatch.setattr("data_sync_service.service.correlation.em_industry_for_ts_code", lambda ts: "白酒")
        monkeypatch.setattr("data_sync_service.service.correlation.evaluate_correlation_cap", lambda positions, industries, include_matrix: {"overLimit": []})
        out = br.correlation_status(include_matrix=False)
        assert out["ok"] is True and out["overLimit"] == []

    def test_exception(self, monkeypatch) -> None:
        monkeypatch.setattr("data_sync_service.db.watchlist_automation.list_registry", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
        with pytest.raises(HTTPException) as exc:
            br.correlation_status()
        assert exc.value.status_code == 500


def test_router_prefix() -> None:
    assert br.router.prefix == "/api/backtest"


class TestOverview:
    def test_overview_reads_baseline_files(self, monkeypatch, tmp_path) -> None:
        import json

        reports = tmp_path / "backtest_reports"
        reports.mkdir()
        (reports / "walk_forward_baseline.json").write_text(json.dumps({
            "generatedAt": "2026-08-12T00:00:00Z",
            "tag": "baseline",
            "results": {
                "OOS2": {"totalNetPnlPct": 112.654, "winRate": 0.48, "sharpe": 5.22, "maxDrawdownPct": 23.346},
                "train": {"totalNetPnlPct": 76.734},
            },
        }))
        (reports / "walk_forward_hk_baseline.json").write_text(json.dumps({
            "generatedAt": "2026-08-10T00:00:00Z",
            "results": {"valid": {"totalNetPnlPct": 60.647, "winRate": 0.417, "sharpe": 6.32, "maxDrawdownPct": 8.329}},
        }))
        (reports / "rolling_oos_latest.json").write_text(json.dumps({
            "windowStart": "2026-05-13", "windowEnd": "2026-08-11",
            "warning": True, "warnings": ["HK: -8.5%"],
            "markets": {"HK": {"closed": 55, "winRate": 0.255, "totalNetPnlPct": -8.451, "maxDrawdownPct": 19.5, "sharpe": -3.2}},
        }))
        monkeypatch.setattr(br, "REPORTS_DIR", reports)

        out = br.backtest_overview()
        assert out["ok"] is True
        assert out["cnBaseline"]["windows"]["OOS2"]["totalNetPnlPct"] == 112.654
        assert out["hkBaseline"]["windows"]["valid"]["sharpe"] == 6.32
        assert out["rollingOos"]["warning"] is True
        assert out["rollingOos"]["markets"]["HK"]["closed"] == 55
        # Frozen long-window constants are always present.
        assert out["longWindowCN"]["totalNetPnlPct"] == 250.8
        assert out["longWindowCN"]["byYear"]["2023"] == -263.0

    def test_overview_missing_files(self, monkeypatch, tmp_path) -> None:
        monkeypatch.setattr(br, "REPORTS_DIR", tmp_path)
        out = br.backtest_overview()
        assert out["cnBaseline"] is None
        assert out["hkBaseline"] is None
        assert out["rollingOos"] is None
        assert out["longWindowCN"] is not None
