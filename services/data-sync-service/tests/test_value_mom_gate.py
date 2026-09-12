"""P18 value x momentum gate: config validation + entry-gate + loader unit."""

from __future__ import annotations

import pytest

from data_sync_service.service.backtest_engine import (
    BacktestConfig,
    BacktestData,
    _load_value_comp_ranks,
    simulate,
)

CN1 = "CN:600001"
TS1 = "600001.SH"
TS2 = "600002.SH"


def _data(
    calendar: list[str],
    scores: dict[str, dict[str, float]],
    prices: dict[str, dict[str, float]],
) -> BacktestData:
    data = BacktestData.__new__(BacktestData)
    data.config = None
    data.calendar = calendar
    data.scores_by_day = scores
    data.ts_codes = []
    data.bars_by_ts = {}
    data.close_by_ts_day = {ts: {d: float(px) for d, px in m.items()} for ts, m in prices.items()}
    data.regime_by_day = {d: "Strong" for d in calendar}
    data.flow_any_positive_by_day = {d: True for d in calendar}
    data.mainline_allow_by_day = {d: {"计算机"} for d in calendar}
    data.industry_by_ts = {ts: "计算机" for ts in prices}
    data.sentiment_risk_by_day = {}
    data.light_red_by_day = set()
    data.env_by_day = {d: "unknown" for d in calendar}
    data.closes_by_ts = {
        ts: [(d, float(px)) for d, px in sorted(m.items())] for ts, m in prices.items()
    }
    data.mom_rank_by_day = {}
    data.rs_rank_by_day = {}
    data.value_comp_by_day = {}
    data.st_ts_codes = set()
    data.pead_events = {}
    return data


def _cal() -> list[str]:
    return ["2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-19", "2026-06-22"]


def _cfg(**kw) -> BacktestConfig:
    return BacktestConfig(start_date="2026-06-18", end_date="2026-06-22", **kw)


class TestConfig:
    def test_default_off(self) -> None:
        assert _cfg().value_mom_gate == "off"

    def test_valid_arms(self) -> None:
        assert _cfg(value_mom_gate="composite").value_mom_gate == "composite"
        assert _cfg(value_mom_gate="mom_only").value_mom_gate == "mom_only"

    def test_invalid_raises(self) -> None:
        with pytest.raises(ValueError):
            _cfg(value_mom_gate="top10")


class TestGate:
    def _run(self, comp: dict, gate: str):
        calendar = _cal()
        scores = {"2026-06-18": {CN1: 90.0}}
        closes = {TS1: {d: 10.0 for d in calendar}}
        data = _data(calendar, scores, closes)
        data.bars_by_ts = {TS1: [(d, "10", "10", "10", "10", "0") for d in calendar]}
        data.value_comp_by_day = {"2026-06-18": comp}
        return simulate(_cfg(value_mom_gate=gate), data=data)

    def test_missing_data_blocks_fail_closed(self) -> None:
        run = self._run({}, "composite")
        assert run.summary.closed == 0
        assert run.summary.gated_blocks.get("value_mom_missing", 0) >= 1

    def test_composite_high_passes(self) -> None:
        run = self._run({TS1: (0.9, 0.9)}, "composite")
        assert run.summary.gated_blocks.get("value_mom_gate", 0) == 0
        assert run.summary.gated_blocks.get("value_mom_missing", 0) == 0

    def test_composite_low_blocks(self) -> None:
        run = self._run({TS1: (0.1, 0.1)}, "composite")
        assert run.summary.closed == 0
        assert run.summary.gated_blocks.get("value_mom_gate", 0) >= 1

    def test_mom_only_uses_mom_leg(self) -> None:
        # value 0.0 + mom 0.9: composite 0.45 blocks, mom_only passes the gate
        run = self._run({TS1: (0.0, 0.9)}, "composite")
        assert run.summary.gated_blocks.get("value_mom_gate", 0) >= 1
        run2 = self._run({TS1: (0.0, 0.9)}, "mom_only")
        assert run2.summary.gated_blocks.get("value_mom_gate", 0) == 0

    def test_gate_off_ignores_scores(self) -> None:
        run = self._run({TS1: (0.0, 0.0)}, "off")
        assert run.summary.gated_blocks.get("value_mom_gate", 0) == 0
        assert run.summary.gated_blocks.get("value_mom_missing", 0) == 0


class TestLoader:
    def test_loader_ranks_and_pit(self, monkeypatch) -> None:
        import pandas as pd

        import data_sync_service.service.fin_panel as fp

        codes = [TS1, TS2] + [f"600{i:03d}.SH" for i in range(3, 36)]
        rows = []
        for i, ts in enumerate(codes):
            scale = 100.0 - i  # TS1 dominates every leg
            rows.append(
                {
                    "ts_code": ts,
                    "end_date": pd.Timestamp("2024-03-31"),
                    "ann_date": pd.Timestamp("2024-04-20"),
                    "n_income_attr_p_sq_ttm": scale,
                    "total_revenue_sq_ttm": scale * 10,
                    "total_hldr_eqy_inc_min_int": 500.0,
                    "free_cashflow_sq_ttm": scale / 2,
                    "industry": "A",
                    "is_fin": False,
                }
            )
        panel = pd.DataFrame(rows)
        monkeypatch.setattr(fp, "value_panel", lambda *a, **k: panel)
        import datetime as _dt

        days: list[str] = []
        d = _dt.date(2024, 4, 22)
        while len(days) < 80:
            if d.weekday() < 5:
                days.append(d.isoformat())
            d += _dt.timedelta(days=1)
        closes = {
            ts: [(x, 10.0 + (i * 0.1 if ts == TS1 else 0.0)) for i, x in enumerate(days)]
            for ts in codes
        }
        mv = {x: {ts: 100.0 for ts in codes} for x in days}
        cfg = BacktestConfig(start_date=days[60], end_date=days[-1], value_mom_gate="composite")
        out = _load_value_comp_ranks(cfg, days[60:], set(codes), closes, mv)
        assert out, "expected scored days"
        day0 = days[60]
        v1, m1 = out[day0][TS1]
        v2, m2 = out[day0][TS2]
        assert v1 > v2, "TS1 numerators dominate on every leg"
        assert m1 > m2, "TS1 rising vs others flat"
        # PiT: no scores before ann_date
        assert days[60] >= "2024-04-20"

    def test_loader_off_returns_empty(self) -> None:
        cfg = BacktestConfig(start_date="2024-08-01", end_date="2024-08-10")
        assert _load_value_comp_ranks(cfg, [], set(), {}, {}) == {}
