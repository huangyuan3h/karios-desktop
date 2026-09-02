"""twin_star_daily (双子星 14:30 提醒信号) unit tests — no DB."""

from __future__ import annotations

from datetime import date

import pytest

from data_sync_service.service import twin_star_daily as tsd


def _mk_dates(n: int, start: str = "2026-01-05") -> list[str]:
    from datetime import timedelta

    d = date.fromisoformat(start)
    out: list[str] = []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _mk_series(dates: list[str], gap_idx: int, gap_pct: float, amp: float) -> list[dict]:
    series = []
    prev = 10.0
    for i, ds in enumerate(dates):
        if i == gap_idx:
            open_px = prev * (1.0 + gap_pct)
            close = open_px * (1.0 + 0.01)
        else:
            open_px = prev
            close = open_px * (1.0 + 0.005)
        series.append(
            {
                "date": ds,
                "open": round(open_px, 4),
                "high": round(max(open_px, close) * 1.005, 4),
                "low": round(min(open_px, close) * 0.995, 4),
                "close": round(close, 4),
                "pre_close": round(prev, 4),
                "amount": 1e8,
            }
        )
        prev = close
    return series


class TestSatSignal:
    @pytest.fixture(autouse=True)
    def _no_db_names(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(tsd, "fill_candidate_names", lambda *a, **k: None)

    def test_gate_open_candidates_and_asof(self, monkeypatch) -> None:
        dates = _mk_dates(25)
        gap_idx = len(dates) - 2  # signal day (t-1 for today=last date)
        per_ts = {
            "A.SH": _mk_series(dates, gap_idx, 0.05, 0.01),
            "B.SH": _mk_series(dates, gap_idx, 0.05, 0.04),
            "C.SH": _mk_series(dates, gap_idx, 0.05, 0.09),
            "D.SH": _mk_series(dates, None, 0.0, 0.02),
        }
        mv = {ds: {ts: 100.0 for ts in per_ts} for ds in dates}
        monkeypatch.setattr(tsd, "_load_calendar", lambda w, e: dates)
        monkeypatch.setattr(tsd, "_load_rows", lambda w, e: per_ts)
        monkeypatch.setattr(tsd, "_load_mv", lambda w, e: mv)
        # today = last date (trading day) -> signal from previous close
        today = date.fromisoformat(dates[-1])
        sat = tsd._sat_signal(today)
        assert sat["asOf"] == dates[-2]
        assert sat["gateOpen"] is True and sat["breadth"] > 0.9
        assert sat["gapCount"] == 3
        # top-33% of 3 gap stocks = 1 candidate, lowest amp = A.SH
        assert sat["candidates"][0]["ts"] == "A.SH"
        assert sat["note"] is None

    def test_strict_does_not_refill_when_top_bucket_locked(self, monkeypatch) -> None:
        """Winning recipe: top 1/3 of ALL gaps, then drop locked — no next-name refill."""
        dates = _mk_dates(25)
        gap_idx = len(dates) - 2
        per_ts = {
            "A.SH": _mk_series(dates, gap_idx, 0.05, 0.01),
            "B.SH": _mk_series(dates, gap_idx, 0.05, 0.04),
            "C.SH": _mk_series(dates, gap_idx, 0.05, 0.09),
        }
        sig = per_ts["A.SH"][gap_idx]
        pc = sig["pre_close"]
        sig["close"] = round(pc * 1.10, 4)
        sig["high"] = sig["close"]
        sig["low"] = sig["close"]
        mv = {ds: {ts: 100.0 for ts in per_ts} for ds in dates}
        monkeypatch.setattr(tsd, "_load_calendar", lambda w, e: dates)
        monkeypatch.setattr(tsd, "_load_rows", lambda w, e: per_ts)
        monkeypatch.setattr(tsd, "_load_mv", lambda w, e: mv)
        sat = tsd._sat_signal(date.fromisoformat(dates[-1]))
        assert sat["gapCount"] == 3
        assert sat["candidates"] == []
        assert sat["blocked"][0]["ts"] == "A.SH"
        assert sat["alternates"][0]["ts"] == "B.SH"

    def test_mv_lag_fallback_and_note(self, monkeypatch) -> None:
        dates = _mk_dates(25)
        per_ts = {"A.SH": _mk_series(dates, 20, 0.05, 0.01)}
        mv = {d: {} for d in dates}
        mv[dates[20]] = {"A.SH": 100.0}  # mv only on one older day
        monkeypatch.setattr(tsd, "_load_calendar", lambda w, e: dates)
        monkeypatch.setattr(tsd, "_load_rows", lambda w, e: per_ts)
        monkeypatch.setattr(tsd, "_load_mv", lambda w, e: mv)
        today = date.fromisoformat(dates[-1])
        sat = tsd._sat_signal(today)
        assert sat["asOf"] == dates[20]
        assert sat["note"] is not None and "滞后" in sat["note"]

    def test_no_data_returns_none(self, monkeypatch) -> None:
        monkeypatch.setattr(tsd, "_load_calendar", lambda w, e: [])
        assert tsd._sat_signal(date(2026, 2, 1)) is None


class TestLiveSatHoldings:
    def test_weekdays_inclusive(self) -> None:
        assert tsd.count_weekdays_inclusive("2026-01-19", "2026-01-21") == 3
        assert tsd.count_weekdays_inclusive("2026-01-17", "2026-01-19") == 1

    def test_etf_pick_counts_all_cn(self) -> None:
        health = {
            "holdings": [
                {"symbol": "CN:000001", "name": "平安银行", "positionPct": 12.5, "entryDate": "2026-01-19"},
                {"symbol": "ETF:518880", "positionPct": 50, "entryDate": "2026-01-01"},
                {"symbol": "CN:600000", "positionPct": 0, "entryDate": "2026-01-19"},
            ]
        }
        live = tsd.live_sat_holdings(health=health, pick_key="OIL", sat_ts=set())
        assert [h["ts"] for h in live] == ["000001.SZ"]

    def test_stock_pick_only_recipe_names(self) -> None:
        health = {
            "holdings": [
                {"symbol": "CN:000001", "positionPct": 12.5, "entryDate": "2026-01-19"},
                {"symbol": "CN:600000", "positionPct": 12.5, "entryDate": "2026-01-19"},
            ]
        }
        live = tsd.live_sat_holdings(
            health=health, pick_key="STOCK", sat_ts={"600000.SH"}
        )
        assert [h["ts"] for h in live] == ["600000.SH"]

    def test_none_pick_counts_all_cn(self) -> None:
        health = {
            "holdings": [
                {"symbol": "CN:000001", "positionPct": 12.5, "entryDate": "2026-01-19"},
            ]
        }
        live = tsd.live_sat_holdings(health=health, pick_key=None, sat_ts=set())
        assert [h["ts"] for h in live] == ["000001.SZ"]


class TestReminderPayload:
    def test_payload_mentions_core_and_sat(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "data_sync_service.service.twin_star_intraday.load_intraday_sat",
            lambda today: None,
        )
        monkeypatch.setattr(tsd, "_sat_signal", lambda today: {
            "asOf": "2026-01-20",
            "gateOpen": True,
            "breadth": 0.8,
            "gapCount": 2,
            "candidates": [{"ts": "A.SH", "amp": 1.0, "gapPct": 5.0, "close": 10.5}],
            "note": None,
        })
        monkeypatch.setattr(
            tsd,
            "_sat_book",
            lambda today: {
                "asOf": "2026-01-20",
                "holdings": [
                    {
                        "ts": "B.SH",
                        "entryDate": "2026-01-19",
                        "heldDays": 2,
                        "daysLeft": 1,
                        "exitDue": "2026-01-21",
                    }
                ],
                "exitsDue": [
                    {
                        "ts": "B.SH",
                        "entryDate": "2026-01-19",
                        "heldDays": 2,
                        "daysLeft": 1,
                        "exitDue": "2026-01-21",
                    }
                ],
                "body": 3,
            },
        )
        monkeypatch.setattr(
            "data_sync_service.service.portfolio_health.build_portfolio_health",
            lambda **kw: {
                "multiAssetSleeve": {
                    "active": True,
                    "action": "BUY",
                    "label": "买入",
                    "pick": {"key": "GOLD", "symbol": "ETF:518880"},
                    "message": "择强买入",
                },
                "holdings": [
                    {
                        "symbol": "CN:000001",
                        "name": "平安银行",
                        "positionPct": 12.5,
                        "entryDate": "2026-01-19",
                    }
                ],
            },
        )
        payload = tsd.build_twin_star_reminder_payload(date(2026, 1, 21))
        assert "14:30" in payload["title"]
        assert "GOLD" in payload["detail"] or "买入" in payload["detail"]
        assert "A.SH" in payload["detail"]
        assert "R-wide 开闸" in payload["detail"]
        assert "核心50%" in payload["detail"]
        assert "今日卖 000001.SZ" in payload["detail"]
        assert "你卫星仓 1/4" in payload["detail"]
        assert "引擎模拟" in payload["detail"]
        assert "持仓簿" not in payload["detail"]
        assert payload["sat"]["coreTargetPct"] == 50
        assert payload["sat"]["book"]["liveHeld"] == 1
        assert payload["sat"]["book"]["engineHeld"] == 1


class TestCoreTargetPct:
    def test_idle_is_100(self) -> None:
        assert tsd._core_target_pct(gate_open=False, candidates=[], holdings=[]) == 100
        assert tsd._core_target_pct(gate_open=True, candidates=[], holdings=[]) == 100

    def test_open_or_holding_is_50(self) -> None:
        assert tsd._core_target_pct(
            gate_open=True, candidates=[{"ts": "A"}], holdings=[]
        ) == 50
        assert tsd._core_target_pct(
            gate_open=False, candidates=[], holdings=[{"ts": "A"}]
        ) == 50

    def test_engine_replay_does_not_count_as_live_occupancy(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "data_sync_service.service.twin_star_intraday.load_intraday_sat",
            lambda today: None,
        )
        monkeypatch.setattr(
            tsd,
            "_sat_signal",
            lambda today: {
                "asOf": "2026-01-20",
                "gateOpen": False,
                "breadth": 0.4,
                "gapCount": 0,
                "candidates": [],
                "note": None,
            },
        )
        monkeypatch.setattr(
            tsd,
            "_sat_book",
            lambda today: {
                "asOf": "2026-01-20",
                "holdings": [{"ts": f"6000{i:02d}.SH"} for i in range(15)],
                "exitsDue": [],
                "body": 3,
            },
        )
        monkeypatch.setattr(
            "data_sync_service.service.portfolio_health.build_portfolio_health",
            lambda **kw: {
                "multiAssetSleeve": {
                    "active": True,
                    "action": "HOLD",
                    "label": "持有",
                    "pick": {"key": "OIL", "symbol": "ETF:513350"},
                },
                "holdings": [],
            },
        )
        out = tsd.build_twin_star_daily_action(date(2026, 1, 21))
        assert out["sat"]["coreTargetPct"] == 100
        assert out["sat"]["book"]["liveHeld"] == 0
        assert out["sat"]["book"]["engineHeld"] == 15
        assert out["sat"]["book"]["liveFreeSlots"] == 4

    def test_oil_two_live_cn_is_half_core(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "data_sync_service.service.twin_star_intraday.load_intraday_sat",
            lambda today: None,
        )
        monkeypatch.setattr(
            tsd,
            "_sat_signal",
            lambda today: {
                "asOf": "2026-01-20",
                "gateOpen": False,
                "breadth": 0.4,
                "gapCount": 0,
                "candidates": [],
                "note": None,
            },
        )
        monkeypatch.setattr(
            tsd,
            "_sat_book",
            lambda today: {"asOf": "2026-01-20", "holdings": [], "exitsDue": [], "body": 3},
        )
        monkeypatch.setattr(
            "data_sync_service.service.portfolio_health.build_portfolio_health",
            lambda **kw: {
                "multiAssetSleeve": {
                    "active": True,
                    "action": "HOLD",
                    "label": "持有",
                    "pick": {"key": "OIL", "symbol": "ETF:513350"},
                },
                "holdings": [
                    {"symbol": "CN:000001", "positionPct": 12.5, "entryDate": "2026-01-19"},
                    {"symbol": "CN:000002", "positionPct": 12.5, "entryDate": "2026-01-20"},
                ],
            },
        )
        out = tsd.build_twin_star_daily_action(date(2026, 1, 21))
        assert out["sat"]["coreTargetPct"] == 50
        assert out["sat"]["book"]["liveHeld"] == 2
        assert out["sat"]["book"]["liveFreeSlots"] == 2


class TestFillCandidateNames:
    def test_attaches_missing_names(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "data_sync_service.db.stock_basic.fetch_names",
            lambda codes: {"000712.SZ": "锦江投资"},
        )
        rows = [{"ts": "000712.SZ"}, {"ts": "600000.SH", "name": "浦发银行"}]
        tsd.fill_candidate_names(rows)
        assert rows[0]["name"] == "锦江投资"
        assert rows[1]["name"] == "浦发银行"
