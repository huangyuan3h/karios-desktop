"""OPT-222: live 14:30 satellite panel (snapshot builder + job + parity).

Pure-unit coverage injects a fake quote map and a fake context so the tests
run without Postgres; the parity test (requires_postgres) proves the synthetic
path reproduces the replay panel for a historical day.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from data_sync_service.service import satellite_live as sl


class _FakeCtx(dict):
    """Minimal context shape used by ``_inject_day``."""


def _mini_ctx() -> _FakeCtx:
    per_ts = {}
    date_idx = {}
    for i, ts in enumerate(("AAA.SZ", "BBB.SZ", "CCC.SZ")):
        dates = [f"2026-08-{d:02d}" for d in range(1, 22)]
        per_ts[ts] = [
            {
                "date": d,
                "open": 10.0 + i,
                "high": 10.5 + i,
                "low": 9.5 + i,
                "close": 10.2 + i,
                "pre_close": 10.0 + i,
                "amount": 1e6,
            }
            for d in dates
        ]
        date_idx[ts] = {d: j for j, d in enumerate(dates)}
    return _FakeCtx(
        per_ts=per_ts,
        date_idx=date_idx,
        cal=["2026-08-21"],
        idx_by_day={"2026-08-21": 0},
        close_by_ts={ts: {"2026-08-21": 10.2 + i} for i, ts in enumerate(per_ts)},
        mv_map={"2026-08-21": {ts: 100.0 for ts in per_ts}},
        px_by_hhmm={},
        px_1430={},
        px_hl_1430={},
        feat_cache={},
    )


def _quote(ts: str, *, price: float = 11.0, open_px: float = 10.8, high: float = 11.2, low: float = 10.7, pre_close: float = 10.5) -> dict:
    return {
        "ts_code": ts,
        "price": str(price),
        "open": str(open_px),
        "high": str(high),
        "low": str(low),
        "pre_close": str(pre_close),
        "amount": "5000000",
    }


class TestInjectDay:
    def test_appends_row_and_prints_and_uses_previous_mv(self) -> None:
        ctx = _mini_ctx()
        n = sl._inject_day(ctx, "2026-09-17", {"AAA.SZ": _quote("AAA.SZ")})
        assert n == 1
        row = ctx["per_ts"]["AAA.SZ"][-1]
        assert row["date"] == "2026-09-17" and row["close"] == 11.0
        assert ctx["date_idx"]["AAA.SZ"]["2026-09-17"] == len(ctx["per_ts"]["AAA.SZ"]) - 1
        assert ctx["px_by_hhmm"]["1430"]["AAA.SZ"]["2026-09-17"] == 11.0
        assert ctx["px_by_hhmm"]["1500"]["AAA.SZ"]["2026-09-17"] == 11.0
        assert ctx["px_hl_1430"]["AAA.SZ"]["2026-09-17"] == (11.2, 10.7)
        assert ctx["close_by_ts"]["AAA.SZ"]["2026-09-17"] == 11.0
        # mv carried from the previous session (quotes have no market cap)
        assert ctx["mv_map"]["2026-09-17"]["AAA.SZ"] == 100.0
        assert "2026-09-17" in ctx["idx_by_day"]

    def test_skips_quotes_without_valid_prices(self) -> None:
        ctx = _mini_ctx()
        bad = _quote("BBB.SZ")
        bad["price"] = "0"
        n = sl._inject_day(ctx, "2026-09-17", {"BBB.SZ": bad})
        assert n == 0
        assert ctx["mv_map"].get("2026-09-17") == {}

    def test_overwrites_existing_day_row(self) -> None:
        ctx = _mini_ctx()
        ctx["per_ts"]["AAA.SZ"].append(
            {"date": "2026-09-17", "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "pre_close": 1.0, "amount": 1.0}
        )
        n = sl._inject_day(ctx, "2026-09-17", {"AAA.SZ": _quote("AAA.SZ", price=12.0)})
        assert n == 1
        assert ctx["per_ts"]["AAA.SZ"][-1]["close"] == 12.0
        assert len(ctx["per_ts"]["AAA.SZ"]) == 22  # replaced, not appended


class TestBuildLivePanel:
    def test_builds_panel_from_injected_quotes(self) -> None:
        ctx = _mini_ctx()

        def fake_panel(ctx_arg, day, *, today=None):
            assert day == "2026-09-17"
            assert ctx_arg["per_ts"]["AAA.SZ"][-1]["date"] == day
            return {
                "decisionAvailable": True,
                "gateOpen": False,
                "breadth1430": 0.28,
                "gapCount": 3,
                "bucketSize": 1,
                "poolSize": 1,
                "ranked": [
                    {"ts": "AAA.SZ", "ampRank": 1, "inBucket": True, "gapPct": 3.1,
                     "amp1430Pct": 2.0, "px1430": 11.0, "skipReason": None,
                     "fillable": True, "wouldFill": False},
                ],
            }

        with (
            patch.object(sl, "_fetch_quotes", return_value={"AAA.SZ": _quote("AAA.SZ"), "BBB.SZ": _quote("BBB.SZ")}),
            patch("data_sync_service.service.state_bucket_track.load_sgap_context", return_value=ctx),
            patch("data_sync_service.service.satellite_signals.satellite_signals_for_day", fake_panel),
        ):
            panel = sl.build_live_panel(day="2026-09-17")
        assert panel["tradeDate"] == "2026-09-17"
        assert panel["gateOpen"] is False and panel["breadth1430"] == 0.28
        assert panel["coverage"] == round(2 / 3, 4)
        assert panel["wouldFill"] == []
        assert panel["ranked"][0]["ts"] == "AAA.SZ"


class TestPersistIfComplete:
    def test_saves_complete_panel(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setattr(sl, "report_path", lambda: tmp_path / "panel.json")
        saved, note = sl.persist_if_complete(
            {"decisionAvailable": True, "coverage": 0.9, "gateOpen": True, "wouldFill": ["A", "B"]}
        )
        assert saved is True and "gate=open" in note
        assert sl.load_live_panel()["wouldFill"] == ["A", "B"]

    def test_never_writes_incomplete_panel(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setattr(sl, "report_path", lambda: tmp_path / "panel.json")
        saved, note = sl.persist_if_complete(
            {"decisionAvailable": False, "reason": "day not in trading calendar", "coverage": 0.9}
        )
        assert saved is False and "unavailable" in note
        saved2, note2 = sl.persist_if_complete(
            {"decisionAvailable": True, "coverage": 0.1, "gateOpen": False, "wouldFill": []}
        )
        assert saved2 is False and "coverage" in note2
        assert sl.load_live_panel() is None  # nothing written


class TestSatelliteLiveJob:
    def test_skips_non_trading_day(self, monkeypatch) -> None:
        from data_sync_service.scheduler import satellite_live_job as job

        records: list[dict] = []
        monkeypatch.setattr(job, "insert_record", lambda *a, **kw: records.append(kw))
        monkeypatch.setattr(
            "data_sync_service.db.trade_calendar.is_trading_day", lambda *a: False
        )
        out = job.run()
        assert out["skipped"] == "not_trading_day"
        assert records[0]["success"] is True

    def test_persists_complete_panel(self, monkeypatch, tmp_path) -> None:
        from data_sync_service.scheduler import satellite_live_job as job

        records: list[dict] = []
        monkeypatch.setattr(job, "insert_record", lambda *a, **kw: records.append(kw))
        monkeypatch.setattr("data_sync_service.db.trade_calendar.is_trading_day", lambda *a: True)
        monkeypatch.setattr(sl, "report_path", lambda: tmp_path / "panel.json")
        monkeypatch.setattr(job, "_emit_action", lambda panel: None)  # no webhook/DB writes
        monkeypatch.setattr(
            "data_sync_service.service.satellite_live.build_live_panel",
            lambda day: {"tradeDate": day, "decisionAvailable": True, "coverage": 0.9,
                         "gateOpen": False, "wouldFill": [], "gapCount": 1, "poolSize": 1,
                         "breadth1430": 0.3},
        )
        out = job.run()
        assert out["ok"] is True
        assert records[0]["success"] is True and "gate=closed" in records[0]["error_message"]
        assert (tmp_path / "panel.json").exists()

    def test_emit_action_pushes_for_selected_mode(self, monkeypatch) -> None:
        """OPT-223: the push carries the selected strategy's action."""
        from data_sync_service.scheduler import satellite_live_job as job
        from data_sync_service.service import multi_asset_sleeve as mas

        emitted: list[dict] = []
        monkeypatch.setattr(
            "data_sync_service.api.settings_routes.selected_strategy_mode", lambda: "starship"
        )
        monkeypatch.setattr(
            "data_sync_service.db.webhook.emit_event",
            lambda et, payload, dedupe_key: emitted.append(
                {"type": et, "payload": payload, "key": dedupe_key}
            ),
        )
        monkeypatch.setattr(
            mas,
            "_pick",
            lambda *, as_of=None: {
                "key": "OIL", "ts": "513350.SH", "symbol": "ETF:513350",
                "name": "富国油气QDII", "mom60": 31.7,
            },
        )
        panel = {
            "tradeDate": "2026-09-17",
            "decisionAvailable": True,
            "gateOpen": False,
            "breadth1430": 0.279,
            "exits": [{"ts": "000978.SZ", "exitDue": "2026-09-17"}],
            "heldLegs": [],
            "ranked": [
                {"ts": "002128.SZ", "inBucket": True, "skipReason": None, "fillable": True,
                 "gapPct": 3.04, "amp1430Pct": 2.92, "px1430": 27.7, "ampRank": 3},
            ],
        }
        job._emit_action(panel)
        assert len(emitted) == 1
        assert emitted[0]["type"] == "satellite_action"
        assert emitted[0]["key"] == "satellite_action:2026-09-17:starship"
        payload = emitted[0]["payload"]
        assert payload["strategyLabel"] == "星舰 v2"
        assert payload["exits"] == ["000978.SZ"]
        assert "卖出资金停入 H2 停车腿（富国油气QDII 513350.SH）" in payload["lines"]
        assert any("闸关" in ln for ln in payload["lines"])

    def test_emit_action_skips_non_satellite_modes(self, monkeypatch) -> None:
        from data_sync_service.scheduler import satellite_live_job as job

        emitted: list[dict] = []
        monkeypatch.setattr(
            "data_sync_service.api.settings_routes.selected_strategy_mode", lambda: "harbor"
        )
        monkeypatch.setattr(
            "data_sync_service.db.webhook.emit_event",
            lambda *a, **kw: emitted.append((a, kw)),
        )
        job._emit_action({"tradeDate": "2026-09-17", "decisionAvailable": True})
        assert emitted == []

    def test_incomplete_panel_records_failure(self, monkeypatch) -> None:
        from data_sync_service.scheduler import satellite_live_job as job

        records: list[dict] = []
        monkeypatch.setattr(job, "insert_record", lambda *a, **kw: records.append(kw))
        monkeypatch.setattr("data_sync_service.db.trade_calendar.is_trading_day", lambda *a: True)
        monkeypatch.setattr(
            "data_sync_service.service.satellite_live.build_live_panel",
            lambda day: {"tradeDate": day, "decisionAvailable": False, "reason": "no prints",
                         "coverage": 0.0},
        )
        out = job.run()
        assert out["ok"] is False
        assert records[0]["success"] is False


@pytest.mark.requires_postgres
def test_live_panel_matches_replay_for_historical_day() -> None:
    """Parity lock: synthesizing a historical day from its stored quotes
    reproduces the replay panel for that day (same code path, same inputs)."""
    from data_sync_service.service import satellite_signals as ss
    from data_sync_service.service.state_bucket_track import (
        HABIT_CTX_TIMES,
        load_sgap_context,
    )

    day = "2026-09-11"
    prev = "2026-09-10"
    # 1) the panel the replay computes for the day (day present in the context)
    ctx_full = load_sgap_context(day, day, times=HABIT_CTX_TIMES)
    replay_panel = ss.satellite_signals_for_day(ctx_full, day, today="2026-09-30")
    assert replay_panel.get("decisionAvailable") is True
    # 2) quotes synthesized from the day's stored daily row + prints, injected
    #    into a context that ends the session before. The optional overrides
    #    (mv / px1430 / px1500 / hl1430) pin every input to the stored values,
    #    so the synthetic path must reproduce the replay panel exactly.
    ctx_prev = load_sgap_context(prev, prev, times=HABIT_CTX_TIMES)
    px1430 = (ctx_full.get("px_1430") or {})
    px1500 = ((ctx_full.get("px_by_hhmm") or {}).get("1500") or {})
    hl1430 = ctx_full.get("px_hl_1430") or {}
    quote_row: dict[str, dict] = {}
    for ts, series in ctx_full["per_ts"].items():
        row = series[-1]
        if row.get("date") != day or not row.get("close"):
            continue
        q: dict = {
            "price": row.get("close"),
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "pre_close": row.get("pre_close"),
            "amount": row.get("amount"),
            "mv": (ctx_full["mv_map"].get(day) or {}).get(ts),
            "px1430": (px1430.get(ts) or {}).get(day),
            "px1500": (px1500.get(ts) or {}).get(day),
            "hl1430": (hl1430.get(ts) or {}).get(day),
        }
        quote_row[ts] = q
    assert quote_row, "no stored rows for the parity day"
    n = sl._inject_day(ctx_prev, day, quote_row)
    assert n > 0
    live_panel = ss.satellite_signals_for_day(ctx_prev, day, today="2026-09-30")

    assert live_panel["gateOpen"] == replay_panel["gateOpen"]
    assert live_panel["breadth1430"] == replay_panel["breadth1430"]
    assert [e["ts"] for e in live_panel["ranked"]] == [e["ts"] for e in replay_panel["ranked"]]
    assert [e["wouldFill"] for e in live_panel["ranked"]] == [
        e["wouldFill"] for e in replay_panel["ranked"]
    ]
