"""P2 webhook event mounts: E2/E4/E5/E6/E7 emit points."""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.scheduler import candidate_diff_job as cdj
from data_sync_service.service import trading_brief


class TestE2PaperChainIssue:
    def test_emits_when_missing(self, monkeypatch) -> None:
        from data_sync_service.scheduler import paper_chain_watchdog_job as pcw

        emitted: list[dict] = []
        monkeypatch.setattr(pcw, "insert_record", lambda *a, **k: None)
        monkeypatch.setattr(pcw, "_run_ok", lambda job: False)  # all missing
        monkeypatch.setattr(pcw, "_today", lambda: "2026-08-12")

        def fake_emit(event_type, payload, dedupe_key):
            emitted.append({"type": event_type, "payload": payload, "key": dedupe_key})
            return True

        with patch("data_sync_service.db.webhook.emit_event", side_effect=fake_emit):
            pcw.run()
        assert len(emitted) == 1
        assert emitted[0]["type"] == "paper_chain_issue"
        assert emitted[0]["payload"]["day"] == "2026-08-12"
        assert "watchlist_automation" in emitted[0]["payload"]["missing"]
        assert emitted[0]["key"] == "paper_chain:2026-08-12"

    def test_no_emit_when_chain_ok(self, monkeypatch) -> None:
        from data_sync_service.scheduler import paper_chain_watchdog_job as pcw

        monkeypatch.setattr(pcw, "insert_record", lambda *a, **k: None)
        monkeypatch.setattr(pcw, "_run_ok", lambda job: True)
        monkeypatch.setattr(pcw, "_today", lambda: "2026-08-12")
        with patch("data_sync_service.db.webhook.emit_event") as emit_mock:
            pcw.run()
        emit_mock.assert_not_called()


class TestE4NearStop:
    def test_emits_alert_rows(self, monkeypatch) -> None:
        emitted: list[dict] = []

        def fake_emit(event_type, payload, dedupe_key):
            emitted.append({"type": event_type, "payload": payload, "key": dedupe_key})
            return True

        alerts = [{"type": "alert", "symbol": "CN:600000", "market": "CN", "line": "stop", "pnlPct": -4.6, "distancePct": 0.4}]
        monkeypatch.setattr(trading_brief, "_health", lambda: {"holdings": []})
        monkeypatch.setattr(trading_brief, "_alerts_section", lambda h: alerts)
        monkeypatch.setattr(trading_brief, "_regime_section", lambda h: [])
        monkeypatch.setattr(trading_brief, "_candidates_section", lambda h: [])
        monkeypatch.setattr(trading_brief, "_holdings_section", lambda h: [])
        monkeypatch.setattr(trading_brief, "_recon_section", lambda n: [])
        monkeypatch.setattr(trading_brief, "_news_section", lambda n: [])
        monkeypatch.setattr(trading_brief, "render_markdown", lambda s, t: "#")
        monkeypatch.setattr(trading_brief, "upsert_brief", lambda **k: {"id": 1})
        monkeypatch.setattr(trading_brief, "_now", lambda: "2026-08-12T12:00:00")
        with patch("data_sync_service.db.webhook.emit_event", side_effect=fake_emit):
            trading_brief.generate_trading_brief("midday")
        assert len(emitted) == 1
        assert emitted[0]["type"] == "near_stop"
        assert emitted[0]["payload"]["symbol"] == "CN:600000"
        assert emitted[0]["payload"]["line"] == "stop"
        assert emitted[0]["key"] == "near_stop:CN:600000:stop:2026-08-12"

    def test_no_emit_without_alerts(self, monkeypatch) -> None:
        monkeypatch.setattr(trading_brief, "_health", lambda: {"holdings": []})
        monkeypatch.setattr(trading_brief, "_alerts_section", lambda h: [])
        monkeypatch.setattr(trading_brief, "_regime_section", lambda h: [])
        monkeypatch.setattr(trading_brief, "_candidates_section", lambda h: [])
        monkeypatch.setattr(trading_brief, "_holdings_section", lambda h: [])
        monkeypatch.setattr(trading_brief, "render_markdown", lambda s, t: "#")
        monkeypatch.setattr(trading_brief, "upsert_brief", lambda **k: {"id": 1})
        monkeypatch.setattr(trading_brief, "_now", lambda: "2026-08-12T12:00:00")
        with patch("data_sync_service.db.webhook.emit_event") as emit_mock:
            trading_brief.generate_trading_brief("midday")
        emit_mock.assert_not_called()


class TestE5CandidateDiff:
    def test_emits_added_only(self, monkeypatch) -> None:
        emitted: list[dict] = []

        def fake_emit(event_type, payload, dedupe_key):
            emitted.append({"type": event_type, "payload": payload, "key": dedupe_key})
            return True

        def fake_candidates(*, trade_date, market):
            if market != "CN":
                return []  # HK no candidates today or yesterday
            if trade_date == "2026-08-12":
                return [{"symbol": "CN:600001"}, {"symbol": "CN:600002"}]
            return [{"symbol": "CN:600001"}]  # previous day

        monkeypatch.setattr(cdj, "build_s3_candidates", fake_candidates)
        monkeypatch.setattr(cdj, "last_trading_day", lambda exchange, d: __import__("datetime").date(2026, 8, 11))
        with patch("data_sync_service.db.webhook.emit_event", side_effect=fake_emit):
            out = cdj.candidate_diff(trade_date="2026-08-12")
        assert out["added_by_market"]["CN"] == ["CN:600002"]
        assert len(emitted) == 1
        assert emitted[0]["type"] == "candidate_added"
        assert emitted[0]["payload"]["added"] == ["CN:600002"]
        assert emitted[0]["key"] == "candidate_added:CN:2026-08-12"

    def test_disappearances_are_silent(self, monkeypatch) -> None:
        emitted: list[dict] = []

        def fake_emit(event_type, payload, dedupe_key):
            emitted.append({"type": event_type, "payload": payload, "key": dedupe_key})
            return True

        def fake_candidates(*, trade_date, market):
            if market != "CN":
                return []
            # Yesterday had a candidate; today it disappeared (gate closed).
            return [] if trade_date == "2026-08-12" else [{"symbol": "CN:600001"}]

        monkeypatch.setattr(cdj, "build_s3_candidates", fake_candidates)
        monkeypatch.setattr(cdj, "last_trading_day", lambda exchange, d: __import__("datetime").date(2026, 8, 11))
        with patch("data_sync_service.db.webhook.emit_event", side_effect=fake_emit):
            out = cdj.candidate_diff(trade_date="2026-08-12")
        assert out["added_by_market"] == {}
        assert emitted == []


class TestE6E7:
    def test_e6_oos_warning_emits(self, monkeypatch) -> None:
        from data_sync_service.scheduler import rolling_oos_job as roj

        emitted: list[dict] = []

        def fake_emit(event_type, payload, dedupe_key):
            emitted.append({"type": event_type, "payload": payload, "key": dedupe_key})
            return True

        monkeypatch.setattr(roj, "_rolling_window", lambda: ("2026-05-13", "2026-08-11"))
        monkeypatch.setattr(roj, "simulate", lambda cfg: __import__("types").SimpleNamespace())
        monkeypatch.setattr(
            roj,
            "_summarize",
            lambda run: {"totalNetPnlPct": -8.5, "maxDrawdownPct": 19.5, "sharpe": -3.2, "closed": 55},
        )
        monkeypatch.setattr(roj, "insert_record", lambda *a, **k: None)
        monkeypatch.setattr(roj, "REPORT_FILE", type("F", (), {"parent": type("P", (), {"mkdir": lambda *a, **k: None}), "write_text": lambda *a, **k: None})())
        with patch("data_sync_service.db.webhook.emit_event", side_effect=fake_emit):
            roj.run()
        assert len(emitted) == 1
        assert emitted[0]["type"] == "oos_warning"
        assert any("HK" in w for w in emitted[0]["payload"]["warnings"])

    def test_e7_recon_missing_emits(self, monkeypatch) -> None:
        from data_sync_service.scheduler import backtest_recon_job as brj

        emitted: list[dict] = []

        def fake_emit(event_type, payload, dedupe_key):
            emitted.append({"type": event_type, "payload": payload, "key": dedupe_key})
            return True

        monkeypatch.setattr(brj, "insert_record", lambda *a, **k: None)
        monkeypatch.setattr(brj, "run_and_persist", lambda day: {
            "reconDate": day,
            "markets": {
                "HK": {"available": True, "missing": 3},
                "CN": {"available": True, "missing": 0},
            },
        })
        with patch("data_sync_service.db.webhook.emit_event", side_effect=fake_emit):
            brj.run()
        assert len(emitted) == 1
        assert emitted[0]["type"] == "recon_missing"
        assert emitted[0]["payload"]["markets"] == ["HK"]

    def test_e7_clean_recon_silent(self, monkeypatch) -> None:
        from data_sync_service.scheduler import backtest_recon_job as brj

        monkeypatch.setattr(brj, "insert_record", lambda *a, **k: None)
        monkeypatch.setattr(brj, "run_and_persist", lambda day: {
            "reconDate": day,
            "markets": {"HK": {"available": True, "missing": 0}, "CN": {"available": True, "missing": 0}},
        })
        with patch("data_sync_service.db.webhook.emit_event") as emit_mock:
            brj.run()
        emit_mock.assert_not_called()
