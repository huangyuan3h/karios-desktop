"""scheduler/intraday_alarm_job.py (E3) + db/webhook emit on job failure (E1)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import patch

from data_sync_service.db import sync_job_record
from data_sync_service.scheduler import intraday_alarm_job as ia


class TestIntradayAlarm:
    def test_no_open_trades(self, monkeypatch) -> None:
        monkeypatch.setattr(ia.paper_trading, "get_open_paper_trades", lambda: [])
        out = ia.check_intraday_drawdowns()
        assert out == {"ok": True, "checked": 0, "alarms": 0, "skipped": 0}

    def test_alarm_emitted_below_threshold(self, monkeypatch) -> None:
        trades = [
            {"symbol": "CN:600001", "entry_price": 10.0},
            {"symbol": "HK:00622", "entry_price": 2.0},
        ]
        monkeypatch.setattr(ia.paper_trading, "get_open_paper_trades", lambda: trades)
        monkeypatch.setattr(ia, "_resolve_ts_code", lambda s: ("CN", "600001.SH") if s == "CN:600001" else ("HK", "00622.HK"))
        monkeypatch.setattr(
            ia,
            "fetch_realtime_quotes",
            lambda codes: {
                "ok": True,
                "items": [
                    {"ts_code": "600001.SH", "price": "9.1"},   # -9% -> alarm
                    {"ts_code": "00622.HK", "price": "1.98"},   # -1% -> no alarm
                ],
            },
        )
        emitted: list[tuple] = []
        monkeypatch.setattr(ia, "emit_event", lambda *a, **k: emitted.append((a, k)))
        out = ia.check_intraday_drawdowns()
        assert out["alarms"] == 1
        assert len(emitted) == 1
        assert emitted[0][0][0] == "intraday_drawdown"
        assert emitted[0][0][1]["symbol"] == "CN:600001"
        assert emitted[0][0][1]["drawdown_pct"] == -9.0
        assert emitted[0][1]["dedupe_key"] == f"intraday_drawdown:CN:600001:{date.today().isoformat()}"

    def test_quote_failure_returns_error(self, monkeypatch) -> None:
        monkeypatch.setattr(ia.paper_trading, "get_open_paper_trades", lambda: [{"symbol": "CN:600001", "entry_price": 10.0}])
        monkeypatch.setattr(ia, "_resolve_ts_code", lambda s: ("CN", "600001.SH"))
        monkeypatch.setattr(ia, "fetch_realtime_quotes", lambda codes: {"ok": False, "error": "no key"})
        out = ia.check_intraday_drawdowns()
        assert out["ok"] is False


class TestJobFailureEvent:
    def test_failure_emits_job_failed_event(self, monkeypatch) -> None:
        emitted: list[dict] = []

        def fake_emit(event_type, payload, dedupe_key):
            emitted.append({"type": event_type, "payload": payload, "key": dedupe_key})
            return True

        with patch("data_sync_service.db.sync_job_record.get_connection") as conn_mock:
            cm = conn_mock.return_value.__enter__.return_value
            cm.cursor.return_value.__enter__.return_value.rowcount = 0
            with patch("data_sync_service.db.webhook.emit_event", side_effect=fake_emit):
                # insert_record calls emit_event only when success=False; the
                # SQL layer is replaced by the patched connection above.
                sync_job_record.insert_record("intraday_alarm", success=False, error_message="boom")
        assert len(emitted) == 1
        assert emitted[0]["type"] == "job_failed"
        assert emitted[0]["payload"]["job_type"] == "intraday_alarm"
        assert emitted[0]["payload"]["error"] == "boom"
        assert emitted[0]["key"].startswith(f"job_failed:intraday_alarm:{datetime.now(UTC).date()}")

    def test_success_emits_nothing(self, monkeypatch) -> None:
        with patch("data_sync_service.db.sync_job_record.get_connection") as conn_mock:
            cm = conn_mock.return_value.__enter__.return_value
            cm.cursor.return_value.__enter__.return_value.rowcount = 0
            with patch("data_sync_service.db.webhook.emit_event") as emit_mock:
                sync_job_record.insert_record("intraday_alarm", success=True)
        emit_mock.assert_not_called()
