"""bar_5min last-hour helpers — no network, no DB writes."""

from __future__ import annotations

from data_sync_service.scheduler import bar_5min_job
from data_sync_service.service import bar_5min as b5


def test_to_baostock_code() -> None:
    assert b5.to_baostock_code("000001.SZ") == "sz.000001"
    assert b5.to_baostock_code("600000.SH") == "sh.600000"
    assert b5.to_baostock_code("300413.SZ") == "sz.300413"
    assert b5.to_baostock_code("02099.HK") is None


def test_parse_baostock_time() -> None:
    assert b5.parse_baostock_time("20250903093500000") == "0935"
    assert b5.parse_baostock_time("20260902150000000") == "1500"
    assert b5.parse_baostock_time("bad") is None


def test_filter_last_hour_keeps_1430_to_1500() -> None:
    rows = [
        {"trade_date": "2026-09-02", "time": "1425", "close": 1.0},
        {"trade_date": "2026-09-02", "time": "1430", "close": 1.1},
        {"trade_date": "2026-09-02", "time": "1500", "close": 1.2},
        {"trade_date": "2026-09-02", "time": "0935", "close": 1.3},
    ]
    kept = b5.filter_last_hour(rows)
    assert [r["time"] for r in kept] == ["1430", "1500"]


def test_rows_from_baostock_last_hour() -> None:
    raw = [
        ["2026-09-02", "20260902142500000", "sz.000001", "11.9", "11.9", "11.8", "11.9", "100", "1000"],
        ["2026-09-02", "20260902143000000", "sz.000001", "11.9", "12.0", "11.8", "11.95", "200", "2000"],
        ["2026-09-02", "20260902150000000", "sz.000001", "11.9", "11.91", "11.89", "11.91", "1881400", "22400817"],
    ]
    rows = b5.filter_last_hour(b5.rows_from_baostock(raw))
    assert len(rows) == 2
    assert rows[0]["time"] == "1430"
    assert rows[0]["close"] == 11.95
    assert rows[1]["time"] == "1500"
    assert rows[1]["vol"] == 1881400.0


def test_rows_from_tushare() -> None:
    raw = [
        {
            "ts_code": "000001.SZ",
            "trade_time": "2025-09-03 15:00:00",
            "close": 11.75,
            "open": 11.75,
            "high": 11.76,
            "low": 11.74,
            "vol": 3857281.0,
            "amount": 45322132.0,
        },
        {
            "ts_code": "000001.SZ",
            "trade_time": "2025-09-03 11:30:00",
            "close": 11.80,
            "open": 11.80,
            "high": 11.81,
            "low": 11.79,
            "vol": 1.0,
            "amount": 1.0,
        },
    ]
    rows = b5.filter_last_hour(b5.rows_from_tushare(raw))
    assert len(rows) == 1
    assert rows[0]["trade_date"] == "2025-09-03"
    assert rows[0]["time"] == "1500"
    assert rows[0]["close"] == 11.75


def test_bar_5min_job_constants() -> None:
    assert bar_5min_job.JOB_ID == "bar_5min_close"
    assert bar_5min_job.CRON_EXPRESSION == "40 18 * * 1-5"
    assert bar_5min_job.TIMEZONE == "Asia/Shanghai"


def test_bar_5min_job_run_no_symbols(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(bar_5min_job, "list_gap_codes", lambda _d: [])
    monkeypatch.setattr(bar_5min_job, "_open_cn_paper_ts_codes", lambda: [])
    records: list[dict] = []
    monkeypatch.setattr(
        bar_5min_job,
        "insert_record",
        lambda *a, **kw: records.append(kw) or None,
    )
    bar_5min_job.run()
    assert records
    assert records[0]["success"] is True
    assert records[0]["error_message"] == "no-symbols"


def test_bar_5min_job_run_ok(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(bar_5min_job, "list_gap_codes", lambda _d: ["000001.SZ"])
    monkeypatch.setattr(bar_5min_job, "_open_cn_paper_ts_codes", lambda: ["000001.SZ", "600000.SH"])
    monkeypatch.setattr(
        bar_5min_job,
        "backfill_symbols",
        lambda **kw: {"ok": 2, "failed": 0, "skipped": 0, "stored": 14, "pending": 2},
    )
    records: list[dict] = []
    monkeypatch.setattr(
        bar_5min_job,
        "insert_record",
        lambda *a, **kw: records.append({"args": a, **kw}) or None,
    )
    bar_5min_job.run()
    assert records[0]["success"] is True
    assert records[0]["last_ts_code"] == "14"


def test_backfill_skips_covered(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(b5, "coverage_by_ts_code", lambda *_a, **_k: {"000001.SZ": 200})
    monkeypatch.setattr(b5, "trading_day_count", lambda *_a, **_k: 200)
    called: list[str] = []
    monkeypatch.setattr(b5, "fetch_baostock_5min", lambda *a, **k: called.append("fetch") or [])
    out = b5.backfill_symbols(
        ts_codes=["000001.SZ"],
        start_date="2025-09-03",
        end_date="2026-09-03",
        source=b5.SOURCE_BAOSTOCK,
        sleep_seconds=0,
        skip_covered=True,
    )
    assert out["skipped"] == 1
    assert out["pending"] == 0
    assert called == []


def test_backfill_today_does_not_skip_uncovered(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(b5, "coverage_by_ts_code", lambda *_a, **_k: {})
    monkeypatch.setattr(b5, "trading_day_count", lambda *_a, **_k: 1)
    monkeypatch.setattr(
        b5,
        "fetch_baostock_5min",
        lambda *_a, **_k: [{"trade_date": "2026-09-03", "time": "1500", "close": 11.9}],
    )
    monkeypatch.setattr(b5, "upsert_5min_bars", lambda *_a, **_k: 1)
    out = b5.backfill_symbols(
        ts_codes=["000001.SZ"],
        start_date="2026-09-03",
        end_date="2026-09-03",
        sleep_seconds=0,
        skip_covered=True,
    )
    assert out["skipped"] == 0
    assert out["pending"] == 1
    assert out["ok"] == 1


def test_backfill_stores_filtered_rows(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(b5, "coverage_by_ts_code", lambda *_a, **_k: {})
    monkeypatch.setattr(b5, "trading_day_count", lambda *_a, **_k: 10)
    monkeypatch.setattr(
        b5,
        "fetch_baostock_5min",
        lambda *_a, **_k: [{"trade_date": "2026-09-02", "time": "1500", "close": 11.9}],
    )
    stored: list[tuple] = []

    def _upsert(ts_code: str, rows: list, **kw) -> int:  # noqa: ANN001
        stored.append((ts_code, len(rows), kw.get("source")))
        return len(rows)

    monkeypatch.setattr(b5, "upsert_5min_bars", _upsert)
    out = b5.backfill_symbols(
        ts_codes=["000001.SZ"],
        start_date="2025-09-03",
        end_date="2026-09-03",
        sleep_seconds=0,
        skip_covered=True,
    )
    assert out["ok"] == 1
    assert out["stored"] == 1
    assert stored == [("000001.SZ", 1, "baostock")]
