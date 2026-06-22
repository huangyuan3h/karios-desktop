from __future__ import annotations

from data_sync_service.service import top_inst_flow as svc


def test_sync_top_inst_skips_when_today_success_and_watchlist_covered(monkeypatch) -> None:
    monkeypatch.setattr(svc, "ensure_table", lambda: None)
    monkeypatch.setattr(svc, "_latest_cn_trade_date_yyyymmdd", lambda: "20260622")
    monkeypatch.setattr(svc, "_watchlist_ts_codes", lambda: ["002185.SZ", "002156.SZ"])
    monkeypatch.setattr(svc, "get_today_run", lambda job_type: {"success": True})
    monkeypatch.setattr(
        svc,
        "fetch_summaries_for_codes",
        lambda codes, trade_date=None: {code: {"ts_code": code} for code in codes},
    )

    def fail_network(*args, **kwargs):
        raise AssertionError("network sync should be skipped when coverage is complete")

    monkeypatch.setattr(svc, "fetch_em_lhb_tickers_on_date", fail_network)

    out = svc.sync_top_inst_watchlist(force=False)

    assert out["ok"] is True
    assert out["skipped"] is True
    assert out["reason"] == "already_synced_today"
    assert out["covered"] == 2


def test_sync_top_inst_continues_when_today_success_but_watchlist_missing(monkeypatch) -> None:
    inserted_summary_rows: list[list[dict]] = []

    monkeypatch.setattr(svc, "ensure_table", lambda: None)
    monkeypatch.setattr(svc, "_latest_cn_trade_date_yyyymmdd", lambda: "20260622")
    monkeypatch.setattr(svc, "_watchlist_ts_codes", lambda: ["002185.SZ", "002156.SZ"])
    monkeypatch.setattr(svc, "get_today_run", lambda job_type: {"success": True})
    monkeypatch.setattr(
        svc,
        "fetch_summaries_for_codes",
        lambda codes, trade_date=None: {"002185.SZ": {"ts_code": "002185.SZ"}},
    )
    monkeypatch.setattr(svc, "is_trading_day", lambda exchange, cal_date: True)
    monkeypatch.setattr(svc, "fetch_em_lhb_tickers_on_date", lambda trade_date_iso: set())
    monkeypatch.setattr(svc, "fetch_em_org_trades_on_date", lambda trade_date_iso: {})
    monkeypatch.setattr(svc, "upsert_daily_rows", lambda rows: len(rows))

    def capture_summary_rows(rows: list[dict]) -> int:
        inserted_summary_rows.append(rows)
        return len(rows)

    monkeypatch.setattr(svc, "upsert_summary_rows", capture_summary_rows)
    monkeypatch.setattr(svc, "insert_record", lambda **kwargs: None)

    out = svc.sync_top_inst_watchlist(force=False)

    assert out["ok"] is True
    assert out["summaryRows"] == 2
    assert inserted_summary_rows
    assert {row["ts_code"] for row in inserted_summary_rows[0]} == {"002185.SZ", "002156.SZ"}
