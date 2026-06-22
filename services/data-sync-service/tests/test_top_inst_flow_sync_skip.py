from __future__ import annotations

from data_sync_service.service import top_inst_flow as svc
from data_sync_service.service.top_inst_flow import TopInstProviderResult


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


def test_sync_top_inst_uses_tushare_provider_result(monkeypatch) -> None:
    inserted_summary_rows: list[list[dict]] = []
    inserted_daily_rows: list[list[dict]] = []

    monkeypatch.setattr(svc, "ensure_table", lambda: None)
    monkeypatch.setattr(svc, "_latest_cn_trade_date_yyyymmdd", lambda: "20260622")
    monkeypatch.setattr(svc, "_watchlist_ts_codes", lambda: ["603588.SH", "603986.SH"])
    monkeypatch.setattr(svc, "get_today_run", lambda job_type: None)
    monkeypatch.setattr(svc, "is_trading_day", lambda exchange, cal_date: True)
    monkeypatch.setattr(
        svc,
        "fetch_top_inst_provider_result",
        lambda trade_date_iso: (
            TopInstProviderResult(
                source="tushare",
                lhb_tickers={"603588"},
                org_by_ticker={
                    "603588": {
                        "NET_BUY_AMT": -40_000_000.0,
                        "EXPLANATION": "日涨幅偏离值达到7%的前五只证券",
                    }
                },
                buy_seats_by_ts_code={
                    "603588.SH": [
                        {
                            "exalter": "东方财富证券股份有限公司拉萨团结路第二证券营业部",
                            "buy": 80_000_000.0,
                            "side": "buy",
                        }
                    ]
                },
                inst_seats_by_ts_code={
                    "603588.SH": [
                        {
                            "exalter": "机构专用",
                            "buy": 5_000_000.0,
                            "sell": 45_000_000.0,
                            "net_buy": -40_000_000.0,
                            "side": "sell",
                        }
                    ]
                },
                lhb_count=1,
                org_trade_count=1,
            ),
            ["eastmoney: temporary blocked"],
        ),
    )
    monkeypatch.setattr(svc, "upsert_daily_rows", lambda rows: inserted_daily_rows.append(rows) or len(rows))
    monkeypatch.setattr(svc, "upsert_summary_rows", lambda rows: inserted_summary_rows.append(rows) or len(rows))
    monkeypatch.setattr(svc, "insert_record", lambda **kwargs: None)

    out = svc.sync_top_inst_watchlist(force=True)

    assert out["ok"] is True
    assert out["provider"] == "tushare"
    assert out["fallbackUsed"] is True
    assert out["lhbCount"] == 1
    assert out["orgTradeCount"] == 1
    assert out["summaryRows"] == 2
    assert inserted_summary_rows[0][0]["ts_code"] == "603588.SH"
    assert inserted_summary_rows[0][0]["seat_label"] == "机构净卖/拉萨主买"
    assert inserted_summary_rows[0][0]["on_board"] is True
    assert inserted_summary_rows[0][1]["on_board"] is False
    assert inserted_daily_rows[0][0]["exalter"] == "机构专用"


def test_sync_top_inst_rejects_suspicious_empty_lhb(monkeypatch) -> None:
    summary_called = False

    monkeypatch.setattr(svc, "ensure_table", lambda: None)
    monkeypatch.setattr(svc, "_latest_cn_trade_date_yyyymmdd", lambda: "20260622")
    monkeypatch.setattr(svc, "_watchlist_ts_codes", lambda: ["603588.SH"])
    monkeypatch.setattr(svc, "get_today_run", lambda job_type: None)
    monkeypatch.setattr(svc, "is_trading_day", lambda exchange, cal_date: True)
    monkeypatch.setattr(
        svc,
        "fetch_top_inst_provider_result",
        lambda trade_date_iso: (
            TopInstProviderResult(source="eastmoney", lhb_tickers=set(), lhb_count=0),
            [],
        ),
    )
    monkeypatch.setattr(svc, "upsert_daily_rows", lambda rows: len(rows))

    def fail_summary(rows: list[dict]) -> int:
        nonlocal summary_called
        summary_called = True
        return len(rows)

    records: list[dict] = []
    monkeypatch.setattr(svc, "upsert_summary_rows", fail_summary)
    monkeypatch.setattr(svc, "insert_record", lambda **kwargs: records.append(kwargs))

    out = svc.sync_top_inst_watchlist(force=True)

    assert out["ok"] is False
    assert out["reason"] == "suspicious_empty_lhb"
    assert out["summaryRows"] == 0
    assert summary_called is False
    assert records and records[0]["success"] is False
