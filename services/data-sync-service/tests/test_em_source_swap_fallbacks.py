"""ETF history repair + honest metric labels (share-change vs EM main flow)."""

from __future__ import annotations

from datetime import date

from data_sync_service.service import etf_fund_flow as eff


def test_history_repair_starts_at_earliest_missing_fd_share() -> None:
    rows = [
        {"trade_date": "2026-11-02", "fd_share": 1.0},
        {"trade_date": "2026-11-03", "fd_share": None},
        {"trade_date": "2026-11-04", "fd_share": None},
    ]
    start = eff._missing_share_start_date(rows, end_date="20261106")
    assert start == date(2026, 11, 3)


def test_history_repair_skips_em_realtime_rows() -> None:
    rows = [
        {
            "trade_date": "2026-11-05",
            "fd_share": None,
            "source": eff.EM_ETF_FLOW_SOURCE,
            "main_net_inflow": 123.0,
        },
        {"trade_date": "2026-11-06", "fd_share": None, "source": "tushare.fund_share"},
    ]
    start = eff._missing_share_start_date(rows, end_date="20261106")
    assert start == date(2026, 11, 6)


def test_history_repair_ignores_rows_outside_lookback() -> None:
    rows = [{"trade_date": "2025-01-01", "fd_share": None}]
    assert eff._missing_share_start_date(rows, end_date="20261106", lookback_days=120) is None


def test_merge_tushare_frames_tags_history_source() -> None:
    class _DF:
        empty = False

        def iterrows(self):
            return iter(
                [
                    (
                        0,
                        {"trade_date": "20261105", "fd_share": 100.0},
                    )
                ]
            )

    out = eff._merge_tushare_frames(
        "510300.SH", share_df=_DF(), daily_df=None, updated_at="now"
    )
    assert out[0]["source"] == "tushare.fund_share"


def test_sync_history_uses_repair_window(monkeypatch) -> None:
    seen: dict[str, str] = {}

    class _Pro:
        def fund_share(self, **kw):
            seen.update(kw)
            return None

        def fund_daily(self, **kw):
            return None

    monkeypatch.setattr(eff, "get_settings", lambda: type("S", (), {"tushare_tokens": ("k",)})())
    monkeypatch.setattr(eff, "get_pool", lambda: type("P", (), {"pro": lambda self: _Pro()})())
    monkeypatch.setattr(eff, "get_last_trade_date", lambda code: date(2026, 11, 6))
    monkeypatch.setattr(
        eff,
        "fetch_rows_for_codes",
        lambda codes: [
            {"trade_date": "2026-11-05", "fd_share": None},
            {"trade_date": "2026-11-06", "fd_share": 100.0},
        ],
    )
    monkeypatch.setattr(eff, "_date_to_yyyymmdd", lambda d: d.strftime("%Y%m%d"))
    monkeypatch.setattr(eff, "_with_retry", lambda fn: fn())
    monkeypatch.setattr(eff, "_merge_tushare_frames", lambda code, **kw: [{"r": 1}])
    monkeypatch.setattr(eff, "upsert_daily_rows", lambda rows: 1)
    monkeypatch.setattr(eff, "_recompute_net_inflows_for_code", lambda code, updated_at=None: 1)

    out = eff._sync_tushare_history_if_available(
        ts_code="510300.SH", end_date="20261106", updated_at="now"
    )
    assert out == 2
    assert seen["start_date"] == "20261105"  # repair window, not last_date+1


def test_etf_bundle_labels_share_implied_flow(monkeypatch) -> None:
    monkeypatch.setattr(eff, "ensure_table", lambda: None)
    monkeypatch.setattr(eff, "get_latest_date", lambda: "2026-11-06")
    monkeypatch.setattr(eff, "_is_shanghai_sync_window", lambda: False)
    monkeypatch.setattr(
        eff,
        "fetch_rows_for_codes",
        lambda *_a, **_k: [
            {
                "ts_code": "510300.SH",
                "trade_date": "2026-11-06",
                "fd_share": 101.0,
                "close": 4.1,
                "avg_price": 4.1,
                "net_inflow": 10_000_000.0,
                "source": "tushare.fund_share",
            }
        ],
    )
    monkeypatch.setattr(eff, "get_open_dates", lambda *_a, **_k: [date(2026, 11, 6)])
    out = eff.build_etf_fund_flow_bundle(as_of_date="2026-11-06")
    hs300 = next(it for it in out["items"] if it["symbol"] == "510300")
    assert hs300["source"] == "tushare"
    assert hs300["netFlowMetric"] == "share_change_implied"
    assert hs300["mainNetInflow"] is None
    assert out["realtimeAvailable"] is False
    assert "tushare" in out["sources"]


def test_etf_degraded_shape_reports_history_fallback(monkeypatch) -> None:
    records: list[dict] = []
    monkeypatch.setattr(eff, "_should_skip_etf_sync_today", lambda *, force: False)
    monkeypatch.setattr(eff, "ensure_table", lambda: None)
    monkeypatch.setattr(eff, "_shanghai_today_yyyymmdd", lambda: "20261106")
    monkeypatch.setattr(eff, "_now_iso", lambda: "now")
    monkeypatch.setattr(eff, "fetch_em_etf_realtime_flow_for_symbols", lambda symbols: {})
    monkeypatch.setattr(
        eff,
        "get_last_em_etf_fetch_error",
        lambda: "push2.eastmoney.com:direct:RemoteDisconnected",
    )
    monkeypatch.setattr(eff, "_sync_tushare_history_if_available", lambda **kw: 4)
    monkeypatch.setattr(eff, "upsert_daily_rows", lambda rows: len(rows))
    monkeypatch.setattr(eff, "insert_record", lambda **kw: records.append(kw))
    out = eff.sync_etf_fund_flow_watchlist(force=True)
    assert out["ok"] is True and out["degraded"] is True
    assert out["realtimeAvailable"] is False
    assert out["historyAvailable"] is True
    assert out["historySource"] == "tushare.fund_share"
    assert "Tushare" in out["warning"]
    # unavailable realtime must not advance last-success
    assert records and records[-1]["success"] is False


def test_history_repair_never_overwrites_em_realtime_rows(monkeypatch) -> None:
    class _Pro:
        def fund_share(self, **kw):
            return None

        def fund_daily(self, **kw):
            return None

    monkeypatch.setattr(eff, "get_settings", lambda: type("S", (), {"tushare_tokens": ("k",)})())
    monkeypatch.setattr(eff, "get_pool", lambda: type("P", (), {"pro": lambda self: _Pro()})())
    monkeypatch.setattr(eff, "get_last_trade_date", lambda code: date(2026, 11, 6))
    monkeypatch.setattr(
        eff,
        "fetch_rows_for_codes",
        lambda codes: [
            {
                "trade_date": "2026-11-05",
                "fd_share": None,
                "source": eff.EM_ETF_FLOW_SOURCE,
                "main_net_inflow": 999.0,
            },
            {"trade_date": "2026-11-06", "fd_share": None, "source": "tushare.fund_share"},
        ],
    )
    monkeypatch.setattr(eff, "_date_to_yyyymmdd", lambda d: d.strftime("%Y%m%d"))
    monkeypatch.setattr(eff, "_with_retry", lambda fn: fn())
    monkeypatch.setattr(
        eff,
        "_merge_tushare_frames",
        lambda code, **kw: [
            {"trade_date": "2026-11-05", "net_inflow": None},
            {"trade_date": "2026-11-06", "net_inflow": None},
        ],
    )
    written: list[dict] = []
    monkeypatch.setattr(eff, "upsert_daily_rows", lambda rows: written.extend(rows) or len(rows))
    monkeypatch.setattr(eff, "_recompute_net_inflows_for_code", lambda code, updated_at=None: 0)

    eff._sync_tushare_history_if_available(
        ts_code="510300.SH", end_date="20261106", updated_at="now"
    )
    dates = [r["trade_date"] for r in written]
    assert dates == ["2026-11-06"]  # the 11-05 EM realtime row is protected
