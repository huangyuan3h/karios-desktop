"""multi_asset_sleeve decision-signal freshness (t-1 close semantics).

2026-08-31 audit: _pick() used closes[:-1] assuming the last bar is today's
intraday bar — but nothing writes the daily table intraday, so during a
session (e.g. 14:30) the signal silently fell back to t-2. _signal_closes
now excludes today's bar explicitly.
"""

from __future__ import annotations

from data_sync_service.service import multi_asset_sleeve as mas


class TestSignalCloses:
    def test_excludes_today_bar(self, monkeypatch) -> None:
        bars = [
            {"date": "2026-08-26", "close": 4.7},
            {"date": "2026-08-27", "close": 4.8},
            {"date": "2026-08-28", "close": 4.9},
            {"date": "2026-08-31", "close": 5.0},  # today (intraday / just closed)
        ]
        monkeypatch.setattr(mas, "fetch_last_bars", lambda ts, days: bars)
        monkeypatch.setattr(
            "data_sync_service.service.trade_calendar_utils.shanghai_today",
            lambda: __import__("datetime").date(2026, 8, 31),
        )
        out = mas._signal_closes("518880.SH", 260)
        assert out == [4.7, 4.8, 4.9]
        assert out[-1] == 4.9  # t-1 close, not today's 5.0

    def test_no_today_bar_keeps_last(self, monkeypatch) -> None:
        bars = [
            {"date": "2026-08-26", "close": 4.7},
            {"date": "2026-08-27", "close": 4.8},
            {"date": "2026-08-28", "close": 4.9},
        ]
        monkeypatch.setattr(mas, "fetch_last_bars", lambda ts, days: bars)
        monkeypatch.setattr(
            "data_sync_service.service.trade_calendar_utils.shanghai_today",
            lambda: __import__("datetime").date(2026, 8, 31),
        )
        out = mas._signal_closes("518880.SH", 260)
        assert out[-1] == 4.9  # t-1 kept (session case: no today bar yet)

    def test_keeps_weekend_semantics(self, monkeypatch) -> None:
        # Monday: Saturday/Sunday bars do not exist; Friday close is t-1.
        bars = [
            {"date": "2026-08-27", "close": 4.8},
            {"date": "2026-08-28", "close": 4.9},
        ]
        monkeypatch.setattr(mas, "fetch_last_bars", lambda ts, days: bars)
        monkeypatch.setattr(
            "data_sync_service.service.trade_calendar_utils.shanghai_today",
            lambda: __import__("datetime").date(2026, 8, 31),
        )
        assert mas._signal_closes("518880.SH", 260)[-1] == 4.9


class TestSleeveEtfSync:
    def test_uses_fund_daily_and_records(self, monkeypatch) -> None:
        import pandas as pd

        from data_sync_service.service import etf_daily as ed

        state = {"upserted": 0}

        class _Pro:
            def fund_daily(self, **kw) -> pd.DataFrame:
                assert kw["ts_code"] in ed.SLEEVE_ETF_TS_CODES
                return pd.DataFrame(
                    {"ts_code": [kw["ts_code"]], "trade_date": ["20260828"], "close": [4.0]}
                )

        class _Settings:
            tu_share_api_key = "TEST_KEY"

        def _upsert(df) -> int:
            state["upserted"] += len(df)
            return len(df)

        monkeypatch.setattr(ed, "get_settings", lambda: _Settings())
        monkeypatch.setattr(ed, "ts", type("ts", (), {"pro_api": staticmethod(lambda k: _Pro())}))
        monkeypatch.setattr(ed, "upsert_from_dataframe", _upsert)
        monkeypatch.setattr(ed, "get_last_trade_date", lambda ts: __import__("datetime").date(2026, 8, 21))
        monkeypatch.setattr(ed, "_sync_end_date", lambda ts: "20260831")
        monkeypatch.setattr(ed, "time", type("t", (), {"sleep": staticmethod(lambda s: None)}))
        rec = {"rows": []}

        def _insert_record(**kw) -> None:
            rec["rows"].append(kw)

        monkeypatch.setattr(ed, "insert_record", _insert_record)
        r = ed.sync_sleeve_etfs()
        assert r["ok"] is True and r["updated"] == len(ed.SLEEVE_ETF_TS_CODES)
        assert state["upserted"] == len(ed.SLEEVE_ETF_TS_CODES)
        assert rec["rows"][-1]["success"] is True