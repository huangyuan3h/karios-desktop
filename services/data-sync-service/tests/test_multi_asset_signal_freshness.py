"""multi_asset_sleeve decision-signal freshness (Harbor T-close semantics).

Harbor clock: the 18:20 job signals on the latest COMPLETED close (today
included after the daily sync) and executes at T+1 open. During a session the
daily table has no today bar, so the signal naturally stays on t-1.

Basis (2026-09-17 audit): the signal now reads the engine-basis adjusted
series (``harbor.load_etf_closes``); the raw ``daily`` path is only a fallback.
"""

from __future__ import annotations

from types import SimpleNamespace

from data_sync_service.service import multi_asset_sleeve as mas


def _adj(bars):
    return lambda ts: {b["date"]: float(b["close"]) for b in bars}


def _bars(*pairs):
    return [{"date": d, "trade_date": d, "close": c} for d, c in pairs]


class TestSignalCloses:
    def test_includes_today_close(self, monkeypatch) -> None:
        bars = _bars(
            ("2026-08-26", 4.7),
            ("2026-08-27", 4.8),
            ("2026-08-28", 4.9),
            ("2026-08-31", 5.0),  # today (intraday / just closed)
        )
        monkeypatch.setattr(mas, "_adjusted_series", _adj(bars))
        monkeypatch.setattr(
            "data_sync_service.service.trade_calendar_utils.shanghai_today",
            lambda: __import__("datetime").date(2026, 8, 31),
        )
        out = mas._signal_closes("518880.SH", 260)
        assert out == [4.7, 4.8, 4.9, 5.0]
        assert out[-1] == 5.0  # today's completed close is the signal

    def test_no_today_bar_keeps_last(self, monkeypatch) -> None:
        bars = _bars(("2026-08-26", 4.7), ("2026-08-27", 4.8), ("2026-08-28", 4.9))
        monkeypatch.setattr(mas, "_adjusted_series", _adj(bars))
        monkeypatch.setattr(
            "data_sync_service.service.trade_calendar_utils.shanghai_today",
            lambda: __import__("datetime").date(2026, 8, 31),
        )
        out = mas._signal_closes("518880.SH", 260)
        assert out[-1] == 4.9  # t-1 kept (session case: no today bar yet)

    def test_keeps_weekend_semantics(self, monkeypatch) -> None:
        # Monday: Saturday/Sunday bars do not exist; Friday close is t-1.
        bars = _bars(("2026-08-27", 4.8), ("2026-08-28", 4.9))
        monkeypatch.setattr(mas, "_adjusted_series", _adj(bars))
        monkeypatch.setattr(
            "data_sync_service.service.trade_calendar_utils.shanghai_today",
            lambda: __import__("datetime").date(2026, 8, 31),
        )
        assert mas._signal_closes("518880.SH", 260)[-1] == 4.9

    def test_explicit_as_of_excludes_later_bars(self, monkeypatch) -> None:
        """Historical callers (recon / past-day views) must not leak later closes."""
        bars = _bars(
            ("2026-08-26", 4.7),
            ("2026-08-27", 4.8),
            ("2026-09-10", 9.9),  # future vs as_of
        )
        monkeypatch.setattr(mas, "_adjusted_series", _adj(bars))
        assert mas._signal_closes("518880.SH", 260, as_of="2026-08-27") == [4.7, 4.8]
        series = mas._signal_series("518880.SH", 260, as_of="2026-08-27")
        assert max(series) == "2026-08-27"
        assert series["2026-08-27"] == 4.8

    def test_raw_daily_fallback_when_panel_empty(self, monkeypatch) -> None:
        bars = _bars(("2026-08-27", 4.8), ("2026-08-28", 4.9))
        monkeypatch.setattr(mas, "_adjusted_series", lambda ts: {})
        monkeypatch.setattr(mas, "fetch_last_bars", lambda ts, days: bars)
        monkeypatch.setattr(
            "data_sync_service.service.trade_calendar_utils.shanghai_today",
            lambda: __import__("datetime").date(2026, 8, 31),
        )
        assert mas._signal_closes("518880.SH", 260)[-1] == 4.9

    def test_pick_passes_as_of_to_signal_series(self, monkeypatch) -> None:
        seen: list[str | None] = []

        def fake_series(ts, days=260, *, as_of=None):
            seen.append(as_of)
            return {"2026-08-01": 1.0}

        monkeypatch.setattr(mas, "_signal_series", fake_series)
        out = mas._pick(as_of="2026-08-01")
        assert out is None  # coverage gate fails on a single point
        assert seen and all(a == "2026-08-01" for a in seen)


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
            tushare_tokens = ("TEST_KEY",)

        def _upsert(df) -> int:
            state["upserted"] += len(df)
            return len(df)

        monkeypatch.setattr(ed, "get_settings", lambda: _Settings())
        monkeypatch.setattr(ed, "get_pool", lambda: SimpleNamespace(pro=lambda: _Pro()))
        monkeypatch.setattr(ed, "upsert_from_dataframe", _upsert)
        monkeypatch.setattr(
            ed, "get_last_trade_date", lambda ts: __import__("datetime").date(2026, 8, 21)
        )
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
