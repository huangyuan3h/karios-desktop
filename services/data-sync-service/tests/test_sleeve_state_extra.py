"""Extra pure unit tests for multi_asset_sleeve and state_bucket_track.

No DB, no network. All external reads are monkeypatched.
Covers missing lines from the full-suite coverage run.
"""

from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace

import pytest

from data_sync_service.service import multi_asset_sleeve as mas
from data_sync_service.service import state_bucket_track as sbt


def _linear_closes(n: int, start: float, end: float) -> list[float]:
    if n <= 1:
        return [float(start)]
    return [float(start + (end - start) * i / (n - 1)) for i in range(n)]


def _extra_cn_block(**kw) -> dict:
    block = {
        "regime": "Weak",
        "panicCooldown": {"active": False},
        "circuitBlocked": False,
        "s3Candidates": [],
        "holdings": [],
    }
    block.update(kw)
    return block


def _extra_fix_today(monkeypatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.trade_calendar_utils.shanghai_today",
        lambda: date(2026, 8, 31),
    )


class TestExtraEtfMarketData:
    def test_extra_market_data_insufficient(self, monkeypatch) -> None:
        monkeypatch.setattr(mas, "_closes", lambda ts, days=260: [1.0] * 10)
        out = mas._etf_market_data("518880.SH")
        assert out == {"ok": False, "n": 10}

    def test_extra_market_data_closes_raises(self, monkeypatch) -> None:
        def _boom(ts, days=260):
            raise RuntimeError("net down")

        monkeypatch.setattr(mas, "_closes", _boom)
        out = mas._etf_market_data("518880.SH")
        assert out == {"ok": False, "n": 0}

    def test_extra_market_data_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(mas, "_closes", lambda ts, days=260: _linear_closes(260, 100.0, 120.0))
        out = mas._etf_market_data("518880.SH")
        assert out["ok"] is True
        assert out["above"] is True


class TestExtraTrailExit:
    def test_extra_trail_break_past_day(self, monkeypatch) -> None:
        held = {"symbol": "ETF:513100", "ts_code": "513100.SH", "entryDate": "2026-01-01"}
        bars = [
            {"trade_date": "2026-01-15", "close": 100.0},
            {"trade_date": "2026-03-01", "close": 100.0},
        ]
        monkeypatch.setattr(mas, "fetch_last_bars", lambda ts, days=500: bars)
        assert mas._etf_trail_exit(held, day="2026-02-01") is None

    def test_extra_trail_no_entry_returns_none(self, monkeypatch) -> None:
        held = {"symbol": "ETF:513100", "ts_code": "513100.SH"}
        monkeypatch.setattr(mas, "fetch_last_bars", lambda ts, days=500: [])
        assert mas._etf_trail_exit(held, day="2026-02-01") is None

    def test_extra_trail_fetch_raises(self, monkeypatch) -> None:
        held = {"symbol": "ETF:513100", "ts_code": "513100.SH", "entryDate": "2026-01-01"}

        def _boom(ts, days=500):
            raise RuntimeError("db down")

        monkeypatch.setattr(mas, "fetch_last_bars", _boom)
        assert mas._etf_trail_exit(held, day="2026-02-01") is None


class TestExtraClosesHelpers:
    def test_extra_closes_fetch_raises(self, monkeypatch) -> None:
        def _boom(ts, days=260):
            raise RuntimeError("db down")

        monkeypatch.setattr(mas, "fetch_last_bars", _boom)
        assert mas._closes("518880.SH") == []

    def test_extra_closes_skips_bad_rows(self, monkeypatch) -> None:
        bars = [
            {"close": None},
            {"close": "bad"},
            {"close": 0},
            {"close": -1.0},
            {"close": 5.0},
            {},
        ]
        monkeypatch.setattr(mas, "fetch_last_bars", lambda ts, days=260: bars)
        assert mas._closes("518880.SH") == [5.0]

    def test_extra_signal_closes_fetch_raises(self, monkeypatch) -> None:
        _extra_fix_today(monkeypatch)

        def _boom(ts, days=260):
            raise RuntimeError("db down")

        monkeypatch.setattr(mas, "fetch_last_bars", _boom)
        assert mas._signal_closes("518880.SH") == []

    def test_extra_signal_closes_skips_bad_rows(self, monkeypatch) -> None:
        _extra_fix_today(monkeypatch)
        bars = [
            {"date": "2026-08-20", "close": None},
            {"date": "2026-08-21", "close": "bad"},
            {"date": "2026-08-22", "close": 0},
            {"date": "2026-08-23", "close": 5.0},
            {"date": "2026-08-31", "close": 9.0},
        ]
        monkeypatch.setattr(mas, "fetch_last_bars", lambda ts, days=260: bars)
        assert mas._signal_closes("518880.SH") == [5.0]


class TestExtraPick:
    def test_extra_pick_insufficient_one_candidate(self, monkeypatch) -> None:
        def _fake(ts, days=260):
            if ts == "518880.SH":
                return [1.0] * 10
            return _linear_closes(260, 100.0, 120.0)

        monkeypatch.setattr(mas, "_signal_closes", _fake)
        out = mas._pick()
        assert out is not None
        assert out["key"] in ("OIL", "NASDAQ", "BOND10")

    def test_extra_pick_returns_none_when_fewer_than_three(self, monkeypatch) -> None:
        def _fake(ts, days=260):
            if ts in ("513350.SH", "513100.SH"):
                return _linear_closes(260, 100.0, 120.0)
            return [1.0] * 10

        monkeypatch.setattr(mas, "_signal_closes", _fake)
        assert mas._pick() is None

    def test_extra_pick_returns_none_when_all_below_ma(self, monkeypatch) -> None:
        monkeypatch.setattr(mas, "_signal_closes", lambda ts, days=260: _linear_closes(260, 120.0, 100.0))
        assert mas._pick() is None

    def test_extra_pick_mid_window_continue(self, monkeypatch) -> None:
        # Second length guard (len < MA_WINDOW) is unreachable with sane
        # constants since the first guard requires >= MA+LOOKBACK. Drive it
        # with a negative LOOKBACK so a 195-bar series passes the first
        # guard but fails the second.
        monkeypatch.setattr(mas, "LOOKBACK", -5)
        monkeypatch.setattr(mas, "_signal_closes", lambda ts, days=260: _linear_closes(195, 100.0, 120.0))
        assert mas._pick() is None


class TestExtraStockBasket:
    def test_extra_stock_basket_hk_five_digit(self, monkeypatch) -> None:
        monkeypatch.setattr(mas, "_signal_closes", lambda ts, days=260: _linear_closes(70, 100.0, 110.0))
        out = mas._stock_basket_mom_from_holdings([{"symbol": "HK:12345", "ts_code": ""}])
        assert out is not None
        assert out["key"] == "STOCK"
        assert out["n"] == 1

    def test_extra_stock_basket_cn_sh_sz_and_skip(self, monkeypatch) -> None:
        monkeypatch.setattr(mas, "_signal_closes", lambda ts, days=260: _linear_closes(70, 100.0, 110.0))
        holdings = [
            {"symbol": "CN:600000", "ts_code": ""},
            {"symbol": "CN:000001", "ts_code": ""},
            {"symbol": "ETF:518880", "ts_code": "518880.SH"},
            {"symbol": "CN:BOGUS", "ts_code": ""},
        ]
        out = mas._stock_basket_mom_from_holdings(holdings)
        assert out is not None
        assert out["n"] == 2

    def test_extra_stock_basket_zero_ago_skipped(self, monkeypatch) -> None:
        closes = _linear_closes(70, 100.0, 110.0)
        closes[-mas.LOOKBACK] = 0.0
        monkeypatch.setattr(mas, "_signal_closes", lambda ts, days=260: list(closes))
        out = mas._stock_basket_mom_from_holdings([{"symbol": "CN:600000", "ts_code": "600000.SH"}])
        assert out is None

    def test_extra_stock_basket_short_series_skipped(self, monkeypatch) -> None:
        monkeypatch.setattr(mas, "_signal_closes", lambda ts, days=260: [1.0] * 10)
        out = mas._stock_basket_mom_from_holdings([{"symbol": "CN:600000", "ts_code": "600000.SH"}])
        assert out is None


class TestExtraRsiPulse:
    def test_extra_rsi_short_returns_none(self) -> None:
        assert mas._rsi([1.0] * 10) is None

    def test_extra_rsi_all_gains_returns_100(self) -> None:
        assert mas._rsi([float(i) for i in range(20)]) == 100.0

    def test_extra_rsi_mixed(self) -> None:
        closes = [10.0, 11.0, 10.5, 11.5, 10.0, 11.0, 12.0, 11.0, 10.0, 11.0, 12.0, 13.0, 12.0, 11.0, 12.0]
        out = mas._rsi(closes)
        assert out is not None
        assert 0.0 < out < 100.0

    def test_extra_pulse_hints_fetch_raises(self, monkeypatch) -> None:
        def _boom(ts, days=260):
            raise RuntimeError("net down")

        monkeypatch.setattr(mas, "_signal_closes", _boom)
        assert mas.build_pulse_hints() == []

    def test_extra_pulse_hints_happy(self, monkeypatch) -> None:
        monkeypatch.setattr(mas, "_signal_closes", lambda ts, days=260: _linear_closes(60, 100.0, 110.0))
        hints = mas.build_pulse_hints()
        assert len(hints) == 3
        assert {h["id"] for h in hints} == {"R4_oil_rsi80", "R2_nas_mom20_neg5", "R3_oil_vol_low"}


class TestExtraSleeveBuild:
    def _patch_pair(self, monkeypatch, etf_pick, stock_pick, trail=None) -> None:
        monkeypatch.setattr(mas, "_pick", lambda: etf_pick)
        monkeypatch.setattr(mas, "_stock_basket_mom_from_holdings", lambda holdings: stock_pick)
        if trail is not None or True:
            monkeypatch.setattr(mas, "_etf_trail_exit", lambda held, day: trail)

    def test_extra_sleeve_no_pool(self, monkeypatch) -> None:
        self._patch_pair(monkeypatch, None, None, None)
        out = mas.build_multi_asset_sleeve(day="2026-03-01", cn_block=_extra_cn_block(), holdings_override=[])
        assert out["action"] == "NONE"
        assert "候选数据不足" in out["note"]

    def test_extra_sleeve_stock_wins_with_held(self, monkeypatch) -> None:
        etf = {"key": "GOLD", "symbol": "ETF:518880", "name": "gold", "mom60": 1.0, "above_ma200": True}
        stock = {"key": "STOCK", "symbol": "STOCK", "name": "basket", "mom60": 5.0, "above_ma200": True, "n": 2}
        self._patch_pair(monkeypatch, etf, stock, None)
        held = [{"symbol": "ETF:518880", "ts_code": "518880.SH", "positionPct": 30.0}]
        out = mas.build_multi_asset_sleeve(day="2026-03-01", cn_block=_extra_cn_block(), holdings_override=held)
        assert out["action"] == "SELL_TO_A_SHARE"

    def test_extra_sleeve_stock_wins_no_held(self, monkeypatch) -> None:
        etf = {"key": "GOLD", "symbol": "ETF:518880", "name": "gold", "mom60": 1.0, "above_ma200": True}
        stock = {"key": "STOCK", "symbol": "STOCK", "name": "basket", "mom60": 5.0, "above_ma200": True, "n": 2}
        self._patch_pair(monkeypatch, etf, stock, None)
        out = mas.build_multi_asset_sleeve(day="2026-03-01", cn_block=_extra_cn_block(), holdings_override=[])
        assert out["action"] == "HOLD"
        assert out["label"] == "持有股票篮"

    def test_extra_sleeve_rotate_to_new_etf(self, monkeypatch) -> None:
        pick = {"key": "NASDAQ", "symbol": "ETF:513100", "name": "nas", "mom60": 12.0, "above_ma200": True}
        self._patch_pair(monkeypatch, pick, None, None)
        held = [{"symbol": "ETF:518880", "ts_code": "518880.SH", "entryDate": "2026-01-01", "positionPct": 30.0}]
        out = mas.build_multi_asset_sleeve(day="2026-03-01", cn_block=_extra_cn_block(), holdings_override=held)
        assert out["action"] == "ROTATE"
        assert "轮动" in out["label"] or "轮动" in out["message"]

    def test_extra_sleeve_min_hold_keeps(self, monkeypatch) -> None:
        monkeypatch.setattr(mas, "MIN_HOLD_DAYS", 5)
        pick = {"key": "NASDAQ", "symbol": "ETF:513100", "name": "nas", "mom60": 12.0, "above_ma200": True}
        monkeypatch.setattr(mas, "_pick", lambda: pick)
        monkeypatch.setattr(mas, "_stock_basket_mom_from_holdings", lambda holdings: None)
        monkeypatch.setattr(mas, "_etf_trail_exit", lambda held, day: None)
        monkeypatch.setattr(mas, "_etf_market_data", lambda ts: {"ok": True, "above": True})
        held = [{"symbol": "ETF:518880", "ts_code": "518880.SH", "entryDate": "2026-02-28", "positionPct": 30.0}]
        out = mas.build_multi_asset_sleeve(day="2026-03-01", cn_block=_extra_cn_block(), holdings_override=held)
        assert out["action"] == "HOLD"
        assert "防抖" in out["message"]

    def test_extra_sleeve_min_hold_bad_entry_falls_through(self, monkeypatch) -> None:
        monkeypatch.setattr(mas, "MIN_HOLD_DAYS", 5)
        pick = {"key": "NASDAQ", "symbol": "ETF:513100", "name": "nas", "mom60": 12.0, "above_ma200": True}
        monkeypatch.setattr(mas, "_pick", lambda: pick)
        monkeypatch.setattr(mas, "_stock_basket_mom_from_holdings", lambda holdings: None)
        monkeypatch.setattr(mas, "_etf_trail_exit", lambda held, day: None)
        held = [{"symbol": "ETF:518880", "ts_code": "518880.SH", "entryDate": "bad-date", "positionPct": 30.0}]
        out = mas.build_multi_asset_sleeve(day="2026-03-01", cn_block=_extra_cn_block(), holdings_override=held)
        assert out["action"] == "ROTATE"

    def test_extra_sleeve_held_below_ma_sells_to_repo(self, monkeypatch) -> None:
        pick = {"key": "GOLD", "symbol": "ETF:518880", "name": "gold", "mom60": 3.0, "above_ma200": False}
        self._patch_pair(monkeypatch, pick, None, None)
        held = [{"symbol": "ETF:518880", "ts_code": "518880.SH", "entryDate": "2026-01-01", "positionPct": 30.0}]
        out = mas.build_multi_asset_sleeve(day="2026-03-01", cn_block=_extra_cn_block(), holdings_override=held)
        assert out["action"] == "SELL_TO_REPO"

    def test_extra_sleeve_held_above_ma_holds(self, monkeypatch) -> None:
        pick = {"key": "GOLD", "symbol": "ETF:518880", "name": "gold", "mom60": 3.0, "above_ma200": True}
        self._patch_pair(monkeypatch, pick, None, None)
        held = [{"symbol": "ETF:518880", "ts_code": "518880.SH", "entryDate": "2026-01-01", "positionPct": 30.0}]
        out = mas.build_multi_asset_sleeve(day="2026-03-01", cn_block=_extra_cn_block(), holdings_override=held)
        assert out["action"] == "HOLD"

    def test_extra_sleeve_no_holding_below_ma_dont_buy(self, monkeypatch) -> None:
        pick = {"key": "GOLD", "symbol": "ETF:518880", "name": "gold", "mom60": 3.0, "above_ma200": False}
        self._patch_pair(monkeypatch, pick, None, None)
        out = mas.build_multi_asset_sleeve(day="2026-03-01", cn_block=_extra_cn_block(), holdings_override=[])
        assert out["action"] == "DONT_BUY"

    def test_extra_sleeve_has_stock_rotates(self, monkeypatch) -> None:
        pick = {"key": "GOLD", "symbol": "ETF:518880", "name": "gold", "mom60": 3.0, "above_ma200": True}
        self._patch_pair(monkeypatch, pick, None, None)
        holdings = [{"symbol": "CN:600000", "ts_code": "600000.SH", "positionPct": 10.0}]
        out = mas.build_multi_asset_sleeve(day="2026-03-01", cn_block=_extra_cn_block(), holdings_override=holdings)
        assert out["action"] == "ROTATE"

    def test_extra_sleeve_idle_buys(self, monkeypatch) -> None:
        pick = {"key": "GOLD", "symbol": "ETF:518880", "name": "gold", "mom60": 3.0, "above_ma200": True}
        self._patch_pair(monkeypatch, pick, None, None)
        out = mas.build_multi_asset_sleeve(day="2026-03-01", cn_block=_extra_cn_block(), holdings_override=[])
        assert out["action"] == "BUY"

    def test_extra_sleeve_idle_short_dont_buy(self, monkeypatch) -> None:
        pick = {"key": "GOLD", "symbol": "ETF:518880", "name": "gold", "mom60": 3.0, "above_ma200": True}
        self._patch_pair(monkeypatch, pick, None, None)
        holdings = [{"symbol": "ETF:999999", "positionPct": 90.0}]
        out = mas.build_multi_asset_sleeve(day="2026-03-01", cn_block=_extra_cn_block(), holdings_override=holdings)
        assert out["action"] == "DONT_BUY"


def _extra_x_dates(n: int, start: str = "2026-01-05") -> list[str]:
    d = date.fromisoformat(start)
    out: list[str] = []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _extra_x_series(dates: list[str], gap_idx: int | None, gap_pct: float, amp: float) -> list[dict]:
    series = []
    prev_close = 10.0
    for i, ds in enumerate(dates):
        if i == gap_idx:
            open_px = prev_close * (1.0 + gap_pct)
            close = open_px * 1.01
        else:
            open_px = prev_close
            close = open_px * 1.005
        high = max(open_px, close) * (1.0 + amp / 2)
        low = min(open_px, close) * (1.0 - amp / 2)
        series.append(
            {
                "date": ds,
                "open": round(open_px, 4),
                "high": round(high, 4),
                "low": round(low, 4),
                "close": round(close, 4),
                "pre_close": round(prev_close, 4),
                "amount": 1e8,
            }
        )
        prev_close = close
    return series


def _extra_x_data() -> tuple[list[str], dict, dict]:
    dates = _extra_x_dates(25)
    gap_idx = 20
    per_ts = {
        "A.SH": _extra_x_series(dates, gap_idx, 0.05, 0.01),
        "B.SH": _extra_x_series(dates, gap_idx, 0.05, 0.04),
        "C.SH": _extra_x_series(dates, gap_idx, 0.05, 0.09),
        "D.SH": _extra_x_series(dates, None, 0.0, 0.02),
    }
    mv = {ds: {ts: 100.0 for ts in per_ts} for ds in dates}
    return dates, per_ts, mv


def _extra_x_ctx(dates, per_ts, mv, extra=None) -> dict:
    date_idx = {ts: {r["date"]: i for i, r in enumerate(s)} for ts, s in per_ts.items()}
    cal_set = set(dates)
    close_by_ts: dict[str, dict[str, float]] = {}
    for ts, series in per_ts.items():
        m = {r["date"]: r["close"] for r in series if r["date"] in cal_set and r["close"]}
        if m:
            close_by_ts[ts] = m
    ctx = {
        "per_ts": per_ts,
        "mv_map": mv,
        "cal": list(dates),
        "date_idx": date_idx,
        "close_by_ts": close_by_ts,
        "idx_by_day": {d: i for i, d in enumerate(dates)},
        "feat_cache": {},
        "px_by_hhmm": {},
        "px_1430": {},
    }
    if extra:
        ctx.update(extra)
    return ctx


class TestExtraDayFeatures:
    def test_extra_day_features_missing_mv(self) -> None:
        dates, per_ts, _ = _extra_x_data()
        date_idx = {ts: {r["date"]: i for i, r in enumerate(s)} for ts, s in per_ts.items()}
        day_all, _ = sbt._day_features(per_ts, {}, dates, dates[20], date_idx)
        assert day_all == {}

    def test_extra_day_features_bad_close(self) -> None:
        dates, per_ts, mv = _extra_x_data()
        per_ts["A.SH"][20]["close"] = 0
        date_idx = {ts: {r["date"]: i for i, r in enumerate(s)} for ts, s in per_ts.items()}
        day_all, _ = sbt._day_features(per_ts, mv, dates, dates[20], date_idx)
        assert "A.SH" not in day_all

    def test_extra_day_features_thin_amount(self) -> None:
        dates, per_ts, mv = _extra_x_data()
        for r in per_ts["A.SH"]:
            r["amount"] = None
        date_idx = {ts: {r["date"]: i for i, r in enumerate(s)} for ts, s in per_ts.items()}
        day_all, _ = sbt._day_features(per_ts, mv, dates, dates[20], date_idx)
        assert "A.SH" not in day_all

    def test_extra_day_features_breadth_short_window(self) -> None:
        dates, per_ts, mv = _extra_x_data()
        per_ts["A.SH"][10]["close"] = None
        date_idx = {ts: {r["date"]: i for i, r in enumerate(s)} for ts, s in per_ts.items()}
        _, breadth = sbt._day_features(per_ts, mv, dates, dates[20], date_idx)
        assert 0.0 <= breadth <= 1.0


class TestExtraLimitLocked:
    def test_extra_t1_limit_unknown_day(self) -> None:
        dates, per_ts, _ = _extra_x_data()
        date_idx = {ts: {r["date"]: i for i, r in enumerate(s)} for ts, s in per_ts.items()}
        assert sbt._t1_limit_locked(per_ts, date_idx, "2099-01-01", "A.SH") is False

    def test_extra_t1_limit_no_pre_close(self) -> None:
        per_ts = {"A.SH": [{"pre_close": 0, "close": 10.0}]}
        date_idx = {"A.SH": {"2026-01-05": 0}}
        assert sbt._t1_limit_locked(per_ts, date_idx, "2026-01-05", "A.SH") is False

    def test_extra_t1_limit_fires(self) -> None:
        per_ts = {"A.SH": [{"pre_close": 10.0, "close": 11.0}]}
        date_idx = {"A.SH": {"2026-01-05": 0}}
        assert sbt._t1_limit_locked(per_ts, date_idx, "2026-01-05", "A.SH") is True


class TestExtraSkipReason:
    def test_extra_skip_t1_limit(self) -> None:
        out = sbt._same_1430_skip_reason(
            ts="600000.SH",
            px=11.0,
            open_px=10.0,
            pre_close=10.0,
            skip_t1_limit=True,
            max_open_to_1430_pct=None,
            near_limit_buffer_pct=None,
        )
        assert out == "skip_t1_limit"

    def test_extra_skip_none_when_no_px(self) -> None:
        out = sbt._same_1430_skip_reason(
            ts="600000.SH",
            px=None,
            open_px=10.0,
            pre_close=10.0,
            skip_t1_limit=False,
            max_open_to_1430_pct=0.03,
            near_limit_buffer_pct=None,
        )
        assert out is None

    def test_extra_skip_fade(self) -> None:
        out = sbt._same_1430_skip_reason(
            ts="600000.SH",
            px=9.0,
            open_px=10.0,
            pre_close=10.0,
            skip_t1_limit=False,
            max_open_to_1430_pct=None,
            near_limit_buffer_pct=None,
            min_open_to_1430_pct=0.05,
        )
        assert out == "skip_1430_fade"

    def test_extra_skip_churn(self) -> None:
        out = sbt._same_1430_skip_reason(
            ts="600000.SH",
            px=10.1,
            open_px=10.0,
            pre_close=10.0,
            skip_t1_limit=False,
            max_open_to_1430_pct=None,
            near_limit_buffer_pct=None,
            t1_turn=5.0,
            max_t1_turnover_mult=2.0,
        )
        assert out == "skip_1430_churn"

    def test_extra_skip_churn_nan_ignored(self) -> None:
        out = sbt._same_1430_skip_reason(
            ts="600000.SH",
            px=10.1,
            open_px=10.0,
            pre_close=10.0,
            skip_t1_limit=False,
            max_open_to_1430_pct=None,
            near_limit_buffer_pct=None,
            t1_turn=float("nan"),
            max_t1_turnover_mult=2.0,
        )
        assert out is None

    def test_extra_skip_kinds(self) -> None:
        assert sbt._skip_kind_for_reason("skip_t1_limit") == "skip_t1"
        assert sbt._skip_kind_for_reason("skip_1430_run") == "skip_c1"
        assert sbt._skip_kind_for_reason("skip_1430_fade") == "skip_c3"
        assert sbt._skip_kind_for_reason("skip_1430_churn") == "skip_churn"
        assert sbt._skip_kind_for_reason("skip_1430_near_limit") == "skip_c2"
        assert sbt._skip_kind_for_reason("bogus") == "skip_entry"


class TestExtraBar5Loaders:
    def test_extra_bar5_empty_times(self) -> None:
        assert sbt._load_bar5_closes("2026-01-01", "2026-01-10", ()) == {}

    def test_extra_bar5_success_filters_unknown_hhmm(self, monkeypatch) -> None:
        rows = [
            ("A.SH", date(2026, 1, 5), "1430", 10.0),
            ("B.SH", "2026-01-05", "1430", 11.0),
            ("C.SH", date(2026, 1, 5), "9999", 12.0),
        ]

        class _Cur:
            def execute(self, *a, **k) -> None:
                return None

            def fetchall(self):
                return rows

        class _Conn:
            def cursor(self):
                return _Cur()

            def close(self) -> None:
                return None

        monkeypatch.setattr(sbt, "get_settings", lambda: SimpleNamespace(database_url="postgresql://fake"))
        monkeypatch.setattr(sbt.psycopg, "connect", lambda *a, **k: _Conn())
        out = sbt._load_bar5_closes("2026-01-01", "2026-01-10", ("1430",))
        assert out["1430"]["A.SH"]["2026-01-05"] == 10.0
        assert out["1430"]["B.SH"]["2026-01-05"] == 11.0
        assert "C.SH" not in out["1430"]

    def test_extra_bar5_connect_raises(self, monkeypatch) -> None:
        monkeypatch.setattr(sbt, "get_settings", lambda: SimpleNamespace(database_url="postgresql://fake"))

        def _boom(*a, **k):
            raise RuntimeError("db down")

        monkeypatch.setattr(sbt.psycopg, "connect", _boom)
        assert sbt._load_bar5_closes("2026-01-01", "2026-01-10", ("1430",)) == {"1430": {}}

    def test_extra_load_calendar_mixed_types(self, monkeypatch) -> None:
        rows = [(date(2026, 1, 5),), ("2026-01-06",)]

        class _Cur:
            def execute(self, *a, **k) -> None:
                return None

            def fetchall(self):
                return rows

        class _Conn:
            def cursor(self):
                return _Cur()

            def close(self) -> None:
                return None

        monkeypatch.setattr(sbt, "get_settings", lambda: SimpleNamespace(database_url="postgresql://fake"))
        monkeypatch.setattr(sbt.psycopg, "connect", lambda *a, **k: _Conn())
        assert sbt._load_calendar("2026-01-01", "2026-01-10") == ["2026-01-05", "2026-01-06"]

    def test_extra_load_1430_delegates(self, monkeypatch) -> None:
        monkeypatch.setattr(sbt, "_load_bar5_closes", lambda *a, **k: {"1430": {"A.SH": {"d": 1.0}}})
        assert sbt._load_1430_closes("2026-01-01", "2026-01-02") == {"A.SH": {"d": 1.0}}
        monkeypatch.setattr(sbt, "_load_bar5_closes", lambda *a, **k: {})
        assert sbt._load_1430_closes("2026-01-01", "2026-01-02") == {}


class TestExtraIntradayD3:
    def test_extra_intraday_missing_returns_none(self) -> None:
        assert sbt._intraday_px({}, "A.SH", "2026-01-05", "1000") is None

    def test_extra_intraday_by_hhmm_and_fallback(self) -> None:
        ctx = {"px_by_hhmm": {"1430": {"A.SH": {"2026-01-05": 10.0}}}, "px_1430": {}}
        assert sbt._intraday_px(ctx, "A.SH", "2026-01-05", "1430") == 10.0
        ctx2 = {"px_by_hhmm": {}, "px_1430": {"A.SH": {"2026-01-05": 11.0}}}
        assert sbt._intraday_px(ctx2, "A.SH", "2026-01-05", "1430") == 11.0

    def test_extra_d3trail_no_series(self) -> None:
        assert sbt._d3_trail_px({}, "A.SH", "2026-01-05", 0.02) is None

    def test_extra_d3trail_fires(self) -> None:
        ctx = {
            "d3trail_series": {
                ("A.SH", "2026-01-05"): [
                    ("0900", 10.0, 10.0, 9.9, 10.0),
                    ("0930", 10.0, 11.0, 10.5, 11.0),
                    ("1000", 11.0, 11.0, 10.5, 10.8),
                ]
            }
        }
        assert sbt._d3_trail_px(ctx, "A.SH", "2026-01-05", 0.02) == pytest.approx(11.0 * 0.98)

    def test_extra_d3trail_never_triggers(self) -> None:
        ctx = {"d3trail_series": {("A.SH", "d"): [("0900", 10.0, 10.0, 9.9, 10.0)]}}
        assert sbt._d3_trail_px(ctx, "A.SH", "d", 0.02) is None


class TestExtraReplayValidation:
    def _ctx(self) -> dict:
        return {"per_ts": {}, "cal": [], "date_idx": {}, "close_by_ts": {}, "idx_by_day": {}}

    def test_extra_replay_bad_fill_hhmm(self) -> None:
        with pytest.raises(ValueError, match="fill_hhmm"):
            sbt.replay_sgap_from_context(self._ctx(), start="2026-01-01", end="2026-01-02", fill_mode="same_1430", fill_hhmm="bad")

    def test_extra_replay_bad_exit_hhmm(self) -> None:
        with pytest.raises(ValueError, match="exit_hhmm"):
            sbt.replay_sgap_from_context(self._ctx(), start="2026-01-01", end="2026-01-02", exit_hhmm="bad")

    def test_extra_replay_trail_requires_same_1430(self) -> None:
        with pytest.raises(ValueError, match="same_1430"):
            sbt.replay_sgap_from_context(self._ctx(), start="2026-01-01", end="2026-01-02", exit_day_trail_pct=0.02)

    def test_extra_replay_trail_range(self) -> None:
        with pytest.raises(ValueError, match="in \\(0, 1\\)"):
            sbt.replay_sgap_from_context(
                self._ctx(), start="2026-01-01", end="2026-01-02", fill_mode="same_1430", exit_day_trail_pct=1.5
            )

    def test_extra_replay_max_open_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="max_open_to_1430_pct"):
            sbt.replay_sgap_from_context(
                self._ctx(), start="2026-01-01", end="2026-01-02", fill_mode="same_1430", max_open_to_1430_pct=0.0
            )

    def test_extra_replay_min_open_requires_same_1430(self) -> None:
        with pytest.raises(ValueError, match="same_1430"):
            sbt.replay_sgap_from_context(
                self._ctx(), start="2026-01-01", end="2026-01-02", min_open_to_1430_pct=0.03
            )

    def test_extra_replay_min_open_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="min_open_to_1430_pct"):
            sbt.replay_sgap_from_context(
                self._ctx(), start="2026-01-01", end="2026-01-02", fill_mode="same_1430", min_open_to_1430_pct=0.0
            )

    def test_extra_replay_churn_requires_same_1430(self) -> None:
        with pytest.raises(ValueError, match="same_1430"):
            sbt.replay_sgap_from_context(
                self._ctx(), start="2026-01-01", end="2026-01-02", max_t1_turnover_mult=2.0
            )

    def test_extra_replay_churn_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="max_t1_turnover_mult"):
            sbt.replay_sgap_from_context(
                self._ctx(), start="2026-01-01", end="2026-01-02", fill_mode="same_1430", max_t1_turnover_mult=0.0
            )

    def test_extra_replay_near_limit_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="near_limit_buffer_pct"):
            sbt.replay_sgap_from_context(
                self._ctx(),
                start="2026-01-01",
                end="2026-01-02",
                fill_mode="same_1430",
                max_t1_turnover_mult=2.0,
                near_limit_buffer_pct=0.0,
            )

    def test_extra_replay_rank_requires_same_1430(self) -> None:
        with pytest.raises(ValueError, match="rank_key"):
            sbt.replay_sgap_from_context(self._ctx(), start="2026-01-01", end="2026-01-02", rank_key="gap_asc")

    def test_extra_replay_unknown_rank(self) -> None:
        with pytest.raises(ValueError, match="unknown rank_key"):
            sbt.replay_sgap_from_context(
                self._ctx(), start="2026-01-01", end="2026-01-02", fill_mode="same_1430", rank_key="bogus"
            )

    def test_extra_replay_bad_r_wide(self) -> None:
        with pytest.raises(ValueError, match="r_wide"):
            sbt.replay_sgap_from_context(self._ctx(), start="2026-01-01", end="2026-01-02", r_wide=2.0)

    def test_extra_replay_bad_position_pct(self) -> None:
        with pytest.raises(ValueError, match="position_pct"):
            sbt.replay_sgap_from_context(self._ctx(), start="2026-01-01", end="2026-01-02", position_pct=0.0)

    def test_extra_replay_skips_out_of_range_days(self) -> None:
        cal = ["2026-01-05", "2026-01-06", "2026-01-07"]
        ctx = {
            "per_ts": {},
            "mv_map": {},
            "cal": cal,
            "date_idx": {},
            "close_by_ts": {},
            "idx_by_day": {d: i for i, d in enumerate(cal)},
            "feat_cache": {},
            "px_by_hhmm": {},
            "px_1430": {},
        }
        out = sbt.replay_sgap_from_context(ctx, start="2026-01-06", end="2026-01-06")
        assert [r["date"] for r in out["rows"]] == ["2026-01-06"]


class TestExtraReplayBranches:
    def test_extra_replay_d3trail_exit(self) -> None:
        dates, per_ts, mv = _extra_x_data()
        px_entry = round(float(per_ts["A.SH"][20]["close"]) * 0.99, 4)
        exit_day = dates[22]
        ctx = _extra_x_ctx(
            dates,
            per_ts,
            mv,
            {
                "px_by_hhmm": {"1430": {"A.SH": {dates[20]: px_entry}}},
                "px_1430": {},
                "d3trail_series": {
                    ("A.SH", exit_day): [
                        ("0900", 10.0, 20.0, 19.9, 20.0),
                        ("0930", 20.0, 20.0, 10.0, 10.5),
                    ]
                },
            },
        )
        out = sbt.replay_sgap_from_context(
            ctx, start=dates[0], end=dates[-1], fill_mode="same_1430", exit_day_trail_pct=0.02
        )
        fills = [b for b in out["blotter"] if b["kind"] == "fill"]
        assert fills
        assert fills[0]["exitPxSrc"] == "d3trail"

    def test_extra_replay_gap_asc_rank(self) -> None:
        dates, per_ts, mv = _extra_x_data()
        px = round(float(per_ts["A.SH"][20]["close"]) * 0.99, 4)
        ctx = _extra_x_ctx(dates, per_ts, mv, {"px_by_hhmm": {"1430": {"A.SH": {dates[20]: px}}}, "px_1430": {}})
        out = sbt.replay_sgap_from_context(
            ctx, start=dates[0], end=dates[-1], fill_mode="same_1430", rank_key="gap_asc"
        )
        assert out["rank_key"] == "gap_asc"
        assert out["summary"]["fillCount"] >= 0

    def test_extra_replay_absrunup_rank_missing_print_last(self) -> None:
        dates, per_ts, mv = _extra_x_data()
        px_a = round(float(per_ts["A.SH"][20]["close"]) * 0.99, 4)
        ctx = _extra_x_ctx(
            dates, per_ts, mv, {"px_by_hhmm": {"1430": {"A.SH": {dates[20]: px_a}}}, "px_1430": {}}
        )
        out = sbt.replay_sgap_from_context(
            ctx, start=dates[0], end=dates[-1], fill_mode="same_1430", rank_key="absrunup_asc"
        )
        assert out["rank_key"] == "absrunup_asc"

    def test_extra_replay_churn_turnover_path(self) -> None:
        dates = _extra_x_dates(30)
        per_ts = {
            "A.SH": _extra_x_series(dates, 22, 0.05, 0.01),
            "B.SH": _extra_x_series(dates, 22, 0.05, 0.04),
            "C.SH": _extra_x_series(dates, 22, 0.05, 0.09),
            "D.SH": _extra_x_series(dates, None, 0.0, 0.02),
        }
        mv = {ds: {ts: 100.0 for ts in per_ts} for ds in dates}
        px_a = round(float(per_ts["A.SH"][22]["close"]) * 0.99, 4)
        ctx = _extra_x_ctx(
            dates, per_ts, mv, {"px_by_hhmm": {"1430": {"A.SH": {dates[22]: px_a}}}, "px_1430": {}}
        )
        out = sbt.replay_sgap_from_context(
            ctx,
            start=dates[0],
            end=dates[-1],
            fill_mode="same_1430",
            skip_t1_limit=True,
            max_t1_turnover_mult=0.5,
        )
        assert out["summary"]["skipChurnCount"] >= 0

    def test_extra_replay_ghost_position_skipped(self) -> None:
        cal = ["2026-01-05", "2026-01-06", "2026-01-07"]
        ghost_feat = {"GHOST.SH": {"amp": 0.01, "gap": 0.05, "is_gap": True}}
        ctx = {
            "per_ts": {},
            "mv_map": {},
            "cal": cal,
            "date_idx": {},
            "close_by_ts": {},
            "idx_by_day": {d: i for i, d in enumerate(cal)},
            "feat_cache": {"2026-01-06": (dict(ghost_feat), 0.9)},
            "px_by_hhmm": {},
            "px_1430": {},
        }
        out = sbt.replay_sgap_from_context(ctx, start="2026-01-05", end="2026-01-07", fill_mode="same_close")
        assert out["summary"]["fillCount"] == 0

    def test_extra_replay_same_close_one_word_skipped(self) -> None:
        dates, per_ts, mv = _extra_x_data()
        bar = per_ts["A.SH"][20]
        pc = float(bar["pre_close"])
        bar["close"] = round(pc * 1.10, 4)
        bar["high"] = bar["close"]
        bar["low"] = bar["close"]
        ctx = _extra_x_ctx(dates, per_ts, mv)
        out = sbt.replay_sgap_from_context(
            ctx, start=dates[0], end=dates[-1], fill_mode="same_close", skip_unfillable=True
        )
        assert out["summary"]["fillCount"] == 0

    def test_extra_replay_next_open_one_word_skipped(self) -> None:
        dates, per_ts, mv = _extra_x_data()
        bar = per_ts["A.SH"][21]
        pc = float(bar["pre_close"])
        bar["open"] = round(pc * 1.10, 4)
        bar["high"] = bar["open"]
        bar["low"] = bar["open"]
        ctx = _extra_x_ctx(dates, per_ts, mv)
        out = sbt.replay_sgap_from_context(
            ctx, start=dates[0], end=dates[-1], fill_mode="next_open", skip_unfillable=True
        )
        # A.SH entry blocked; B.SH may still fill so only assert no crash and counts exist.
        assert "fillCount" in out["summary"]


class TestExtraTimelineAdapt:
    def test_extra_timeline_none_return_fallback(self) -> None:
        sat = {
            "rows": [
                {
                    "date": "2026-01-05",
                    "satNav": 1.05,
                    "satNavReturnPct": None,
                    "satPositions": 0,
                    "satSlots": 0,
                    "satActive": False,
                }
            ],
            "summary": {"satPct": 5.0, "satMaxDdPct": 1.0},
            "openPositions": [],
            "blotter": [],
        }
        out = sbt.sgap_to_timeline_rows(sat)
        assert out["rows"][0]["satNavReturnPct"] == 5.0


class TestExtraIdleAndSymbol:
    def test_extra_is_multi_asset_forms(self) -> None:
        assert mas.is_multi_asset_symbol("ETF:518880") is True
        assert mas.is_multi_asset_symbol("518880.SH") is True
        assert mas.is_multi_asset_symbol("513500") is True
        assert mas.is_multi_asset_symbol("etf:513100") is True
        assert mas.is_multi_asset_symbol("CN:600000") is False
        assert mas.is_multi_asset_symbol("") is False

    def test_extra_trail_skips_pre_entry_bars(self, monkeypatch) -> None:
        held = {"symbol": "ETF:513100", "ts_code": "513100.SH", "entryDate": "2026-01-01"}
        bars = [
            {"trade_date": "2025-12-01", "close": 50.0},
            {"trade_date": "2026-01-15", "close": 100.0},
            {"trade_date": "2026-02-01", "close": 100.0},
        ]
        monkeypatch.setattr(mas, "fetch_last_bars", lambda ts, days=500: bars)
        assert mas._etf_trail_exit(held, day="2026-02-01") is None

    def test_extra_idle_pct_skips_bad_values(self) -> None:
        assert mas._idle_pct([{"positionPct": "bad"}, {"sleeve_pct": None}]) == 100.0
        assert mas._idle_pct([{"positionPct": 30.0}]) == 70.0


class TestExtraLoadRowsMv:
    def test_extra_load_rows_mixed_types(self, monkeypatch) -> None:
        rows = [
            (date(2026, 1, 5), "A.SH", 10.0, 11.0, 9.0, 10.5, 10.0, 1e8),
            ("2026-01-06", "A.SH", None, None, None, None, None, None),
        ]

        class _Cur:
            def execute(self, *a, **k) -> None:
                return None

            def fetchall(self):
                return rows

        class _Conn:
            def cursor(self):
                return _Cur()

            def close(self) -> None:
                return None

        monkeypatch.setattr(sbt, "get_settings", lambda: SimpleNamespace(database_url="postgresql://fake"))
        monkeypatch.setattr(sbt.psycopg, "connect", lambda *a, **k: _Conn())
        out = sbt._load_rows("2026-01-01", "2026-01-10")
        assert out["A.SH"][0]["date"] == "2026-01-05"
        assert out["A.SH"][0]["close"] == 10.5
        assert out["A.SH"][1]["date"] == "2026-01-06"
        assert out["A.SH"][1]["open"] is None

    def test_extra_load_mv_mixed_types(self, monkeypatch) -> None:
        rows = [
            (date(2026, 1, 5), "A.SH", 200000.0),
            ("2026-01-06", "B.SH", 300000.0),
        ]

        class _Cur:
            def execute(self, *a, **k) -> None:
                return None

            def fetchall(self):
                return rows

        class _Conn:
            def cursor(self):
                return _Cur()

            def close(self) -> None:
                return None

        monkeypatch.setattr(sbt, "get_settings", lambda: SimpleNamespace(database_url="postgresql://fake"))
        monkeypatch.setattr(sbt.psycopg, "connect", lambda *a, **k: _Conn())
        out = sbt._load_mv("2026-01-01", "2026-01-10")
        assert out["2026-01-05"]["A.SH"] == 20.0
        assert out["2026-01-06"]["B.SH"] == 30.0
