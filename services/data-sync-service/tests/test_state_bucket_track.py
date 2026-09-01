"""state_bucket_track (双子星 S-gap 卫星引擎) unit tests — synthetic data, no DB."""

from __future__ import annotations

from data_sync_service.service import state_bucket_track as sbt


def _mk_dates(n: int, start: str = "2026-01-05") -> list[str]:
    from datetime import date, timedelta

    d = date.fromisoformat(start)
    out: list[str] = []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _mk_series(dates: list[str], gap_idx: int | None, gap_pct: float, amp: float) -> list[dict]:
    """Rising series; on gap_idx day opens gap_pct above prev close with given intraday amp."""
    series = []
    prev_close = 10.0
    for i, ds in enumerate(dates):
        if i == gap_idx:
            open_px = prev_close * (1.0 + gap_pct)
            close = open_px * (1.0 + 0.01)
        else:
            open_px = prev_close
            close = open_px * (1.0 + 0.005)
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


def _mk_data() -> tuple[list[str], dict, dict, dict]:
    dates = _mk_dates(25)
    gap_idx = 20  # first idx with idx>=20 eligibility
    per_ts = {
        "A.SH": _mk_series(dates, gap_idx, 0.05, 0.01),   # lowest amp -> top bucket
        "B.SH": _mk_series(dates, gap_idx, 0.05, 0.04),
        "C.SH": _mk_series(dates, gap_idx, 0.05, 0.09),   # outside top-33%
        "D.SH": _mk_series(dates, None, 0.0, 0.02),       # no gap
    }
    mv = {ds: {ts: 100.0 for ts in per_ts} for ds in dates}
    return dates, per_ts, mv, {}


def _patch_loaders(monkeypatch, dates, per_ts, mv):
    monkeypatch.setattr(sbt, "_load_calendar", lambda w_start, end: dates)
    monkeypatch.setattr(sbt, "_load_rows", lambda w_start, end: per_ts)
    monkeypatch.setattr(sbt, "_load_mv", lambda w_start, end: mv)


class TestDayFeatures:
    def test_gap_detection_and_breadth(self) -> None:
        dates, per_ts, mv, _ = _mk_data()
        date_idx = {ts: {r["date"]: i for i, r in enumerate(s)} for ts, s in per_ts.items()}
        day = dates[20]
        day_all, breadth = sbt._day_features(per_ts, mv, dates, day, date_idx)
        assert day_all["A.SH"]["is_gap"] is True
        assert day_all["D.SH"]["is_gap"] is False
        # all stocks above their MA20 (rising series) -> breadth = 1.0
        assert breadth > 0.9

    def test_amp_ranking(self) -> None:
        dates, per_ts, mv, _ = _mk_data()
        date_idx = {ts: {r["date"]: i for i, r in enumerate(s)} for ts, s in per_ts.items()}
        day_all, _ = sbt._day_features(per_ts, mv, dates, dates[20], date_idx)
        # ranking matters for bucket (lowest amp first), not absolute value
        assert day_all["A.SH"]["amp"] < day_all["B.SH"]["amp"] < day_all["C.SH"]["amp"]


class TestBuildSgapTimeline:
    def test_entry_exit_cost_and_slots(self, monkeypatch) -> None:
        dates, per_ts, mv, _ = _mk_data()
        _patch_loaders(monkeypatch, dates, per_ts, mv)
        fills: list[tuple[str, str]] = []
        r = sbt.build_sgap_timeline(start=dates[0], end=dates[-1], debug_fills=fills)
        rows = r["rows"]
        # only A.SH fills (lowest amp, top-33% of 3 gap stocks)
        assert fills == [(dates[21], "A.SH")]
        # entered day 21 (next open after gap on day 20)
        entry_day = [i for i, row in enumerate(rows) if row["satPositions"] == 1]
        assert entry_day and rows[entry_day[0]]["date"] == dates[21]
        # body=3: held days 21,22 overnight (pos=1); exit AT day-23 close
        # (pos=0 after close, but satActive=True so opportunity blend keeps costs)
        held_days = [i for i, row in enumerate(rows) if row["satPositions"] == 1]
        assert len(held_days) == 2
        exit_row = next(row for row in rows if row["date"] == dates[23])
        assert exit_row["satPositions"] == 0
        assert exit_row["satActive"] is True
        # open book empty after body exit (and window ends with no new fill)
        assert r.get("openPositions") == [] or all(
            p["ts"] != "A.SH" for p in r.get("openPositions") or []
        )
        # no entry before idx>=20 eligibility / no entry on first window day
        assert rows[0]["satPositions"] == 0
        # final NAV: entry next_open, exit at 3rd-day close, 0.3% round trip cost
        exp_entry = per_ts["A.SH"][21]["open"]
        exp_exit = per_ts["A.SH"][23]["close"]
        exp_nav = 1.0 + ((exp_exit / exp_entry - 1) - 0.003) * 0.10
        assert r["rows"][-1]["satNav"] == round(exp_nav, 6)
        assert r["summary"]["satPct"] == round((exp_nav - 1) * 100, 2)

    def test_no_entry_without_r_wide(self, monkeypatch) -> None:
        dates, per_ts, mv, _ = _mk_data()
        # drop mv for D on gap day -> breadth < 1; keep > 0.5 but also drop B mv
        # simpler: make only 1 stock hold mv -> breadth 1.0... instead use empty gap day
        for s in per_ts["D.SH"]:
            s["open"] = s["close"] / (1.0 + 0.005)  # neutralize any gap on D
        # remove gap from A/B/C on the gap day by setting open=prev close
        for ts in ("A.SH", "B.SH", "C.SH"):
            per_ts[ts][20]["open"] = per_ts[ts][19]["close"]
        _patch_loaders(monkeypatch, dates, per_ts, mv)
        fills: list[tuple[str, str]] = []
        r = sbt.build_sgap_timeline(start=dates[0], end=dates[-1], debug_fills=fills)
        assert fills == []
        assert all(row["satPositions"] == 0 for row in r["rows"])
        assert r["summary"]["satPct"] == 0.0

    def test_position_cap_respected(self, monkeypatch) -> None:
        dates, per_ts, mv, _ = _mk_data()
        # 4 gap stocks all low amp -> with max_pos=2, only 2 fill
        for ts in ("A.SH", "B.SH", "C.SH"):
            per_ts[ts][20]["open"] = per_ts[ts][19]["close"] * 1.05
        per_ts["D.SH"][20]["open"] = per_ts["D.SH"][19]["close"] * 1.05
        _patch_loaders(monkeypatch, dates, per_ts, mv)
        fills: list[tuple[str, str]] = []
        r = sbt.build_sgap_timeline(
            start=dates[0], end=dates[-1], bucket_q=1, max_pos=2, debug_fills=fills
        )
        # 4 gap candidates (bucket_q=1 -> all), max_pos=2 -> only lowest-amp 2 fill
        assert len(fills) == 2
        assert fills[0][1] == "A.SH" and fills[1][1] == "D.SH"
        assert len([row for row in r["rows"] if row["satPositions"]]) == 2  # held 3 days, visible 2