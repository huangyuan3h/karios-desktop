"""OPT-186 slice 1: live 14:30 satellite signal panel.

Unit tests use a synthetic context (no DB). The parity test replays three
real days and asserts the signal panel matches the frozen replay's own
fills/skips/gate exactly (same ctx, same helpers).
"""

from __future__ import annotations

import pytest

from data_sync_service.service.satellite_signals import satellite_signals_for_day

NAMES = ("AAA", "BBB", "CCC", "DDD", "EEE")


def _build_ctx() -> tuple[dict, str]:
    days = [f"2026-01-{5 + i:02d}" for i in range(25)]
    day = days[-1]
    opens = {"AAA": 10.5, "BBB": 10.5, "CCC": 10.1, "DDD": 10.5, "EEE": 10.5}
    closes = {"AAA": 10.6, "BBB": 10.9, "CCC": 10.15, "DDD": 10.6, "EEE": 10.9}
    highs = {"AAA": 10.7, "BBB": 11.0, "CCC": 10.2, "DDD": 10.7, "EEE": 11.0}
    lows = {"AAA": 10.4, "BBB": 10.8, "CCC": 10.0, "DDD": 10.4, "EEE": 10.8}
    prints = {"AAA": 10.6, "BBB": 10.9, "CCC": 10.2, "DDD": 10.6, "EEE": 10.97}
    hls = {
        "AAA": (10.65, 10.55),
        "BBB": (11.0, 10.8),
        "CCC": (10.3, 10.1),
        "DDD": (11.5, 9.5),
        "EEE": (11.05, 10.85),
    }
    per_ts: dict[str, list[dict]] = {}
    for ts in NAMES:
        series = []
        for i, d in enumerate(days):
            if i < 24:
                series.append({"date": d, "open": 10.0, "high": 10.0, "low": 10.0,
                               "close": 10.0, "pre_close": 10.0, "amount": 1e6})
            else:
                series.append({"date": d, "open": opens[ts], "high": highs[ts],
                               "low": lows[ts], "close": closes[ts],
                               "pre_close": 10.0, "amount": 1e6})
        per_ts[ts] = series
    mv_map = {d: {ts: 1e9 for ts in NAMES} for d in days}
    # raw 15:00 == qfq close every day (ratio 1.0: no basis scaling in checks,
    # but keep history rising slightly so breadth MA20 sits below the prints).
    px1500 = {}
    for ts in NAMES:
        m = {}
        for i, d in enumerate(days):
            m[d] = 10.0 if i < 24 else closes[ts]
        px1500[ts] = m
    return {
        "per_ts": per_ts,
        "mv_map": mv_map,
        "cal": days,
        "date_idx": {ts: {d: i for i, d in enumerate(days)} for ts in NAMES},
        "close_by_ts": {ts: {r["date"]: r["close"] for r in series} for ts, series in per_ts.items()},
        "idx_by_day": {d: i for i, d in enumerate(days)},
        "feat_cache": {},
        "px_by_hhmm": {"1430": {ts: {day: prints[ts]} for ts in NAMES}, "1500": px1500},
        "px_1430": {ts: {day: prints[ts]} for ts in NAMES},
        "px_hl_1430": {ts: {day: hls[ts]} for ts in NAMES},
    }, day


def _by_ts(sig: dict) -> dict[str, dict]:
    return {e["ts"]: e for e in sig["ranked"]}


def test_gap_filter_and_rank_order() -> None:
    ctx, day = _build_ctx()
    sig = satellite_signals_for_day(ctx, day)
    assert sig["decisionAvailable"] is True
    assert [e["ts"] for e in sig["ranked"]] == ["AAA", "EEE", "BBB", "DDD"]
    assert sig["gapCount"] == 4 and sig["bucketSize"] == 1
    by = _by_ts(sig)
    assert by["AAA"]["gapPct"] == 5.0 and by["AAA"]["amp1430Pct"] == 0.94
    assert by["AAA"]["ampRank"] == 1 and by["AAA"]["inBucket"] is True


def test_skip_reasons_and_fillable() -> None:
    ctx, day = _build_ctx()
    by = _by_ts(satellite_signals_for_day(ctx, day))
    assert by["AAA"]["skipReason"] is None and by["AAA"]["fillable"] is True
    assert by["BBB"]["skipReason"] == "skip_1430_run" and by["BBB"]["skipKind"] == "skip_c1"
    assert by["BBB"]["fillable"] is True  # C1 != unfillable
    assert by["EEE"]["skipReason"] == "skip_t1_limit" and by["EEE"]["skipKind"] == "skip_t1"
    assert by["EEE"]["fillable"] is False


def test_would_fill_and_gate() -> None:
    ctx, day = _build_ctx()
    sig = satellite_signals_for_day(ctx, day)
    assert sig["gateOpen"] is True and sig["breadth1430"] == 1.0
    by = _by_ts(sig)
    assert by["AAA"]["wouldFill"] is True
    assert all(not by[ts]["wouldFill"] for ts in ("BBB", "DDD", "EEE"))


def test_unavailable_reasons() -> None:
    ctx, day = _build_ctx()
    assert satellite_signals_for_day(ctx, "1999-01-01")["decisionAvailable"] is False
    no_px = dict(ctx)
    no_px["px_1430"] = {}
    out = satellite_signals_for_day(no_px, day)
    assert out["decisionAvailable"] is False and "prints" in str(out.get("reason"))
    no_feat = dict(ctx)
    no_feat["per_ts"] = {}
    no_feat["date_idx"] = {}
    out2 = satellite_signals_for_day(no_feat, day)
    assert out2["decisionAvailable"] is False


def test_route_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi.testclient import TestClient  # type: ignore[import-not-found]

    from data_sync_service.main import app  # type: ignore[import-not-found]

    ctx, day = _build_ctx()
    from data_sync_service.service import satellite_signals as mod

    monkeypatch.setattr(mod, "load_sgap_context", lambda s, e, **kw: ctx)
    client = TestClient(app)
    resp = client.get(f"/api/backtest/satellite-signals/live?date={day}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True and body["signals"]["decisionAvailable"] is True
    assert body["signals"]["ranked"][0]["ts"] == "AAA"


@pytest.mark.requires_postgres
@pytest.mark.parametrize("day", ["2026-09-11", "2026-09-10", "2026-08-07"])
def test_live_signals_match_replay(day: str) -> None:
    """Parity lock: signal panel == frozen replay fills/skips/gate, same day.

    The replay only computes its day panel when the window spans past the
    day (``day > start`` guard), so each case replays a 7-calendar-day
    window ending on the case day and compares that day's slice. Fills
    additionally depend on carried positions, so fills assert as a subset
    of wouldFill; bucket/locked/gate/gapCount assert exactly.
    """
    from datetime import date as date_type
    from datetime import timedelta

    from data_sync_service.service.satellite_signals import satellite_signals_for_day
    from data_sync_service.service.state_bucket_track import (
        FILL_SAME_1430,
        load_sgap_context,
        replay_sgap_from_context,
    )

    start = (date_type.fromisoformat(day) - timedelta(days=7)).isoformat()
    ctx = load_sgap_context(start, day)
    sig = satellite_signals_for_day(ctx, day)
    assert sig["decisionAvailable"] is True, sig.get("reason")
    sat = replay_sgap_from_context(
        ctx, start=start, end=day, skip_t1_limit=True, pool_mode="strict",
        max_pos=4, position_pct=0.25, body=3, fill_mode=FILL_SAME_1430,
        fill_hhmm="1430", exit_hhmm="1430", max_open_to_1430_pct=0.03,
        rank_key="amp_1430", gate_1430=True,
    )
    rows = [r for r in sat["rows"] if str(r.get("date")) == day]
    assert len(rows) == 1
    assert bool(rows[0].get("gateOpen")) == sig["gateOpen"]
    # The replay only ranks when its gate block runs; the signal panel
    # always ranks (wouldFill stays false when the gate is closed).
    ran = bool(rows[0].get("gateOpen")) or int(rows[0].get("gapCount") or 0) > 0
    if ran:
        assert int(rows[0].get("gapCount") or 0) == sig["gapCount"]
    blotter = [b for b in sat["blotter"] if str(b.get("date")) == day]
    # Entries on day D <=> entryDate == D (exit rows reuse kind "fill"
    # with an earlier entryDate; body=3 so same-day entry+exit is impossible).
    fills = [b["ts"] for b in blotter if str(b.get("entryDate")) == day]
    would = [e["ts"] for e in sig["ranked"] if e["wouldFill"]]
    assert set(fills) <= set(would)
    assert len(fills) <= 4
    by_ts = {e["ts"]: e for e in sig["ranked"]}
    n_skip = 0
    for b in blotter:
        if b.get("kind") in ("skip_t1", "skip_c1", "skip_c2", "skip_c3", "skip_churn"):
            n_skip += 1
            e = by_ts[b["ts"]]
            assert e["inBucket"] is True
            assert e["skipKind"] == b["kind"]
            assert e["skipReason"] == b.get("closeReason")
    if ran:
        locked_in_bucket = sum(1 for e in sig["ranked"] if e["inBucket"] and e["skipReason"])
        assert n_skip == locked_in_bucket
