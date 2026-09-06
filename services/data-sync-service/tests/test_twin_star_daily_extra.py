"""twin_star_daily uncovered branches (H1): guards, book cache, signal edges, reminder lines."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from data_sync_service.service import twin_star_daily as tsd


def _mk_dates(n: int, start: str = "2026-01-05") -> list[str]:
    d = date.fromisoformat(start)
    out: list[str] = []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _mk_series(dates: list[str], gap_idx: int | None, gap_pct: float, amp: float) -> list[dict]:
    series = []
    prev = 10.0
    for i, ds in enumerate(dates):
        if gap_idx is not None and i == gap_idx:
            open_px = prev * (1.0 + gap_pct)
            close = open_px * (1.0 + 0.01)
        else:
            open_px = prev
            close = open_px * (1.0 + 0.005)
        series.append(
            {
                "date": ds,
                "open": round(open_px, 4),
                "high": round(max(open_px, close) * 1.005, 4),
                "low": round(min(open_px, close) * 0.995, 4),
                "close": round(close, 4),
                "pre_close": round(prev, 4),
                "amount": 1e8,
            }
        )
        prev = close
    return series


@pytest.fixture()
def _clean_book_cache():
    saved = dict(tsd._book_ts_cache)
    tsd._book_ts_cache.clear()
    try:
        yield
    finally:
        tsd._book_ts_cache.clear()
        tsd._book_ts_cache.update(saved)


# -- small guards -----------------------------------------------------------------


def test_count_sessions_exception_returns_zero(monkeypatch) -> None:
    import data_sync_service.service.trade_calendar_utils as tcu

    monkeypatch.setattr(tcu, "count_open_sessions", lambda a, b: (_ for _ in ()).throw(RuntimeError("x")))
    assert tsd.count_sessions_inclusive("2026-01-19", "2026-01-21") == 0


def test_sat_body_progress_missing_and_nth_raises(monkeypatch) -> None:
    import data_sync_service.service.trade_calendar_utils as tcu

    out = tsd.sat_body_progress(None, "2026-01-21")
    assert out["missingEntry"] is True and out["heldDays"] is None
    monkeypatch.setattr(tcu, "nth_open_session", lambda d, n: (_ for _ in ()).throw(RuntimeError("x")))
    out = tsd.sat_body_progress("2026-01-19", "2026-01-21")
    assert out["exitDue"] is None and out["missingEntry"] is False


def test_ts_from_cn_symbol_rejects() -> None:
    assert tsd.ts_from_cn_symbol("CN:123") is None
    assert tsd.ts_from_cn_symbol("CN:abcdef") is None
    assert tsd.ts_from_cn_symbol("HK:00700") is None
    assert tsd.ts_from_cn_symbol("") is None
    assert tsd.ts_from_cn_symbol("CN:600000") == "600000.SH"
    assert tsd.ts_from_cn_symbol("CN:000001") == "000001.SZ"
    assert tsd.cn_symbol_from_ts("600000.SH") == "CN:600000"


def test_now_cn_shanghai() -> None:
    assert str(tsd.now_cn().tzinfo) == "Asia/Shanghai"


def test_action_intraday_load_failure_survives(monkeypatch) -> None:
    tsi = _stub_action_base(monkeypatch, health={"multiAssetSleeve": {}, "holdings": []})
    day = date(2026, 1, 21)
    monkeypatch.setattr(tsi, "session_date", lambda now=None: day)
    monkeypatch.setattr(tsi, "load_intraday_sat", lambda d: (_ for _ in ()).throw(RuntimeError("cache down")))
    out = tsd.build_twin_star_daily_action(day)
    assert out["sat"]["asOf"] == "2026-01-20"  # T-1 signal kept, snapshot flags default False


# -- book cache ---------------------------------------------------------------------


def test_sat_book_ts_codes_cache_and_failure(monkeypatch, _clean_book_cache) -> None:
    calls = {"n": 0}

    def _book(day):
        calls["n"] += 1
        return {"holdings": [{"ts": "A.SH"}, {"ts": ""}, {}]}

    monkeypatch.setattr(tsd, "_sat_book", _book)
    day = date(2026, 1, 21)
    assert tsd.sat_book_ts_codes(day) == {"A.SH"}
    assert tsd.sat_book_ts_codes(day) == {"A.SH"}  # cached
    assert calls["n"] == 1

    def _boom(day):
        raise RuntimeError("replay down")

    monkeypatch.setattr(tsd, "_sat_book", _boom)
    assert tsd.sat_book_ts_codes(date(2026, 1, 22)) == set()


def test_sat_book_cache_evicts_at_16(monkeypatch, _clean_book_cache) -> None:
    monkeypatch.setattr(tsd, "_sat_book", lambda day: {"holdings": []})
    for i in range(16):
        tsd.sat_book_ts_codes(date(2026, 1, 5) + timedelta(days=i))
    assert len(tsd._book_ts_cache) == 16
    tsd.sat_book_ts_codes(date(2026, 3, 1))
    assert len(tsd._book_ts_cache) == 1


# -- live_sat_ts_codes -----------------------------------------------------------------


def test_live_sat_ts_codes_all_sources_fail(monkeypatch) -> None:
    import data_sync_service.service.twin_star_intraday as tsi

    monkeypatch.setattr(tsi, "load_intraday_sat", lambda day: (_ for _ in ()).throw(RuntimeError("x")))
    monkeypatch.setattr(
        "data_sync_service.db.paper_trading.list_paper_trades",
        lambda **k: (_ for _ in ()).throw(RuntimeError("db")),
    )
    monkeypatch.setattr(tsd, "sat_book_ts_codes", lambda day: (_ for _ in ()).throw(RuntimeError("y")))
    assert tsd.live_sat_ts_codes(date(2026, 1, 21)) == set()


def test_live_sat_ts_codes_merges_three_legs(monkeypatch, _clean_book_cache) -> None:
    import data_sync_service.service.twin_star_intraday as tsi

    monkeypatch.setattr(tsi, "load_intraday_sat", lambda day: {"candidates": [{"ts": "A.SH"}], "blocked": []})
    monkeypatch.setattr(tsi, "session_date", lambda now=None: date(2026, 1, 21))
    monkeypatch.setattr(
        "data_sync_service.db.paper_trading.list_paper_trades",
        lambda **k: [
            {"symbol": "CN:600000", "source": "twin_star"},
            {"symbol": "CN:000001", "source": "s3"},
            {"symbol": "CN:bad", "source": "twin_star"},
        ],
    )
    monkeypatch.setattr(tsd, "_sat_book", lambda day: {"holdings": [{"ts": "B.SH"}]})
    assert tsd.live_sat_ts_codes(date(2026, 1, 21)) == {"A.SH", "600000.SH", "B.SH"}


# -- holdings / names ---------------------------------------------------------------------


def test_live_sat_holdings_bad_pct_skipped() -> None:
    health = {
        "holdings": [
            {"symbol": "CN:000001", "positionPct": "nonsense"},
            {"symbol": "CN:600000", "positionPct": None},
            {"symbol": "CN:000002", "positionPct": 5.0},
        ]
    }
    assert [h["ts"] for h in tsd.live_sat_holdings(health=health, pick_key=None, sat_ts=set())] == ["000002.SZ"]


def test_fill_candidate_names_edges(monkeypatch) -> None:
    tsd.fill_candidate_names([])  # no codes → return
    tsd.fill_candidate_names([{"ts": "A.SH"}])  # real fetch_names w/o DB raises → return

    import data_sync_service.db.stock_basic as sb

    monkeypatch.setattr(sb, "fetch_names", lambda codes: (_ for _ in ()).throw(RuntimeError("db")))
    tsd.fill_candidate_names([{"ts": "A.SH"}])

    monkeypatch.setattr(sb, "fetch_names", lambda codes: {})
    rows = [{"ts": "A.SH"}]
    tsd.fill_candidate_names(rows)
    assert rows == [{"ts": "A.SH"}]

    monkeypatch.setattr(sb, "fetch_names", lambda codes: {"A.SH": "平安"})
    rows = [{"ts": "A.SH"}, {"ts": "B.SH", "name": "已有"}]
    tsd.fill_candidate_names(rows)
    assert rows[0]["name"] == "平安" and rows[1]["name"] == "已有"


# -- _sat_signal edges -----------------------------------------------------------------------


def test_sat_signal_load_failure_and_empty_trim(monkeypatch) -> None:
    monkeypatch.setattr(tsd, "_load_calendar", lambda w, e: (_ for _ in ()).throw(RuntimeError("db")))
    assert tsd._sat_signal(date(2026, 2, 1)) is None

    # Only today in calendar → trimmed to empty → None.
    monkeypatch.setattr(tsd, "_load_calendar", lambda w, e: ["2026-02-02"])
    monkeypatch.setattr(tsd, "_load_rows", lambda w, e: {})
    monkeypatch.setattr(tsd, "_load_mv", lambda w, e: {})
    assert tsd._sat_signal(date(2026, 2, 2)) is None


def test_sat_signal_zero_preclose_not_locked(monkeypatch) -> None:
    dates = _mk_dates(25)
    gap_idx = len(dates) - 2
    per_ts = {"A.SH": _mk_series(dates, gap_idx, 0.05, 0.01)}
    per_ts["A.SH"][gap_idx]["pre_close"] = 0
    mv = {ds: {"A.SH": 100.0} for ds in dates}
    monkeypatch.setattr(tsd, "_load_calendar", lambda w, e: dates)
    monkeypatch.setattr(tsd, "_load_rows", lambda w, e: per_ts)
    monkeypatch.setattr(tsd, "_load_mv", lambda w, e: mv)
    monkeypatch.setattr(tsd, "fill_candidate_names", lambda *a, **k: None)
    sat = tsd._sat_signal(date.fromisoformat(dates[-1]))
    assert sat is not None  # zero-preclose gap is not limit-locked, no crash


# -- _sat_book ----------------------------------------------------------------------------------


def test_sat_book_unavailable_and_exits_branches(monkeypatch) -> None:
    monkeypatch.setattr(
        tsd, "build_sgap_timeline", lambda **k: (_ for _ in ()).throw(RuntimeError("replay"))
    )
    out = tsd._sat_book(date(2026, 1, 21))
    assert out["error"] == "book_unavailable" and out["holdings"] == []

    due = {"ts": "A.SH", "daysLeft": 0}
    monkeypatch.setattr(tsd, "build_sgap_timeline", lambda **k: {"openPositions": [due, {"ts": "B.SH", "daysLeft": 2}]})
    out = tsd._sat_book(date(2026, 1, 21))
    assert [h["ts"] for h in out["exitsDue"]] == ["A.SH"]

    soon = {"ts": "C.SH", "daysLeft": 1}
    monkeypatch.setattr(tsd, "build_sgap_timeline", lambda **k: {"openPositions": [soon]})
    out = tsd._sat_book(date(2026, 1, 21))
    assert [h["ts"] for h in out["exitsDue"]] == ["C.SH"]


# -- build action branches ----------------------------------------------------------------------------


def _stub_action_base(monkeypatch, **kw):
    monkeypatch.setattr(tsd, "_sat_signal", lambda today: kw.get("signal", {
        "asOf": "2026-01-20", "gateOpen": True, "breadth": 0.8, "gapCount": 1,
        "candidates": [{"ts": "A.SH", "amp": 1.0, "gapPct": 5.0, "close": 10.5}],
    }))
    monkeypatch.setattr(tsd, "_sat_book", lambda today: kw.get("book", {"asOf": "x", "holdings": [], "exitsDue": [], "body": 3}))
    monkeypatch.setattr(tsd, "fill_candidate_names", lambda *a, **k: None)
    monkeypatch.setattr(tsd, "sat_book_ts_codes", lambda day: set())
    import data_sync_service.db.paper_trading as ptdb

    monkeypatch.setattr(ptdb, "list_paper_trades", lambda **k: [])
    import data_sync_service.service.twin_star_intraday as tsi

    monkeypatch.setattr(tsi, "load_intraday_sat", lambda day: None)
    if "health" in kw or "health_raises" in kw:
        import data_sync_service.service.portfolio_health as ph

        if kw.get("health_raises"):
            monkeypatch.setattr(ph, "build_portfolio_health", lambda **k: (_ for _ in ()).throw(RuntimeError("db")))
        else:
            health = kw.get("health")
            monkeypatch.setattr(ph, "build_portfolio_health", lambda **k: health)
    return tsi


def test_action_health_failure_uses_defaults(monkeypatch) -> None:
    _stub_action_base(monkeypatch, health_raises=True)
    out = tsd.build_twin_star_daily_action(date(2026, 1, 21))
    assert out["core"] == {"pick": None, "label": None, "action": None, "message": None}
    assert out["sat"]["coreTargetPct"] == 50  # gate open + candidates → 50/50


def test_action_intraday_overrides_and_snapshot_status(monkeypatch) -> None:
    tsi = _stub_action_base(monkeypatch, health={"multiAssetSleeve": {}, "holdings": []})
    day = date(2026, 1, 21)
    monkeypatch.setattr(tsi, "session_date", lambda now=None: day)
    monkeypatch.setattr(
        tsi, "load_intraday_sat", lambda d: {"candidates": [{"ts": "B.SH"}], "blocked": [], "alternates": [], "skippedC1": []}
    )
    monkeypatch.setattr(
        tsi, "intraday_snapshot_status", lambda **k: {"missing": False, "stale": True, "ageSeconds": 99, "reason": "late"}
    )
    out = tsd.build_twin_star_daily_action(day)
    assert out["sat"]["candidates"] == [{"ts": "B.SH"}]
    assert out["sat"]["snapshotStale"] is True
    assert out["sat"]["exitHhmm"] == tsd.HABIT_EXIT_HHMM


def test_action_exithhmm_none_defaults(monkeypatch) -> None:
    _stub_action_base(
        monkeypatch,
        health_raises=True,
        signal={"asOf": "2026-01-20", "gateOpen": False, "breadth": 0.1, "gapCount": 0, "candidates": [], "exitHhmm": None},
    )
    out = tsd.build_twin_star_daily_action(date(2026, 1, 21))
    assert out["sat"]["exitHhmm"] == tsd.HABIT_EXIT_HHMM
    assert out["sat"]["coreTargetPct"] == 100


# -- reminder payload lines ------------------------------------------------------------------------------


def _stub_payload(monkeypatch, signal: dict, book: dict | None = None, health: dict | None = None):
    _stub_action_base(
        monkeypatch,
        signal=signal,
        book=book if book is not None else {"asOf": "x", "holdings": [], "exitsDue": [], "body": 3},
        health={"multiAssetSleeve": {}, "holdings": []} if health is None else health,
    )


def test_payload_no_data_and_gate_closed(monkeypatch) -> None:
    _stub_payload(monkeypatch, {"asOf": None, "gateOpen": None, "candidates": []})
    detail = tsd.build_twin_star_reminder_payload(date(2026, 1, 21))["detail"]
    assert "卫星: 数据不可用" in detail

    _stub_payload(monkeypatch, {"asOf": "2026-01-20", "gateOpen": False, "breadth": 0.123, "gapCount": 0, "candidates": []})
    detail = tsd.build_twin_star_reminder_payload(date(2026, 1, 21))["detail"]
    assert "R-wide 关闸" in detail


def test_payload_blocked_swap_c1_approx_note(monkeypatch) -> None:
    _stub_payload(
        monkeypatch,
        {
            "asOf": "2026-01-20", "gateOpen": True, "breadth": 0.9, "gapCount": 3,
            "candidates": [{"ts": "A.SH", "amp": 1.0, "gapPct": 5.0}],
            "blocked": [{"ts": "B.SH"}], "alternates": [{"ts": "C.SH"}],
            "skippedC1": [{"ts": "D.SH"}], "approx": True, "snapshotAt": None,
            "note": "lag note",
        },
    )
    detail = tsd.build_twin_star_reminder_payload(date(2026, 1, 21))["detail"]
    assert "涨停跳过 B.SH 换 C.SH" in detail
    assert "C1跳过 1 只" in detail
    assert "当日行情" in detail
    assert "lag note" in detail
