"""sat_hold_path — day1/2/3 marks vs T-open (no DB)."""

from __future__ import annotations

from data_sync_service.service.sat_hold_path import (
    mark_to_entry_pct,
    path_for_fill,
    paths_from_blotter,
    summarize_paths,
)


def _ctx() -> dict:
    cal = ["2026-03-02", "2026-03-03", "2026-03-04", "2026-03-05"]
    return {
        "cal": cal,
        "idx_by_day": {d: i for i, d in enumerate(cal)},
        "close_by_ts": {
            "A.SH": {
                "2026-03-02": 10.0,
                "2026-03-03": 9.5,
                "2026-03-04": 10.2,
            }
        },
        "per_ts": {
            "A.SH": [
                {"date": "2026-03-02", "open": 10.0},
                {"date": "2026-03-03", "open": 9.6},
                {"date": "2026-03-04", "open": 9.8},
            ]
        },
        "date_idx": {"A.SH": {"2026-03-02": 0, "2026-03-03": 1, "2026-03-04": 2}},
    }


def test_mark_to_entry_pct() -> None:
    assert mark_to_entry_pct(11.0, 10.0) == 10.0
    assert mark_to_entry_pct(9.5, 10.0) == -5.0


def test_path_d2_red_d3_recovers() -> None:
    p = path_for_fill(_ctx(), ts="A.SH", entry_date="2026-03-02", entry_price=10.0)
    assert p is not None
    assert p["pnl1"] == 0.0
    assert p["pnl2"] == -5.0
    assert p["pnl3"] == 2.0


def test_summarize_d2_red_recovery() -> None:
    paths = [
        {"pnl1": 1.0, "pnl2": -2.0, "pnl3": 3.0},   # red d2 → green d3
        {"pnl1": -1.0, "pnl2": -6.0, "pnl3": -1.0},  # red d2, improved, still red; hit -5%
        {"pnl1": 2.0, "pnl2": 4.0, "pnl3": 5.0},     # never red
    ]
    s = summarize_paths(paths)
    assert s["n"] == 3
    assert s["d2Red"]["n"] == 2
    assert s["d2Red"]["recoveredGreen"] == 1
    assert s["d2Red"]["pctRecoveredGreen"] == 50.0
    assert s["d2Red"]["improved"] == 2
    assert s["hitProtectByD2"]["n"] == 1
    assert s["hitProtectByD2"]["d3Green"] == 0


def test_paths_from_blotter_skips_skips_and_open() -> None:
    blotter = [
        {"kind": "skip_t1", "ts": "A.SH", "closeReason": "skip_t1_limit"},
        {
            "kind": "fill",
            "ts": "A.SH",
            "entryDate": "2026-03-02",
            "closeReason": "body_exit",
            "heldDays": 3,
            "pnlPct": 2.0,
        },
        {
            "kind": "fill",
            "ts": "A.SH",
            "entryDate": "2026-03-02",
            "closeReason": "body_exit",
            "heldDays": 2,
            "pnlPct": -1.0,
        },
    ]
    rows = paths_from_blotter(_ctx(), blotter)
    assert len(rows) == 1
    assert rows[0]["pnl3"] == 2.0
