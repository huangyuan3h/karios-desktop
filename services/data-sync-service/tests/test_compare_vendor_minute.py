"""Unit test for scripts/compare_vendor_minute.py pure helpers (OPT-145 A)."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import importlib.util

spec = importlib.util.spec_from_file_location(
    "compare_vendor_minute",
    Path(__file__).resolve().parents[1] / "scripts" / "compare_vendor_minute.py",
)
cvm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cvm)


def _row(day, hhmm, close, o=10.0, h=10.5, low=9.9):
    return {"trade_date": day, "time": hhmm, "open": o, "high": h, "low": low, "close": close}


def test_match_points_join_and_diffs() -> None:
    vendor = [
        _row("2024-01-02", "1430", 10.00),
        _row("2024-01-02", "1500", 10.50),  # +0.50 vs indep → mismatch
        _row("2024-01-02", "1440", 10.10),  # no indep side → missing
    ]
    indep = [
        _row("2024-01-02", "1430", 10.005),  # within 1分
        _row("2024-01-02", "1500", 10.00, o=9.0),  # open differs → ohl mismatch
    ]
    pts = cvm.match_points(vendor, indep)
    assert len(pts) == 3
    p1430, p1500, p1440 = pts
    assert p1430["abs_match"] is True and p1430["ohl_match"] is True
    assert abs(p1430["rel_diff"] - 0.005 / 10.005) < 1e-9
    assert p1500["abs_match"] is False and p1500["ohl_match"] is False
    assert p1440["missing"] is True


def test_summarize_rates_and_percentiles() -> None:
    matches = [
        {"trade_date": "d", "time": "1430", "abs_match": True, "rel_diff": 0.0, "ohl_match": True},
        {
            "trade_date": "d",
            "time": "1430",
            "abs_match": True,
            "rel_diff": 0.001,
            "ohl_match": True,
        },
        {
            "trade_date": "d",
            "time": "1500",
            "abs_match": False,
            "rel_diff": 0.05,
            "ohl_match": False,
        },
        {"trade_date": "d", "time": "1500", "missing": True},
    ]
    s = cvm.summarize(matches)
    assert s["n"] == 3 and s["missing"] == 1
    assert s["matched"] == 2 and abs(s["rate"] - 2 / 3) < 1e-9
    assert s["rel_p50"] == 0.001 and s["rel_max"] == 0.05
    assert s["ohl_mismatch"] == 1
    assert s["per_time"]["1430"]["rate"] == 1.0
    assert s["per_time"]["1500"]["rate"] == 0.0


def test_pick_sample_blue_first_deterministic() -> None:
    codes = [f"{i:06d}.SH" for i in range(600000, 600100)]
    a = cvm.pick_sample(codes, seed=7, blue_chips=["600000.SH", "999999.SH"], n=10)
    b = cvm.pick_sample(codes, seed=7, blue_chips=["600000.SH", "999999.SH"], n=10)
    assert a == b and a[0] == "600000.SH" and len(a) == 10  # absent blue skipped


def test_verdict_reports_both_gates() -> None:
    v = cvm.verdict({"rate": 0.9059, "per_time": {"1430": {"rate": 0.86}, "1500": {"rate": 0.98}}})
    assert v["original_pass"] is False
    assert v["note_1500"] == 0.98 and "slippage" in v["revised"]
