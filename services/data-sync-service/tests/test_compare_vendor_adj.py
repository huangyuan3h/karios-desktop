"""Unit test for scripts/compare_vendor_adj.py pure helpers (OPT-145 D)."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import importlib.util

spec = importlib.util.spec_from_file_location(
    "compare_vendor_adj",
    Path(__file__).resolve().parents[1] / "scripts" / "compare_vendor_adj.py",
)
cva = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cva)


def test_read_factor_and_jump_dates(tmp_path: Path) -> None:
    p = tmp_path / "600000.SH.csv"
    with p.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["股票代码", "交易日期", "复权因子"])
        w.writerow(["600000.SH", "2024-06-03", "10.0"])
        w.writerow(["600000.SH", "2024-06-04", "10.0"])  # flat: no jump
        w.writerow(["600000.SH", "2024-06-05", "10.5"])  # +5%: jump
        w.writerow(["600000.SH", "2024-06-06", "not-a-number"])  # skipped
    series = cva._read_factor(p)
    assert len(series) == 3
    assert series["2024-06-03"] == 10.0
    jumps = cva._jump_dates(series)
    assert list(jumps) == ["2024-06-05"]
    assert abs(jumps["2024-06-05"] - 0.05) < 1e-9


def test_jump_dates_ignores_daily_noise() -> None:
    series = {"2024-01-02": 11.4, "2024-01-03": 11.3704, "2024-01-04": 11.3851}
    jumps = cva._jump_dates(series)
    # -0.26% / +0.13%: price-like wiggle around the 0.1% gate
    assert "2024-01-03" in jumps
    assert "2024-01-04" in jumps
