"""Point-in-time / look-ahead regression guards (OPT-171).

The A1 vol-target pilot shipped with a same-month selection look-ahead
(~+20pt/yr): the monthly rebalance picked the top-liquidity basket using that
same month's end-of-month liquidity. ``build_panel_from_px`` now shifts one
month. These tests pin the contract so it cannot silently regress.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import strategy_a1_voltarget as a1  # noqa: E402


def _days(start: str, days: int, *, amount: float) -> pd.DataFrame:
    dates = pd.date_range(start, periods=days, freq="D")
    return pd.DataFrame(
        {
            "ts_code": "600000.SH",
            "trade_date": dates,
            "close": 10.0,
            "pct_chg": 0.0,
            "amount": amount,
        }
    )


def _three_month_frame() -> pd.DataFrame:
    # Calendar days (not bdays) so each month keeps >=30 daily rows for the
    # 60d/min30 rolling mean. Amounts are constant per month.
    px = pd.concat(
        [
            _days("2020-01-01", 31, amount=100.0),
            _days("2020-02-01", 28, amount=200.0),
            _days("2020-03-01", 31, amount=300.0),
        ],
        ignore_index=True,
    )
    return px


def test_liq_is_prior_month_not_same_month() -> None:
    mo = a1.build_panel_from_px(_three_month_frame()).sort_values("ym").reset_index(drop=True)
    assert len(mo) == 3
    jan, feb, mar = mo["liq"].tolist()

    # January has no prior month -> unselectable.
    assert pd.isna(jan)
    # February must use December/January liquidity (constant 100), NOT its own
    # end-of-month liq60 (blend ~148) — the exact look-ahead that was fixed.
    assert feb == pytest.approx(100.0)
    assert feb != pytest.approx(148.0, abs=1.0)
    # March uses February's monthly liquidity, which is not March's own.
    assert mar > 100.0


def test_liq_column_monotonic_with_rising_amounts() -> None:
    mo = a1.build_panel_from_px(_three_month_frame()).sort_values("ym").reset_index(drop=True)
    assert mo["liq"].iloc[1] < mo["liq"].iloc[2]


def test_selection_module_has_no_negative_shift() -> None:
    """Forward-looking ``shift(-n)`` must never appear in the selection path."""
    src = (SCRIPTS / "strategy_a1_voltarget.py").read_text(encoding="utf-8")
    offenders = [
        line.strip()
        for line in src.splitlines()
        if ".shift(-" in line and not line.strip().startswith("#")
    ]
    assert not offenders, f"negative shift (future leak) in selection code: {offenders}"
