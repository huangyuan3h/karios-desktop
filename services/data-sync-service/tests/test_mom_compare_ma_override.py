"""ma_window_by_key for build_mom_compare_timeline (index §8).

Flag-off (None) must equal legacy behavior bit-for-bit; an override must
affect only the named key.
"""

import copy

from data_sync_service.service import pick_strong_track as pst
from data_sync_service.service.pick_strong_track import build_mom_compare_timeline


def _fixture():
    # 60 flat days at 10, then 20,20,20,18.5,18.5: on the last row the MA
    # window sits on prev=D063 (18.5): above MA5 (17.7) but below MA3 (19.5).
    closes = [10.0] * 60 + [20.0, 20.0, 20.0, 18.5, 18.5]
    days = [f"D{i:03d}" for i in range(len(closes))]
    etf = {"K1": dict(zip(days, closes, strict=True)), "K2": dict(zip(days, closes, strict=True))}
    return days, etf


def _run(days, etf, **kw):
    import copy

    pst.MULTI_TS = {"K1": "K1.TS", "K2": "K2.TS"}
    try:
        return build_mom_compare_timeline(
            calendar=list(days),
            positions_by_day=[],
            close_by_ts_day={},
            etf_close=copy.deepcopy(etf),
            ma_window=5,
            trail_pct=0,
            **kw,
        )
    finally:
        pst.MULTI_TS = {
            "GOLD": "518880.SH",
            "OIL": "513350.SH",
            "NASDAQ": "513110.SH",
            "BOND10": "511260.SH",
        }


def test_flag_off_equals_legacy():
    days, etf = _fixture()
    a = _run(days, etf)
    b = _run(days, etf, ma_window_by_key=None)
    assert [r["pick"] for r in a["rows"]] == [r["pick"] for r in b["rows"]]
    assert a["summary"] == b["summary"]
    # default MA5 includes K on the dip day (both keys picked over days)
    assert a["rows"][-1]["pick"] in ("K1", "K2")


def test_override_scoped_to_named_key():
    days, etf = _fixture()
    # Override only K1 with MA3 -> K1 excluded on dip day, K2 still picked.
    r = _run(days, etf, ma_window_by_key={"K1": 3})
    assert r["rows"][-1]["pick"] == "K2"
    # Overriding an absent key changes nothing.
    r2 = _run(days, etf, ma_window_by_key={"NOPE": 3})
    base = _run(days, etf)
    assert [x["pick"] for x in r2["rows"]] == [x["pick"] for x in base["rows"]]


# ---------------------------------------------------------------------------
# TIP-016 E (pre-registered): absolute-strength floor on the ETF leg
# ---------------------------------------------------------------------------


def _floor_fixture():
    # 55 days at 20, 8 days at 10, 2 days at 11: on the last row
    # mom60 = 11/20-1 = -45%, while close (11) sits above MA5 (10.4) —
    # so the MA gate passes and only the floor can veto.
    closes = [20.0] * 55 + [10.0] * 8 + [11.0, 11.0]
    days = [f"F{i:03d}" for i in range(len(closes))]
    etf = {"K1": dict(zip(days, closes, strict=True))}
    return days, etf


def _run_floor(days, etf, **kw):
    pst.MULTI_TS = {"K1": "K1.TS"}
    try:
        return build_mom_compare_timeline(
            calendar=list(days),
            positions_by_day=[],
            close_by_ts_day={},
            etf_close=copy.deepcopy(etf),
            ma_window=5,
            trail_pct=0,
            **kw,
        )
    finally:
        pst.MULTI_TS = {
            "GOLD": "518880.SH",
            "OIL": "513350.SH",
            "NASDAQ": "513110.SH",
            "BOND10": "511260.SH",
        }


def test_etf_floor_off_picks_weak_etf():
    """Frozen: mom60 -50% but above MA -> picked (the documented hole)."""
    days, etf = _floor_fixture()
    out = _run_floor(days, etf)
    assert out["rows"][-1]["pick"] == "K1"


def test_etf_floor_zero_falls_to_repo():
    days, etf = _floor_fixture()
    out = _run_floor(days, etf, etf_mom_floor=0.0)
    assert out["rows"][-1]["pick"] == "REPO"


def test_etf_floor_below_mom_still_picks():
    days, etf = _floor_fixture()
    out = _run_floor(days, etf, etf_mom_floor=-0.6)
    assert out["rows"][-1]["pick"] == "K1"
