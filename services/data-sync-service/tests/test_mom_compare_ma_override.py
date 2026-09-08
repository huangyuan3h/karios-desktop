"""ma_window_by_key for build_mom_compare_timeline (index §8).

Flag-off (None) must equal legacy behavior bit-for-bit; an override must
affect only the named key.
"""
from data_sync_service.service import pick_strong_track as pst
from data_sync_service.service.pick_strong_track import build_mom_compare_timeline


def _fixture():
    # 60 flat days at 10, then 20,20,20,18.5,18.5: on the last row the MA
    # window sits on prev=D063 (18.5): above MA5 (17.7) but below MA3 (19.5).
    closes = [10.0] * 60 + [20.0, 20.0, 20.0, 18.5, 18.5]
    days = [f"D{i:03d}" for i in range(len(closes))]
    etf = {"K1": dict(zip(days, closes)), "K2": dict(zip(days, closes))}
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
        pst.MULTI_TS = {"GOLD": "518880.SH", "OIL": "513350.SH",
                        "NASDAQ": "513110.SH", "BOND10": "511260.SH"}


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
