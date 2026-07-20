from __future__ import annotations

import pandas as pd

from data_sync_service.service.option_iv import (
    classify_iv_signal,
    compute_iv_pct_chg,
    select_atm_put_iv,
)
from data_sync_service.service.top_inst_flow import (
    _is_inst_seat,
    _safe_float,
    build_inst_flow_payload,
    classify_seat_label,
    detect_lhasa_dominant,
    format_inst_flow_display,
    normalize_tushare_top_inst_rows,
    normalize_tushare_top_list_rows,
)


def test_detect_lhasa_dominant_top_seat() -> None:
    seats = [
        {"exalter": "东方财富证券股份有限公司拉萨团结路第二证券营业部", "buy": 50_000_000},
        {"exalter": "机构专用", "buy": 10_000_000},
    ]
    assert detect_lhasa_dominant(seats) is True


def test_detect_lhasa_dominant_false() -> None:
    seats = [
        {"exalter": "机构专用", "buy": 50_000_000},
        {"exalter": "中信证券上海分公司", "buy": 10_000_000},
    ]
    assert detect_lhasa_dominant(seats) is False


def test_classify_seat_label() -> None:
    assert classify_seat_label(inst_net_buy=1e8, lhasa_dominant=False) == "机构主买"
    assert classify_seat_label(inst_net_buy=-1e8, lhasa_dominant=True) == "机构净卖/拉萨主买"
    assert classify_seat_label(inst_net_buy=-1e8, lhasa_dominant=False) == "机构净卖"


def test_format_inst_flow_display() -> None:
    assert format_inst_flow_display(inst_net_buy_yi=3.2, label="机构主买") == "+3.2亿 (机构主买)"
    assert format_inst_flow_display(inst_net_buy_yi=-1.5, label="机构净卖/拉萨主买") == "-1.5亿 (机构净卖/拉萨主买)"


def test_build_inst_flow_payload_on_board() -> None:
    payload = build_inst_flow_payload(
        {
            "trade_date": "2026-06-19",
            "on_board": True,
            "inst_net_buy_yi": 3.2,
            "seat_label": "机构主买",
            "lhasa_dominant": False,
        },
        buy_seats=[
            {"exalter": "机构专用", "buy": 50_000_000},
            {"exalter": "东方财富证券股份有限公司拉萨团结路第二证券营业部", "buy": 10_000_000},
        ],
    )
    assert payload["display"].startswith("+3.2亿 (机构主买)")
    assert payload["instNetBuyYi"] == 3.2
    assert len(payload["topBuySeats"]) == 2
    assert payload["topBuySeats"][0]["isInst"] is True


def test_build_inst_flow_payload_off_board() -> None:
    payload = build_inst_flow_payload(
        {
            "trade_date": "2026-06-22",
            "on_board": False,
        }
    )
    assert payload["display"] == "未上榜"
    assert payload["synced"] is True
    assert payload["onBoard"] is False


def test_build_inst_flow_payload_not_synced() -> None:
    payload = build_inst_flow_payload(None)
    assert payload["display"] == "未同步"
    assert payload["synced"] is False


def test_select_atm_put_iv_from_analysis_df() -> None:
    df = pd.DataFrame(
        [
            {
                "期权名称": "300ETF沽6月4000",
                "隐含波动率": 28.5,
                "到期日": "2026-06-25",
                "标的最新价": 4.02,
            },
            {
                "期权名称": "300ETF沽7月3800",
                "隐含波动率": 24.0,
                "到期日": "2026-07-24",
                "标的最新价": 4.02,
            },
            {
                "期权名称": "50ETF沽6月2900",
                "隐含波动率": 19.0,
                "到期日": "2026-06-25",
                "标的最新价": 2.74,
            },
        ]
    )
    picked = select_atm_put_iv(df)
    assert picked is not None
    assert picked["ivPct"] == 28.5
    assert "300ETF" in picked["contractName"]


def test_classify_iv_signal_deep_panic() -> None:
    signal, label = classify_iv_signal(iv_pct=29.0, pct_chg=5.0)
    assert signal == "red"
    assert label == "Deep Panic"


def test_classify_iv_signal_complacent() -> None:
    signal, label = classify_iv_signal(iv_pct=12.0, pct_chg=None)
    assert signal == "light_green"
    assert label == "Complacent"


def test_compute_iv_pct_chg() -> None:
    assert compute_iv_pct_chg(28.5, 24.0) == 18.75


def test_is_inst_seat() -> None:
    assert _is_inst_seat("机构专用") is True
    assert _is_inst_seat("东方财富拉萨团结路") is False


def test_safe_float() -> None:
    assert _safe_float("816710018.96") == 816710018.96
    assert _safe_float(None) is None


def test_normalize_tushare_top_list_rows() -> None:
    rows = [
        {"ts_code": "603588.SH", "name": "高能环境"},
        {"ts_code": "603986.SH", "name": "兆易创新"},
        {"ts_code": "bad"},
    ]
    assert normalize_tushare_top_list_rows(rows) == {"603588", "603986"}


def test_normalize_tushare_top_inst_rows() -> None:
    rows = [
        {
            "trade_date": "20260622",
            "ts_code": "603588.SH",
            "exalter": "东方财富证券股份有限公司拉萨团结路第二证券营业部",
            "side": "0",
            "buy": 80_000_000.0,
            "sell": 1_000_000.0,
            "net_buy": 79_000_000.0,
            "reason": "日涨幅偏离值达到7%的前五只证券",
        },
        {
            "trade_date": "20260622",
            "ts_code": "603588.SH",
            "exalter": "机构专用",
            "side": "1",
            "buy": 5_000_000.0,
            "sell": 45_000_000.0,
            "net_buy": -40_000_000.0,
            "reason": "日涨幅偏离值达到7%的前五只证券",
        },
    ]

    org_by_ticker, buy_seats_by_ts_code, inst_seats_by_ts_code = normalize_tushare_top_inst_rows(rows)

    assert org_by_ticker["603588"]["NET_BUY_AMT"] == -40_000_000.0
    assert len(buy_seats_by_ts_code["603588.SH"]) == 1
    assert detect_lhasa_dominant(buy_seats_by_ts_code["603588.SH"]) is True
    assert len(inst_seats_by_ts_code["603588.SH"]) == 1
    assert inst_seats_by_ts_code["603588.SH"][0]["side"] == "sell"


def test_org_net_from_em_row() -> None:
    inst_net = _safe_float(816710018.96) or 0.0
    assert round(inst_net / 100_000_000.0, 2) == 8.17
    label = classify_seat_label(inst_net_buy=inst_net, lhasa_dominant=False)
    assert label == "机构主买"
