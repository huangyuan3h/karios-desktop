"""Tests for service/third_asset_sleeve.py (T6 513100 idle-cash hint).

Covers the state machine: buy / sell-to-A-share / sell-to-repo / don't-buy,
plus data-insufficiency and sync-bypass paths.
"""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import patch

from data_sync_service.service import third_asset_sleeve as tas

MA_WINDOW = tas.MA_WINDOW


def _bars(n: int = MA_WINDOW + 5, last_close: float = 120.0) -> list[dict]:
    """Ascending daily bars; flat at 100 then a final close of ``last_close``."""
    base = date(2026, 1, 1)
    bars = []
    for i in range(n - 1):
        d = base + timedelta(days=i)
        bars.append(
            {
                "date": d.isoformat(),
                "open": "100", "high": "100", "low": "100", "close": "100",
                "volume": "0", "amount": "0",
            }
        )
    last = base + timedelta(days=n)
    bars.append(
        {
            "date": last.isoformat(),
            "open": str(last_close), "high": str(last_close), "low": str(last_close),
            "close": str(last_close), "volume": "0", "amount": "0",
        }
    )
    return bars


def _cn_block(**kw) -> dict:
    block = {
        "regime": "Weak",
        "panicCooldown": {"active": False},
        "circuitBlocked": False,
        "s3Candidates": [],
        "holdings": [],
    }
    block.update(kw)
    return block


def _run(bars, cn_block, *, last_date: date | None = None, holdings_override=None):
    with (
        patch("data_sync_service.service.third_asset_sleeve.get_last_trade_date", return_value=last_date),
        patch("data_sync_service.service.third_asset_sleeve.fetch_last_bars", return_value=bars),
        patch("data_sync_service.service.etf_daily.sync_etf_daily_for_ts_code", return_value={}),
    ):
        return tas.build_third_asset_sleeve(
            day="2026-08-07", cn_block=cn_block, holdings_override=holdings_override
        )


def test_buy_when_idle_and_above_ma200_gate_open():
    # gate must be OPEN (Strong/Diverging, no panic/circuit) to suggest buying
    block = _cn_block(regime="Strong")
    out = _run(_bars(), block, last_date=date(2026, 8, 6))
    assert out["active"] is True
    assert out["action"] == tas.ACTION_BUY
    assert out["aboveMa200"] is True
    assert out["gateOpen"] is True
    assert "闲置资金" in out["message"]
    assert out["idlePct"] == 100.0
    assert out["price"] == 120.0


def test_gate_closed_weak_suppresses_buy():
    """2026-08-19: Weak regime + panic cooldown → never hint BUY, even above MA200."""
    block = _cn_block(regime="Weak", panicCooldown={"active": True})
    out = _run(_bars(), block, last_date=date(2026, 8, 6))
    assert out["active"] is True
    assert out["action"] == tas.ACTION_DONT_BUY
    assert out["gateOpen"] is False
    assert "市场闸门关闭" in out["message"]
    assert "今日不买" in out["message"]


def test_gate_closed_circuit_suppresses_buy():
    block = _cn_block(regime="Strong", circuitBlocked=True)
    out = _run(_bars(), block, last_date=date(2026, 8, 6))
    assert out["action"] == tas.ACTION_DONT_BUY
    assert "回撤熔断" in out["message"]


def test_sell_to_a_share_when_buy_setup():
    block = _cn_block(regime="Strong", s3Candidates=[{"symbol": "CN:600000.SH", "score": 80}])
    out = _run(_bars(), block, last_date=date(2026, 8, 6))
    assert out["active"] is True
    assert out["action"] == tas.ACTION_SELL_TO_A_SHARE
    assert "A股有买点" in out["message"]
    assert "闲置资金改投 A 股候选" in out["message"]


def test_sell_to_a_share_takes_priority_over_buy():
    block = _cn_block(regime="Diverging", s3Candidates=[{"symbol": "X", "score": 90}])
    out = _run(_bars(), block, last_date=date(2026, 8, 6))
    assert out["action"] == tas.ACTION_SELL_TO_A_SHARE


def test_sell_to_repo_when_below_ma200_and_holding():
    holdings = [{"symbol": tas.THIRD_ASSET_SYMBOL, "positionPct": 40.0}]
    out = _run(_bars(last_close=80.0), _cn_block(), last_date=date(2026, 8, 6), holdings_override=holdings)
    assert out["active"] is True
    assert out["action"] == tas.ACTION_SELL_TO_REPO
    assert out["aboveMa200"] is False
    assert "卖出转逆回购" in out["message"]
    assert out["holding513100"] is True


def test_dont_buy_when_below_ma200_not_holding():
    out = _run(_bars(last_close=80.0), _cn_block(), last_date=date(2026, 8, 6))
    assert out["active"] is True
    assert out["action"] == tas.ACTION_DONT_BUY
    assert out["aboveMa200"] is False
    assert "别买" in out["message"]
    assert "逆回购" in out["message"]


def test_dont_buy_when_fully_deployed_above_ma200():
    block = _cn_block(holdings=[{"symbol": "CN:600000.SH", "positionPct": 100.0}])
    out = _run(_bars(), block, last_date=date(2026, 8, 6))
    assert out["active"] is True
    assert out["action"] == tas.ACTION_DONT_BUY
    assert out["idlePct"] == 0.0
    assert "今日不买" in out["message"]


def test_dont_buy_when_idle_below_threshold():
    block = _cn_block(holdings=[{"symbol": "CN:600000.SH", "positionPct": 90.0}])
    out = _run(_bars(), block, last_date=date(2026, 8, 6))
    assert out["active"] is True
    assert out["action"] == tas.ACTION_DONT_BUY
    assert out["idlePct"] == 10.0
    assert "今日不买" in out["message"]


def test_none_when_insufficient_bars():
    bars = _bars(n=50)
    out = _run(bars, _cn_block(), last_date=date(2026, 8, 6))
    assert out["active"] is False
    assert out["action"] == tas.ACTION_NONE
    assert "不足" in (out.get("note") or "")


def test_missing_local_bars_triggers_sync():
    with (
        patch("data_sync_service.service.third_asset_sleeve.get_last_trade_date", return_value=None),
        patch("data_sync_service.service.third_asset_sleeve.fetch_last_bars", return_value=_bars()),
        patch("data_sync_service.service.etf_daily.sync_etf_daily_for_ts_code", return_value={}) as sync,
    ):
        out = tas.build_third_asset_sleeve(day="2026-08-07", cn_block=_cn_block(regime="Strong"))
    sync.assert_called_once_with(tas.THIRD_ASSET_TS)
    assert out["action"] == tas.ACTION_BUY


def test_sleeve_for_paper_uses_paper_book():
    from data_sync_service.service import third_asset_sleeve as m

    with (
        patch("data_sync_service.service.portfolio_health._health_block", return_value=_cn_block(holdings=[])) as hb,
        patch("data_sync_service.db.paper_trading.list_paper_trades") as list_pt,
        patch.object(m, "build_third_asset_sleeve") as core,
    ):
        core.return_value = {"active": True, "action": tas.ACTION_BUY, "message": "buy"}
        list_pt.return_value = [
            {"symbol": "CN:600000.SH", "sleeve_pct": 10.0},
            {"symbol": tas.THIRD_ASSET_SYMBOL, "sleeve_pct": 40.0, "ts_code": tas.THIRD_ASSET_TS},
        ]
        out = m.build_third_asset_sleeve_for_paper(day="2026-08-07")
    hb.assert_called_once_with(market="CN", day="2026-08-07")
    list_pt.assert_called_once_with(status="open")
    # paper holdings passed through with sleeve_pct mapped, 513100 detected
    assert core.call_args.kwargs["holdings_override"] == [
        {"symbol": "CN:600000.SH", "ts_code": None, "sleeve_pct": 10.0},
        {"symbol": tas.THIRD_ASSET_SYMBOL, "ts_code": tas.THIRD_ASSET_TS, "sleeve_pct": 40.0},
    ]
    assert out["action"] == tas.ACTION_BUY


def test_resolve_held_third_asset_derives_ts_code() -> None:
    """Registry rows carry no ts_code — the helper derives it from the symbol."""
    rows = [{"symbol": "ETF:513110", "positionPct": 23.61, "costPrice": 2.45, "entryDate": "2026-08-20"}]
    held = tas.resolve_held_third_asset(rows)
    assert held is not None
    assert held["symbol"] == "ETF:513110"
    assert held["ts_code"] == "513110.SH"

    sz = tas.resolve_held_third_asset([{"symbol": "ETF:159941"}])
    assert sz is not None and sz["ts_code"] == "159941.SZ"

    assert tas.resolve_held_third_asset([{"symbol": "CN:600000.SH"}]) is None


def _md(*, above: bool, close: float = 2.45, ma200: float = 2.4) -> dict:
    return {"ok": True, "ts": "513110.SH", "close": close, "ma200": ma200,
            "above_ma200": above, "as_of": "2026-08-19", "pct_chg": -1.0}


def test_build_third_asset_holding_hold_when_above_ma200() -> None:
    """A held NASDAQ-100 ETF above its 200d MA with no A-share buy setup -> HOLD."""
    holdings = [{"symbol": "ETF:513110", "ts_code": "513110.SH", "positionPct": 23.61, "costPrice": 2.45}]
    with patch("data_sync_service.service.third_asset_sleeve._etf_market_data", return_value=_md(above=True)):
        out = tas.build_third_asset_holding(day="2026-08-20", cn_block=_cn_block(regime="Diverging"), holdings_override=holdings)
    assert out is not None
    assert out["active"] is True
    assert out["action"] == tas.ACTION_HOLD
    assert out["aboveMa200"] is True
    assert out["price"] > 0
    assert out["pnlPct"] is not None


def test_build_third_asset_holding_sell_when_below_ma200() -> None:
    holdings = [{"symbol": "ETF:513110", "ts_code": "513110.SH", "positionPct": 23.61, "costPrice": 2.45}]
    with patch("data_sync_service.service.third_asset_sleeve._etf_market_data", return_value=_md(above=False, close=2.1, ma200=2.4)):
        out = tas.build_third_asset_holding(day="2026-08-20", cn_block=_cn_block(regime="Weak"), holdings_override=holdings)
    assert out is not None
    assert out["action"] == tas.ACTION_SELL_TO_REPO
    assert out["aboveMa200"] is False
    assert "跌破200日线" in out["message"]


def test_build_third_asset_holding_sell_when_a_share_buy_setup() -> None:
    holdings = [{"symbol": "ETF:513110", "ts_code": "513110.SH", "positionPct": 23.61, "costPrice": 2.45}]
    block = _cn_block(regime="Strong", s3Candidates=[{"symbol": "CN:600000.SH", "score": 80}])
    with patch("data_sync_service.service.third_asset_sleeve._etf_market_data", return_value=_md(above=True)):
        out = tas.build_third_asset_holding(day="2026-08-20", cn_block=block, holdings_override=holdings)
    assert out is not None
    assert out["action"] == tas.ACTION_SELL_TO_A_SHARE
    assert "换回 A 股" in out["message"]


def test_build_third_asset_holding_none_when_not_held() -> None:
    out = tas.build_third_asset_holding(day="2026-08-20", cn_block=_cn_block(), holdings_override=[])
    assert out is None


def test_cn_holdings_exclude_third_asset_etf() -> None:
    """ETF:513110 must NOT appear in the CN A-share holdings block (separate region)."""
    from data_sync_service.service import portfolio_health as ph

    reg = [
        {"symbol": "ETF:513110", "payload": {"positionPct": 23.61, "costPrice": 2.45, "entryDate": "2026-08-20"}},
        {"symbol": "CN:300628", "payload": {"positionPct": 6.4, "costPrice": 39.9, "entryDate": "2026-08-04"}},
    ]
    with (
        patch.object(ph, "list_registry", return_value=reg),
        patch.object(ph, "_holding_check", lambda **kw: {"symbol": kw["name"], "action": "HOLD", "pnlPct": 1.0}),
        patch.object(ph, "_pyramided_symbols", return_value=set()),
    ):
        holdings = ph._build_holdings_block(market="CN", day="2026-08-20")
    syms = {h["symbol"] for h in holdings}
    assert "CN:300628" in syms
    assert "ETF:513110" not in syms


def test_build_portfolio_health_embeds_sleeve():
    """portfolio-health response carries the thirdAssetSleeve block."""
    from data_sync_service.service import portfolio_health as ph

    fake_block = {"regime": "Weak", "panicCooldown": {"active": False},
                  "circuitBlocked": False, "s3Candidates": [], "holdings": []}
    with (
        patch.object(ph, "_health_block", return_value=fake_block),
        patch.object(ph, "list_registry", return_value=[]),
        patch(
            "data_sync_service.service.third_asset_sleeve.build_third_asset_sleeve",
            return_value={"active": True, "action": tas.ACTION_BUY, "message": "buy", "etf": "ETF:513100"},
        ) as sleeve_fn,
        patch(
            "data_sync_service.service.third_asset_sleeve.build_third_asset_holding",
            return_value={"active": True, "symbol": "ETF:513100", "action": tas.ACTION_HOLD},
        ) as holding_fn,
    ):
        out = ph.build_portfolio_health(trade_date="2026-08-07", markets=("CN",))
    sleeve_fn.assert_called_once()
    holding_fn.assert_called_once()
    assert out["thirdAssetSleeve"]["action"] == tas.ACTION_BUY
    assert out["thirdAssetHolding"]["action"] == tas.ACTION_HOLD
    assert "hkHealth" in out
    assert out["regime"] == "Weak"