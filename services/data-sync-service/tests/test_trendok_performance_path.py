from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import patch

from data_sync_service.service.trendok import (  # type: ignore[import-not-found]
    _resolve_effective_stoploss,
    _resolve_inst_summaries_for_trendok,
    clear_trendok_cache,
    compute_trendok_for_symbols,
)


def _trend_bars(days: int = 80, start: float = 10.0) -> list[tuple[str, str, str, str, str, str]]:
    base = date(2026, 1, 1)
    rows = []
    for idx in range(days):
        close = start + idx * 0.1
        open_price = close - 0.04
        high = close + 0.08
        low = close - 0.08
        rows.append(
            (
                (base + timedelta(days=idx)).isoformat(),
                f"{open_price:.3f}",
                f"{high:.3f}",
                f"{low:.3f}",
                f"{close:.3f}",
                "100000",
            )
        )
    return rows


def test_compute_trendok_uses_lightweight_market_regime() -> None:
    with (
        patch(
            "data_sync_service.service.trendok.fetch_last_ohlcv_batch",
            return_value={},
        ),
        patch(
            "data_sync_service.service.trendok._build_industry_flow_context",
            return_value={"ok": False},
        ),
        patch(
            "data_sync_service.service.trendok.get_market_regime",
            return_value={"regime": "Strong", "bias": None, "indexSignals": []},
        ) as get_regime,
        patch(
            "data_sync_service.service.eastmoney_industry.ensure_em_industries_for_ts_codes",
        ) as ensure_em,
        patch(
            "data_sync_service.service.eastmoney_industry.fetch_em_industries_for_ts_codes",
        ) as fetch_em,
    ):
        out = compute_trendok_for_symbols(["CN:999999"], realtime=False)
    assert isinstance(out, list)
    get_regime.assert_called_once()
    assert get_regime.call_args.kwargs.get("include_breadth") is False
    ensure_em.assert_not_called()
    fetch_em.assert_not_called()


def test_compute_trendok_calls_stock_basic_lookup_once() -> None:
    with (
        patch(
            "data_sync_service.service.trendok.fetch_last_ohlcv_batch",
            return_value={"999999.SZ": [("2024-01-20", 10.0, 11.0, 9.0, 10.5, 1000.0)]},
        ),
        patch(
            "data_sync_service.service.trendok._build_industry_flow_context",
            return_value={"ok": False},
        ),
        patch(
            "data_sync_service.service.trendok.get_market_regime",
            return_value={"regime": "Strong", "bias": None, "indexSignals": []},
        ),
        patch(
            "data_sync_service.service.trendok._lookup_stock_basic",
            return_value=({"999999.SZ": "Test"}, {"999999.SZ": "电子"}),
        ) as lookup_basic,
        patch(
            "data_sync_service.service.trendok._lookup_em_industry_boards",
            return_value={},
        ),
    ):
        out = compute_trendok_for_symbols(["CN:999999"], realtime=False)

    assert isinstance(out, list)
    lookup_basic.assert_called_once()


def test_compute_trendok_cache_hit_skips_heavy_lookups() -> None:
    bars = {"999999.SZ": [("2024-01-20", "10", "11", "9", "10.5", "1000")]}
    clear_trendok_cache()
    with (
        patch(
            "data_sync_service.service.trendok.fetch_last_ohlcv_batch",
            return_value=bars,
        ) as fetch_bars,
        patch(
            "data_sync_service.service.trendok._build_industry_flow_context",
            return_value={"ok": False},
        ) as flow_ctx,
        patch(
            "data_sync_service.service.trendok.get_market_regime",
            return_value={"regime": "Strong", "bias": None, "indexSignals": []},
        ) as get_regime,
        patch(
            "data_sync_service.service.trendok._lookup_stock_basic",
            return_value=({"999999.SZ": "Test"}, {"999999.SZ": "电子"}),
        ) as lookup_basic,
        patch(
            "data_sync_service.service.trendok._lookup_em_industry_boards",
            return_value={},
        ),
    ):
        first = compute_trendok_for_symbols(["CN:999999"], realtime=False)
        second = compute_trendok_for_symbols(["CN:999999"], realtime=False)

    assert isinstance(first, list)
    assert isinstance(second, list)
    assert fetch_bars.call_count == 2
    lookup_basic.assert_called_once()
    flow_ctx.assert_called_once()
    get_regime.assert_called_once()


def test_clear_trendok_cache_forces_recompute() -> None:
    bars = {"999999.SZ": [("2024-01-20", "10", "11", "9", "10.5", "1000")]}
    clear_trendok_cache()
    with (
        patch(
            "data_sync_service.service.trendok.fetch_last_ohlcv_batch",
            return_value=bars,
        ),
        patch(
            "data_sync_service.service.trendok._build_industry_flow_context",
            return_value={"ok": False},
        ) as flow_ctx,
        patch(
            "data_sync_service.service.trendok.get_market_regime",
            return_value={"regime": "Strong", "bias": None, "indexSignals": []},
        ),
        patch(
            "data_sync_service.service.trendok._lookup_stock_basic",
            return_value=({"999999.SZ": "Test"}, {"999999.SZ": "电子"}),
        ),
        patch(
            "data_sync_service.service.trendok._lookup_em_industry_boards",
            return_value={},
        ),
    ):
        compute_trendok_for_symbols(["CN:999999"], realtime=False)
        clear_trendok_cache()
        compute_trendok_for_symbols(["CN:999999"], realtime=False)

    assert flow_ctx.call_count == 2


def test_compute_trendok_batches_stoploss_and_daily_seats() -> None:
    clear_trendok_cache()
    bars = {
        "600519.SH": _trend_bars(start=10.0),
        "000001.SZ": _trend_bars(start=20.0),
    }
    summaries = {
        "600519.SH": {
            "trade_date": "2026-03-21",
            "on_board": True,
            "inst_net_buy_yi": 1.2,
            "seat_label": "机构净买",
            "lhasa_dominant": False,
        },
        "000001.SZ": {
            "trade_date": "2026-03-21",
            "on_board": True,
            "inst_net_buy_yi": 0.8,
            "seat_label": "机构净买",
            "lhasa_dominant": False,
        },
    }

    with (
        patch("data_sync_service.service.trendok.fetch_last_ohlcv_batch", return_value=bars),
        patch("data_sync_service.service.trendok._build_industry_flow_context", return_value={"ok": False}),
        patch(
            "data_sync_service.service.trendok.get_market_regime",
            return_value={"regime": "Strong", "bias": None, "indexSignals": []},
        ),
        patch(
            "data_sync_service.service.trendok._lookup_stock_basic",
            return_value=(
                {"600519.SH": "Kweichow Moutai", "000001.SZ": "Ping An Bank"},
                {"600519.SH": "Food", "000001.SZ": "Bank"},
            ),
        ),
        patch("data_sync_service.service.trendok._lookup_em_industry_boards", return_value={}),
        patch("data_sync_service.service.trendok.fetch_summaries_for_codes", return_value=summaries),
        patch("data_sync_service.service.trendok.fetch_daily_seats_batch", return_value={}) as seats_batch,
        patch(
            "data_sync_service.service.trendok.get_stoploss_batch",
            return_value={"600519.SH": {"stop_loss_price": 999.0}},
        ) as stoploss_batch,
        patch("data_sync_service.service.trendok.upsert_stoploss_batch") as upsert_batch,
    ):
        out = compute_trendok_for_symbols(["CN:600519", "CN:000001"], realtime=False)

    assert len(out) == 2
    stoploss_batch.assert_called_once_with(["600519.SH", "000001.SZ"])
    seats_batch.assert_called_once_with([("600519.SH", "2026-03-21"), ("000001.SZ", "2026-03-21")])
    upsert_rows = upsert_batch.call_args.args[0]
    assert len(upsert_rows) == 1
    assert upsert_rows[0]["ts_code"] == "000001.SZ"
    assert next(row for row in out if row["symbol"] == "CN:600519")["stopLossPrice"] == 999.0


def test_compute_trendok_realtime_flag_separate_cache() -> None:
    bars = {"999999.SZ": [("2024-01-20", "10", "11", "9", "10.5", "1000")]}
    clear_trendok_cache()
    with (
        patch(
            "data_sync_service.service.trendok.fetch_last_ohlcv_batch",
            return_value=bars,
        ),
        patch(
            "data_sync_service.service.trendok.fetch_realtime_quotes",
            return_value={"ok": True, "items": []},
        ),
        patch(
            "data_sync_service.service.trendok._build_industry_flow_context",
            return_value={"ok": False},
        ) as flow_ctx,
        patch(
            "data_sync_service.service.trendok.get_market_regime",
            return_value={"regime": "Strong", "bias": None, "indexSignals": []},
        ),
        patch(
            "data_sync_service.service.trendok._lookup_stock_basic",
            return_value=({"999999.SZ": "Test"}, {"999999.SZ": "电子"}),
        ),
        patch(
            "data_sync_service.service.trendok._lookup_em_industry_boards",
            return_value={},
        ),
    ):
        compute_trendok_for_symbols(["CN:999999"], realtime=False)
        compute_trendok_for_symbols(["CN:999999"], realtime=True)

    assert flow_ctx.call_count == 2


def test_compute_trendok_inst_summary_falls_back_when_latest_bar_date_missing() -> None:
    clear_trendok_cache()
    prior_summary = {
        "trade_date": "2026-06-22",
        "on_board": False,
        "inst_net_buy_yi": None,
        "seat_label": "",
        "lhasa_dominant": False,
    }

    def fake_fetch(codes: list[str], *, trade_date: str | None = None) -> dict[str, dict]:
        if trade_date == "2026-06-23":
            return {}
        if trade_date is None:
            return {"002185.SZ": prior_summary}
        return {}

    with (
        patch(
            "data_sync_service.service.trendok.fetch_summaries_for_codes",
            side_effect=fake_fetch,
        ) as fetch_inst,
    ):
        resolved = _resolve_inst_summaries_for_trendok(["002185.SZ"], latest_bar_date="2026-06-23")

    assert fetch_inst.call_count == 2
    assert resolved["002185.SZ"]["trade_date"] == "2026-06-22"


def test_resolve_effective_stoploss_uses_stored_higher_without_upsert() -> None:
    with (
        patch(
            "data_sync_service.service.trendok.get_stoploss_batch",
            return_value={"600519.SH": {"stop_loss_price": 42.0}},
        ) as get_batch,
        patch("data_sync_service.service.trendok.upsert_stoploss_batch") as upsert_batch,
    ):
        effective, used_stored = _resolve_effective_stoploss("600519.SH", 40.0, "2026-03-21", None)

    assert effective == 42.0
    assert used_stored is True
    get_batch.assert_called_once_with(["600519.SH"])
    upsert_batch.assert_not_called()


def test_resolve_effective_stoploss_upserts_when_computed_is_higher() -> None:
    with (
        patch(
            "data_sync_service.service.trendok.get_stoploss_batch",
            return_value={"600519.SH": {"stop_loss_price": 38.0}},
        ),
        patch("data_sync_service.service.trendok.upsert_stoploss_batch") as upsert_batch,
    ):
        effective, used_stored = _resolve_effective_stoploss("600519.SH", 40.0, "2026-03-21", None)

    assert effective == 40.0
    assert used_stored is False
    upsert_batch.assert_called_once_with(
        [{"ts_code": "600519.SH", "stop_loss_price": 40.0, "as_of_date": "2026-03-21"}]
    )
