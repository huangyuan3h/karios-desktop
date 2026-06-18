from __future__ import annotations

from data_sync_service.service import watchlist_automation as wa  # type: ignore[import-not-found]


def test_should_remove_symbol_alpha_exempt() -> None:
    ok, reason = wa.should_remove_symbol(
        symbol="CN:600000",
        source="alpha_radar",
        trade_dates=["2026-06-16", "2026-06-17", "2026-06-18"],
        top_5d_industries=set(),
        current_industry="Banking",
    )
    assert ok is False
    assert reason == "alpha_radar_exempt"


def test_should_remove_symbol_insufficient_history() -> None:
    ok, reason = wa.should_remove_symbol(
        symbol="CN:600000",
        source="manual",
        trade_dates=["2026-06-17", "2026-06-18"],
        top_5d_industries=set(),
        current_industry="Banking",
    )
    assert ok is False
    assert reason == "insufficient_history"


def test_should_remove_symbol_null_score_breaks_streak(monkeypatch) -> None:
    monkeypatch.setattr(
        wa,
        "get_scores_for_symbol",
        lambda symbol, trade_dates: [
            {"trade_date": "2026-06-16", "score": 20.0, "industry": "Banking"},
            {"trade_date": "2026-06-17", "score": None, "industry": "Banking"},
            {"trade_date": "2026-06-18", "score": 15.0, "industry": "Banking"},
        ],
    )
    ok, reason = wa.should_remove_symbol(
        symbol="CN:600000",
        source="manual",
        trade_dates=["2026-06-16", "2026-06-17", "2026-06-18"],
        top_5d_industries=set(),
        current_industry="Banking",
    )
    assert ok is False
    assert reason == "null_score_breaks_streak"


def test_should_remove_symbol_score_too_high(monkeypatch) -> None:
    def fake_scores(symbol: str, trade_dates: list[str]) -> list[dict]:
        return [
            {"trade_date": "2026-06-16", "score": 25.0, "industry": "Banking"},
            {"trade_date": "2026-06-17", "score": 25.0, "industry": "Banking"},
            {"trade_date": "2026-06-18", "score": 35.0, "industry": "Banking"},
        ]

    monkeypatch.setattr(wa, "get_scores_for_symbol", fake_scores)
    ok, reason = wa.should_remove_symbol(
        symbol="CN:600000",
        source="manual",
        trade_dates=["2026-06-16", "2026-06-17", "2026-06-18"],
        top_5d_industries=set(),
        current_industry="Banking",
    )
    assert ok is False
    assert reason == "score_not_low_enough"


def test_should_remove_symbol_industry_in_top5(monkeypatch) -> None:
    def fake_scores(symbol: str, trade_dates: list[str]) -> list[dict]:
        return [{"trade_date": d, "score": 20.0, "industry": "Banking"} for d in trade_dates]

    monkeypatch.setattr(wa, "get_scores_for_symbol", fake_scores)
    ok, reason = wa.should_remove_symbol(
        symbol="CN:600000",
        source="manual",
        trade_dates=["2026-06-16", "2026-06-17", "2026-06-18"],
        top_5d_industries={"Banking"},
        current_industry="Banking",
    )
    assert ok is False
    assert reason == "industry_still_in_top5"


def test_should_remove_symbol_all_conditions_met(monkeypatch) -> None:
    def fake_scores(symbol: str, trade_dates: list[str]) -> list[dict]:
        return [{"trade_date": d, "score": 20.0, "industry": "Coal"} for d in trade_dates]

    monkeypatch.setattr(wa, "get_scores_for_symbol", fake_scores)
    ok, reason = wa.should_remove_symbol(
        symbol="CN:600000",
        source="screener",
        trade_dates=["2026-06-16", "2026-06-17", "2026-06-18"],
        top_5d_industries={"Banking", "Tech"},
        current_industry="Coal",
    )
    assert ok is True
    assert reason == "score_low_3d_and_industry_outside_top5"


def test_compute_alpha_additions_filters_score_and_grade(monkeypatch) -> None:
    monkeypatch.setattr(
        wa,
        "list_catalyst_stocks",
        lambda limit=200: {
            "items": [
                {
                    "symbol": "600000",
                    "name": "Test",
                    "catalystScore": 90.0,
                    "articles": [{"catalystGrade": "A"}],
                },
                {
                    "symbol": "600001",
                    "name": "S Stock",
                    "catalystScore": 86.0,
                    "articles": [{"catalystGrade": "S"}],
                },
                {
                    "symbol": "600002",
                    "name": "Low",
                    "catalystScore": 80.0,
                    "articles": [{"catalystGrade": "S"}],
                },
            ]
        },
    )
    out = wa.compute_alpha_additions()
    assert len(out) == 1
    assert out[0]["symbol"] == "CN:600001"
    assert out[0]["catalystScore"] == 86.0


def test_precheck_skips_without_close_sync(monkeypatch) -> None:
    monkeypatch.setattr(wa, "is_trading_day", lambda exchange, d: True)
    monkeypatch.setattr(wa, "get_today_run", lambda job_type: None)
    skipped, reason = wa._precheck(force=False)
    assert skipped is True
    assert reason == "close_sync_not_ready"


def test_precheck_force_bypasses(monkeypatch) -> None:
    monkeypatch.setattr(wa, "is_trading_day", lambda exchange, d: False)
    skipped, reason = wa._precheck(force=True)
    assert skipped is False
    assert reason is None


def test_normalize_trade_date() -> None:
    assert wa._normalize_trade_date("20260618") == "2026-06-18"
    assert wa._normalize_trade_date("2026-06-18") == "2026-06-18"
