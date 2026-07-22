from __future__ import annotations

from data_sync_service.service import watchlist_automation as wa  # type: ignore[import-not-found]


def test_should_remove_symbol_alpha_s_exempt(monkeypatch) -> None:
    def fake_scores(symbol: str, trade_dates: list[str]) -> list[dict]:
        return [{"trade_date": d, "score": 20.0, "industry": "Coal"} for d in trade_dates]

    monkeypatch.setattr(wa, "get_scores_for_symbol", fake_scores)
    ok, reason = wa.should_remove_symbol(
        symbol="CN:600000",
        source="alpha_radar",
        trade_dates=["2026-06-16", "2026-06-17", "2026-06-18"],
        top_5d_industries={"Banking"},
        current_industry="Coal",
        alpha_s_symbols={"CN:600000"},
    )
    assert ok is False
    assert reason == "alpha_s_exempt"


def test_should_remove_symbol_alpha_non_s_can_gc(monkeypatch) -> None:
    def fake_scores(symbol: str, trade_dates: list[str]) -> list[dict]:
        return [{"trade_date": d, "score": 20.0, "industry": "Coal"} for d in trade_dates]

    monkeypatch.setattr(wa, "get_scores_for_symbol", fake_scores)
    ok, reason = wa.should_remove_symbol(
        symbol="CN:600000",
        source="alpha_radar",
        trade_dates=["2026-06-16", "2026-06-17", "2026-06-18"],
        top_5d_industries={"Banking"},
        current_industry="Coal",
        alpha_s_symbols={"CN:600001"},
        position_pct=0,
    )
    assert ok is True
    assert reason == "score_low_3d_and_industry_outside_top5"


def test_symbols_with_max_grade_s() -> None:
    payload = {
        "items": [
            {"symbol": "600000", "articles": [{"catalystGrade": "A"}]},
            {"symbol": "CN:600001", "articles": [{"catalystGrade": "S"}]},
            {"symbol": "600002", "articles": []},
        ]
    }
    assert wa.symbols_with_max_grade_s(payload) == {"CN:600001"}


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
        position_pct=0,
    )
    assert ok is True
    assert reason == "score_low_3d_and_industry_outside_top5"


def test_should_remove_symbol_skips_held_position(monkeypatch) -> None:
    def fake_scores(symbol: str, trade_dates: list[str]) -> list[dict]:
        return [{"trade_date": d, "score": 20.0, "industry": "Coal"} for d in trade_dates]

    monkeypatch.setattr(wa, "get_scores_for_symbol", fake_scores)
    ok, reason = wa.should_remove_symbol(
        symbol="CN:600000",
        source="screener",
        trade_dates=["2026-06-16", "2026-06-17", "2026-06-18"],
        top_5d_industries={"Banking", "Tech"},
        current_industry="Coal",
        position_pct=5.0,
    )
    assert ok is False
    assert reason == "held_position"


def test_is_defense_sector() -> None:
    assert wa.is_defense_sector("银行") is True
    assert wa.is_defense_sector("股份制银行") is True
    assert wa.is_defense_sector("半导体") is False
    assert wa.is_defense_sector(None) is False


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
    out, rejected = wa.compute_alpha_additions(
        industry_by_symbol={"CN:600001": "半导体"},
        top_industries={"半导体", "电子"},
    )
    assert len(out) == 1
    assert out[0]["symbol"] == "CN:600001"
    assert out[0]["catalystScore"] == 86.0
    assert rejected.get("no_s_grade") == 1
    assert rejected.get("low_score") == 1


def test_compute_alpha_additions_rejects_defense_and_non_top10() -> None:
    payload = {
        "items": [
            {
                "symbol": "600000",
                "name": "Bank",
                "catalystScore": 90.0,
                "articles": [{"catalystGrade": "S"}],
            },
            {
                "symbol": "600001",
                "name": "Cold",
                "catalystScore": 90.0,
                "articles": [{"catalystGrade": "S"}],
            },
            {
                "symbol": "600002",
                "name": "Hot",
                "catalystScore": 90.0,
                "articles": [{"catalystGrade": "S"}],
            },
            {
                "symbol": "600003",
                "name": "NoInd",
                "catalystScore": 90.0,
                "articles": [{"catalystGrade": "S"}],
            },
        ]
    }
    out, rejected = wa.compute_alpha_additions(
        catalyst_payload=payload,
        industry_by_symbol={
            "CN:600000": "银行",
            "CN:600001": "纺织服装",
            "CN:600002": "半导体",
        },
        top_industries={"半导体", "电子元件"},
    )
    assert [x["symbol"] for x in out] == ["CN:600002"]
    assert rejected.get("defense_sector") == 1
    assert rejected.get("not_in_top10") == 1
    assert rejected.get("missing_industry") == 1


def test_compute_alpha_additions_top10_fail_open_when_empty() -> None:
    payload = {
        "items": [
            {
                "symbol": "600002",
                "name": "Hot",
                "catalystScore": 90.0,
                "articles": [{"catalystGrade": "S"}],
            },
        ]
    }
    out, rejected = wa.compute_alpha_additions(
        catalyst_payload=payload,
        industry_by_symbol={"CN:600002": "半导体"},
        top_industries=set(),
    )
    assert len(out) == 1
    assert "not_in_top10" not in rejected


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


def test_record_score_snapshots_returns_rows(monkeypatch) -> None:
    fixture_rows = [
        {
            "symbol": "CN:600000",
            "asOfDate": "2026-06-18",
            "score": 72.0,
            "values": {"emIndustry": "Banking"},
        }
    ]
    monkeypatch.setattr(wa, "compute_trendok_for_symbols", lambda symbols, realtime=False: fixture_rows)
    monkeypatch.setattr(wa, "upsert_score_daily", lambda rows: len(rows))

    trade_date, count, rows = wa.record_score_snapshots(["CN:600000"])

    assert trade_date == "2026-06-18"
    assert count == 1
    assert rows == fixture_rows


def test_run_watchlist_automation_computes_trendok_once(monkeypatch) -> None:
    compute_calls: list[tuple[list[str], bool]] = []

    def fake_compute(symbols: list[str], realtime: bool = False) -> list[dict]:
        compute_calls.append((symbols, realtime))
        return [
            {
                "symbol": "CN:600000",
                "asOfDate": "2026-06-18",
                "score": 20.0,
                "values": {"emIndustry": "Coal"},
            },
            {
                "symbol": "CN:600001",
                "asOfDate": "2026-06-18",
                "score": 85.0,
                "values": {"emIndustry": "Tech"},
            },
        ]

    monkeypatch.setattr(wa, "compute_trendok_for_symbols", fake_compute)
    monkeypatch.setattr(wa, "sync_cn_industry_fund_flow", lambda **kwargs: {"ok": True})
    monkeypatch.setattr(wa, "_sync_screeners_step", lambda **kwargs: {"ok": True})
    monkeypatch.setattr(
        wa,
        "list_registry",
        lambda: [
            {"symbol": "CN:600000", "source": "manual"},
            {"symbol": "CN:600001", "source": "manual"},
        ],
    )
    monkeypatch.setattr(wa, "upsert_score_daily", lambda rows: len(rows))
    monkeypatch.setattr(wa, "insert_automation_run", lambda **kwargs: "run-1")
    monkeypatch.setattr(wa, "get_top_5d_industry_names", lambda as_of_date=None, top_n=5: set())
    monkeypatch.setattr(
        wa,
        "get_last_n_trading_dates",
        lambda n, end=None: ["2026-06-16", "2026-06-17", "2026-06-18"],
    )
    monkeypatch.setattr(
        wa,
        "list_catalyst_stocks",
        lambda limit=200: {
            "items": [
                {
                    "symbol": "600000",
                    "name": "Weak Alpha",
                    "catalystScore": 90.0,
                    "articles": [{"catalystGrade": "A"}],
                }
            ]
        },
    )
    monkeypatch.setattr(wa, "_resolve_em_industries_for_symbols", lambda symbols: {})

    def fake_scores(symbol: str, trade_dates: list[str]) -> list[dict]:
        return [{"trade_date": d, "score": 20.0, "industry": "Coal"} for d in trade_dates]

    monkeypatch.setattr(wa, "get_scores_for_symbol", fake_scores)

    result = wa.run_watchlist_automation(trigger="manual", force=True)

    assert len(compute_calls) == 1
    assert compute_calls[0][0] == ["CN:600000", "CN:600001"]
    assert compute_calls[0][1] is False
    assert result["meta"]["scoreSnapshots"] == 2
    assert result["meta"]["alphaSSymbols"] == 0
    assert result["meta"]["alphaRejected"]["no_s_grade"] == 1
    assert result["alphaAdd"] == []
    assert "remove" in result
    assert isinstance(result["remove"], list)
