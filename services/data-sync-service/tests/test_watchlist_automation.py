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


def test_load_catalyst_window_s_exempt_uses_full_window(monkeypatch) -> None:
    """S-exemption must not be limited to the score-ranked add slice (TIP-005)."""
    many = [
        {
            "symbol": f"{600000 + i}",
            "name": f"S{i}",
            "catalystScore": float(100 - i),
            "articles": [{"catalystGrade": "S"}],
        }
        for i in range(5)
    ]
    monkeypatch.setattr(wa, "default_max_age_days", lambda: 30)

    def _fake_fetch(max_age_days: int = 30):
        return []

    monkeypatch.setattr(
        "data_sync_service.db.alpha_radar.fetch_trends_for_catalyst",
        _fake_fetch,
    )
    monkeypatch.setattr(wa, "aggregate_catalyst_stocks", lambda trends: many)

    add_payload, alpha_s = wa.load_catalyst_window(add_limit=2)
    assert len(add_payload["items"]) == 2
    assert add_payload["total"] == 5
    assert alpha_s == {f"CN:{600000 + i}" for i in range(5)}


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
    assert wa.is_defense_sector("电力设备") is False
    assert wa.is_defense_sector("电力") is True
    assert wa.is_defense_sector("水力发电") is False


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
        industry_by_symbol={"CN:600001": "电子"},
        top_industries={"电子", "计算机"},
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
                "name": "Cold SW",
                "catalystScore": 90.0,
                "articles": [{"catalystGrade": "S"}],
            },
            {
                "symbol": "600002",
                "name": "Hot SW",
                "catalystScore": 90.0,
                "articles": [{"catalystGrade": "S"}],
            },
            {
                "symbol": "600003",
                "name": "NoInd",
                "catalystScore": 90.0,
                "articles": [{"catalystGrade": "S"}],
            },
            {
                "symbol": "600004",
                "name": "EM granular",
                "catalystScore": 90.0,
                "articles": [{"catalystGrade": "S"}],
            },
        ]
    }
    out, rejected = wa.compute_alpha_additions(
        catalyst_payload=payload,
        industry_by_symbol={
            "CN:600000": "银行",
            "CN:600001": "纺织服饰",
            "CN:600002": "电子",
            "CN:600004": "半导体",
        },
        top_industries={"电子", "计算机"},
    )
    # 电子 in Top10; 半导体 is non-SW EM → Top10 fail-open; 银行 defense; 纺织服饰 SW not in Top10
    assert {x["symbol"] for x in out} == {"CN:600002", "CN:600004"}
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


def test_compute_alpha_additions_score_min_lower_floor_research() -> None:
    """TIP-012: research channel passes its own lower floor (70) and keeps
    the channel tag for the frontend registry source."""
    payload = {
        "items": [
            {
                "symbol": "600000",
                "name": "Buy Rated",
                "catalystScore": 80.0,
                "channel": "research",
                "articles": [{"catalystGrade": "S"}],
            },
            {
                "symbol": "600001",
                "name": "Mid",
                "catalystScore": 65.0,
                "channel": "research",
                "articles": [{"catalystGrade": "S"}],
            },
        ]
    }
    out, rejected = wa.compute_alpha_additions(
        catalyst_payload=payload,
        industry_by_symbol={"CN:600000": "电子", "CN:600001": "电子"},
        top_industries={"电子"},
        score_min=70.0,
    )
    assert len(out) == 1
    assert out[0]["symbol"] == "CN:600000"
    assert out[0]["channel"] == "research"
    assert rejected.get("low_score") == 1


def test_compute_alpha_additions_default_floor_rejects_research_low() -> None:
    """Without score_min, an 80 research score is still below the 85 floor."""
    payload = {
        "items": [
            {
                "symbol": "600000",
                "name": "Buy Rated",
                "catalystScore": 80.0,
                "channel": "research",
                "articles": [{"catalystGrade": "S"}],
            },
        ]
    }
    out, rejected = wa.compute_alpha_additions(
        catalyst_payload=payload,
        industry_by_symbol={"CN:600000": "电子"},
        top_industries={"电子"},
    )
    assert len(out) == 0
    assert rejected.get("low_score") == 1


def test_compute_alpha_additions_non_research_channel_null() -> None:
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
    out, _ = wa.compute_alpha_additions(
        catalyst_payload=payload,
        industry_by_symbol={"CN:600002": "半导体"},
        top_industries=set(),
    )
    assert len(out) == 1
    assert out[0].get("channel") is None


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


def test_merge_funnel_into_meta_preserves_alpha_rejected() -> None:
    from data_sync_service.db.watchlist_automation import merge_funnel_into_meta

    meta = {"alphaRejected": {"defense_sector": 2}, "trigger": "manual"}
    funnel = {
        "tvHit": 10,
        "passPullback": 3,
        "passTrendOk": 1,
        "addedNew": 1,
        "droppedByPullback": 7,
    }
    merged = merge_funnel_into_meta(meta, funnel)
    assert merged["alphaRejected"] == {"defense_sector": 2}
    assert merged["funnel"]["tvHit"] == 10
    assert merge_funnel_into_meta(meta, None) == meta


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
        "load_catalyst_window",
        lambda add_limit=200: (
            {
                "items": [
                    {
                        "symbol": "600000",
                        "name": "Weak Alpha",
                        "catalystScore": 90.0,
                        "articles": [{"catalystGrade": "A"}],
                    }
                ],
                "total": 1,
            },
            set(),
        ),
    )
    monkeypatch.setattr(wa, "_resolve_em_industries_for_symbols", lambda symbols: {})
    monkeypatch.setattr(
        wa,
        "build_research_catalyst_payload",
        lambda limit=100: {"stalenessBasis": "test", "maxAgeDays": 14, "total": 0, "items": []},
    )

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


def test_list_fallback_universe_skips_defense_and_caps(monkeypatch) -> None:
    monkeypatch.setattr(
        wa,
        "get_top_5d_industry_names_ordered",
        lambda as_of_date=None, top_n=5: ["银行", "电子", "计算机"],
    )

    def fake_search(keyword: str, *, limit: int = 12) -> list[dict]:
        if keyword == "银行":
            return [{"symbol": "CN:601988", "name": "Bank"}]
        if keyword == "电子":
            return [
                {"symbol": f"CN:30000{i}", "name": f"E{i}"} for i in range(min(limit, 5))
            ]
        if keyword == "计算机":
            return [
                {"symbol": f"CN:68800{i}", "name": f"C{i}"} for i in range(min(limit, 5))
            ]
        return []

    monkeypatch.setattr(
        "data_sync_service.db.stock_eastmoney_industry.search_stocks_by_industry_keyword",
        fake_search,
    )
    out = wa.list_fallback_universe_symbols(max_total=7, per_industry=5)
    assert out["industries"] == ["电子", "计算机"]
    assert out["skippedDefense"] == ["银行"]
    assert "CN:601988" not in out["symbols"]
    assert out["count"] == 7
    assert out["truncated"] is True
    assert len(out["symbols"]) == len(set(out["symbols"]))


def test_list_fallback_universe_empty_top5(monkeypatch) -> None:
    monkeypatch.setattr(wa, "get_top_5d_industry_names_ordered", lambda as_of_date=None, top_n=5: [])
    out = wa.list_fallback_universe_symbols()
    assert out["symbols"] == []
    assert out["count"] == 0
    assert out["truncated"] is False


def test_get_top_5d_industry_names_empty_when_no_flow_date(monkeypatch) -> None:
    monkeypatch.setattr(wa, "resolve_effective_as_of", lambda x: None)
    assert wa.get_top_5d_industry_names(as_of_date="2026-06-18") == set()
    assert wa.get_top_5d_industry_names_ordered(as_of_date="2026-06-18") == []


def test_get_top_5d_industry_names_ordered_rank(monkeypatch) -> None:
    monkeypatch.setattr(
        wa, "resolve_effective_as_of", lambda x: "2026-06-18"
    )
    monkeypatch.setattr(
        wa, "trade_dates_upto", lambda d, n, fallback_dates_fn=None: ["2026-06-12", "2026-06-18"]
    )
    monkeypatch.setattr(
        wa,
        "get_sum_by_industry_for_dates",
        lambda dates: [
            {"industry_name": " 银行 "},
            {"industry_name": ""},
            {"industry_name": "半导体"},
        ],
    )
    out = wa.get_top_5d_industry_names_ordered(as_of_date="2026-06-18", top_n=5)
    assert out == ["银行", "半导体"]
    assert wa.get_top_5d_industry_names(as_of_date="2026-06-18") == {"银行", "半导体"}


def test_list_fallback_universe_filters_defense_and_caps(monkeypatch) -> None:
    monkeypatch.setattr(
        wa,
        "get_top_5d_industry_names_ordered",
        lambda as_of_date=None, top_n=5: ["半导体", "电力", "电子", "煤炭"],
    )

    def fake_search(keyword: str, limit: int) -> list[dict]:
        if keyword == "半导体":
            return [{"symbol": f"CN:30000{i}", "name": f"s{i}"} for i in range(3)]
        if keyword == "电子":
            return [{"symbol": f"CN:00000{i}", "name": f"b{i}"} for i in range(5)]
        if keyword == "煤炭":
            return [{"symbol": f"CN:60000{i}", "name": f"m{i}"} for i in range(4)]
        return []

    monkeypatch.setattr(
        "data_sync_service.db.stock_eastmoney_industry.search_stocks_by_industry_keyword",
        fake_search,
    )
    out = wa.list_fallback_universe_symbols(max_total=6, per_industry=4, top_n=5)
    assert "电力" in out["skippedDefense"]
    assert out["industries"] == ["半导体", "电子"]
    assert out["truncated"] is True
    assert len(out["symbols"]) == 6
    assert out["count"] == 6


def test_cn_symbol_to_ts_code_brushes() -> None:
    assert wa._cn_symbol_to_ts_code("CN:600000") == "600000.SH"
    assert wa._cn_symbol_to_ts_code("CN:000021") == "000021.SZ"
    assert wa._cn_symbol_to_ts_code("HK:700") == "00700.HK"
    assert wa._cn_symbol_to_ts_code("ETF:510300") == "510300.SH"
    assert wa._cn_symbol_to_ts_code("ETF:159915") == "159915.SZ"
    assert wa._cn_symbol_to_ts_code("CN:12345") is None
    assert wa._cn_symbol_to_ts_code("ETF:ABC123") is None
    assert wa._cn_symbol_to_ts_code("") is None


def test_record_score_snapshots_skips_invalid_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        wa,
        "compute_trendok_for_symbols",
        lambda symbols, realtime=False: [
            {"symbol": "CN:600000", "asOfDate": "2026-06-18", "score": 20.0, "values": {"emIndustry": "银行"}},
            {"symbol": "", "asOfDate": "2026-06-18", "score": 10.0},  # no symbol → skip
            {"symbol": "CN:600001", "asOfDate": "", "score": 30.0},   # no asOfDate → today
            "not-a-dict",                                             # skip
        ],
    )
    captured: list[list[dict]] = []
    monkeypatch.setattr(wa, "upsert_score_daily", lambda rows: captured.append(rows) or len(rows))
    td, count, rows = wa.record_score_snapshots(["CN:600000", "CN:600001"])
    assert td == "2026-06-18"
    assert count == 2
    assert captured[0][0]["industry"] == "银行"
    assert captured[0][1]["trade_date"] == wa._shanghai_today_iso()


def test_run_watchlist_automation_research_channel(monkeypatch) -> None:
    monkeypatch.setattr(wa, "compute_trendok_for_symbols", lambda symbols, realtime=False: [])
    monkeypatch.setattr(wa, "sync_cn_industry_fund_flow", lambda **kwargs: {"ok": True})
    monkeypatch.setattr(wa, "_sync_screeners_step", lambda **kwargs: {"ok": True})
    monkeypatch.setattr(wa, "list_registry", lambda: [])
    monkeypatch.setattr(wa, "upsert_score_daily", lambda rows: 0)
    monkeypatch.setattr(wa, "insert_automation_run", lambda **kwargs: "run-r")
    monkeypatch.setattr(wa, "get_top_5d_industry_names", lambda as_of_date=None, top_n=5: {"银行"})
    monkeypatch.setattr(
        wa, "get_last_n_trading_dates", lambda n, end=None: ["2026-06-16", "2026-06-17", "2026-06-18"]
    )
    monkeypatch.setattr(
        wa,
        "load_catalyst_window",
        lambda add_limit=200: ({"items": [], "total": 0}, set()),
    )
    monkeypatch.setattr(wa, "_resolve_em_industries_for_symbols", lambda symbols: {})
    monkeypatch.setattr(wa, "get_scores_for_symbol", lambda symbol, trade_dates: [])

    research_calls: dict = {}

    def fake_research(limit: int = 100) -> dict:
        research_calls["limit"] = limit
        return {
            "stalenessBasis": "test",
            "maxAgeDays": 14,
            "total": 1,
            "items": [
                {
                    "symbol": "CN:600999",
                    "name": "Research Pick",
                    "catalystScore": 88.0,
                    "industryName": "银行",
                    "articles": [{"catalystGrade": "S"}],
                }
            ],
        }

    monkeypatch.setattr(wa, "build_research_catalyst_payload", fake_research)

    added_calls: dict = {}

    def fake_additions(
        catalyst_payload=None,
        industry_by_symbol=None,
        top_industries=None,
        score_min=None,
        limit=200,
    ) -> tuple[list[dict], dict[str, int]]:
        added_calls["score_min"] = score_min
        return (
            [
                {
                    "symbol": "CN:600999",
                    "name": "Research Pick",
                    "catalystScore": 88.0,
                    "channel": "research",
                    "source": "ALPHA",
                }
            ],
            {},
        )

    monkeypatch.setattr(wa, "compute_alpha_additions", fake_additions)

    result = wa.run_watchlist_automation(trigger="scheduled", force=True)
    assert result["skipped"] is False
    assert research_calls["limit"] == 100
    assert result["meta"]["researchCandidates"] == 1
    assert result["meta"]["researchRejected"] == {}


def test_normalize_trade_date_variants() -> None:
    assert wa._normalize_trade_date(None) is None
    assert wa._normalize_trade_date("") is None
    assert wa._normalize_trade_date("20260618") == "2026-06-18"
    assert wa._normalize_trade_date("2026-06-18T00:00:00") == "2026-06-18"
    assert wa._normalize_trade_date("garbage") == "garbage"


def test_symbols_with_max_grade_s_ignores_non_dict_rows() -> None:
    payload = {
        "items": [
            {"symbol": "600000", "articles": [{"catalystGrade": "S"}]},
            "not-a-dict",
            {"symbol": "600001", "articles": [{"catalystGrade": "A"}]},
            {"symbol": "", "articles": [{"catalystGrade": "S"}]},
        ]
    }
    out = wa.symbols_with_max_grade_s(payload)
    assert out == {"CN:600000"}
    assert wa.symbols_with_max_grade_s(None) == set()
    assert wa.symbols_with_max_grade_s({"items": "not-a-list"}) == set()


def test_symbols_with_max_grade_s_article_not_dict() -> None:
    payload = {"items": [{"symbol": "600000", "articles": ["S", {"catalystGrade": "S"}]}]}
    out = wa.symbols_with_max_grade_s(payload)
    assert out == {"CN:600000"}


def test_compute_removals_wraps_registry_and_trendok(monkeypatch) -> None:
    """compute_removals: skips empty symbols, drives should_remove, filters."""
    calls: list[tuple] = []

    def fake_should(**kw):
        calls.append(kw)
        return kw.get("symbol") == "CN:600000", "score_low_3d_and_industry_outside_top5"

    monkeypatch.setattr(wa, "should_remove_symbol", fake_should)
    monkeypatch.setattr(wa, "_industry_from_trendok", lambda trend: trend.get("industry"))

    registry = [
        {"symbol": "CN:600000", "source": "manual", "positionPct": None},
        {"symbol": "", "source": "manual"},
        {"symbol": "CN:600001", "source": "alpha_radar", "positionPct": "bad-value"},
    ]
    trendok = {
        "CN:600000": {"industry": "半导体"},
        "CN:600001": {"industry": "银行"},
    }
    out = wa.compute_removals(
        registry,
        trade_dates=["2026-06-16"],
        top_5d_industries=set(),
        trendok_by_symbol=trendok,
        alpha_s_symbols={"CN:600001"},
    )
    assert out == [{"symbol": "CN:600000", "reason": "score_low_3d_and_industry_outside_top5"}]
    # only 2 calls: empty symbol skipped; the alpha_s symbol still evaluated
    assert len(calls) == 2
    assert calls[0]["symbol"] == "CN:600000"


def test_industry_from_trendok_em_preferred_then_row() -> None:
    assert wa._industry_from_trendok({"values": {"emIndustry": "半导体"}}) == "半导体"
    assert wa._industry_from_trendok({"values": {"em_industry": "银行"}, "industry": "煤炭"}) == "银行"
    assert wa._industry_from_trendok({"values": {}, "industry": " 医药 "}) == "医药"
    assert wa._industry_from_trendok({"values": None, "industry": None}) is None


def test_resolve_em_industries_for_symbols_maps_cn_only(monkeypatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.eastmoney_industry.lookup_em_industries_for_ts_codes",
        lambda codes: {"600000.SH": " 银行 ", "00700.HK": "资讯科技"},
    )
    out = wa._resolve_em_industries_for_symbols(
        ["CN:600000", "HK:700", "ETF:510300", "bad-symbol"]
    )
    assert out == {"CN:600000": "银行", "HK:700": "资讯科技"}
    # ETF:510300 / bad-symbol are dropped by _cn_symbol_to_ts_code


def test_precheck_skips_on_non_trading_day(monkeypatch) -> None:
    monkeypatch.setattr(wa, "_cn_today", lambda: __import__("datetime").date(2026, 8, 8))
    monkeypatch.setattr(wa, "is_trading_day", lambda ex, d: False)
    assert wa._precheck(force=False) == (True, "not_trading_day")
    assert wa._precheck(force=True) == (False, None)


def test_precheck_waits_for_close_sync(monkeypatch) -> None:
    monkeypatch.setattr(wa, "_cn_today", lambda: __import__("datetime").date(2026, 8, 7))
    monkeypatch.setattr(wa, "is_trading_day", lambda ex, d: True)
    monkeypatch.setattr(wa, "get_today_run", lambda job: None)
    assert wa._precheck(force=False) == (True, "close_sync_not_ready")


def test_get_automation_helpers_delegate(monkeypatch) -> None:
    monkeypatch.setattr(wa, "get_pending_run", lambda td: {"tradeDate": "2026-08-07"})
    assert wa.get_automation_pending("2026-08-07")["tradeDate"] == "2026-08-07"

    monkeypatch.setattr(
        "data_sync_service.db.watchlist_automation.get_latest_run", lambda: {"id": "r1"}
    )
    assert wa.get_automation_latest()["id"] == "r1"

    monkeypatch.setattr(
        "data_sync_service.db.watchlist_automation.list_recent_runs",
        lambda limit: [{"id": "r1"}],
    )
    assert wa.get_automation_runs(limit=3) == [{"id": "r1"}]

    monkeypatch.setattr(
        "data_sync_service.db.watchlist_automation.ack_run",
        lambda rid, screener_added=None, funnel=None: {"id": rid},
    )
    assert wa.ack_automation_run("r1", screener_added=1)["id"] == "r1"

    monkeypatch.setattr(wa, "get_run_by_id", lambda rid: {"id": rid})
    assert wa.get_automation_run("r1")["id"] == "r1"


def test_run_watchlist_automation_skipped_path(monkeypatch) -> None:
    monkeypatch.setattr(wa, "_precheck", lambda force=False: (True, "not_trading_day"))
    monkeypatch.setattr(wa, "insert_automation_run", lambda **kw: "run-s")
    out = wa.run_watchlist_automation(trigger="scheduled", force=False)
    assert out["skipped"] is True
    assert out["skipReason"] == "not_trading_day"
    assert out["remove"] == []


def test_run_watchlist_automation_industry_sync_failure_is_meta(monkeypatch) -> None:
    """Industry sync raising must land in meta, not crash the run."""
    monkeypatch.setattr(wa, "compute_trendok_for_symbols", lambda symbols, realtime=False: [])
    monkeypatch.setattr(
        wa, "sync_cn_industry_fund_flow", lambda **kw: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    monkeypatch.setattr(wa, "_sync_screeners_step", lambda **kw: {"ok": True, "failed": 0})
    monkeypatch.setattr(wa, "list_registry", lambda: [])
    monkeypatch.setattr(wa, "upsert_score_daily", lambda rows: 0)
    monkeypatch.setattr(wa, "insert_automation_run", lambda **kw: "run-2")
    monkeypatch.setattr(wa, "get_top_5d_industry_names", lambda as_of_date=None, top_n=5: set())
    monkeypatch.setattr(
        wa, "get_last_n_trading_dates", lambda n, end=None: ["2026-06-16", "2026-06-17", "2026-06-18"]
    )
    monkeypatch.setattr(
        wa, "load_catalyst_window", lambda add_limit=200: ({"items": [], "total": 0}, set())
    )
    monkeypatch.setattr(wa, "_resolve_em_industries_for_symbols", lambda symbols: {})
    monkeypatch.setattr(wa, "get_scores_for_symbol", lambda symbol, trade_dates: [])
    monkeypatch.setattr(
        wa, "build_research_catalyst_payload", lambda limit=100: {"items": [], "total": 0}
    )
    monkeypatch.setattr(
        wa,
        "compute_alpha_additions",
        lambda **kw: ([], {}),
    )

    out = wa.run_watchlist_automation(trigger="scheduled", force=True)
    assert out["skipped"] is False
    assert out["meta"]["industrySync"] == {"ok": False, "error": "boom"}
    assert out["meta"]["screenerSync"]["ok"] is True

def test_compute_rs_ranks_returns_percentiles(monkeypatch) -> None:
    """compute_rs_ranks maps symbols -> whole-market RS percentiles."""

    from data_sync_service.service import watchlist_automation as wa

    monkeypatch.setattr(wa, "get_connection", lambda: (_ for _ in ()).throw(AssertionError("db should not be hit")))

    def fake_parse(sym):
        return ("CN", "600001", "600001.SH") if sym == "CN:600001" else ("CN", "600002", "600002.SH")

    monkeypatch.setattr("data_sync_service.service.trendok._symbol_to_ts_code", fake_parse)

    class FakeCur:
        def __init__(self):
            self.n = 0

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params=None):
            self.n += 1

        def fetchone(self):
            return ("2026-08-07",)  # latest trade date

        def fetchall(self):
            return [("600001.SH", 10.0), ("600002.SH", -5.0)]  # ret20 rows

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def cursor(self):
            return FakeCur()

    monkeypatch.setattr(wa, "get_connection", lambda: FakeConn())
    ranks = wa.compute_rs_ranks(["CN:600001", "CN:600002"])
    assert ranks["CN:600001"] == 1.0  # strongest of the two
    assert ranks["CN:600002"] == 0.5
