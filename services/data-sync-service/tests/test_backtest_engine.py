from data_sync_service.testback.engine import (  # type: ignore[import-not-found]
    BacktestParams,
    DailyRuleFilter,
    ScoreConfig,
    UniverseFilter,
    run_backtest,
)
from data_sync_service.testback.strategies.sample_momentum import (  # type: ignore[import-not-found]
    SampleMomentumStrategy,
)


def test_run_backtest_basic(monkeypatch) -> None:
    rows = [
        {
            "ts_code": "000001.SZ",
            "trade_date": "2024-01-02",
            "open": 10,
            "high": 11,
            "low": 9,
            "close": 10,
            "pre_close": 9.5,
            "change": 0.5,
            "pct_chg": 5,
            "vol": 1000,
            "amount": 10000,
            "adj_factor": 1.0,
        },
        {
            "ts_code": "000001.SZ",
            "trade_date": "2024-01-03",
            "open": 11,
            "high": 12,
            "low": 10,
            "close": 11,
            "pre_close": 10,
            "change": 1,
            "pct_chg": 10,
            "vol": 1200,
            "amount": 12000,
            "adj_factor": 1.0,
        },
        {
            "ts_code": "000002.SZ",
            "trade_date": "2024-01-02",
            "open": 8,
            "high": 9,
            "low": 7,
            "close": 8,
            "pre_close": 7.8,
            "change": 0.2,
            "pct_chg": 2.5,
            "vol": 900,
            "amount": 8000,
            "adj_factor": 1.0,
        },
        {
            "ts_code": "000002.SZ",
            "trade_date": "2024-01-03",
            "open": 8.5,
            "high": 9.2,
            "low": 8.1,
            "close": 8.8,
            "pre_close": 8,
            "change": 0.8,
            "pct_chg": 10,
            "vol": 950,
            "amount": 8200,
            "adj_factor": 1.0,
        },
    ]

    import data_sync_service.testback.engine as engine  # type: ignore[import-not-found]

    monkeypatch.setattr(engine, "build_universe", lambda **kwargs: ["000001.SZ", "000002.SZ"])
    monkeypatch.setattr(engine, "fetch_daily_for_codes", lambda *args, **kwargs: rows)

    result = run_backtest(
        strategy_cls=SampleMomentumStrategy,
        params=BacktestParams(start_date="2024-01-02", end_date="2024-01-03", initial_cash=1.0),
        universe_filter=UniverseFilter(),
        daily_rules=DailyRuleFilter(),
        score_cfg=ScoreConfig(top_n=1),
    )

    assert "summary" in result
    assert "equity_curve" in result
    assert len(result["equity_curve"]) == 2
    assert result["summary"]["final_equity"] >= 0
