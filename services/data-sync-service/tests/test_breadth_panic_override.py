from data_sync_service.service import dashboard
from data_sync_service.service.market_sentiment import (
    BREADTH_DECLINE_RED_THRESHOLD,
    apply_breadth_panic_index_signals,
    apply_breadth_panic_risk_mode,
    apply_breadth_panic_sentiment_items,
    breadth_panic_active,
)


def test_breadth_panic_active_threshold() -> None:
    assert not breadth_panic_active(BREADTH_DECLINE_RED_THRESHOLD - 1)
    assert breadth_panic_active(BREADTH_DECLINE_RED_THRESHOLD)


def test_apply_breadth_panic_risk_mode_overrides_hot() -> None:
    rules: list[str] = []
    out = apply_breadth_panic_risk_mode("hot", 3200, rules)
    assert out == "extreme_caution"
    assert any("breadth_panic" in r for r in rules)


def test_apply_breadth_panic_index_signals_forces_cn_red() -> None:
    signals = [
        {"name": "上证指数", "signal": "green", "positionRange": "50%-60%", "rules": []},
        {"name": "创业板指", "signal": "yellow", "positionRange": "30%", "rules": []},
        {"name": "恒生指数", "signal": "green", "positionRange": "50%-60%", "rules": []},
    ]
    out = apply_breadth_panic_index_signals(signals, 3100)
    assert out[0]["signal"] == "red"
    assert out[1]["signal"] == "red"
    assert out[2]["signal"] == "green"


def test_apply_breadth_panic_sentiment_items_updates_latest() -> None:
    items = [
        {"date": "2026-05-28", "riskMode": "normal", "rules": [], "downCount": 1200},
        {"date": "2026-05-29", "riskMode": "hot", "rules": ["hot"], "downCount": 3500},
    ]
    out = apply_breadth_panic_sentiment_items(items, 3500)
    assert out[0]["riskMode"] == "normal"
    assert out[1]["riskMode"] == "extreme_caution"
    assert any("breadth_panic" in r for r in out[1]["rules"])


def test_build_market_sentiment_bundle_applies_breadth_panic(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard,
        "list_sentiment_days",
        lambda **_: [
            {
                "date": "2026-05-29",
                "upCount": 1200,
                "downCount": 3800,
                "flatCount": 100,
                "riskMode": "normal",
                "rules": [],
            }
        ],
    )
    monkeypatch.setattr(
        dashboard,
        "get_index_signals",
        lambda **_: [
            {"name": "上证指数", "signal": "yellow", "positionRange": "30%", "rules": []},
            {"name": "创业板指", "signal": "green", "positionRange": "50%-60%", "rules": []},
        ],
    )

    out = dashboard._build_market_sentiment_bundle(as_of_date="2026-05-29", use_realtime_index=True)
    latest = out["items"][-1]
    assert latest["riskMode"] == "extreme_caution"
    assert out["indexSignals"][0]["signal"] == "red"
    assert out["indexSignals"][1]["signal"] == "red"
