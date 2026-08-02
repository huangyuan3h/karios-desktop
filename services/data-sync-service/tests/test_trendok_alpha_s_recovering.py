"""V6.3 Alpha S TrendOK recovering accelerator."""

from __future__ import annotations

from data_sync_service.service.trendok import (
    ALPHA_S_RECOVERING_SCORE_FLOOR,
    ALPHA_S_RECOVERING_SCORE_PART,
    apply_alpha_s_trend_recovering,
)


def _base_res(*, trend_ok: bool = False, score: float = 0.0) -> dict:
    return {
        "trendOk": trend_ok,
        "score": score,
        "scoreParts": {},
        "checks": {},
        "values": {},
    }


def _vols_with_surge(mult: float = 2.5) -> list[float]:
    """10 prior days at 100, today at 100*mult."""
    return [100.0] * 10 + [100.0 * mult]


def test_alpha_s_recovering_triggers_on_volume_and_bullish() -> None:
    res = _base_res(trend_ok=False, score=0.0)
    closes = [10.0] * 10 + [11.0]  # up day
    opens = [10.0] * 10 + [10.2]
    vols = _vols_with_surge(2.6)
    apply_alpha_s_trend_recovering(
        res, closes=closes, opens=opens, vols=vols, is_alpha_s=True
    )
    assert res["trendStatus"] == "recovering"
    assert res["trendOk"] is True
    assert float(res["score"]) >= ALPHA_S_RECOVERING_SCORE_FLOOR
    assert res["checks"]["alphaSTrendRecovering"] is True
    assert res["scoreParts"][ALPHA_S_RECOVERING_SCORE_PART] == ALPHA_S_RECOVERING_SCORE_FLOOR
    assert res["values"]["volVsAvg10"] >= 2.5


def test_alpha_s_recovering_skips_non_s() -> None:
    res = _base_res(trend_ok=False, score=0.0)
    closes = [10.0] * 10 + [11.0]
    opens = [10.0] * 10 + [10.2]
    vols = _vols_with_surge(3.0)
    apply_alpha_s_trend_recovering(
        res, closes=closes, opens=opens, vols=vols, is_alpha_s=False
    )
    assert res["trendStatus"] == "no"
    assert res["trendOk"] is False
    assert float(res["score"]) == 0.0
    assert res["checks"]["alphaSTrendRecovering"] is False


def test_alpha_s_recovering_skips_low_volume() -> None:
    res = _base_res(trend_ok=False, score=5.0)
    closes = [10.0] * 10 + [11.0]
    opens = [10.0] * 10 + [10.2]
    vols = _vols_with_surge(2.0)  # below 2.5×
    apply_alpha_s_trend_recovering(
        res, closes=closes, opens=opens, vols=vols, is_alpha_s=True
    )
    assert res["trendStatus"] == "no"
    assert res["trendOk"] is False
    assert float(res["score"]) == 5.0


def test_alpha_s_recovering_skips_bearish_candle() -> None:
    res = _base_res(trend_ok=False, score=0.0)
    closes = [10.0] * 10 + [9.5]  # down day
    opens = [10.0] * 10 + [10.2]
    vols = _vols_with_surge(3.0)
    apply_alpha_s_trend_recovering(
        res, closes=closes, opens=opens, vols=vols, is_alpha_s=True
    )
    assert res["trendStatus"] == "no"
    assert res["checks"]["alphaSTrendRecovering"] is False


def test_alpha_s_recovering_raises_existing_score_floor_only() -> None:
    res = _base_res(trend_ok=False, score=45.0)
    closes = [10.0] * 10 + [11.0]
    opens = [10.0] * 10 + [10.2]
    vols = _vols_with_surge(2.5)
    apply_alpha_s_trend_recovering(
        res, closes=closes, opens=opens, vols=vols, is_alpha_s=True
    )
    assert float(res["score"]) == ALPHA_S_RECOVERING_SCORE_FLOOR

    res2 = _base_res(trend_ok=True, score=72.0)
    apply_alpha_s_trend_recovering(
        res2, closes=closes, opens=opens, vols=vols, is_alpha_s=True
    )
    assert float(res2["score"]) == 72.0
    assert res2["trendStatus"] == "recovering"


def test_trend_status_ok_when_healthy_non_recovering() -> None:
    res = _base_res(trend_ok=True, score=88.0)
    closes = [10.0] * 11
    opens = [10.0] * 11
    vols = [100.0] * 11
    apply_alpha_s_trend_recovering(
        res, closes=closes, opens=opens, vols=vols, is_alpha_s=False
    )
    assert res["trendStatus"] == "ok"
    assert res["trendOk"] is True
