"""2026-10 datafix: margin completeness + hsgt caliber break guards."""

from __future__ import annotations

from data_sync_service.db import cn_risk_state


def test_margin_exchanges_constant() -> None:
    assert set(cn_risk_state.MARGIN_EXCHANGES) == {"SSE", "SZSE", "BSE"}


def test_hsgt_break_date_constant() -> None:
    assert cn_risk_state.HSGT_BREAK_DATE == "2024-08-19"


def test_is_hsgt_turnover_date() -> None:
    assert cn_risk_state.is_hsgt_turnover_date("2024-08-18") is False
    assert cn_risk_state.is_hsgt_turnover_date("2024-08-19") is True
    assert cn_risk_state.is_hsgt_turnover_date("2026-09-30") is True


def test_sync_margin_total_reports_incomplete_days() -> None:
    """A day with only SSE rows must surface as incomplete (not silent ok)."""
    import pandas as pd

    from data_sync_service.service import cn_risk_state_sync as mod

    def _df(td):
        if td == "20260930":
            return pd.DataFrame(
                [
                    {
                        "trade_date": "20260930",
                        "exchange_id": "SSE",
                        "rzye": 1.0,
                        "rzmre": 0.0,
                        "rzche": 0.0,
                        "rqye": 0.0,
                        "rqmcl": 0.0,
                        "rzrqye": 1.0,
                        "rqyl": 0.0,
                    }
                ]
            )
        return pd.DataFrame(
            [
                {
                    "trade_date": td,
                    "exchange_id": ex,
                    "rzye": 1.0,
                    "rzmre": 0.0,
                    "rzche": 0.0,
                    "rqye": 0.0,
                    "rqmcl": 0.0,
                    "rzrqye": 1.0,
                    "rqyl": 0.0,
                }
                for ex in ("SSE", "SZSE", "BSE")
            ]
        )

    class _Pro:
        def trade_cal(self, **kwargs):
            return pd.DataFrame({"cal_date": ["20260929", "20260930"]})

        def margin(self, trade_date=None):
            return _df(trade_date)

    calls: list[int] = []
    orig_upsert = cn_risk_state.upsert_margin_total
    try:
        cn_risk_state.upsert_margin_total = lambda rows: calls.append(len(rows)) or len(rows)  # type: ignore[method-assign]
        out = mod.sync_margin_total(
            "2026-09-29",
            "2026-09-30",
            pro_factory=lambda: _Pro(),
            sleep=lambda s: None,
        )
    finally:
        cn_risk_state.upsert_margin_total = orig_upsert  # type: ignore[method-assign]
    assert out["ok"] is True
    assert "2026-09-30" in out["incomplete_dates"]
    assert "2026-09-29" not in out["incomplete_dates"]
