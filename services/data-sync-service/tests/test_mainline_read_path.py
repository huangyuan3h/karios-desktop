"""Read-path tests for industry mainline (OPT-022)."""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service.mainline import (  # type: ignore[import-not-found]
    get_cn_industry_mainline,
    sync_cn_industry_mainline,
)


def test_get_cn_industry_mainline_does_not_compute_metrics() -> None:
    with (
        patch(
            "data_sync_service.service.mainline.flow_latest_date",
            return_value="2024-06-18",
        ),
        patch(
            "data_sync_service.service.mainline._trade_dates_upto",
            return_value=["2024-06-18"],
        ),
        patch(
            "data_sync_service.service.mainline.ensure_metrics_for_dates",
        ) as ensure_metrics,
        patch(
            "data_sync_service.service.mainline.ensure_scores_for_dates",
        ) as ensure_scores,
        patch(
            "data_sync_service.service.mainline._compute_industry_metrics_for_date",
        ) as compute_metrics,
        patch(
            "data_sync_service.service.mainline.scores_rows_by_date",
            return_value=[],
        ),
    ):
        out = get_cn_industry_mainline()

    ensure_metrics.assert_not_called()
    ensure_scores.assert_not_called()
    compute_metrics.assert_not_called()
    assert out.get("warning") == "scores_not_ready"
    assert out.get("allScores") == []


def test_sync_cn_industry_mainline_still_ensures_metrics() -> None:
    with (
        patch(
            "data_sync_service.service.mainline.flow_latest_date",
            return_value="2024-06-18",
        ),
        patch(
            "data_sync_service.service.mainline._trade_dates_upto",
            return_value=["2024-06-18"],
        ),
        patch(
            "data_sync_service.service.mainline.ensure_metrics_for_dates",
            return_value={"ensured": 1},
        ) as ensure_metrics,
        patch(
            "data_sync_service.service.mainline.ensure_scores_for_dates",
            return_value={"ensured": 1},
        ) as ensure_scores,
    ):
        out = sync_cn_industry_mainline()

    ensure_metrics.assert_called_once()
    ensure_scores.assert_called_once()
    assert out.get("ok") is True
