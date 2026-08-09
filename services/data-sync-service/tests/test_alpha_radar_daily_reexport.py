"""service/alpha_radar_daily.py — pure re-export layer."""

from __future__ import annotations

import importlib


def test_re_exports() -> None:
    mod = importlib.import_module("data_sync_service.service.alpha_radar_daily")
    for name in ("daily_status", "pipeline_status", "run_alpha_radar_pipeline", "run_daily_generation"):
        assert name in mod.__all__
        assert callable(getattr(mod, name))
