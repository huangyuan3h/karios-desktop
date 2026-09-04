"""Real read paths that are safe on empty AND full DB (no writes, no network).

These pin the empty-DB behavior CI depends on: fresh-migrated databases must
return shapes, not raise.
"""

from __future__ import annotations

from datetime import date

import pytest

pytestmark = pytest.mark.requires_postgres


def test_recon_latest_empty_safe() -> None:
    from data_sync_service.db.reconciliation import latest_recon

    assert latest_recon(limit=2) == [] or isinstance(latest_recon(limit=2), list)


def test_behavior_latest_empty_safe() -> None:
    from data_sync_service.db.behavior_audit import latest_audit

    rows = latest_audit(limit=2)
    assert isinstance(rows, list)


def test_today_iso_shape() -> None:
    from data_sync_service.db.paper_trading import today_iso

    assert today_iso() == date.today().isoformat()


def test_multi_asset_symbol_forms() -> None:
    from data_sync_service.service.multi_asset_sleeve import is_multi_asset_symbol

    assert is_multi_asset_symbol("ETF:513100") is True
    assert is_multi_asset_symbol("513110.SH") is True
    assert is_multi_asset_symbol("513100") is True
    assert is_multi_asset_symbol("CN:600000") is False
    assert is_multi_asset_symbol("") is False


def test_factor_signals_read_paths() -> None:
    from data_sync_service.api.factor_routes import get_signals

    far = get_signals(trade_date="2099-01-05")
    assert far["asOfDate"] == "2099-01-05" and far["signals"] == []
    latest = get_signals(trade_date=None)
    assert set(latest) == {"asOfDate", "signals"}
    assert isinstance(latest["signals"], list)
