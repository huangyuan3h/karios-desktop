"""OPT-206: Live parking universe/constants must single-source `harbor`.

The Watchlist path (`multi_asset_sleeve` -> portfolio_health -> UI) used to
carry its own ETF universe, its own 8.0 trail constant and its own
symbol->key map. They could (and did) drift from the frozen engine. These
tests pin the single-source contract.
"""

from __future__ import annotations

from data_sync_service.service import harbor
from data_sync_service.service import multi_asset_sleeve as mas


def test_candidates_mirror_harbor_universe() -> None:
    ts_codes = {c["ts"] for c in mas.CANDIDATES}
    assert ts_codes == {*harbor.MULTI_TS.values(), *harbor.NASDAQ_ALIASES}
    assert {c["key"] for c in mas.CANDIDATES} == set(harbor.MULTI_TS)
    for c in mas.CANDIDATES:
        assert c["symbol"] == f"ETF:{c['ts'].split('.')[0]}"
        assert c["name"] == harbor.NAMES.get(c["key"], c["key"])


def test_trail_pct_single_source() -> None:
    assert mas.TRAILING_PCT == harbor.TRAIL_PCT == 8.0


def test_multi_key_for_symbol_shapes() -> None:
    assert mas.multi_key_for_symbol("ETF:518880") == "GOLD"
    assert mas.multi_key_for_symbol("518880.SH") == "GOLD"
    assert mas.multi_key_for_symbol("513100") == "NASDAQ"
    assert mas.multi_key_for_symbol("ETF:513100") == "NASDAQ"
    assert mas.multi_key_for_symbol("513350.SH") == "OIL"
    assert mas.multi_key_for_symbol("511260") == "BOND10"
    assert mas.multi_key_for_symbol("CN:600000") is None
    assert mas.multi_key_for_symbol("") is None
    assert mas.multi_key_for_symbol(None) is None


def test_is_multi_asset_symbol_keeps_legacy_shapes() -> None:
    assert mas.is_multi_asset_symbol("ETF:513100") is True
    assert mas.is_multi_asset_symbol("513110.SH") is True
    assert mas.is_multi_asset_symbol("513500") is True
    assert mas.is_multi_asset_symbol("CN:600000") is False
    assert mas.is_multi_asset_symbol("") is False
