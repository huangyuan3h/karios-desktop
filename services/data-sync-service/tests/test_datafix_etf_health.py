"""2026-10 datafix: ETF snapshot service + health-check script structure."""

from __future__ import annotations

import csv
from pathlib import Path


def test_etf_snapshot_universe_matches_script() -> None:
    from data_sync_service.service import etf_snapshot

    import ast

    script = Path(__file__).resolve().parent.parent / "scripts" / "sync_etf_daily.py"
    tree = ast.parse(script.read_text(encoding="utf-8"))
    script_codes: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict):
            for k in node.keys:
                if isinstance(k, ast.Constant) and isinstance(k.value, str) and "." in k.value:
                    script_codes.add(k.value)
    assert set(etf_snapshot.UNIVERSE) == script_codes


def test_etf_snapshot_refresh_window_merges_incrementally(tmp_path, monkeypatch) -> None:
    import pandas as pd

    from data_sync_service.service import etf_snapshot

    csv_path = tmp_path / "etf_daily.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=etf_snapshot.FIELDS)
        w.writeheader()
        w.writerow(
            {
                "ts_code": "510300.SH",
                "trade_date": "20260911",
                "open": "1",
                "high": "1",
                "low": "1",
                "close": "4.0",
                "pre_close": "4.0",
                "vol": "1",
                "amount": "1",
                "adj_factor": "1.0",
                "close_adj": "4.0",
            }
        )
    monkeypatch.setattr(etf_snapshot, "CSV_PATH", csv_path)

    class _Pro:
        def fund_daily(self, ts_code=None, **kwargs):
            if ts_code != "510300.SH":
                return pd.DataFrame([])
            return pd.DataFrame(
                [
                    {
                        "trade_date": "20260912",
                        "open": 4.1,
                        "high": 4.2,
                        "low": 4.0,
                        "close": 4.15,
                        "pre_close": 4.0,
                        "vol": 10.0,
                        "amount": 41.5,
                    }
                ]
            )

        def fund_adj(self, ts_code=None, **kwargs):
            if ts_code != "510300.SH":
                return pd.DataFrame([])
            return pd.DataFrame([{"trade_date": "20260912", "adj_factor": 1.0}])

    out = etf_snapshot.refresh_window(
        "20260912", "20260912", pro_factory=lambda: _Pro(), sleep=lambda s: None
    )
    assert out["ok"] is True
    with csv_path.open(newline="", encoding="utf-8") as fh:
        rows = {(r["ts_code"], r["trade_date"]): r for r in csv.DictReader(fh)}
    # History kept, new day merged.
    assert ("510300.SH", "20260911") in rows
    assert ("510300.SH", "20260912") in rows
    assert rows[("510300.SH", "20260912")]["close_adj"] == "4.15"


def test_data_health_check_tables_cover_key_series() -> None:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
    import data_health_check as dhc

    names = [t for t, _ in dhc.KEY_TABLES]
    for must in (
        "daily",
        "amp_1430",
        "bar_5min",
        "cn_etf_share",
        "index_daily",
        "cn_moneyflow_hsgt",
        "cn_hk_hold",
        "cn_margin_total",
        "cn_margin_detail",
        "cn_moneyflow",
    ):
        assert must in names
