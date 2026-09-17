"""service/zt_pool_snapshot.py coverage (fake akshare, no network/DB)."""

from __future__ import annotations

from datetime import date as _date

import pandas as pd

from data_sync_service.service import zt_pool_snapshot as mod


class _FakeAk:
    def __init__(self, frames: dict[str, pd.DataFrame]) -> None:
        self._frames = frames

    def __getattr__(self, name: str):  # noqa: ANN401
        if name in self._frames:
            return lambda date=None: self._frames[name]  # noqa: ARG005
        raise AttributeError(name)


class _TueDate(_date):
    @classmethod
    def today(cls) -> _TueDate:
        return cls(2026, 9, 15)  # Tuesday


def _fake_ak() -> _FakeAk:
    return _FakeAk(
        {
            "stock_zt_pool_em": pd.DataFrame({"代码": ["000001"], "封板资金": [1.0e8]}),
            "stock_zt_pool_zbgc_em": pd.DataFrame({"代码": ["000002"], "炸板次数": [2]}),
            "stock_zt_pool_strong_em": pd.DataFrame({"代码": ["000003"]}),
            "stock_zt_pool_previous_em": pd.DataFrame({"代码": ["000004"]}),
        }
    )


def test_snapshot_day_writes_and_skips(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("ZT_POOL_DIR", str(tmp_path))
    monkeypatch.setattr(mod, "_akshare", _fake_ak)

    first = mod.snapshot_day(_date(2026, 9, 15))
    assert first["ok"] is True and first["skipped"] is False
    assert first["rows"] == 4
    csv_path = tmp_path / "zt_pool_20260915.csv"
    assert csv_path.exists()
    text = csv_path.read_text(encoding="utf-8")
    assert "pool" in text.splitlines()[0]
    assert "zt" in text and "zbgc" in text and "strong" in text and "previous" in text

    second = mod.snapshot_day(_date(2026, 9, 15))
    assert second["ok"] is True and second["skipped"] is True


def test_snapshot_day_no_data_is_dict_error(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("ZT_POOL_DIR", str(tmp_path))
    monkeypatch.setattr(mod, "_akshare", lambda: _FakeAk({}))
    res = mod.snapshot_day(_date(2026, 9, 15))
    assert res["ok"] is False
    assert res["error"] == "no pool data"
    assert not (tmp_path / "zt_pool_20260915.csv").exists()


def test_snapshot_recent_skips_weekend(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("ZT_POOL_DIR", str(tmp_path))
    monkeypatch.setattr(mod, "_akshare", _fake_ak)
    monkeypatch.setattr(mod, "date", _TueDate)
    res = mod.snapshot_recent(days=3)
    assert res["ok"] is True
    assert res["saved"] == ["2026-09-15", "2026-09-14"]
    assert (tmp_path / "zt_pool_20260915.csv").exists()
    assert (tmp_path / "zt_pool_20260914.csv").exists()
