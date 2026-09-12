"""cn_fin_statements sync — pool/DB seams mocked (no network, no Postgres)."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from data_sync_service import db as dbpkg
from data_sync_service.service import cn_fin_statements as mod


class _Cur:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows
        self.executed: list[tuple] = []

    def execute(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        self.executed.append(args)

    def fetchall(self) -> list[tuple]:
        return list(self._rows)

    def __enter__(self) -> _Cur:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False


class _Conn:
    def __init__(self, cur: _Cur) -> None:
        self._cur = cur

    def cursor(self) -> _Cur:
        return self._cur

    def __enter__(self) -> _Conn:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False


def test_stock_pattern_keeps_a_shares_only() -> None:
    for code in ("600001.SH", "688001.SH", "000001.SZ", "300001.SZ", "830001.BJ"):
        assert mod._STOCK_PAT.match(code), code
    for code in ("510300.SH", "900001.SH", "020001.SZ", "123456.HK"):
        assert not mod._STOCK_PAT.match(code), code


def test_helpers() -> None:
    assert mod._to_iso8(date(2020, 1, 2)) == "20200102"
    assert mod._row_end_iso8("2020-12-31") == "20201231"
    assert mod._row_end_iso8(None) is None
    assert mod._row_end_iso8("nope") is None


def test_with_retry_success_first_try(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod.time, "sleep", lambda *_: None)
    assert mod._with_retry(lambda: 42) == 42


def test_with_retry_exhausts_and_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod.time, "sleep", lambda *_: None)
    calls = {"n": 0}

    def boom() -> None:
        calls["n"] += 1
        raise ValueError("rate limited")

    with pytest.raises(ValueError, match="rate limited"):
        mod._with_retry(boom, tries=3)
    assert calls["n"] == 3


def _patch_upserts(monkeypatch: pytest.MonkeyPatch) -> None:
    for mod_db in (mod.cn_balance, mod.cn_income, mod.cn_cashflow):
        monkeypatch.setattr(mod_db, "ensure_table", lambda: None)
        monkeypatch.setattr(mod_db, "upsert_rows", lambda rows: len(rows))


def test_sync_statements_for_codes_filters_by_cutoff(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_upserts(monkeypatch)
    monkeypatch.setattr(mod.time, "sleep", lambda *_: None)

    frame = pd.DataFrame({"end_date": ["2020-12-31", "2000-01-01"], "v": [1.0, 2.0]})

    class _Pro:
        def balancesheet(self, ts_code: str) -> pd.DataFrame:
            return frame

        def income(self, ts_code: str) -> pd.DataFrame:
            return frame

        def cashflow(self, ts_code: str) -> pd.DataFrame:
            return frame

    monkeypatch.setattr(mod, "_pro", lambda: _Pro())
    out = mod.sync_statements_for_codes(["A.SH"], date(2020, 1, 1), sleep=0.0, progress_every=1)
    assert out["stocks"] == 1
    assert out["failed"] == []
    # one in-window row per table, the 2000 row is dropped
    assert out["updated"] == {"balancesheet": 1, "income": 1, "cashflow": 1}


def test_sync_statements_for_codes_records_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_upserts(monkeypatch)
    monkeypatch.setattr(mod.time, "sleep", lambda *_: None)
    good = pd.DataFrame({"end_date": ["2021-03-31"], "v": [1.0]})

    class _Pro:
        def balancesheet(self, ts_code: str) -> pd.DataFrame:
            if ts_code == "BAD.SH":
                raise ValueError("boom")
            return good

        def income(self, ts_code: str) -> pd.DataFrame:
            return good

        def cashflow(self, ts_code: str) -> pd.DataFrame:
            return good

    monkeypatch.setattr(mod, "_pro", lambda: _Pro())
    out = mod.sync_statements_for_codes(
        ["OK.SH", "BAD.SH"], date(2020, 1, 1), sleep=0.0, progress_every=1
    )
    assert out["stocks"] == 1
    assert out["failed"] == ["BAD.SH"]


def test_sync_statements_for_codes_rate_limit_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_upserts(monkeypatch)
    slept: list[float] = []
    monkeypatch.setattr(mod.time, "sleep", lambda s: slept.append(s))

    class _Pro:
        def balancesheet(self, ts_code: str) -> pd.DataFrame:
            raise ValueError("频率超限")

        def income(self, ts_code: str) -> pd.DataFrame:  # pragma: no cover - never reached
            return pd.DataFrame()

        def cashflow(self, ts_code: str) -> pd.DataFrame:  # pragma: no cover
            return pd.DataFrame()

    monkeypatch.setattr(mod, "_pro", lambda: _Pro())
    out = mod.sync_statements_for_codes(["A.SH"], date(2020, 1, 1), sleep=0.0, progress_every=1)
    assert out["failed"] == ["A.SH"]
    assert 35 in slept  # rate-limit back-off branch


def test_fresh_codes_intersects_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    for mod_db in (mod.cn_balance, mod.cn_income, mod.cn_cashflow):
        monkeypatch.setattr(mod_db, "ensure_table", lambda: None)
    calls = {"n": 0}
    sets = [{"A.SH", "B.SH"}, {"B.SH", "C.SH"}, {"A.SH", "B.SH"}]

    def fake_connection() -> _Conn:
        rows = sorted((s,) for s in sets[calls["n"]])
        calls["n"] += 1
        return _Conn(_Cur(rows))

    monkeypatch.setattr(dbpkg, "get_connection", fake_connection)
    assert mod.fresh_codes(date(2020, 1, 1)) == {"B.SH"}


def test_fresh_codes_swallows_db_error(monkeypatch: pytest.MonkeyPatch) -> None:
    for mod_db in (mod.cn_balance, mod.cn_income, mod.cn_cashflow):
        monkeypatch.setattr(mod_db, "ensure_table", lambda: None)

    class _BadCur(_Cur):
        def execute(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            raise RuntimeError("db down")

    monkeypatch.setattr(dbpkg, "get_connection", lambda: _Conn(_BadCur([])))
    assert mod.fresh_codes(date(2020, 1, 1)) == set()


def test_sync_4y_window_skips_fresh_and_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod, "_get_stock_codes", lambda: ["A.SH", "B.SH", "C.SH"])
    monkeypatch.setattr(mod, "fresh_codes", lambda cutoff: {"B.SH"})
    seen: dict = {}

    def fake_sync(codes, cutoff, *, sleep=0.4, progress_every=50, log_prefix=""):  # noqa: ANN001
        seen["codes"] = list(codes)
        return {
            "updated": {"balancesheet": 0, "income": 0, "cashflow": 0},
            "stocks": 2,
            "failed": [],
        }

    monkeypatch.setattr(mod, "sync_statements_for_codes", fake_sync)
    out = mod.sync_4y_window(cutoff=date(2020, 1, 1))
    assert seen["codes"] == ["A.SH", "C.SH"]
    assert out["skipped_fresh"] == 1
    assert out["universe"] == 3


def test_sync_4y_window_limit_and_offset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod, "_get_stock_codes", lambda: ["A.SH", "B.SH", "C.SH", "D.SH"])
    monkeypatch.setattr(mod, "fresh_codes", lambda cutoff: set())
    seen: dict = {}

    def fake_sync(codes, cutoff, **kwargs):  # noqa: ANN001
        seen["codes"] = list(codes)
        return {"updated": {}, "stocks": len(codes), "failed": []}

    monkeypatch.setattr(mod, "sync_statements_for_codes", fake_sync)
    mod.sync_4y_window(cutoff=date(2020, 1, 1), limit_codes=2, offset=1, skip_fresh=False)
    assert seen["codes"] == ["B.SH", "C.SH"]
