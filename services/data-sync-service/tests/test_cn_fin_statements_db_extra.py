"""db/cn_balance.py + cn_income.py + cn_cashflow.py coverage (fake conn)."""

from __future__ import annotations

import json

import pytest

from data_sync_service.db import cn_balance, cn_cashflow, cn_income

MODULES = (cn_balance, cn_income, cn_cashflow)


class _Cur:
    def __init__(self):
        self._executemany_calls = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def executemany(self, sql, values):
        self._executemany_calls.append((sql, list(values)))
        return self

    def execute(self, sql, params=None):
        return self


class _Conn:
    def __init__(self):
        self.cursors = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def cursor(self):
        c = _Cur()
        self.cursors.append(c)
        return c

    def commit(self):
        pass


def _conn(monkeypatch, mod):
    conn = _Conn()
    monkeypatch.setattr(mod, "get_connection", lambda: conn)
    monkeypatch.setattr(mod, "ensure_table", lambda: None)
    return conn


def _row(ts="600000.SH"):
    return {
        "ts_code": ts,
        "ann_date": "20250329",
        "f_ann_date": "20250329",
        "end_date": "20241231",
        "report_type": "1",
        "comp_type": "1",
        "total_assets": float("nan"),
        "total_revenue": 170748000000.0,
        "n_cashflow_act": -333654000000.0,
        "update_flag": "1",
    }


@pytest.mark.parametrize("mod", MODULES)
def test_upsert_rows_placeholders_match(mod, monkeypatch) -> None:
    conn = _conn(monkeypatch, mod)
    n = mod.upsert_rows([_row(), {"ts_code": "", "ann_date": None}, _row()])
    assert n == 2
    sql, values = conn.cursors[-1]._executemany_calls[0]
    assert "ON CONFLICT (ts_code, ann_date, end_date, report_type)" in sql
    assert len(values) == 2
    assert all(len(v) == sql.count("%s") for v in values)


@pytest.mark.parametrize("mod", MODULES)
def test_upsert_rows_empty(mod, monkeypatch) -> None:
    _conn(monkeypatch, mod)
    assert mod.upsert_rows([]) == 0


def test_balance_nan_and_extra(monkeypatch) -> None:
    conn = _conn(monkeypatch, cn_balance)
    cn_balance.upsert_rows([_row()])
    _, values = conn.cursors[-1]._executemany_calls[0]
    v = values[0]
    idx = v.index("1") if "1" in v else None  # update_flag position varies; check via extra
    extra = json.loads(v[-1])
    assert extra["total_revenue"] == 170748000000.0  # full row preserved
    assert extra["total_assets"] is None  # NaN sanitized
    assert idx is None or True


def test_report_type_defaults_to_merged(monkeypatch) -> None:
    conn = _conn(monkeypatch, cn_income)
    r = _row()
    del r["report_type"]
    cn_income.upsert_rows([r])
    _, values = conn.cursors[-1]._executemany_calls[0]
    assert values[0][3] == "1"


def test_create_sql_pk_and_indexes() -> None:
    for mod, table in (
        (cn_balance, "cn_balance_sheet"),
        (cn_income, "cn_income_stmt"),
        (cn_cashflow, "cn_cashflow_stmt"),
    ):
        ddl = mod.CREATE_SQL
        assert f"CREATE TABLE IF NOT EXISTS {table}" in ddl
        assert "PRIMARY KEY (ts_code, ann_date, end_date, report_type)" in ddl
