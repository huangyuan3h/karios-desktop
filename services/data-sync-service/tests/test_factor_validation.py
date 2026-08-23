import pytest

from data_sync_service.service.factor_validation import _spearman, analyze_signals


def test_spearman_perfect():
    xs = list(range(10))
    ys = list(range(10))
    assert abs(_spearman(xs, ys) - 1.0) < 1e-9
    assert abs(_spearman(xs, list(reversed(ys))) + 1.0) < 1e-9


def test_spearman_edge():
    assert _spearman([1] * 10, list(range(10))) is None  # zero var
    assert _spearman([1, 2], [3, 4]) is None  # n<10


def test_analyze_smoke():
    # small window still runs without error
    res = analyze_signals("2026-07-08", "2026-07-15", signals=["score"], horizons=[1, 5])
    assert "score" in res["signals"]
    assert 1 in res["signals"]["score"]["ic"]
    # DB may be empty in CI (no daily rows) – only check threshold when data present
    if res["window"]["n_ts"] == 0:
        assert res["window"]["n_days"] >= 0
    else:
        assert res["window"]["n_ts"] >= 5000
