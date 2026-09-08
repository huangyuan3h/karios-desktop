"""DB-free unit tests for diag_alpha_trend_forward pure helpers."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from diag_alpha_trend_forward import (  # noqa: E402
    batch_clustered_mean,
    grade_verdict,
    net_relative,
    parse_birth_names,
    parse_current_names,
)


def test_parse_birth_names():
    tj = {"a_share_mapping": ["恒瑞医药", " 紫金矿业 "], "other": 1}
    assert parse_birth_names(tj) == ["恒瑞医药", "紫金矿业"]
    assert parse_birth_names({"a_share_mapping": []}) == []
    assert parse_birth_names(None) == []
    assert parse_birth_names("not json") == []


def test_parse_current_names():
    cs = [{"name": "恒瑞医药"}, {"symbol": "CN:000001"}]
    assert parse_current_names(cs) == ["恒瑞医药"]


def test_net_relative_subtracts_costs():
    assert abs(net_relative(0.05, 0.02) - (0.05 - 0.003 - 0.02)) < 1e-12


def test_grade_verdict_kills_without_gradient():
    v = grade_verdict({"S": 0.01, "A": 0.03, "B": 0.02})
    assert v["killed"] is True
    assert any("K1" in r for r in v["reasons"])


def test_grade_verdict_kills_nonpositive_s():
    v = grade_verdict({"S": -0.01, "A": -0.02, "B": -0.03})
    assert v["killed"] is True
    assert any("K2" in r for r in v["reasons"])


def test_grade_verdict_limited_pass():
    v = grade_verdict({"S": 0.03, "A": 0.01, "B": -0.01})
    assert v["killed"] is False


def test_batch_clustered_mean():
    m, se, n = batch_clustered_mean({"d1": [0.02, 0.04], "d2": [0.06]})
    assert n == 2
    assert abs(m - 0.045) < 1e-12
    assert se >= 0
