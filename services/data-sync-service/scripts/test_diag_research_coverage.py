"""DB-free unit tests for diag_research_coverage pure helpers."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from diag_research_coverage import batch_clustered_mean, net_relative, to_ts  # noqa: E402


def test_net_relative():
    assert abs(net_relative(0.05, 0.02) - (0.05 - 0.003 - 0.02)) < 1e-12


def test_to_ts():
    assert to_ts("600519", "SHANGHAI") == "600519.SH"
    assert to_ts("000001", "SHENZHEN") == "000001.SZ"


def test_batch_clustered_mean_empty():
    m, se, n = batch_clustered_mean({})
    assert n == 0


def test_batch_clustered_mean_two_batches():
    m, se, n = batch_clustered_mean({"d1": [0.02], "d2": [0.04]})
    assert n == 2
    assert abs(m - 0.03) < 1e-12
    assert abs(se - (0.00005) ** 0.5) < 1e-9
