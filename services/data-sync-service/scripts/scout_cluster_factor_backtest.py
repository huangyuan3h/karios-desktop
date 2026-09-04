#!/usr/bin/env python3
"""Per-cluster factor backtest: amplitude for amplitude cluster, turnover for turnover cluster etc."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from data_sync_service.db import get_connection
import json
p=Path("data/backtest_reports/cluster_members.json")
print(p.read_text())
