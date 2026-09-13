#!/usr/bin/env python3
"""B3 data build: supply-chain concept indices (同花顺) + US anchors.

Fetch a curated set of A-share concept-board indices (via akshare 同花顺) and
the matching overseas anchor stock daily bars (via akshare US). Research-only,
gitignored panels under data/chain/.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/sync_chain.py
"""

from __future__ import annotations

import csv
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "chain"

# board -> anchor symbol(s). Curated high-signal supply chains only.
BOARDS: dict[str, str] = {
    "苹果概念": "AAPL",
    "消费电子概念": "AAPL",
    "英伟达概念": "NVDA",
    "共封装光学(CPO)": "NVDA",
    "液冷服务器": "NVDA",
    "PCB概念": "NVDA",
    "铜缆高速连接": "NVDA",
    "数据中心(AIDC)": "NVDA",
    "存储芯片": "MU",
    "半导体概念": "SOX",
    "机器人概念": "TSLA",
    "人形机器人": "TSLA",
    "光刻机": "ASML",
}
ANCHORS = ("AAPL", "NVDA", "MU", "TSLA", "ASML", "AVGO", "AMD", "TSM")


def main() -> int:
    import akshare as ak

    OUT.mkdir(parents=True, exist_ok=True)

    rows = []
    bad = []
    for board in BOARDS:
        for attempt in range(3):
            try:
                df = ak.stock_board_concept_index_ths(symbol=board, start_date="20180101", end_date="20260831")
                for _, r in df.iterrows():
                    rows.append({"board": board, "date": str(r["日期"])[:10], "open": r["开盘价"],
                                 "high": r["最高价"], "low": r["最低价"], "close": r["收盘价"],
                                 "volume": r["成交量"], "amount": r["成交额"]})
                print(f"board {board}: {len(df)} rows", flush=True)
                break
            except Exception as e:  # noqa: BLE001
                if attempt == 2:
                    bad.append(board)
                    print(f"board {board}: ERR {str(e)[:100]}", flush=True)
                time.sleep(1.5)

    with (OUT / "board_index.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["board", "date", "open", "high", "low", "close", "volume", "amount"])
        w.writeheader()
        w.writerows(rows)
    with (OUT / "meta.csv").open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["board", "anchor"])
        for b, a in BOARDS.items():
            w.writerow([b, a])

    arows = []
    for sym in ANCHORS:
        for attempt in range(3):
            try:
                df = ak.stock_us_daily(symbol=sym, adjust="qfq")
                for _, r in df.iterrows():
                    arows.append({"symbol": sym, "date": str(r["date"])[:10], "open": r["open"],
                                  "high": r["high"], "low": r["low"], "close": r["close"], "volume": r["volume"]})
                print(f"anchor {sym}: {len(df)} rows", flush=True)
                break
            except Exception as e:  # noqa: BLE001
                if attempt == 2:
                    print(f"anchor {sym}: ERR {str(e)[:100]}", flush=True)
                time.sleep(1.5)

    with (OUT / "anchor_us.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["symbol", "date", "open", "high", "low", "close", "volume"])
        w.writeheader()
        w.writerows(arows)

    print(f"\nwrote board_index.csv ({len(rows)} rows), anchor_us.csv ({len(arows)} rows)"
          + (f", FAILED boards {bad}" if bad else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
