#!/usr/bin/env python3
"""H-ETF-A pre-registered diagnostic: is the S-3 stock leg alpha or sector beta?

For each frozen S-3 CN closed trade (entry_date→close_date), compare the stock's
close-to-close return against the *same-window* return of:
  - board  : 688→588000, 主板→510500, 创业板→159915
  - broad300: 510300 (always)
  - sector : an industry-matched sector ETF (keyword map from watchlist_score_daily.industry)
excess = stock_cc - bench_cc.

Prereg & decision rule: docs/designs/s3-alpha-vs-etf-prereg-2026-09-12.md
Read-only. Does NOT touch Live, no parameter search.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_s3_alpha_vs_etf.py --save-report
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    simulate,
)
from run_walk_forward import S3_CONFIG  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
ETF_DAILY = ROOT / "data" / "etf" / "etf_daily.csv"
REPORT_DIR = ROOT / "data" / "backtest_reports"
WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "long": ("2021-08-01", "2026-08-07"),
}

# ordered keyword -> sector ETF (first match wins)
SECTOR_MAP: list[tuple[tuple[str, ...], str]] = [
    (("煤炭", "焦炭"), "515220.SH"),
    (("钢铁", "普钢", "特钢", "冶钢"), "515210.SH"),
    (("半导体", "芯片", "集成电路", "光电子", "元件", "印制电路", "消费电子", "电子器件",
      "电子元件", "其他电子", "电子设备", "电子化学", "光学", "PCB", "电子"), "512480.SH"),
    (("软件", "计算机", "IT服务", "互联网", "数字媒体"), "512720.SH"),
    (("通信",), "515880.SH"),
    (("传媒", "游戏", "影视", "广告", "出版", "电视广播", "广播电视", "动漫", "文娱"), "512980.SH"),
    (("医药", "医疗", "中药", "生物", "化学制药", "保健护理", "生物制品"), "512010.SH"),
    (("白酒", "非白酒", "饮料", "食品", "调味", "乳品", "休闲食品"), "515170.SH"),
    (("汽车", "乘用车", "商用车", "摩托车", "汽车零部件", "汽车服务"), "516110.SH"),
    (("银行",), "512800.SH"),
    (("证券", "保险", "非银", "多元金融"), "512880.SH"),
    (("房地产", "地产", "房屋建设", "商业物业", "装修装饰", "建筑施工", "基础建设",
      "专业工程", "工程建设", "工程咨询", "钢结构", "建筑材料", "水泥"), "512200.SH"),
    (("有色", "金属", "贵金属", "小金属", "稀有金属", "工业金属", "基本金属", "能源金属"), "512400.SH"),
    (("军工", "航空", "航天", "兵装", "卫星", "航海", "地面装备", "军工电子"), "512660.SH"),
    (("光伏",), "515790.SH"),
    (("电池", "新能源车", "新能源", "电源", "电机", "输变电", "电气", "电网"), "515030.SH"),
    (("电力", "风电", "公用事业", "燃气", "水务"), "159611.SZ"),
    (("环保", "环境治理"), "512580.SH"),
    (("养殖", "畜牧", "饲料", "渔业", "农业", "种植", "农产品", "林业", "动物保健"), "159865.SZ"),
    (("旅游", "酒店", "餐饮", "景区", "休闲服务", "旅游零售"), "159766.SZ"),
]


def _symbol_to_ts(sym: str) -> str | None:
    c = str(sym).split(":")[-1].split(".")[0]
    if c.startswith("6"):
        return c + ".SH"
    if c.startswith(("0", "3")):
        return c + ".SZ"
    return None


def _board_bench(ts: str) -> str | None:
    c = ts.split(".")[0]
    if c.startswith("688"):
        return "588000.SH"
    if c.startswith("30"):
        return "159915.SZ"
    if c[0] in "06":
        return "510500.SH"
    return None


def _sector_bench(ind: str | None) -> str | None:
    if not ind:
        return None
    for kws, ts in SECTOR_MAP:
        if any(k in ind for k in kws):
            return ts
    return None


def _load_etf() -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = defaultdict(dict)
    with ETF_DAILY.open() as fh:
        for r in csv.DictReader(fh):
            d = str(r["trade_date"])
            iso = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d
            out[r["ts_code"]][iso] = float(r["close_adj"])
    return out


def _industry_map() -> dict[str, str]:
    import psycopg

    from data_sync_service.config import get_settings

    out: dict[str, str] = {}
    with psycopg.connect(get_settings().database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT ON (symbol) symbol, industry FROM watchlist_score_daily "
                "WHERE industry IS NOT NULL AND industry <> '' ORDER BY symbol, trade_date DESC"
            )
            for sym, ind in cur.fetchall():
                out[str(sym)] = str(ind)
    return out


def _etf_ret(pxmap: dict[str, dict[str, float]], ts: str | None, d0: str, d1: str) -> float | None:
    if not ts:
        return None
    mp = pxmap.get(ts)
    if not mp:
        return None
    a, b = mp.get(d0), mp.get(d1)
    if not a or not b or a <= 0:
        return None
    return b / a - 1.0


def _agg(excess: list[float], stock: list[float], bench: list[float]) -> dict:
    if not excess:
        return {"n": 0}
    a = np.array(excess)
    return {
        "n": len(excess),
        "mean_excess_pct": round(float(a.mean()) * 100, 2),
        "median_excess_pct": round(float(np.median(a)) * 100, 2),
        "win_rate_pct": round(float((a > 0).mean()) * 100, 1),
        "mean_stock_pct": round(float(np.mean(stock)) * 100, 2),
        "mean_bench_pct": round(float(np.mean(bench)) * 100, 2),
    }


def _run_window(start: str, end: str, etf: dict, ind: dict) -> dict:
    cfg = BacktestConfig(start_date=start, end_date=end, **S3_CONFIG)
    data = BacktestData(cfg)
    run = simulate(cfg, data)
    close = data.close_by_ts_day
    board_ex: list[float] = []
    sector_ex: list[float] = []
    b300_ex: list[float] = []
    b300_stock: list[float] = []
    b300_bench: list[float] = []
    board_stock: list[float] = []
    board_bench: list[float] = []
    sector_stock: list[float] = []
    sector_bench: list[float] = []
    for t in run.trades:
        ts = _symbol_to_ts(t.symbol)
        if ts is None:
            continue
        mp = close.get(ts)
        if not mp:
            continue
        a, b = mp.get(t.entry_date), mp.get(t.close_date)
        if not a or not b or a <= 0:
            continue
        stock = b / a - 1.0
        # board + broad300
        rt_board = _etf_ret(etf, _board_bench(ts), t.entry_date, t.close_date)
        if rt_board is not None:
            board_ex.append(stock - rt_board)
            board_stock.append(stock)
            board_bench.append(rt_board)
        rt300 = _etf_ret(etf, "510300.SH", t.entry_date, t.close_date)
        if rt300 is not None:
            b300_ex.append(stock - rt300)
            b300_stock.append(stock)
            b300_bench.append(rt300)
        # sector
        industry = ind.get(str(t.symbol)) or ind.get(f"CN:{ts.split('.')[0]}")
        rt_sec = _etf_ret(etf, _sector_bench(industry), t.entry_date, t.close_date)
        if rt_sec is not None:
            sector_ex.append(stock - rt_sec)
            sector_stock.append(stock)
            sector_bench.append(rt_sec)
    n = len(run.trades)
    return {
        "n_trades": n,
        "board": _agg(board_ex, board_stock, board_bench),
        "sector": _agg(sector_ex, sector_stock, sector_bench),
        "broad300": _agg(b300_ex, b300_stock, b300_bench),
        "sector_coverage_pct": round(100.0 * len(sector_ex) / n, 1) if n else 0.0,
    }


def _fmt(a: dict) -> str:
    if a.get("n", 0) == 0:
        return "n=0"
    return (f"n={a['n']} mean {a['mean_excess_pct']:+.2f} median {a['median_excess_pct']:+.2f} "
            f"win {a['win_rate_pct']:.0f}% (stock {a['mean_stock_pct']:+.1f} / bench {a['mean_bench_pct']:+.1f})")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    etf = _load_etf()
    ind = _industry_map()
    print(f"ETF panel: {len(etf)} codes. Industry map: {len(ind)} symbols.\n", flush=True)

    results: dict[str, dict] = {}
    for w, (s, e) in WINDOWS.items():
        print(f"=== {w} ({s}~{e}) running S-3 …", flush=True)
        r = _run_window(s, e, etf, ind)
        results[w] = r
        print(f"  board : {_fmt(r['board'])}", flush=True)
        print(f"  sector: {_fmt(r['sector'])}  coverage {r['sector_coverage_pct']}%", flush=True)
        print(f"  b300  : {_fmt(r['broad300'])}\n", flush=True)

    long = results["long"]
    wf = ["OOS2", "train", "valid"]
    board_win = sum(1 for w in wf if results[w]["board"].get("mean_excess_pct", 0) > 0)
    sector_win = sum(1 for w in wf if results[w]["sector"].get("mean_excess_pct", 0) > 0)
    k1 = long["board"].get("mean_excess_pct", -1) > 0 and board_win >= 2
    k2 = (long["sector"].get("mean_excess_pct", -1) > 0 and sector_win >= 2
          and long["sector_coverage_pct"] >= 50)
    k3 = long["board"].get("median_excess_pct", -1) > 0
    if k1 and k2 and k3:
        concl = "S-3 selection is real alpha → ETF replacement destroys value"
    elif k1 and not k2:
        concl = "alpha is timing/breadth, not intra-sector selection → explore ETF-base + S-3 overlay"
    else:
        concl = "stock leg ≈ sector beta → ETF replacement worth an A/B replay"

    print("## H-ETF-A verdict\n")
    print(f"  K1 board long {long['board'].get('mean_excess_pct')}% / WF wins {board_win}/3 -> {'pass' if k1 else 'FAIL'}")
    print(f"  K2 sector long {long['sector'].get('mean_excess_pct')}% / WF wins {sector_win}/3 / cov {long['sector_coverage_pct']}% -> {'pass' if k2 else 'FAIL'}")
    print(f"  K3 median board long {long['board'].get('median_excess_pct')}% -> {'pass' if k3 else 'FAIL'}")
    print(f"  => {concl}")

    payload = {
        "tag": "s3-alpha-vs-etf-2026-09-12",
        "prereg": "docs/designs/s3-alpha-vs-etf-prereg-2026-09-12.md",
        "windows": results,
        "verdict": {"board_wf_wins": board_win, "sector_wf_wins": sector_win,
                    "k1": k1, "k2": k2, "k3": k3, "conclusion": concl},
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        p = REPORT_DIR / "s3_alpha_vs_etf_2026-09-12.json"
        p.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
