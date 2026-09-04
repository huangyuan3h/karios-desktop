#!/usr/bin/env python3
"""Per-state parameter optimization (R11): 逐态网格寻优.

Grid per state: bucket_q ∈ {2,3,5} (amplitude 分位), max_pos ∈ {5,10,15}, body ∈ {2,3,5}.
Selection windows: OOS2 + train (纪律: 只在 OOS2+train 选参). Metric: mean sharpe.
Verify: valid + past_year + 对齐窗(2025-08-28~2026-08-28). Best params per state -> json.
"""
import itertools, json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scout_state_bucket_pickstrong import (_load_daily, _load_mv_map, _load_list_dates,
    simulate_state_bucket, stats, _load_calendar)

LOAD_S = "2024-04-01"
LOAD_E = "2026-09-10"
STATES = ["S-limit", "S-gap", "S-shrink"]
SEL_WINDOWS = ["OOS2", "train"]
VERIFY_WINDOWS = ["valid", "past_year", "aligned"]
GRID = {"bucket_q": [2, 3, 5], "max_pos": [5, 10, 15], "body": [2, 3, 5]}
WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "past_year": ("2025-08-01", "2026-08-07"),
    "aligned": ("2025-08-28", "2026-08-28"),
}


def main():
    per_ts = _load_daily(LOAD_S, LOAD_E)
    mv_map = _load_mv_map(LOAD_S, LOAD_E)
    list_dates = _load_list_dates()
    date_idx = {ts: {r["date"]: i for i, r in enumerate(series)} for ts, series in per_ts.items()}
    cals = {w: _load_calendar(*rng) for w, rng in WINDOWS.items()}
    results = {}
    for st in STATES:
        rows = []
        for bq, mp, body in itertools.product(GRID["bucket_q"], GRID["max_pos"], GRID["body"]):
            sr_sum, n = 0.0, 0
            info = {}
            for w in SEL_WINDOWS:
                nav, _ = simulate_state_bucket(cals[w], per_ts, mv_map, list_dates, date_idx,
                                               state_filter={st}, bucket_q=bq, max_pos=mp,
                                               hold_map={st: body})
                x = stats(cals[w][:len(nav)], nav)
                sr_sum += x["sharpe"]
                info[w] = x
                n += 1
            mean_sr = sr_sum / n
            rows.append((mean_sr, bq, mp, body, info))
        rows.sort(key=lambda r: -r[0])
        best = rows[0]
        bq, mp, body = best[1], best[2], best[3]
        verify = {}
        for w in VERIFY_WINDOWS:
            nav, _ = simulate_state_bucket(cals[w], per_ts, mv_map, list_dates, date_idx,
                                           state_filter={st}, bucket_q=bq, max_pos=mp,
                                           hold_map={st: body})
            verify[w] = stats(cals[w][:len(nav)], nav)
        results[st] = {
            "best_params": {"bucket_q": bq, "max_pos": mp, "body": body},
            "best_sel_mean_sharpe": round(best[0], 3),
            "select": {w: {k: round(v, 3) for k, v in best[4][w].items()} for w in SEL_WINDOWS},
            "verify": {w: {k: round(v, 3) for k, v in verify[w].items()} for w in VERIFY_WINDOWS},
            "top5": [{"mean_sr": round(r[0], 3), "bucket_q": r[1], "max_pos": r[2], "body": r[3],
                      "OOS2_sr": round(r[4]["OOS2"]["sharpe"], 3), "train_sr": round(r[4]["train"]["sharpe"], 3)}
                     for r in rows[:5]],
        }
        print(f"=== {st} ===")
        print(f"  best: bucket_q={bq} max_pos={mp} body={body}  sel_mean_sr={best[0]:.3f}")
        for w in SEL_WINDOWS:
            x = best[4][w]
            print(f"    sel {w:5s}: total {x['total_pct']:+.1f}%  dd {x['max_dd']:.1f}  sr {x['sharpe']:.2f}")
        for w in VERIFY_WINDOWS:
            x = verify[w]
            print(f"    ver {w:5s}: total {x['total_pct']:+.1f}%  dd {x['max_dd']:.1f}  sr {x['sharpe']:.2f}")
        sys.stdout.flush()
    p = Path("data/backtest_reports/state_optimize_latest.json")
    p.write_text(json.dumps(results, ensure_ascii=False, indent=2))
    print(f"\nreport -> {p}")


if __name__ == "__main__":
    raise SystemExit(main())