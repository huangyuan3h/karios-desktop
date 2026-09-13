#!/usr/bin/env python3
"""H-RARE pre-registered diagnostic: rare extreme-selloff triggers (long-window only).

5 pre-declared triggers (C1..C5) on 000300 / market breadth. Event = signal day T
-> buy 000300 next open, hold N sessions, sell close. Dedup 5 sessions.
Verdict: long window (2015+) only; needs win-rate + return thresholds.

Prereg & thresholds: docs/designs/rare-triggers-prereg-2026-09-12.md
Read-only, no parameter search.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_rare_triggers.py --save-report
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import psycopg

ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "data" / "backtest_reports"
LONG = ("2015-01-01", "2026-08-07")
LOAD_START = "2014-06-01"
NS = (3, 5, 10, 20)
PRIMARY = 5
DEDUP = 5
SUBPERIODS = {"2015-18": ("2015-01-01", "2018-12-31"),
              "2019-22": ("2019-01-01", "2022-12-31"),
              "2023-26": ("2023-01-01", "2026-08-07")}


def _load() -> tuple[list[dict], dict]:
    from data_sync_service.config import get_settings
    with psycopg.connect(get_settings().database_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT trade_date, open, close, amount FROM index_daily WHERE ts_code='000300.SH' "
            "AND trade_date BETWEEN %s AND %s ORDER BY trade_date",
            (LOAD_START, LONG[1]),
        )
        idx = [{"d": str(d), "o": float(o), "c": float(c), "amt": float(a) if a is not None else None}
               for d, o, c, a in cur.fetchall() if o is not None and c is not None]
        cur.execute(
            """
            SELECT trade_date,
                   count(*) FILTER (WHERE pct_chg <= -9.5) AS limdown,
                   count(*) FILTER (WHERE pct_chg > 0) AS up,
                   count(*) FILTER (WHERE pct_chg IS NOT NULL) AS tot
            FROM daily
            WHERE trade_date BETWEEN %s AND %s AND ts_code ~ '^(00|30|60|68)'
            GROUP BY trade_date ORDER BY trade_date
            """,
            (LOAD_START, LONG[1]),
        )
        breadth = {str(d): {"limdown": int(ld or 0), "up": int(u or 0), "tot": int(t or 0)}
                   for d, ld, u, t in cur.fetchall()}
    return idx, breadth


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    idx, breadth = _load()
    n = len(idx)
    closes = np.array([r["c"] for r in idx])
    opens = np.array([r["o"] for r in idx])
    dates = [r["d"] for r in idx]

    flags: dict[str, list[bool]] = {c: [False] * n for c in ("C1", "C2", "C3", "C4", "C5")}
    for i in range(n):
        d = dates[i]
        pct = closes[i] / closes[i - 1] - 1.0 if i >= 1 else 0.0
        ret5 = closes[i] / closes[i - 5] - 1.0 if i >= 5 else 0.0
        amt = idx[i]["amt"]
        amt_ma = np.mean([idx[j]["amt"] for j in range(i - 19, i + 1) if idx[j]["amt"]])
        br = breadth.get(d)
        flags["C1"][i] = pct <= -0.04
        flags["C2"][i] = ret5 <= -0.08
        flags["C3"][i] = bool(br and br["limdown"] >= 300)
        flags["C4"][i] = bool(br and br["tot"] > 0 and br["up"] / br["tot"] <= 0.05)
        flags["C5"][i] = pct <= -0.02 and amt is not None and amt_ma and amt > 1.5 * amt_ma

    def fwd(i: int, k: int) -> float | None:
        j = i + 1 + k
        if i + 1 >= n or j >= n or opens[i + 1] <= 0:
            return None
        return closes[j] / opens[i + 1] - 1.0

    def in_long(d: str) -> bool:
        return LONG[0] <= d <= LONG[1]

    # unconditional baseline over long window
    base: dict[int, list[float]] = {k: [] for k in NS}
    for i in range(n):
        if in_long(dates[i]):
            for k in NS:
                v = fwd(i, k)
                if v is not None:
                    base[k].append(v)

    def stats(vals: list[float]) -> dict:
        if not vals:
            return {"n": 0, "win": None, "mean": None, "median": None}
        a = np.asarray(vals)
        return {"n": len(vals), "win": round(100 * float(np.mean(a > 0)), 1),
                "mean": round(100 * float(np.mean(a)), 2), "median": round(100 * float(np.median(a)), 2)}

    print(f"index days {n} · long {LONG[0]}~{LONG[1]}")
    print("\n## unconditional baseline (%, N=3/5/10/20) " + str({k: stats(base[k]) for k in NS}))

    results: dict[str, dict] = {}
    verdicts: dict[str, bool] = {}
    for c in ("C1", "C2", "C3", "C4", "C5"):
        last = -10**9
        ev: dict[int, list[float]] = {k: [] for k in NS}
        ev_ret: list[float] = []
        ev_dates: list[str] = []
        for i in range(n):
            if not flags[c][i] or not in_long(dates[i]):
                continue
            if i - last <= DEDUP:
                continue
            vr = fwd(i, PRIMARY)
            if vr is None:
                continue
            last = i
            ev_dates.append(dates[i])
            ev_ret.append(vr)
            for k in NS:
                v = fwd(i, k)
                if v is not None:
                    ev[k].append(v)
        res = {f"N{k}": stats(ev[k]) for k in NS}
        res["dates"] = ev_dates
        # subperiod
        sub = {}
        for name, (s, e) in SUBPERIODS.items():
            vals = [fwd(i, PRIMARY) for i in range(n)
                    if flags[c][i] and s <= dates[i] <= e and fwd(i, PRIMARY) is not None]
            sub[name] = stats(vals)
        res["subperiod_N5"] = sub
        results[c] = res

        st5 = res[f"N{PRIMARY}"]
        base5 = np.mean(base[PRIMARY])
        k1 = st5["win"] is not None and st5["win"] >= 75.0
        k2 = st5["mean"] is not None and st5["mean"] >= 3.0 and (st5["median"] or -99) > 0
        k3 = st5["mean"] is not None and (st5["mean"] - 100 * float(base5)) >= 2.0
        cov = st5["n"] >= 8
        ok = cov and k1 and k2 and k3
        verdicts[c] = ok
        print(f"\n## {c}  events {len(ev_dates)}  (n@N5={st5['n']}, n@N3={res['N3']['n']}, n@N20={res['N20']['n']})")
        for k in NS:
            s = res[f"N{k}"]
            print(f"   N{k:<3} win {s['win']}%  mean {s['mean']}  median {s['median']}  n {s['n']}")
        print(f"   subperiod N5: " + "  ".join(f"{nm}:{sub[nm]['win']}%/{sub[nm]['mean']}" for nm in SUBPERIODS))
        print(f"   coverage>=8 {'ok' if cov else 'FAIL'} · K1 win>=75 {'ok' if k1 else 'FAIL'} · "
              f"K2 mean>=3 & med>0 {'ok' if k2 else 'FAIL'} · K3 +2pt vs base {'ok' if k3 else 'FAIL'}"
              f" => {'PASS -> overlay' if ok else 'out'}")
        print(f"   dates: {', '.join(ev_dates[:14])}{' ...' if len(ev_dates) > 14 else ''}")

    passed = [c for c, ok in verdicts.items() if ok]
    print(f"\n## H-RARE verdict: passed candidates = {passed if passed else 'NONE'}")
    print("   (long-window only, thresholded; no 3-window requirement)")

    payload = {"tag": "rare-triggers-2026-09-12",
               "prereg": "docs/designs/rare-triggers-prereg-2026-09-12.md",
               "baseline": {k: stats(base[k]) for k in NS},
               "candidates": results, "passed": passed,
               "as_of": datetime.now(UTC).isoformat(timespec="seconds")}
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "rare_triggers_2026-09-12.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("\nsaved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
