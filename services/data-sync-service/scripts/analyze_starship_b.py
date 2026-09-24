#!/usr/bin/env python3
"""Analyze 星舰 B (Starship B): where/when it loses money, and why.

Read-only. Reproduces the long-window NAV and decomposes every losing day into
satellite / parking / cost, lists worst days, drawdown episodes, yearly stats,
loss concentration, parking-leg culprits, and the satellite blotter's worst
trades by exit reason.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/analyze_starship_b.py
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from eval_sat_idle_parking import _compose, _sat_book, _true_cash_share  # noqa: E402
from eval_starship_b import PARK_B, load_raw  # noqa: E402
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.service.homeport import (  # noqa: E402
    _series_on_cal,
    _vol_at,
    inverse_vol_weights,
    load_risk_closes,
)

TRANSFER_BPS = 5.0
LABEL = {"511260.SH": "国债", "518880.SH": "黄金", "513100.SH": "纳指"}


def _metrics(nav: list[float]) -> dict:
    rets = [nav[i] / nav[i - 1] - 1 for i in range(1, len(nav)) if nav[i - 1]]
    years = len(rets) / 252.0
    cagr = (nav[-1] / nav[0]) ** (1 / years) - 1 if years > 0 and nav[-1] > 0 else 0.0
    peak, mdd = nav[0], 0.0
    for v in nav:
        peak = max(peak, v)
        mdd = min(mdd, v / peak - 1)
    mean, std = (float(np.mean(rets)), float(np.std(rets))) if rets else (0.0, 0.0)
    sharpe = mean / std * np.sqrt(252) if std > 0 else 0.0
    return {
        "total": 100 * (nav[-1] - 1),
        "cagr": 100 * cagr,
        "mdd": 100 * mdd,
        "sharpe": sharpe,
        "win": 100 * sum(1 for r in rets if r > 0) / len(rets) if rets else 0.0,
        "n": len(rets),
    }


def _dd_episodes(dates: list[str], nav: list[float], top: int = 8) -> list[dict]:
    eps = []
    peak_v, peak_i = nav[0], 0
    trough_i, trough_v = 0, nav[0]
    in_dd = False
    for i in range(1, len(nav)):
        if nav[i] >= peak_v:
            if in_dd:
                eps.append(
                    {
                        "peak": dates[peak_i],
                        "trough": dates[trough_i],
                        "recover": dates[i],
                        "depth": 100 * (nav[trough_i] / peak_v - 1),
                        "days": i - peak_i,
                    }
                )
                in_dd = False
            peak_v, peak_i = nav[i], i
        else:
            if not in_dd:
                in_dd = True
                trough_i, trough_v = i, nav[i]
            if nav[i] < trough_v:
                trough_i, trough_v = i, nav[i]
    if in_dd:
        eps.append(
            {
                "peak": dates[peak_i],
                "trough": dates[trough_i],
                "recover": "(open)",
                "depth": 100 * (nav[trough_i] / peak_v - 1),
                "days": len(nav) - 1 - peak_i,
            }
        )
    eps.sort(key=lambda e: e["depth"])
    return eps[:top]


def blend_b_detail(series_map, cal, *, lookback=60, cost=0.0005):
    ts_list = list(series_map)
    ser = {ts: _series_on_cal(series_map[ts], cal) for ts in ts_list}
    w_by_i = {}
    for i in range(len(cal)):
        if i < lookback:
            w_by_i[i] = {ts: 1.0 / len(ts_list) for ts in ts_list}
        elif cal[i][:7] == cal[i - 1][:7]:
            w_by_i[i] = w_by_i[i - 1]
        else:
            vol = {ts: (_vol_at(ser[ts], i, lookback) or 1e-9) for ts in ts_list}
            w_by_i[i] = inverse_vol_weights(vol)
    nav, contrib, cur = [1.0], [], w_by_i[0]
    for i in range(1, len(cal)):
        per = {
            ts: float(cur.get(ts, 0.0) * (ser[ts][i] / ser[ts][i - 1] - 1.0))
            for ts in ts_list
            if ser[ts][i - 1] and ser[ts][i]
        }
        r = sum(per.values())
        nav.append(nav[-1] * (1.0 + r))
        turn = sum(abs(w_by_i[i][ts] - cur.get(ts, 0.0)) for ts in ts_list) / 2.0
        if w_by_i[i] != cur:
            nav[-1] *= 1.0 - cost * turn
            cur = w_by_i[i]
        contrib.append(per)
    return nav, contrib


def main() -> int:
    s, e = WINDOWS["long"]
    print(f"星舰 B long window {s}~{e}\n", flush=True)
    park_sm = {ts: load_raw(ts) for ts in PARK_B}

    sat = _sat_book(s, e)
    dates = sat["dates"]
    sat_nav, rows = sat["nav"], sat["rows"]
    calB = sorted({d for d in park_sm[PARK_B[0]] if d in set(dates)})
    parkB, park_contrib = blend_b_detail(park_sm, calB)
    byd = dict(zip(calB, parkB, strict=True))
    pcd = dict(zip(calB[1:], park_contrib, strict=True))
    last, navB, pcs = 1.0, [], []
    for d in dates:
        last = byd.get(d, last)
        navB.append(last)
        pcs.append(pcd.get(d, {}))
    cash = _true_cash_share(sat, from_rows=True)
    w = [0.0] + cash[:-1]
    nav = _compose(sat_nav, w, navB, TRANSFER_BPS)

    n = min(len(dates), len(sat_nav), len(navB), len(w), len(nav))
    dates, sat_nav, navB, w, nav = dates[:n], sat_nav[:n], navB[:n], w[:n], nav[:n]
    pcs = pcs[:n]
    m = _metrics(nav)
    print(
        f"OVERALL  total {m['total']:+.1f}%  cagr {m['cagr']:.1f}%  mdd {m['mdd']:.1f}%  "
        f"sharpe {m['sharpe']:.2f}  win {m['win']:.1f}%  n={m['n']}"
    )

    days = []
    for t in range(1, n):
        r_sat = sat_nav[t] / sat_nav[t - 1] - 1 if sat_nav[t - 1] else 0.0
        r_park = navB[t] / navB[t - 1] - 1 if navB[t - 1] else 0.0
        cost = TRANSFER_BPS / 1e4 * abs(w[t] - w[t - 1])
        days.append(
            {
                "date": dates[t],
                "tot": r_sat + w[t] * r_park - cost,
                "sat": r_sat,
                "park": w[t] * r_park,
                "w": w[t],
                "pos": int(rows[t].get("satPositions") or 0),
                "pc": pcs[t],
            }
        )
    tot = np.array([d["tot"] for d in days])
    sat_r = np.array([d["sat"] for d in days])
    park_r = np.array([d["park"] for d in days])
    print(
        f"SUM contrib%: sat {100 * sat_r.sum():+.1f}  park {100 * park_r.sum():+.1f}  "
        f"cost -{100 * sum(TRANSFER_BPS / 1e4 * abs(w[i] - w[i - 1]) for i in range(1, n)):.2f}  "
        f"= {100 * tot.sum():+.1f}"
    )

    losing = [d for d in days if d["tot"] < 0]
    sat_loss = [d for d in days if d["sat"] < 0]
    park_loss = [d for d in days if d["park"] < 0]
    both = [d for d in days if d["sat"] < 0 and d["park"] < 0]
    print(
        f"\nLOSS DAYS  total<0: {len(losing)}/{len(days)} ({100 * len(losing) / len(days):.0f}%)  "
        f"sat<0: {len(sat_loss)}  park<0: {len(park_loss)}  both<0: {len(both)}"
    )
    print(
        f"  on total<0 days: mean {100 * np.mean([d['tot'] for d in losing]):+.2f}%  "
        f"sat-part mean {100 * np.mean([d['sat'] for d in losing]):+.2f}%  "
        f"park-part mean {100 * np.mean([d['park'] for d in losing]):+.2f}%"
    )
    gross_loss = sum(d["tot"] for d in losing)
    top10 = sum(sorted(d["tot"] for d in losing)[:10])
    print(
        f"  loss concentration: gross loss {100 * gross_loss:.1f}%  top10 days "
        f"{100 * top10:.1f}% ({100 * top10 / gross_loss:.0f}% of it)"
    )

    print("\nWORST 15 DAYS (total%)  sat / park / w / pos")
    for d in sorted(days, key=lambda x: x["tot"])[:15]:
        print(
            f"  {d['date']}  tot {100 * d['tot']:+6.2f}  sat {100 * d['sat']:+6.2f}  "
            f"park {100 * d['park']:+6.2f}  w {d['w']:.2f}  pos {d['pos']}"
        )

    print("\nWORST PARK-ONLY DAYS (park contribution)  culprit leg")
    for d in sorted(days, key=lambda x: x["park"])[:10]:
        per = {LABEL.get(k, k): 100 * v * d["w"] for k, v in d["pc"].items()}
        worst = min(per, key=per.get) if per else "-"
        print(
            f"  {d['date']}  park {100 * d['park']:+.2f}  tot {100 * d['tot']:+.2f}  "
            f"w {d['w']:.2f}  [{worst} {per.get(worst, 0):+.2f}]  "
            + " ".join(f"{k}{v:+.2f}" for k, v in per.items())
        )

    print("\nDRAWDOWN EPISODES (top by depth)")
    for ep in _dd_episodes(dates, nav):
        print(
            f"  {ep['peak']} -> {ep['trough']} (recover {ep['recover']})  "
            f"depth {ep['depth']:+.1f}%  {ep['days']}d"
        )

    print("\nYEARLY")
    by_year: dict[str, list[float]] = {}
    for d in days:
        by_year.setdefault(d["date"][:4], []).append(d["tot"])
    for y in sorted(by_year):
        arr = np.array(by_year[y])
        print(
            f"  {y}  ret {100 * (np.prod(1 + arr) - 1):+7.1f}%  days {len(arr):>3}  "
            f"win {100 * (arr > 0).mean():.0f}%  worst {100 * arr.min():+.2f}%"
        )

    bl = sat.get("blotter") or []
    fills = [b for b in bl if b.get("kind") == "fill" and b.get("contribPct") is not None]
    loss_fills = [b for b in fills if float(b["contribPct"]) < 0]
    print(
        f"\nSATELLITE TRADES  n={len(fills)}  losers={len(loss_fills)} ({100 * len(loss_fills) / len(fills):.0f}%)"
    )
    reasons = Counter(str(b.get("closeReason")) for b in loss_fills)
    print("  losing exits by reason:", dict(reasons))
    print("  worst trades:")
    for b in sorted(fills, key=lambda x: float(x["contribPct"]))[:12]:
        print(
            f"    {b.get('ts')}  {b.get('entryDate')}->{b.get('exitDate')}  "
            f"contrib {float(b['contribPct']):+.2f}%  reason {b.get('closeReason')}"
        )

    # ---------------- temporal clustering ----------------
    print("\nTEMPORAL CLUSTERING OF LOSSES")
    exits = Counter(str(b["exitDate"]) for b in loss_fills)
    entries = Counter(str(b["entryDate"]) for b in loss_fills)

    def _dist(c: Counter) -> dict[int, int]:
        return {k: sum(1 for v in c.values() if v == k) for k in sorted(set(c.values()))}

    print(
        f"  {len(loss_fills)} losing fills over {len(exits)} exit-days / {len(entries)} entry-days"
    )
    print(f"  exit-day multiplicity (n losers on same day -> #days): {_dist(exits)}")
    print(f"  entry-day multiplicity: {_dist(entries)}")
    print(f"  top loss exit-days: {exits.most_common(8)}")

    # market context: CSI300 ETF on worst satellite days
    etf = load_risk_closes()
    s300 = _series_on_cal(etf.get("510300.SH", {}), dates)
    r300 = [s300[i] / s300[i - 1] - 1 if s300[i - 1] else 0.0 for i in range(1, n)]
    r300 = np.array(r300)
    worst20 = sorted(range(len(tot)), key=lambda i: tot[i])[:20]
    ok = np.isfinite(r300) & (np.abs(r300) > 0)
    c30 = float(np.corrcoef(sat_r[ok], r300[ok])[0, 1]) if ok.sum() > 10 else float("nan")
    print(
        f"  CSI300 same-day return: all days {100 * r300[ok].mean():+.3f}%  "
        f"worst-20 sat days {100 * r300[worst20].mean():+.2f}%  corr(sat,r300) {c30:.2f}"
    )

    # autocorrelation + consecutive-negative runs vs shuffle baseline
    def ac(x, k):
        return float(np.corrcoef(x[:-k], x[k:])[0, 1]) if len(x) > k else 0.0

    def maxrun(neg):
        best = run = 0
        for v in neg:
            run = run + 1 if v else 0
            best = max(best, run)
        return best

    for name, arr in (("total", tot), ("sat", sat_r), ("park", park_r)):
        print(f"  {name}: lag1-5 autocorr {[round(ac(arr, k), 3) for k in range(1, 6)]}")
    obs = maxrun(tot < 0)
    rng = np.random.default_rng(0)
    sims = np.array([maxrun(rng.permutation(tot) < 0) for _ in range(2000)])
    print(
        f"  max consecutive losing days total={obs} vs shuffle p50={np.percentile(sims, 50):.0f} "
        f"p95={np.percentile(sims, 95):.0f} p99={np.percentile(sims, 99):.0f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
