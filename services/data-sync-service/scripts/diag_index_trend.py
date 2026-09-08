"""Index-trend continuation diagnosis + replay (pre-registered, read-only).

Universe (frozen): 000300.SH (SSE50 proxy, declared) + 000688.SH (STAR50).
Source: index_daily ONLY (explicit universe gate; prints row counts).
Rule (frozen): bull(t) = close>MA20 & MA20>MA60, strictly trailing;
replay enters next-session open, exits next-session open, full-capital
compounding, 5bps/side (ETF proxy, declared approximation).
Windows: OOS2 2024-08-01..2025-08-01 / train 2025-08-01..2026-02-01 /
valid 2026-03-01..2026-08-07. Diagnosis on OOS2+train only.
"""
from __future__ import annotations

import datetime as _dt
import sys

from data_sync_service.db import get_connection

UNIVERSE = ("000300.SH", "000688.SH")
WINDOWS = {
    "OOS2": (_dt.date(2024, 8, 1), _dt.date(2025, 8, 1)),
    "train": (_dt.date(2025, 8, 1), _dt.date(2026, 2, 1)),
    "valid": (_dt.date(2026, 3, 1), _dt.date(2026, 8, 7)),
}
COST_SIDE = 0.0005


def load(code: str) -> list[dict]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT trade_date, open, high, low, close FROM index_daily "
            "WHERE ts_code=%s ORDER BY trade_date",
            (code,),
        )
        rows = [
            {"date": r[0], "open": float(r[1]), "high": float(r[2]),
             "low": float(r[3]), "close": float(r[4])}
            for r in cur.fetchall()
        ]
    print(f"[universe] {code}: {len(rows)} rows {rows[0]['date']}..{rows[-1]['date']}")
    return rows


def with_ma(rows: list[dict]) -> list[dict]:
    closes = [r["close"] for r in rows]
    out = []
    for i, r in enumerate(rows):
        rr = dict(r)
        rr["ma20"] = sum(closes[i - 19:i + 1]) / 20 if i >= 19 else None
        rr["ma60"] = sum(closes[i - 59:i + 1]) / 60 if i >= 59 else None
        rr["bull"] = (
            rr["ma20"] is not None and rr["ma60"] is not None
            and rr["close"] > rr["ma20"] and rr["ma20"] > rr["ma60"]
        )
        out.append(rr)
    return out


def fwdrets(rows: list[dict], horizon: int) -> tuple[list[float], list[float]]:
    cond, uncond = [], []
    for i in range(len(rows) - horizon):
        r = rows[i + horizon]["close"] / rows[i]["close"] - 1
        uncond.append(r)
        if rows[i]["bull"]:
            cond.append(r)
    return cond, uncond


def mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else float("nan")


def replay(rows: list[dict], start: _dt.date, end: _dt.date) -> dict:
    bydate = {r["date"]: i for i, r in enumerate(rows)}
    sess = [r for r in rows if start <= r["date"] <= end]
    if not sess:
        return {}
    nav, peak, maxdd, pos, entry = 1.0, 1.0, 0.0, 0, 0.0
    trades: list[float] = []
    in_mkt = 0
    for s in sess:
        i = bydate[s["date"]]
        if pos == 0 and s["bull"] and i + 1 < len(rows):
            pos, entry = 1, rows[i + 1]["open"] * (1 + COST_SIDE)
        elif pos == 1 and not s["bull"] and i + 1 < len(rows):
            px = rows[i + 1]["open"] * (1 - COST_SIDE)
            trades.append(px / entry - 1)
            nav *= px / entry
            pos = 0
        if pos == 1:
            in_mkt += 1
        peak = max(peak, nav)
        maxdd = min(maxdd, nav / peak - 1)
    if pos == 1:  # mark-to-close at window end
        px = sess[-1]["close"] * (1 - COST_SIDE)
        trades.append(px / entry - 1)
        nav *= px / entry
        peak = max(peak, nav)
        maxdd = min(maxdd, nav / peak - 1)
    wins = sum(1 for t in trades if t > 0)
    bh = sess[-1]["close"] / sess[0]["open"] - 1
    return {
        "nav": round((nav - 1) * 100, 1), "n": len(trades),
        "winpct": round(wins / len(trades) * 100, 1) if trades else float("nan"),
        "avgpp": round(mean(trades) * 100, 2) if trades else float("nan"),
        "maxdd": round(maxdd * 100, 1),
        "inmkt": round(in_mkt / len(sess) * 100, 1),
        "buyhold": round(bh * 100, 1),
    }


def main() -> None:
    data = {c: with_ma(load(c)) for c in UNIVERSE}
    print("\n== §1 diagnosis (OOS2+train): bull-state forward returns ==")
    diag_pool = []
    for c in UNIVERSE:
        rows = [r for r in data[c]
                if WINDOWS["OOS2"][0] <= r["date"] <= WINDOWS["train"][1]]
        for h in (20, 60):
            cond, uncond = fwdrets(rows, h)
            cov = len(cond) / len(uncond) if uncond else float("nan")
            print(f"{c} fwd{h}: bull n={len(cond)} cov={cov:.2f} "
                  f"cond={mean(cond)*100:+.2f}% uncond={mean(uncond)*100:+.2f}% "
                  f"edge={mean(cond)*100-mean(uncond)*100:+.2f}pp")
            diag_pool.append((c, h, len(cond), cov, mean(cond), mean(uncond)))
    print("\n== §2/§3 replay + buyhold per window ==")
    for w, (s, e) in WINDOWS.items():
        for c in UNIVERSE:
            print(f"{w} {c}: {replay(data[c], s, e)}")


if __name__ == "__main__":
    sys.exit(main())
