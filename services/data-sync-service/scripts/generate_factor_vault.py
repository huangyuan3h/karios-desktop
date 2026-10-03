"""Regenerate the frozen factor-vault JSON (read-only display data).

Recomputes the uniform H2k health check for every once-effective factor from
existing audit data (no DB queries, no live-param changes, no orders):

- S-gap last-40: ~/Projects/wealth-ideas/karios-audit-2026-10/scratch/h2f_trades.json
  (1130 trades, exact rolling mean/sum/win/t/percentile via factor_vault
  SGAP calibration + sgap_decay helpers where imported).
- Harbor S-3 stock leg last-40: scratch/h2d_blotter_{long,valid,holdout}.json
  (416 long trades) with the H2d placebo calibration.
- All other pilots: frozen window aggregates embedded in
  service/factor_vault.py (FINAL/H2/G/H2b/H2d/H2j/H2k).

Output (tracked frozen files served by GET /api/backtest/factor-vault):
- services/data-sync-service/data/backtest_reports/factor_vault.json
- services/data-sync-service/data/backtest_reports/factor_vault_history.json
  (append-only daily {date, states}; drives days_since_change).

Usage:
  cd services/data-sync-service && PYTHONPATH=src python3 scripts/generate_factor_vault.py
  cd services/data-sync-service && PYTHONPATH=src python3 scripts/generate_factor_vault.py --output /tmp/fv.json --history /tmp/fvh.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.service import factor_vault as fv  # noqa: E402

AUDIT_SCRATCH = (
    Path.home() / "Projects" / "wealth-ideas" / "karios-audit-2026-10" / "scratch"
)
DEFAULT_OUTPUT = (
    Path(__file__).resolve().parents[1] / "data" / "backtest_reports" / "factor_vault.json"
)
DEFAULT_HISTORY = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "backtest_reports"
    / "factor_vault_history.json"
)


def _mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _t_stat(xs: list[float]) -> float:
    import math

    n = len(xs)
    if n < 2:
        return 0.0
    m = sum(xs) / n
    var = sum((x - m) ** 2 for x in xs) / (n - 1)
    if not math.isfinite(var) or var <= 0:
        return 0.0
    return m / (math.sqrt(var) / math.sqrt(n))


def _sgap_override() -> dict[str, dict] | None:
    """Exact S-gap last-40 from h2f_trades.json (same contrib unit as sgap_decay)."""
    src = AUDIT_SCRATCH / "h2f_trades.json"
    if not src.exists():
        return None
    try:
        trades = json.loads(src.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if isinstance(trades, dict):
        trades = trades.get("trades", [])
    rows: list[tuple[str, float]] = []
    for t in trades:
        if not isinstance(t, dict):
            continue
        entry = t.get("entry") or t.get("entryDate")
        raw = t.get("contrib", t.get("contribPct"))
        try:
            c = float(raw)
        except (TypeError, ValueError):
            continue
        import math as _m

        if not _m.isfinite(c) or not entry:
            continue
        rows.append((str(entry)[:10], c))
    rows.sort(key=lambda r: r[0])
    if len(rows) < 1:
        return None
    tail = [c for _, c in rows[-40:]]
    n = len(tail)
    win = sum(1 for x in tail if x > 0) / n if n else 0.0
    net = sum(tail)
    pct = fv.percentile_of_sum(net, n, fv.SGAP_MU, fv.SGAP_SIGMA)
    return {
        "starship_b": {
            "percentile": pct,
            "net": net,
            "win_rate": win,
            "trades": n,
            "window": "holdout",
        }
    }


def _harbor_override() -> dict[str, dict] | None:
    """Exact Harbor S-3 stock-leg last-40 from the long blotter only.

    h2d_blotter_long.json (416 trades) already contains every window;
    the per-window files are subsets, so concatenating would double-count.
    Sorted by entry date (stable) to match the H2d replay order.
    """
    p = AUDIT_SCRATCH / "h2d_blotter_long.json"
    if not p.exists():
        return None
    try:
        blotter = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(blotter, list):
        return None
    rows: list[tuple[str, float]] = []
    for d in blotter:
        if not isinstance(d, dict):
            continue
        try:
            pnl = float(d.get("pnl", 0.0))
            pos = float(d.get("pos", 0.1))
        except (TypeError, ValueError):
            continue
        import math as _m

        if not (_m.isfinite(pnl) and _m.isfinite(pos)):
            continue
        entry = str(d.get("entry") or "")[:10]
        rows.append((entry, pnl * pos))
    rows.sort(key=lambda r: r[0])
    vals = [v for _, v in rows]
    # Keep entry order (files are already chronological); take the last 40.
    if not vals:
        return None
    tail = vals[-40:]
    n = len(tail)
    net = sum(tail)
    win = sum(1 for _ in [])  # blotter has no per-trade win flag; leave None
    pct = fv.percentile_of_sum(net, n, fv.HARBOR_MU, fv.HARBOR_SIGMA)
    _ = _t_stat(tail)
    _ = _mean(tail)
    return {
        "harbor": {
            "percentile": pct,
            "net": net,
            "win_rate": None,
            "trades": n,
            "window": "valid" if n <= 16 else "long",
        }
    }


def _load_history(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    if isinstance(data, dict):
        hist = data.get("history", [])
        return hist if isinstance(hist, list) else []
    return data if isinstance(data, list) else []


def main() -> int:
    ap = argparse.ArgumentParser(description="Regenerate factor_vault.json")
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    ap.add_argument("--history", default=str(DEFAULT_HISTORY))
    ap.add_argument("--date", default=date.today().isoformat())
    args = ap.parse_args()

    overrides: dict[str, dict] = {}
    for builder in (_sgap_override, _harbor_override):
        try:
            ov = builder()
        except Exception:
            ov = None
        if ov:
            overrides.update(ov)

    out_path = Path(args.output)
    hist_path = Path(args.history)
    prev_history = _load_history(hist_path)

    payload = fv.compute_factor_vault(
        overrides=overrides or None,
        prev_history=prev_history or None,
        generated_at=args.date,
    )
    payload["meta"]["source_path"] = "scratch/h2f_trades.json + h2d_blotter_*.json + frozen FACTOR_DEFS"
    payload["meta"]["overrides"] = sorted(overrides.keys())

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    # Append-only daily history (one entry per date).
    states = {r["id"]: r["status"] for r in payload["factors"]}
    entry = {"date": args.date, "states": states}
    hist = list(prev_history)
    if not hist or (hist[-1].get("date") != args.date):
        hist.append(entry)
    # Recompute days_since_change against the full history for consistency.
    payload2 = fv.compute_factor_vault(
        overrides=overrides or None,
        prev_history=hist[:-1] or None,
        generated_at=args.date,
    )
    payload["factors"] = payload2["factors"]
    out_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    hist_path.parent.mkdir(parents=True, exist_ok=True)
    hist_path.write_text(
        json.dumps({"history": hist}, ensure_ascii=False), encoding="utf-8"
    )
    s = payload["summary"]
    print(
        f"wrote {out_path} ({s['n_factors']} factors: "
        f"{s['n_cold']} cold / {s['n_watch']} watch / {s['n_revived']} revived; "
        f"overrides={sorted(overrides.keys()) or 'frozen-only'})"
    )
    print(f"history {hist_path} ({len(hist)} daily entries)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
