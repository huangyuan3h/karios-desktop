"""Regenerate the frozen factor-vault JSON (read-only display data).

Recomputes the uniform H2k health check for every once-effective factor from
existing audit data (no DB queries, no live-param changes, no orders):

- S-gap last-40: the frozen ~/Projects/wealth-ideas/karios-audit-2026-10/
  scratch/h2f_trades.json prefix (1130 trades, read-only) **plus** the live
  overlay's new closed trades (factor_live_trades.json) when present; exact
  rolling mean/sum/win/t/percentile via factor_vault SGAP calibration.
- Harbor S-3 stock leg last-40: h2d_blotter_long.json (416 frozen trades,
  read-only) plus live stock-leg appends and live parking episodes from the
  same overlay (all additive Harbor NAV points), with the H2d calibration.
- H01/H02/H03 daily event pilots last-40: the frozen-prefix sidecar extracted
  from h2_records.pkl (read-only) plus live closed appends, reported as the
  mean % per trade (``%/笔``) — never revived without a real percentile.
- All other pilots: frozen window aggregates embedded in
  service/factor_vault.py (FINAL/H2/G/H2b/H2d/H2j/H2k).

The four audit windows (OOS2/train/valid/holdout) are never recomputed —
only the rolling last-40 override follows the live blotter.

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

from data_sync_service.service import factor_live as fl  # noqa: E402
from data_sync_service.service import factor_live_daily as fld  # noqa: E402
from data_sync_service.service import factor_vault as fv  # noqa: E402

AUDIT_SCRATCH = fl.audit_scratch_dir()
DEFAULT_OUTPUT = (
    Path(__file__).resolve().parents[1] / "data" / "backtest_reports" / "factor_vault.json"
)
DEFAULT_HISTORY = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "backtest_reports"
    / "factor_vault_history.json"
)


def _sgap_override(live: dict | None) -> dict[str, dict] | None:
    """Exact S-gap last-40: frozen h2f prefix + live closed appends."""
    frozen = fl.load_frozen_sgap(AUDIT_SCRATCH / fl.SGAP_FROZEN_FILENAME)
    stats = fl.live_override_stats(
        live, family="sgap", frozen=frozen, mu=fv.SGAP_MU, sigma=fv.SGAP_SIGMA
    )
    if stats is None:
        return None
    return {
        "starship_b": {
            "percentile": stats["percentile"],
            "net": stats["net"],
            "win_rate": stats["win_rate"],
            "trades": stats["trades"],
            "window": "live" if live else "holdout",
            "as_of": stats["as_of"],
            "last_trade_as_of": stats["last_trade_as_of"],
            "basis": "live:sgap" if live else "frozen:sgap",
        }
    }


def _harbor_override(live: dict | None) -> dict[str, dict] | None:
    """Exact Harbor last-40: frozen H2d stock leg + live stock/parking legs.

    h2d_blotter_long.json (416 trades) already contains every frozen window;
    the per-window files are subsets, so concatenating would double-count.
    Live stock trades and parking episodes are additive Harbor NAV points, so
    they can extend the same rolling window (the frozen prefix stays stock-only
    because the H2d audit never recorded the parking leg).
    """
    frozen = fl.load_frozen_harbor(AUDIT_SCRATCH / fl.HARBOR_FROZEN_FILENAME)
    stats = fl.live_override_stats(
        live, family="harbor", frozen=frozen, mu=fv.HARBOR_MU, sigma=fv.HARBOR_SIGMA
    )
    if stats is None:
        return None
    return {
        "harbor": {
            "percentile": stats["percentile"],
            "net": stats["net"],
            "win_rate": stats["win_rate"],
            "trades": stats["trades"],
            "window": "live" if live else ("valid" if stats["trades"] <= 16 else "long"),
            "as_of": stats["as_of"],
            "last_trade_as_of": stats["last_trade_as_of"],
            "basis": "live:harbor" if live else "frozen:harbor",
        }
    }


def _live_meta(live: dict | None) -> dict | None:
    if not live:
        return None
    meta = live.get("meta") or {}
    return {
        "status": meta.get("status"),
        "asOf": meta.get("asOf"),
        "updatedAt": meta.get("updatedAt"),
        "reason": meta.get("reason"),
        "appended": {
            fam: len((live.get("appended") or {}).get(fam) or [])
            for fam in ("sgap", "harbor", *fld.FAMILIES)
        },
        "parkingEpisodes": len((live.get("parking") or {}).get("episodes") or []),
    }


def _daily_override(
    live: dict | None, prefix: dict | None
) -> dict[str, dict] | None:
    """Exact H01/H02/H03 last-40 as mean %/笔 (frozen prefix + live appends).

    The audit windows stay frozen; only the rolling window follows the live
    daily replay. Without both a prefix row and an appended row the builder
    returns nothing and the vault keeps its embedded holdout proxy.
    """
    if not prefix:
        return None
    out: dict[str, dict] = {}
    appended = (live or {}).get("appended") or {}
    for family in fld.FAMILIES:
        frozen = fld.family_rows(prefix, family)
        stats = fl.daily_override_stats(live, family=family, frozen=frozen)
        if stats is None:
            continue
        is_live = bool(appended.get(family))
        out[family] = {
            "percentile": stats["percentile"],
            "net": stats["net"],
            "win_rate": stats["win_rate"],
            "trades": stats["trades"],
            "window": "live" if is_live else "holdout",
            "as_of": stats["as_of"],
            "last_trade_as_of": stats["last_trade_as_of"],
            "basis": f"live:{family}" if is_live else f"frozen:{family}",
        }
    return out or None


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

    out_path = Path(args.output)
    hist_path = Path(args.history)
    live = fl.load_live_payload(out_path.parent / fl.LIVE_FILENAME)
    try:
        daily_prefix = fld.load_frozen_prefix(out_path.parent)
    except Exception:
        daily_prefix = None

    overrides: dict[str, dict] = {}
    for builder in (_sgap_override, _harbor_override):
        try:
            ov = builder(live)
        except Exception:
            ov = None
        if ov:
            overrides.update(ov)
    try:
        daily_ov = _daily_override(live, daily_prefix)
    except Exception:
        daily_ov = None
    if daily_ov:
        overrides.update(daily_ov)

    prev_history = _load_history(hist_path)

    payload = fv.compute_factor_vault(
        overrides=overrides or None,
        prev_history=prev_history or None,
        generated_at=args.date,
    )
    payload["meta"]["source_path"] = (
        "scratch/h2f_trades.json + h2d_blotter_long.json + h2_records.pkl "
        "(frozen prefix, read-only) + factor_live_trades.json (live overlay) "
        "+ frozen FACTOR_DEFS"
        if live
        else "scratch/h2f_trades.json + h2d_blotter_long.json + h2_records.pkl "
        "+ frozen FACTOR_DEFS"
    )
    payload["meta"]["overrides"] = sorted(overrides.keys())
    payload["meta"]["liveOverlay"] = _live_meta(live)

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
