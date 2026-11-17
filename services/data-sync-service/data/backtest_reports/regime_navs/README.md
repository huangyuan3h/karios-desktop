# Regime NAV snapshots (2026-11-15 research)

Exact daily NAVs used by the 2-state regime study and the Watchlist auto target.

- `daily_navs.csv` — 2021-08-02..2026-08-07, columns:
  `h2a25, starport, m30, harbor, m50, satellite, b3`.
  `h2a25 = starship_robust`; `starport = homeport M50 × satellite w=0.2`;
  `b3` from the audit `compare_nav` panel.
- `*.json` — raw timeline payloads from
  `GET /api/backtest/timeline` (`homeport_m30`, `homeport`, `harbor`,
  `starship`, `starship_robust`, `starport`). Large and regenerable.
- `regime_switch_exact.json` / `regime_switch_exact_robust.json` — exact
  rerun (2-state / E / D, path MC + strict regime Markov MC).
- Paper log: `../regime_paper/paper_log.csv`; plan:
  `docs/backtests/stable/regime-switch-paper-2026-11-15.md`.

Regenerate:

```bash
cd services/data-sync-service
PYTHONPATH=src python3 scripts/export_regime_navs.py \
  --start 2021-08-02 --end 2026-08-07 --resume
PYTHONPATH=src python3 scripts/regime_switch_exact.py
PYTHONPATH=src python3 scripts/regime_switch_paper.py --append
```

The Watchlist page reads `GET /watchlist/regime-target` (month-end 000300 vs
MA200, 2-state); it is the only source for the auto target. Do not use an
intraday MA200 cross for this label.
