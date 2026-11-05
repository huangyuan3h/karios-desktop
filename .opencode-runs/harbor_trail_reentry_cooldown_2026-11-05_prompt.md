# Task: Harbor trail re-entry cooldown re-run + whip metrics (2026-11-05)

You run unattended on Yuan's Mac in `~/Projects/karios-desktop` on branch `feat/watchlist-follow-m30`. Never ask questions; make sensible decisions; write `.opencode-runs/harbor_trail_reentry_cooldown_2026-11-05_report.md`. Read `AGENTS.md` first.

## Why (live context, Asia/Shanghai 2026-11-05)
- Yuan sold parking OIL `513350` after 8% trail (`SELL_TO_REPO`). Registry 0% is correct.
- Same day/next, engine ROTATE wants OIL again → whip-saw conflict with the just-executed trail.
- He asked: backtest whether same-key re-entry cooldown after trail exit helps (N=0 vs 3 vs 5).

## CRITICAL: prior experiment already exists
- Prereg: `docs/designs/parking-cooldown-prereg-2026-09-13.md` (H-PARK-C)
- Report: `docs/backtests/stable/parking-cooldown-2026-09-13.md` → **REJECT** for k∈{1,2,3,5}
- Harness: `services/data-sync-service/scripts/eval_parking_cooldown.py`
- Engine: `service/harbor.py` `parking_replay(..., cooldown_days=k)` + `pick_parking(exclude_keys=...)`
- Mechanism already matches the ask: after **trail** exit, exited **key** ineligible for k sessions; next eligible candidate else REPO. Rotation-only exits do NOT start cooldown. `k=0` = incumbent. TRAIL_PCT stays 8%.

Do **not** invent a parallel one-off sim. Extend this harness.

## What to do

### 1) Research-only (NO live product change)
- Do **not** wire cooldown into live watchlist / multi_asset_sleeve / Bark / m30-follow.
- Do **not** change default `cooldown_days` (must remain 0 in product paths).
- Do **not** change `TRAIL_PCT`, T-close / T+1-open clock, M30 70/30, B3.
- Optional hysteresis (px > trail×1.02) — **SKIP** unless the base grid finishes in <<10 min and you still have time; prefer whip metrics over D.

### 2) Extend harness metrics (minimal diff)
In `eval_parking_cooldown.py` (and tiny helpers in harbor replay only if needed), for each k and window add:
- trail_exits count
- whip_rebuy_within_1 / _3 / _5: count of times the **same key** is bought/rotated back within 1/3/5 trading days after a trail exit (k=0 baseline must show these; cooldown should cut them)
- time_in_oil / time_in_repo (or time_in_key for OIL + REPO/cash days) as fractions or day counts
- existing: total, delta vs k0, mdd, sharpe, trades, cooldown_days_active

Keep grid at least `{0,3,5}`; including `{1,2}` is fine for continuity with H-PARK-C.

Reuse frozen K1–K5 from the prereg (do not invent softer gates). Re-state them in the new report.

### 3) Run
```bash
cd ~/Projects/karios-desktop/services/data-sync-service
export PATH="$HOME/.nvm/versions/node/v24.21.0/bin:$HOME/Library/pnpm:$PATH"
# Eastmoney direct if any fetch; don't set EASTMONEY_PROXY
unset EASTMONEY_PROXY || true
PYTHONPATH=src:scripts python3 scripts/eval_parking_cooldown.py --grid 0,3,5 --save-report
# If you keep full grid: --grid 0,1,2,3,5
```
- ETF panel is `data/etf/etf_daily.csv` (through ~2026-11-02). Do not need a full resync unless load fails.
- Windows: same as script (`OOS2`/`train`/`valid`/`long` from `run_walk_forward.WINDOWS`).
- Save JSON under `data/backtest_reports/` with a **new** dated name, e.g. `parking_cooldown_rerun_2026-11-05.json` (do not overwrite `parking_cooldown_2026-09-13.json`).

### 4) Docs
Write Chinese report matching stable style:
- `docs/backtests/stable/parking-cooldown-rerun-2026-11-05.md`
Must include: link to 09-13 REJECT + prereg; what changed (data through date + whip metrics); tables for 0/3/5; K1–K5 verdict; whip counts vs baseline; recommendation (**research only**); reproduce command.
Also write the `.opencode-runs/..._report.md` executive summary.
Optional one-line pointer in `docs/backtests/SUMMARY.md` if that file already indexes H-PARK-C — keep minimal.

### 5) Tests + commit
- If you touch `harbor.py`, keep `tests/test_harbor_parking.py` green; add a small unit only if whip helper needs it.
- pytest the touched parking tests.
- **Local commit only**, explicit paths, no `git add -A`, **no push**.
- Worktree is DIRTY (AccountBar/MobileShell/WatchlistPage/factor_vault/em_push2/zt_pool/…). Never revert/stash unrelated files. Stage ONLY:
  - `services/data-sync-service/scripts/eval_parking_cooldown.py` (and harbor.py / tests if changed)
  - `services/data-sync-service/data/backtest_reports/parking_cooldown_rerun_2026-11-05.json`
  - `docs/backtests/stable/parking-cooldown-rerun-2026-11-05.md`
  - `.opencode-runs/harbor_trail_reentry_cooldown_2026-11-05_{prompt,report}.md`
  - optional SUMMARY one-liner
- Commit message e.g. `docs(backtest): harbor trail reentry cooldown rerun 2026-11-05 (whip metrics)`

## Verdict guidance (write explicitly)
- If k=3/5 still fail K1–K5 like 09-13 → **recommend keep k=0**; live conflict = **ops/UX** (suppress same-day buy prompts after trail fill), not economic rule change.
- If a k newly passes all gates on fresh data → mark **candidate only**, still not Live; say paper/todo needed.
- Call out if whip metrics fall but total/MDD/Sharpe worsen (classic #1 right-tail cut).

## Hard safety
- Docker Postgres LIVE — no drop/alter/truncate; no Alembic against it.
- Prefer not killing Next/AI/data-sync; this task needs no restart.
- PATH: `export PATH="$HOME/.nvm/versions/node/v24.21.0/bin:$HOME/Library/pnpm:$PATH"`

## Success criteria
1. Fresh run numbers for k=0/3/5 on current ETF panel with whip metrics.
2. Report + JSON + opencode report committed locally.
3. Clear winner recommendation for Yuan (0 vs 3 vs 5) with key deltas.
4. Live cooldown **not** deployed.

HARBOUR TRAIL REENTRY COOLDOWN RERUN DONE when report exists and commit hash is recorded in the report.
