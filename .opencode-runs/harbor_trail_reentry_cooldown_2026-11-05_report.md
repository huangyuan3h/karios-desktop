# Harbor trail re-entry cooldown rerun — 2026-11-05 (executive)

**Verdict: keep k=0. The cooldown is NOT deployed (research only).**

## TL;DR for Yuan

| | k=0 (today) | k=3 | k=5 |
|---|---|---|---|
| valid total | +50.2 | **+36.8 (−13.4)** | **+40.7 (−9.5)** |
| long total | +192.9 | +188.5 (−4.4) | +202.5 (+9.7) |
| valid MDD | −21.8 | −20.6 (+1.2pt, gate ≥3.0) | −24.6 (−2.8pt worse) |
| valid Sharpe | 2.13 | 1.73 (−0.40) | 1.93 (−0.20) |
| valid whips (1/3/5d) | 5 / 5 / 5 | 0 / 0 / **5** | 0 / 0 / 0 |

- The 2026-11-05 OIL `513350` trail → ROTATE-buy-back pain is **real and frequent**: in the valid window 5 of 6 trail exits rebought the same key the next session (long: 9 of 12).
- But the cooldown **only delays** the rebuy: k=3 pushes it from T+1 to T+4, and same-key rebuys within 5 sessions stay **18 across windows, identical to k=0**. The 3 blocked days get parked in GOLD/BOND10 (REPO share unchanged 5.5%), so MDD barely improves and returns lose the V-bounce.
- All frozen K1–K5 gates fail again exactly as 2026-09-13 (valid deltas reproduce within 0.1pt on k1/k3/k5). No new mechanism evidence, no reason to reopen.

## Recommendation

1. **Product/Live: keep `cooldown_days = 0`.** No change to `multi_asset_sleeve`, watchlist, Bark, m30-follow, TRAIL_PCT, M30 70/30, or B3.
2. **Fix the actual discomfort in ops/UX, not economics**: after a trail fill (`SELL_TO_REPO`), downgrade/annotate the same-key ROTATE prompt on the same/next session ("刚止损，回买=鞭打；历史上 83% 发生且事后正收益，请人工确认"). Engine output unchanged.
3. If a mechanical same-key gate is ever wanted, it needs a **new prereg** (normalizing to idle share, not the REJECTed k grid) — not this harness.

## What was run

- Harness extended (metrics only): `trail_exits`, `whip_rebuy_within_{1,3,5}`, `days_by_key` / `time_in_oil` / `time_in_repo`; new `--report-name` so the 09-13 JSON is untouched.
- Grid `{0,3,5}` archived + `{0,1,2,3,5}` continuity run (not saved).
- Panel: `data/etf/etf_daily.csv` through **2026-11-02** (5 series / 1411 sessions). k0 drift vs 09-13 is qfq re-basing (+62.0/+45.3/+50.3/+219.9 → +50.6/+51.7/+50.2/+192.9); **valid is stable (50.3 → 50.2)** so the REJECT reproduces.
- Tests: `tests/test_eval_parking_cooldown_whip.py` (new, 3 cases) + `tests/test_harbor_parking.py` → 14 passed.

## Artifacts

- `docs/backtests/stable/parking-cooldown-rerun-2026-11-05.md` (full Chinese report)
- `services/data-sync-service/data/backtest_reports/parking_cooldown_rerun_2026-11-05.json`
- `services/data-sync-service/scripts/eval_parking_cooldown.py` (metrics + `--report-name`)
- `services/data-sync-service/tests/test_eval_parking_cooldown_whip.py`
- Commit: `<COMMIT>` — local only, no push; unrelated dirty files untouched.

## Safety

No Docker Postgres writes, no Alembic, no service restarts, no live path changes.
