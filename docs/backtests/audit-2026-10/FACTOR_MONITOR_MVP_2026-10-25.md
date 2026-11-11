# Factor / S-gap 必达 MVP — 落地记录 (2026-10-25)

Goal: when a vault factor status upgrades (cold→watch / *→revived) or the
S-gap K2 `revival_pos` steps up, Yuan's phone Bark fires within ~1 minute of
EOD detection, and NotificationHub shows a research-lane sticky until
localStorage seen-ack.

## What landed

| Layer | Change |
|-------|--------|
| Transition detect | `service/factor_vault.py:diff_factor_transitions()` — pure diff of previous `states` vs today's factors; emits only `factor_status_up` (cold→watch) and `factor_revived` (*→revived); fades ignored |
| Vault job | `scheduler/factor_vault_job.py` — snapshot prev history → subprocess → on fail `record_failure` (job_failed Bark via HIGH), on success diff→`emit_event`→append `factor_vault_transitions.json`→`record_success`; all JSON/emit steps fail-open |
| S-gap job (new) | `scheduler/sgap_decay_job.py` (`sgap_decay_refresh`, `15 19 * * mon-fri` Asia/Shanghai), registered in `scheduler/__init__.py`; compares `summary.revival_pos` before/after; step-up emits `sgap_revival` and appends to the shared transitions file; missing `h2f_trades.json` → `record_failure` (no silent skip) |
| Bark templates | `service/webhook_format.py` — `factor_status_up` (`🧬 {name} 冷库→观察`), `factor_revived` (`🟢 因子复活 {name}`), `sgap_revival` (`🟢 S-gap 复活位 {old}→{new}`) |
| Known types | `api/webhook_routes.py:KNOWN_EVENT_TYPES` += the three types |
| Dark paging | `db/system_events.py:HIGH_JOB_TYPES` += `factor_vault_refresh`, `sgap_decay_refresh` (`TRADING_JOB_TYPES` untouched — no Hub system-cron flood) |
| Job visibility | `api/sync_routes.py:SYNC_JOB_TYPES` += both; `packages/shared` scheduler catalog += both (group `factors`) |
| Hub sticky | `service/notifications.py:_factor_monitor_alerts()` — last-7d transitions → `lane/research`, `book/research`, `anchor/backtest`, `nid` = webhook dedupe_key; `notification-lanes.ts` fallback maps the three types → research |
| Ops script | `scripts/expand_bark_factor_events.py` — idempotent, reversible (`--dry-run`, `--restore-twin-star`, `--drop-factor-events`) |

Event names (exact): `factor_status_up`, `factor_revived`, `sgap_revival`.
Dedupe keys: `factor_status_up:{id}:{new}:{day}`, `factor_revived:{id}:{day}`, `sgap_revival:{pos}:{day}`.

## Bark sub 280 — auto-updated ✅

Script run 2026-10-25 against live Docker Postgres (`karios-desktop`):

- before: `job_failed, satellite_action, twin_star_reminder`
- after: `factor_revived, factor_status_up, job_failed, satellite_action, sgap_revival`

`twin_star_reminder` slot replaced (job retired); same url/secret/provider/enabled. Re-ran to confirm idempotent no-op. Revert: `PYTHONPATH=src python3 scripts/expand_bark_factor_events.py --restore-twin-star`.

## Honest h2f freeze caveat

`h2f_trades.json` mtime still 2026-10-03 — inputs frozen, so both jobs
recompute identical output and emit nothing (verified: live diff = `[]`, no
spam). The day new trades land, the first refresh with a changed status
fires. Starship_b stays cold until then; the path is wired, not the data.

## Verification

- `tests/test_webhook_format.py`, `test_notifications.py`, `test_factor_monitor.py` (new: diff + both jobs' emit/fail paths + registry), `test_factor_vault.py`, `test_sgap_decay.py`, `test_scheduler_jobs_extra.py` (expected set += sgap job), `test_api.py`, `test_webhook_routes.py`, `test_webhook_db.py` — all green.
- Frontend: `notification-lanes.test.ts`, shared `scheduler.test.ts` — green.
- Live simulation: real history vs real vault → `[]`; Bark templates render.

## Follow-ups (deferred per prompt)

`factor_improve` / `factor_monitor_dark` events, unfreezing `h2f_trades`,
auto live trading, Tier B/C ingest, intraday IC, osascript for research,
agent routine, F) UI banner (skipped — Hub is enough for MVP), `todo.md`
untouched.

Backend restart needed: **yes** — new `sgap_decay_refresh` job + new
`_factor_monitor_alerts` wiring only take effect after the uvicorn
data-sync process restarts (next deploy / manual restart; no code action
taken here per safety rules).
