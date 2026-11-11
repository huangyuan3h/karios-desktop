# Factor Monitor + 必达通知 — Design Draft (2026-10-24)

Research-only. No code changes this turn. Repo: `~/Projects/karios-desktop` (Mac `05baa045`).

## 1. What exists today

### 1.1 Detection / display (「已经有了」= 体检展示，不是推送)

| Piece | Path | Role | Freshness (as of audit) |
|-------|------|------|-------------------------|
| S-gap decay tab | `service/sgap_decay.py`, `SgapDecayPanel.tsx`, `data/.../sgap_decay.json` | H2k K2 `revival_pos` 0/10/20, rolling-40 | `generated_at` **2026-10-03**; **no scheduler job** |
| Factor vault tab | `service/factor_vault.py`, `FactorVaultPanel.tsx`, `factor_vault.json` + `_history.json` | 27 factors → cold/watch/revived (H2k) | history **1 entry** (2026-10-03); files mtime Oct 4 |
| Vault refresh job | `scheduler/factor_vault_job.py` `factor_vault_refresh` cron `10 19 * * mon-fri` Asia/Shanghai | Re-runs `scripts/generate_factor_vault.py` (file-only) | Registered in `scheduler/__init__.py` but **not producing daily history** → effectively stale |
| Trade input for S-gap | `~/Projects/wealth-ideas/.../scratch/h2f_trades.json` | Exact last-40 for starship_b | mtime **2026-10-03** — no live append |
| Classify rule | `factor_vault.classify_factor` | cold: pct&lt;50 or net≤0; watch: 50–75 or n&lt;20; revived: pct≥75 and net&gt;0; no true pct → never revived | Matches UI copy / FACTOR_VAULT_report |

Current vault snapshot: cold 15 / watch 11 / revived 1 (`harbor` ref). **`starship_b` = cold**, pct≈2.2%, net≈−14.79, position 0.

### 1.2 Notify stack that already works for *other* events

```
emit_event(type, payload, dedupe_key)
  → webhook_events
  → webhook_delivery_job (every minute)
  → Bark (provider) via format_bark
```

Also: `GET /api/notifications` → `NotificationHub` (poll `refetchInterval` 5 min, `localStorage` seen-ack). Lanes: trade / system / research.

Live Bark subscription (**id=280**, enabled):

- `event_types`: **`job_failed`, `satellite_action`, `twin_star_reminder` only**
- `twin_star_reminder` job was **retired** (archive OPT / etf-parking) — dead slot in sub list
- **No** `factor_status` / `factor_revival` / `sgap_revival`

`format_bark` supports: execution_card, audit_issues, near_stop, intraday_drawdown, satellite_action, candidate_added, recon_missing, job_failed, test — **not factor events**.

`notifications.py` aggregates exits / pyramid / cron / oos / recon / satellite — **not vault/sgap**.

macOS: `com.karios.healthcheck` LaunchAgent 08:30 `--notify` → osascript **CRITICAL-only** (data freshness), not research factors.

### 1.3 End-to-end today: 「策略复活 → Yuan 被通知」

| Step | Status |
|------|--------|
| New S-gap / factor trades land | ❌ frozen scratch JSON |
| Daily reclassify | ⚠️ job registered, history stuck → not reliably running |
| Detect cold→watch / watch→revived / revival_pos↑ | ✅ logic in vault/sgap (if refreshed) |
| Persist transition | ⚠️ history file only; no event row |
| `emit_event` | ❌ none |
| Bark format + subscription | ❌ |
| NotificationHub item | ❌ |
| Persistent UI banner / ack | ❌ (only localStorage seen for unrelated types) |
| macOS toast for research | ❌ |

**结论：检测 UI 有；必达推送没有。用户说的「已经有了」指冷库/S-gap 体检，不是手机/桌面必达。**

---

## 2. Hardening gaps（必达加固清单）

1. **No transition emit** — vault/sgap never call `emit_event` on status change.
2. **Bark sub blind** — current sub ignores new event types until Yuan adds them (or we expand default + doc).
3. **Stale compute pipeline** — `factor_vault_history` 1 day; `h2f_trades` frozen; no `sgap_decay` daily job → cannot see recovery even in UI.
4. **Pull-only UI** — Hub needs desktop open; vault `staleTime` 30 min; no badge for research revival.
5. **Mac sleep** — 19:10 APScheduler dies with lid-closed Mac; need LaunchAgent watchdog or catch-up on wake (same class as paper-chain 18:05).
6. **Job silent fail** — `factor_vault_job` logs warning, no `insert_record` / no Bark on consecutive fail → Yuan never learns monitor is dark.
7. **Dead twin_star slot** — sub still lists retired event; wastes attention / confuses ops.
8. **No hysteresis / mute for research** — if we only emit on every regenerate, spam risk; need transition-only + cooldown + ack.
9. **False “never revive”** — pilots without true placebo pct stay watch forever; 「明显变好」needs a second metric lane (Δnet / Δpct vs own baseline), not only H2k revived badge.
10. **No multi-channel ack** — Bark delivered ≠ Yuan saw it; need Hub sticky + optional morning brief line + optional agent routine ping.

---

## 3. Factor-monitor design（可落地）

### 3.1 Watch list sources

| Tier | Source | Examples |
|------|--------|----------|
| A 必盯 | Factor vault FACTOR_DEFS (27) + S-gap K2 | `starship_b`, harbor/m30/bleg refs |
| B 观察仓 | STRATEGY_LEDGER「观察」rows | 星舰B re-enable gates; DIV_LV1 条件观察 |
| C 研究池 | hyp_search survivors, ep82–95 overall top variants, PAPER_ONLY candidates | series-output `research/*` registries — **promote into vault defs only after ledger gate**, don’t auto-trade |

MVP = Tier A only (reuse vault). Expand B/C after emit path proven.

### 3.2 「明显变好」metrics

Reuse H2k for primary badge; add **improve** events that are not full revive:

| Event | Rule (hysteresis) |
|-------|-------------------|
| `factor_status_up` | status cold→watch or watch→revived (from history last≠today) |
| `sgap_revival_step` | `revival_pos` 0→10 or 10→20 |
| `factor_improve` | same status, but rolling-40 **net** +≥X vs prior snapshot AND **pct** +≥10pt (defaults: X= +2pt for contrib units / or +5% for %/trade factors); require n≥20; cooldown 5 trading days per factor |
| `factor_monitor_dark` | vault refresh failed 2× consecutive OR history date lag &gt; 2 trading days |

OFF / spam guards: emit only on **transition or improve threshold cross**; dedupe_key `factor_status:{id}:{new_status}:{date}`; mute 7d after ack in Hub.

Baseline = **own prior vault snapshot** (history), not vs M30 (M30 stays live baseline for *strategy found*; this monitor is *health wake-up*).

### 3.3 Cadence

**EOD weekdays after close chain** — keep / fix `19:10` vault refresh; add sibling `sgap_decay_refresh` ~19:15 (same inputs).

Not intraday: S-gap/most vault factors are trade/period based; no reliable real-time IC feed. 「实时性」= **push within ~1 min of EOD detection** (webhook_delivery already `* * * * *`), not tick-level.

Catch-up: on scheduler start / LaunchAgent wake, if last history date &lt; last open day → run refresh once.

### 3.4 Delivery layers（必达）

1. **Bark** (phone) — new event types; expand sub 280 (or new sub) to include them; `format_bark` titles e.g. `🧬 星舰B 冷库→观察`, `🟢 因子复活 harbor`.
2. **NotificationHub** — `notifications.py` append research-lane items from latest transitions file / webhook_events; sticky until ack; lane already has `research`.
3. **Backtest tab banner** — FactorVaultPanel / SgapDecayPanel top strip when any transition in last 7d unacked.
4. **Morning brief line** (optional P1.5) — 08:30 brief section「冷库变动」.
5. **Monitor-dark path** — if refresh fails: `job_failed` already Bark-subscribed; **wire `factor_vault_refresh` through `insert_record` + HIGH** so dark pipeline pages Yuan.
6. **Agent routine** (optional) — external Grok routine weekday 20:00: pull `/api/backtest/factor-vault` meta.generated_at + summary; ping if lag or revived countΔ — backup if Mac slept through Bark.

Ack/mute: Hub `seen` + new `muted_until` per factor id (localStorage or small DB table later).

### 3.5 Code landings (OpenCode later)

| Layer | Change |
|-------|--------|
| Job | Harden `factor_vault_job`: `insert_record`, emit transitions after generate; add `sgap_decay_job` |
| Script | `generate_factor_vault.py`: return `transitions[]`; write `factor_vault_transitions.json` (or emit inline) |
| Trade feed | Append path for starship paper/closed blotter → h2f or DB-backed last-40 (unblock real recovery) |
| Webhook | `format_bark` + cookbook; update sub event_types |
| API/UI | `notifications.py` research items; vault/sgap banner; lane map `factor_status*` → research |
| Ops | LaunchAgent catch-up OR document caffeinate; replace dead `twin_star_reminder` in Bark sub |

---

## 4. MVP slice（最小有用）

**Goal:** Yuan’s phone Bark fires when `starship_b` leaves cold OR any vault factor hits revived, and when the monitor goes dark.

1. After `generate_factor_vault` succeeds: diff vs `factor_vault_history` last states → `emit_event("factor_status_up"|"factor_revived", …)`.
2. `format_bark` + add those types to Bark sub 280 (Yuan confirms once).
3. `insert_record` on `factor_vault_refresh` so fail → existing `job_failed` Bark.
4. One NotificationHub research item from same transitions (ack via existing seen).
5. Manual once: refresh h2f or document that without new trades starship stays cold (honest).

Defer: Tier B/C auto-ingest, intraday IC, macOS osascript for research, agent routine (P1.5).

---

## 5. Evidence pointers

- Vault job: `services/data-sync-service/src/data_sync_service/scheduler/factor_vault_job.py`
- Classify: `.../service/factor_vault.py` (`classify_factor` ~L607)
- Bark sub live: id 280 → `job_failed|satellite_action|twin_star_reminder`
- Print: `docs/backtests/audit-2026-10/FACTOR_VAULT_report.md`
- Ledger: `docs/backtests/audit-2026-10/STRATEGY_LEDGER.md` (星舰B 观察; M30 基线)
