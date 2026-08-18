import { apiGetJson, apiPostJson } from '@/lib/api/client';
import {
  ensureWatchlistHydrated,
  loadWatchlist,
  saveWatchlist,
  type WatchlistItem,
} from '@/lib/watchlist-storage';

export type AutomationRemoveItem = {
  symbol: string;
  reason?: string;
};

export type AutomationAlphaAddItem = {
  symbol: string;
  name?: string;
  catalystScore?: number;
  /** TIP-012: 'research' marks 研报 → Alpha channel candidates. */
  channel?: string | null;
};

export type AutomationRun = {
  runId: string;
  tradeDate?: string;
  skipped?: boolean;
  skipReason?: string | null;
  remove?: AutomationRemoveItem[];
  alphaAdd?: AutomationAlphaAddItem[];
  meta?: Record<string, unknown>;
  trigger?: string;
  appliedAt?: string | null;
  screenerAdded?: number | null;
  createdAt?: string;
};

export type ApplyAutomationResult = {
  removed: number;
  screenerAdded: number;
  alphaAdded: number;
};

export { isAutomationPollWindow } from '@/lib/market-hours';

export async function fetchAutomationPending(tradeDate?: string): Promise<AutomationRun | null> {
  const q = tradeDate ? `?tradeDate=${encodeURIComponent(tradeDate)}` : '';
  const res = await apiGetJson<{ pending: boolean } & AutomationRun>(`/watchlist/automation/pending${q}`);
  if (!res.pending || !res.runId) return null;
  return res;
}

export async function fetchAutomationLatest(): Promise<AutomationRun | null> {
  const res = await apiGetJson<{ found: boolean } & AutomationRun>('/watchlist/automation/latest');
  if (!res.found || !res.runId) return null;
  return res;
}

/** TIP-002: N-day funnel history — one acknowledged run per trade_date, newest first. */
export async function fetchFunnelHistory(limit = 10): Promise<AutomationRun[]> {
  const res = await apiGetJson<{ ok: boolean; runs?: AutomationRun[] }>(
    `/watchlist/automation/runs?limit=${limit}`,
  );
  return Array.isArray(res.runs) ? res.runs : [];
}

export async function triggerAutomationRun(force = true): Promise<AutomationRun> {
  return apiPostJson<AutomationRun>(`/watchlist/automation/run?force=${force ? 'true' : 'false'}`);
}

export async function ackAutomationRun(runId: string): Promise<void> {
  await apiPostJson(`/watchlist/automation/${encodeURIComponent(runId)}/ack`, {});
}

export function funnelFromMeta(meta: Record<string, unknown> | undefined): Record<string, number> | null {
  const raw = meta?.funnel;
  if (!raw || typeof raw !== 'object') return null;
  const f = raw as Record<string, unknown>;
  const num = (k: string) => (typeof f[k] === 'number' && Number.isFinite(f[k]) ? Number(f[k]) : 0);
  return {
    tvHit: num('tvHit'),
    passPullback: num('passPullback'),
    passTrendOk: num('passTrendOk'),
    addedNew: num('addedNew'),
    droppedByPullback: num('droppedByPullback'),
    fallbackUsed: Number(Boolean(f.fallbackUsed)),
    fallbackHit: num('fallbackHit'),
    fallbackTrendOk: num('fallbackTrendOk'),
    fallbackAdded: num('fallbackAdded'),
  };
}

/** Derive sync ok from automation meta blobs (success paths may omit explicit ok). */
export function isAutomationSyncOk(sync: unknown): boolean {
  if (!sync || typeof sync !== 'object') return true;
  const s = sync as Record<string, unknown>;
  if (s.ok === false) return false;
  if (typeof s.error === 'string' && s.error.trim()) return false;
  if (typeof s.failed === 'number' && s.failed > 0) return false;
  return true;
}

export function formatAutomationSyncPart(meta: Record<string, unknown> | undefined): string {
  if (!meta) return '';
  const hasIndustry = Object.prototype.hasOwnProperty.call(meta, 'industrySync');
  if (!hasIndustry) return '';
  const ind = hasIndustry ? (isAutomationSyncOk(meta.industrySync) ? '✓' : '✗') : '—';
  return ` | sync ind${ind}`;
}

export function formatAutomationTop5Part(meta: Record<string, unknown> | undefined): string {
  const raw = meta?.top5dIndustries;
  if (!Array.isArray(raw) || !raw.length) return '';
  const names = raw
    .map((x) => String(x ?? '').trim())
    .filter(Boolean)
    .slice(0, 5);
  if (!names.length) return '';
  return ` | top5 ${names.join(',')}`;
}

export async function applyAutomationRun(
  run: AutomationRun,
  options?: {
    silent?: boolean;
    onStage?: (label: string) => void;
    existingItems?: WatchlistItem[];
  },
): Promise<ApplyAutomationResult> {
  if (run.skipped) {
    throw new Error(run.skipReason || 'automation skipped');
  }

  const onStage = options?.onStage;
  let items = options?.existingItems ?? loadWatchlist();

  onStage?.('Removing weak symbols…');
  const removeSet = new Set((run.remove ?? []).map((x) => x.symbol));
  const before = items.length;
  items = items.filter((x) => !removeSet.has(x.symbol));
  const removed = before - items.length;
  await saveWatchlist(items);

  onStage?.('Appending Alpha Radar S candidates…');
  const existing = new Set(items.map((x) => x.symbol));
  const now = new Date().toISOString();
  let alphaAdded = 0;
  for (const row of run.alphaAdd ?? []) {
    const sym = String(row.symbol || '').trim();
    if (!sym || existing.has(sym)) continue;
    existing.add(sym);
    items.push({
      symbol: sym,
      name: row.name ?? null,
      addedAt: now,
      color: '#e0e7ff',
      // TIP-012: research-channel candidates (研报 → α) keep their own
      // registry source so the Watchlist / journal can attribute them.
      source: row.channel === 'research' ? 'research' : 'alpha_radar',
    });
    alphaAdded += 1;
  }
  if (alphaAdded > 0) await saveWatchlist(items);

  onStage?.('Acknowledging automation run…');
  await ackAutomationRun(run.runId);

  if (typeof window !== 'undefined') {
    try {
      window.localStorage.setItem('karios.watchlist.automation.ackedRunId', run.runId);
    } catch {
      // ignore
    }
  }

  return {
    removed,
    screenerAdded: 0,
    alphaAdded,
  };
}

export async function runManualAutomation(options?: {
  force?: boolean;
  onStage?: (label: string) => void;
}): Promise<{ run: AutomationRun; result?: ApplyAutomationResult }> {
  options?.onStage?.('Ensuring registry is synced…');
  await ensureWatchlistHydrated();
  options?.onStage?.('Running backend automation…');
  const run = await triggerAutomationRun(options?.force ?? true);
  if (run.skipped) {
    return { run };
  }
  const result = await applyAutomationRun(run, { onStage: options?.onStage });
  return { run, result };
}

export function formatAutomationSummary(
  run: AutomationRun | null,
  result?: ApplyAutomationResult | null,
): string | null {
  if (!run) return null;
  if (run.skipped) {
    return `Skipped: ${run.skipReason || 'unknown'}`;
  }
  const removed = result?.removed ?? run.remove?.length ?? 0;
  const alpha = result?.alphaAdded ?? run.alphaAdd?.length ?? 0;
  const research = run.meta?.researchCandidates ?? 0;
  const when = run.createdAt ? new Date(run.createdAt).toLocaleString() : '—';
  const trigger = run.trigger || 'unknown';
  const researchPart =
    typeof research === 'number' && research > 0 ? ` | 研报α +${research}` : '';
  const rejected = run.meta?.alphaRejected;
  let rejectPart = '';
  if (rejected && typeof rejected === 'object') {
    const entries = Object.entries(rejected as Record<string, unknown>)
      .filter(([, v]) => typeof v === 'number' && v > 0)
      .map(([k, v]) => `${k}:${v}`);
    if (entries.length) rejectPart = ` | alphaReject ${entries.join(',')}`;
  }
  return `Last automation: ${when} (${trigger}) | −${removed} removed · alpha +${alpha}${researchPart}${rejectPart}${formatAutomationSyncPart(run.meta)}${formatAutomationTop5Part(run.meta)}`;
}
