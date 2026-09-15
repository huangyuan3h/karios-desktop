import { apiGetJson, apiPostJson } from '@/lib/api/client';
import { hydrateWatchlist } from '@/lib/watchlist-storage';

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
  const res = await apiGetJson<{ pending: boolean } & AutomationRun>(
    `/watchlist/automation/pending${q}`,
  );
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

export type PoolRunCounts = {
  s3PoolSize: number;
  s3PoolPrevSize: number;
  satellitePoolSize: number;
  addedS3: number;
  addedSatellite: number;
  removedS3: number;
  removedSatellite: number;
};

const numOr = (v: unknown, fallback = 0): number =>
  typeof v === 'number' && Number.isFinite(v) ? v : fallback;

/** OPT-209 pool run meta (replaces the retired TV funnel counts). */
export function poolFromMeta(meta: Record<string, unknown> | undefined): PoolRunCounts | null {
  if (!meta || typeof meta !== 'object') return null;
  const added = (meta.poolAdded ?? {}) as Record<string, unknown>;
  const removed = (meta.poolRemoved ?? {}) as Record<string, unknown>;
  const hasPool = meta.poolAdded != null || meta.poolRemoved != null || meta.poolCaliber != null;
  if (!hasPool) return null;
  return {
    s3PoolSize: numOr(meta.s3PoolSize),
    s3PoolPrevSize: numOr(meta.s3PoolPrevSize),
    satellitePoolSize: numOr(meta.satellitePoolSize),
    addedS3: numOr(added.s3),
    addedSatellite: numOr(added.satellite),
    removedS3: numOr(removed.s3),
    removedSatellite: numOr(removed.satellite),
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

/**
 * OPT-209: the backend now auto-applies the pool run to the registry (S-3 +
 * 星舰 add/GC), so the client only re-hydrates the registry afterwards.
 */
export async function runManualAutomation(options?: {
  force?: boolean;
  onStage?: (label: string) => void;
}): Promise<{ run: AutomationRun; result?: ApplyAutomationResult }> {
  options?.onStage?.('Running backend pool automation…');
  const run = await triggerAutomationRun(options?.force ?? true);
  if (run.skipped) {
    return { run };
  }
  options?.onStage?.('Reloading registry…');
  await hydrateWatchlist();
  return {
    run,
    result: {
      removed: run.remove?.length ?? 0,
      screenerAdded: 0,
      alphaAdded: run.alphaAdd?.length ?? 0,
    },
  };
}

export function formatAutomationSummary(
  run: AutomationRun | null,
  result?: ApplyAutomationResult | null,
): string | null {
  if (!run) return null;
  if (run.skipped) {
    return `Skipped: ${run.skipReason || 'unknown'}`;
  }
  const when = run.createdAt ? new Date(run.createdAt).toLocaleString() : '—';
  const trigger = run.trigger || 'unknown';
  const pool = poolFromMeta(run.meta);
  if (pool) {
    return `Last pool automation: ${when} (${trigger}) | S-3 池 ${pool.s3PoolSize} (+${pool.addedS3} / −${pool.removedS3}) · 星舰 ${pool.satellitePoolSize} (+${pool.addedSatellite} / −${pool.removedSatellite})`;
  }
  // Legacy runs (pre-OPT-209 funnel/alpha counts).
  const removed = result?.removed ?? run.remove?.length ?? 0;
  const alpha = result?.alphaAdded ?? run.alphaAdd?.length ?? 0;
  return `Last automation: ${when} (${trigger}) | −${removed} removed · alpha +${alpha}${formatAutomationSyncPart(run.meta)}${formatAutomationTop5Part(run.meta)}`;
}
