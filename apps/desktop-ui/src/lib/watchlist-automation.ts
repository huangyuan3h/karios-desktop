import { apiGetJson, apiPostJson } from '@/lib/api/client';
import { importFromScreener } from '@/lib/watchlist-screener-import';
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

export async function triggerAutomationRun(force = true): Promise<AutomationRun> {
  return apiPostJson<AutomationRun>(`/watchlist/automation/run?force=${force ? 'true' : 'false'}`);
}

export async function ackAutomationRun(
  runId: string,
  screenerAdded: number,
): Promise<void> {
  await apiPostJson(`/watchlist/automation/${encodeURIComponent(runId)}/ack`, {
    screenerAdded,
  });
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

  onStage?.('Importing from screener…');
  const screener = await importFromScreener({
    existingItems: loadWatchlist(),
    silent: options?.silent,
    onStage: (label) => onStage?.(label),
  });
  items = loadWatchlist();

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
      source: 'alpha_radar',
    });
    alphaAdded += 1;
  }
  if (alphaAdded > 0) await saveWatchlist(items);

  onStage?.('Acknowledging automation run…');
  await ackAutomationRun(run.runId, screener.addedCount);

  if (typeof window !== 'undefined') {
    try {
      window.localStorage.setItem('karios.watchlist.automation.ackedRunId', run.runId);
    } catch {
      // ignore
    }
  }

  return {
    removed,
    screenerAdded: screener.addedCount,
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
  const screener = result?.screenerAdded ?? run.screenerAdded ?? 0;
  const alpha = result?.alphaAdded ?? run.alphaAdd?.length ?? 0;
  const when = run.createdAt ? new Date(run.createdAt).toLocaleString() : '—';
  const trigger = run.trigger || 'unknown';
  return `Last automation: ${when} (${trigger}) | −${removed} screener +${screener} alpha +${alpha}`;
}
