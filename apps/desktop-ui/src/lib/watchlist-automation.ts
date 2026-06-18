import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import { importFromScreener } from '@/lib/watchlist-screener-import';
import {
  loadWatchlist,
  saveWatchlist,
  syncRegistryToBackend,
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

async function apiGetJson<T>(path: string): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, { cache: 'no-store' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

async function apiPostJson<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, {
    method: 'POST',
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

export function getShanghaiMinutes(): number {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Shanghai',
    weekday: 'short',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(new Date());
  const map = new Map(parts.map((p) => [p.type, p.value]));
  const hour = Number(map.get('hour') ?? 0);
  const minute = Number(map.get('minute') ?? 0);
  return hour * 60 + minute;
}

export function isAutomationPollWindow(): boolean {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Shanghai',
    weekday: 'short',
  }).formatToParts(new Date());
  const weekday = parts.find((p) => p.type === 'weekday')?.value ?? '';
  if (!['Mon', 'Tue', 'Wed', 'Thu', 'Fri'].includes(weekday)) return false;
  const minutes = getShanghaiMinutes();
  return minutes >= 17 * 60 + 30 && minutes <= 20 * 60;
}

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
  saveWatchlist(items);

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
  if (alphaAdded > 0) saveWatchlist(items);

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
  options?.onStage?.('Syncing registry…');
  await syncRegistryToBackend();
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
