import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import { loadJson, saveJson } from '@/lib/storage';

export const WATCHLIST_STORAGE_KEY = 'karios.watchlist.v1';
export const WATCHLIST_UPDATED_EVENT = 'karios:watchlist-updated';

export type WatchlistSource = 'manual' | 'screener' | 'alpha_radar';

export type WatchlistItem = {
  symbol: string;
  name?: string | null;
  nameStatus?: 'resolved' | 'not_found';
  addedAt: string;
  color?: string;
  positionPct?: number | null;
  costPrice?: number | null;
  maxPrice?: number | null;
  source?: WatchlistSource;
};

export function loadWatchlist(): WatchlistItem[] {
  return loadJson<WatchlistItem[]>(WATCHLIST_STORAGE_KEY, []);
}

export function saveWatchlist(items: WatchlistItem[]): void {
  saveJson(WATCHLIST_STORAGE_KEY, items);
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent(WATCHLIST_UPDATED_EVENT, { detail: { items } }));
  }
}

export async function syncRegistryToBackend(items?: WatchlistItem[]): Promise<void> {
  const payload = items ?? loadWatchlist();
  const res = await fetch(`${DATA_SYNC_BASE_URL}/watchlist/registry`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ items: payload }),
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  }
}

export function normalizeWatchlistItem(item: Partial<WatchlistItem> & { symbol: string }): WatchlistItem {
  return {
    symbol: item.symbol,
    name: item.name ?? null,
    nameStatus: item.nameStatus,
    addedAt: item.addedAt || new Date().toISOString(),
    color: item.color ?? '#ffffff',
    positionPct: item.positionPct ?? null,
    costPrice: item.costPrice ?? null,
    maxPrice: item.maxPrice ?? null,
    source: item.source ?? 'manual',
  };
}
