import type { WatchlistItem, WatchlistSource } from '@karios/shared';
import { WatchlistItemSchema } from '@karios/shared';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import { loadJson, saveJson } from '@/lib/storage';

export const WATCHLIST_STORAGE_KEY = 'karios.watchlist.v1';
export const WATCHLIST_UPDATED_EVENT = 'karios:watchlist-updated';
export const WATCHLIST_REGISTRY_SYNCED_KEY = 'karios.watchlist.registrySynced.v1';
export const WATCHLIST_PENDING_SYNC_KEY = 'karios.watchlist.pendingSync.v1';

export type { WatchlistItem, WatchlistSource };

export type HydrateWatchlistResult = {
  source: 'registry' | 'local_uplift' | 'local_fallback' | 'empty';
  items: WatchlistItem[];
  pendingSync: boolean;
};

export type PersistWatchlistResult = {
  ok: boolean;
  synced: boolean;
};

const ALLOWED_FLAG_COLORS = new Set([
  '#ffffff',
  '#fee2e2',
  '#ffedd5',
  '#fef9c3',
  '#dcfce7',
  '#dbeafe',
  '#f3e8ff',
  '#f4f4f5',
]);

let hydratePromise: Promise<HydrateWatchlistResult> | null = null;

function dispatchWatchlistUpdated(items: WatchlistItem[]): void {
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent(WATCHLIST_UPDATED_EVENT, { detail: { items } }));
  }
}

function readPendingSync(): boolean {
  if (typeof window === 'undefined') return false;
  try {
    return window.localStorage.getItem(WATCHLIST_PENDING_SYNC_KEY) === 'true';
  } catch {
    return false;
  }
}

function setPendingSync(value: boolean): void {
  if (typeof window === 'undefined') return;
  try {
    if (value) {
      window.localStorage.setItem(WATCHLIST_PENDING_SYNC_KEY, 'true');
    } else {
      window.localStorage.removeItem(WATCHLIST_PENDING_SYNC_KEY);
    }
  } catch {
    // ignore
  }
}

function setRegistrySynced(value: boolean): void {
  if (typeof window === 'undefined') return;
  try {
    if (value) {
      window.localStorage.setItem(WATCHLIST_REGISTRY_SYNCED_KEY, 'true');
    } else {
      window.localStorage.removeItem(WATCHLIST_REGISTRY_SYNCED_KEY);
    }
  } catch {
    // ignore
  }
}

export function normalizeWatchlistItems(raw: unknown): WatchlistItem[] {
  const arr = Array.isArray(raw) ? raw : [];
  return arr
    .filter((x) => x && typeof x === 'object')
    .map((x) => {
      const it = x as Partial<WatchlistItem> & { note?: unknown };
      const rawColor = typeof it.color === 'string' ? it.color.trim().toLowerCase() : '';
      const color = ALLOWED_FLAG_COLORS.has(rawColor) ? rawColor : '#ffffff';
      const symbol = String(it.symbol ?? '').trim();
      const normalized = {
        symbol,
        name: it.name ?? null,
        nameStatus:
          it.nameStatus === 'resolved' || it.nameStatus === 'not_found' ? it.nameStatus : undefined,
        addedAt: String(it.addedAt ?? new Date().toISOString()),
        color,
        positionPct:
          typeof it.positionPct === 'number' && Number.isFinite(it.positionPct)
            ? Math.max(0, Math.min(100, it.positionPct))
            : null,
        costPrice:
          typeof it.costPrice === 'number' && Number.isFinite(it.costPrice) ? it.costPrice : null,
        maxPrice:
          typeof it.maxPrice === 'number' && Number.isFinite(it.maxPrice) ? it.maxPrice : null,
        entryDate:
          typeof it.entryDate === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(it.entryDate.trim())
            ? it.entryDate.trim()
            : null,
        source:
          it.source === 'manual' ||
          it.source === 'screener' ||
          it.source === 'screener_fallback' ||
          it.source === 'alpha_radar'
            ? it.source
            : 'manual',
      } satisfies WatchlistItem;
      const parsed = WatchlistItemSchema.safeParse(normalized);
      return parsed.success ? parsed.data : normalized;
    })
    .filter((x) => Boolean(x.symbol));
}

/**
 * V6.2 Zero-Pos Auto-Purge: when Pos% is 0/null, clear holding anchors so Action
 * leaves HOLD/ENTRY_DATE_MISSING and can re-evaluate as WATCH / WATCH_SILENT / PURGE.
 */
export function applyZeroPositionCleanup(item: WatchlistItem): WatchlistItem {
  const pct = item.positionPct;
  const cleared = pct == null || (typeof pct === 'number' && Number.isFinite(pct) && pct <= 0);
  if (!cleared) return item;
  return {
    ...item,
    positionPct: pct != null && pct <= 0 ? pct : null,
    costPrice: null,
    maxPrice: null,
    entryDate: null,
  };
}

export function loadWatchlist(): WatchlistItem[] {
  return normalizeWatchlistItems(loadJson<WatchlistItem[]>(WATCHLIST_STORAGE_KEY, []));
}

export function saveWatchlistLocal(items: WatchlistItem[]): void {
  const normalized = normalizeWatchlistItems(items);
  saveJson(WATCHLIST_STORAGE_KEY, normalized);
  dispatchWatchlistUpdated(normalized);
}

export async function fetchWatchlistFromBackend(): Promise<WatchlistItem[]> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}/watchlist/registry`, { cache: 'no-store' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  }
  const payload = txt ? (JSON.parse(txt) as { items?: unknown }) : { items: [] };
  return normalizeWatchlistItems(payload.items ?? []);
}

export async function syncRegistryToBackend(items?: WatchlistItem[]): Promise<void> {
  const payload = normalizeWatchlistItems(items ?? loadWatchlist());
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

export async function persistWatchlist(items: WatchlistItem[]): Promise<PersistWatchlistResult> {
  const normalized = normalizeWatchlistItems(items);
  try {
    await syncRegistryToBackend(normalized);
    saveWatchlistLocal(normalized);
    setPendingSync(false);
    setRegistrySynced(true);
    return { ok: true, synced: true };
  } catch {
    saveWatchlistLocal(normalized);
    setPendingSync(true);
    return { ok: true, synced: false };
  }
}

/** POST registry first, then update local cache (authoritative backend). */
export async function saveWatchlist(items: WatchlistItem[]): Promise<PersistWatchlistResult> {
  return persistWatchlist(items);
}

async function upliftLocalToBackend(local: WatchlistItem[]): Promise<boolean> {
  if (!local.length) return true;
  try {
    await syncRegistryToBackend(local);
    setPendingSync(false);
    setRegistrySynced(true);
    return true;
  } catch {
    setPendingSync(true);
    return false;
  }
}

function watchlistPositionSignature(items: WatchlistItem[]): string {
  return JSON.stringify(
    [...items]
      .map((x) => ({
        symbol: x.symbol,
        positionPct: x.positionPct ?? null,
        costPrice: x.costPrice ?? null,
        maxPrice: x.maxPrice ?? null,
        entryDate: x.entryDate ?? null,
      }))
      .sort((a, b) => a.symbol.localeCompare(b.symbol)),
  );
}

export async function hydrateWatchlist(): Promise<HydrateWatchlistResult> {
  const localBefore = loadWatchlist();
  let pendingSync = readPendingSync();

  try {
    const remote = await fetchWatchlistFromBackend();
    if (remote.length > 0) {
      const merged = mergeWatchlistRemoteWithLocal(remote, localBefore);
      saveWatchlistLocal(merged);
      // If we kept local-only held rows or filled null position fields, push back up.
      if (watchlistPositionSignature(merged) !== watchlistPositionSignature(remote)) {
        const synced = await upliftLocalToBackend(merged);
        pendingSync = !synced;
        return { source: 'registry', items: merged, pendingSync };
      }
      setPendingSync(false);
      return { source: 'registry', items: merged, pendingSync: false };
    }

    if (localBefore.length > 0 || pendingSync) {
      const synced = await upliftLocalToBackend(localBefore);
      pendingSync = !synced;
      saveWatchlistLocal(localBefore);
      return {
        source: synced ? 'local_uplift' : 'local_fallback',
        items: localBefore,
        pendingSync,
      };
    }

    saveWatchlistLocal([]);
    setPendingSync(false);
    return { source: 'empty', items: [], pendingSync: false };
  } catch {
    pendingSync = readPendingSync();
    if (localBefore.length > 0 && pendingSync) {
      const synced = await upliftLocalToBackend(localBefore);
      pendingSync = !synced;
      saveWatchlistLocal(localBefore);
      return {
        source: synced ? 'local_uplift' : 'local_fallback',
        items: localBefore,
        pendingSync,
      };
    }
    return {
      source: 'local_fallback',
      items: localBefore,
      pendingSync,
    };
  }
}

export function ensureWatchlistHydrated(): Promise<HydrateWatchlistResult> {
  if (!hydratePromise) {
    hydratePromise = hydrateWatchlist().catch((err) => {
      hydratePromise = null;
      throw err;
    });
  }
  return hydratePromise;
}

/** @internal Test helper */
export function resetWatchlistHydrationForTests(): void {
  hydratePromise = null;
}

export function normalizeWatchlistItem(item: Partial<WatchlistItem> & { symbol: string }): WatchlistItem {
  const [normalized] = normalizeWatchlistItems([item]);
  return (
    normalized ?? {
      symbol: item.symbol,
      name: item.name ?? null,
      nameStatus: item.nameStatus,
      addedAt: item.addedAt || new Date().toISOString(),
      color: item.color ?? '#ffffff',
      positionPct: item.positionPct ?? null,
      costPrice: item.costPrice ?? null,
      maxPrice: item.maxPrice ?? null,
      entryDate: item.entryDate ?? null,
      source: item.source ?? 'manual',
    }
  );
}

function hasHeldPosition(item: WatchlistItem): boolean {
  return typeof item.positionPct === 'number' && Number.isFinite(item.positionPct) && item.positionPct > 0;
}

/**
 * Prefer remote membership, but keep local position economics when remote left them null.
 * Also retain local-only held rows so a polluted/partial registry cannot wipe positions.
 */
export function mergeWatchlistRemoteWithLocal(
  remote: WatchlistItem[],
  local: WatchlistItem[],
): WatchlistItem[] {
  const localBySym = new Map(local.map((x) => [x.symbol, x]));
  const merged: WatchlistItem[] = remote.map((r) => {
    const loc = localBySym.get(r.symbol);
    if (!loc) return r;
    return {
      ...r,
      name: r.name ?? loc.name ?? null,
      nameStatus: r.nameStatus ?? loc.nameStatus,
      color: r.color && r.color !== '#ffffff' ? r.color : (loc.color ?? r.color),
      positionPct: r.positionPct ?? loc.positionPct ?? null,
      costPrice: r.costPrice ?? loc.costPrice ?? null,
      maxPrice: r.maxPrice ?? loc.maxPrice ?? null,
      entryDate: r.entryDate ?? loc.entryDate ?? null,
    };
  });
  const remoteSyms = new Set(remote.map((x) => x.symbol));
  for (const loc of local) {
    if (remoteSyms.has(loc.symbol)) continue;
    if (hasHeldPosition(loc)) merged.push(loc);
  }
  return normalizeWatchlistItems(merged);
}
