import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  WATCHLIST_PENDING_SYNC_KEY,
  WATCHLIST_STORAGE_KEY,
  ensureWatchlistHydrated,
  hydrateWatchlist,
  loadWatchlist,
  normalizeWatchlistItems,
  persistWatchlist,
  resetWatchlistHydrationForTests,
  type WatchlistItem,
} from './watchlist-storage';

const BASE = 'http://127.0.0.1:4330';

function mockLocalStorage() {
  const store = new Map<string, string>();
  const ls = {
    getItem: (key: string) => store.get(key) ?? null,
    setItem: (key: string, value: string) => {
      store.set(key, value);
    },
    removeItem: (key: string) => {
      store.delete(key);
    },
    clear: () => store.clear(),
  };
  vi.stubGlobal('localStorage', ls);
  vi.stubGlobal('window', {
    localStorage: ls,
    dispatchEvent: vi.fn(),
  });
  return store;
}

function registryResponse(items: WatchlistItem[]) {
  return {
    ok: true,
    status: 200,
    text: async () => JSON.stringify({ ok: true, items, count: items.length }),
  };
}

beforeEach(() => {
  vi.stubGlobal('fetch', vi.fn());
  mockLocalStorage();
  resetWatchlistHydrationForTests();
});

afterEach(() => {
  vi.unstubAllGlobals();
  resetWatchlistHydrationForTests();
});

describe('normalizeWatchlistItems', () => {
  it('clamps positionPct to 0-100', () => {
    const out = normalizeWatchlistItems([
      { symbol: 'CN:600000', addedAt: '2026-01-01', positionPct: 150 },
      { symbol: 'CN:000001', addedAt: '2026-01-01', positionPct: -5 },
    ]);
    expect(out[0]?.positionPct).toBe(100);
    expect(out[1]?.positionPct).toBe(0);
  });
});

describe('hydrateWatchlist', () => {
  it('prefers registry over localStorage', async () => {
    localStorage.setItem(
      WATCHLIST_STORAGE_KEY,
      JSON.stringify([{ symbol: 'CN:LOCAL', addedAt: '2026-01-01' }]),
    );
    vi.mocked(fetch).mockResolvedValueOnce(
      registryResponse([{ symbol: 'CN:REMOTE', addedAt: '2026-06-18' }]) as Response,
    );

    const result = await hydrateWatchlist();
    expect(result.source).toBe('registry');
    expect(loadWatchlist()[0]?.symbol).toBe('CN:REMOTE');
  });

  it('uplifts local when registry empty', async () => {
    localStorage.setItem(
      WATCHLIST_STORAGE_KEY,
      JSON.stringify([{ symbol: 'CN:LOCAL', addedAt: '2026-01-01' }]),
    );
    vi.mocked(fetch)
      .mockResolvedValueOnce(registryResponse([]) as Response)
      .mockResolvedValueOnce({ ok: true, status: 200, text: async () => '{}' } as Response);

    const result = await hydrateWatchlist();
    expect(result.source).toBe('local_uplift');
    expect(fetch).toHaveBeenCalledTimes(2);
    expect(fetch).toHaveBeenLastCalledWith(
      `${BASE}/watchlist/registry`,
      expect.objectContaining({ method: 'POST' }),
    );
  });

  it('falls back to local on GET failure', async () => {
    localStorage.setItem(
      WATCHLIST_STORAGE_KEY,
      JSON.stringify([{ symbol: 'CN:LOCAL', addedAt: '2026-01-01' }]),
    );
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: false,
      status: 500,
      statusText: 'Error',
      text: async () => 'fail',
    } as Response);

    const result = await hydrateWatchlist();
    expect(result.source).toBe('local_fallback');
    expect(loadWatchlist()[0]?.symbol).toBe('CN:LOCAL');
  });
});

describe('persistWatchlist', () => {
  it('posts then saves local on success', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true,
      status: 200,
      text: async () => '{"ok":true,"count":1}',
    } as Response);

    await persistWatchlist([{ symbol: 'CN:600519', addedAt: '2026-06-18' }]);
    expect(loadWatchlist()[0]?.symbol).toBe('CN:600519');
    expect(localStorage.getItem(WATCHLIST_PENDING_SYNC_KEY)).toBeNull();
  });

  it('saves local with pendingSync on POST failure', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: false,
      status: 503,
      statusText: 'Unavailable',
      text: async () => '',
    } as Response);

    const result = await persistWatchlist([{ symbol: 'CN:600519', addedAt: '2026-06-18' }]);
    expect(result.synced).toBe(false);
    expect(loadWatchlist()[0]?.symbol).toBe('CN:600519');
    expect(localStorage.getItem(WATCHLIST_PENDING_SYNC_KEY)).toBe('true');
  });
});

describe('ensureWatchlistHydrated', () => {
  it('dedupes concurrent hydrate calls', async () => {
    vi.mocked(fetch).mockImplementation(
      () =>
        new Promise((resolve) => {
          setTimeout(
            () => resolve(registryResponse([]) as Response),
            10,
          );
        }),
    );

    const [a, b] = await Promise.all([ensureWatchlistHydrated(), ensureWatchlistHydrated()]);
    expect(a.source).toBe(b.source);
    expect(fetch).toHaveBeenCalledTimes(1);
  });
});
