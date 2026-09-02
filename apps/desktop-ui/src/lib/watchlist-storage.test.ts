import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  WATCHLIST_PENDING_SYNC_KEY,
  WATCHLIST_STORAGE_KEY,
  applyZeroPositionCleanup,
  ensureWatchlistHydrated,
  hydrateWatchlist,
  loadWatchlist,
  mergeWatchlistRemoteWithLocal,
  normalizeWatchlistItems,
  persistWatchlist,
  resetWatchlistHydrationForTests,
  upsertWatchlistOpenTrade,
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

describe('mergeWatchlistRemoteWithLocal', () => {
  it('fills null remote position fields from local', () => {
    const merged = mergeWatchlistRemoteWithLocal(
      [{ symbol: 'CN:000001', addedAt: '2026-06-18', positionPct: null, costPrice: null }],
      [
        {
          symbol: 'CN:000001',
          addedAt: '2026-01-01',
          positionPct: 10,
          costPrice: 12.5,
          entryDate: '2026-06-01',
        },
      ],
    );
    expect(merged[0]?.positionPct).toBe(10);
    expect(merged[0]?.costPrice).toBe(12.5);
    expect(merged[0]?.entryDate).toBe('2026-06-01');
  });

  it('keeps local-only held positions dropped by remote', () => {
    const merged = mergeWatchlistRemoteWithLocal(
      [{ symbol: 'CN:600519', addedAt: '2026-06-18', name: '贵州茅台' }],
      [
        {
          symbol: 'CN:000001',
          addedAt: '2026-01-01',
          positionPct: 8,
          costPrice: 10,
        },
      ],
    );
    expect(merged.map((x) => x.symbol).sort()).toEqual(['CN:000001', 'CN:600519']);
  });
});

describe('hydrateWatchlist', () => {
  it('prefers registry over localStorage but merges local position fields', async () => {
    localStorage.setItem(
      WATCHLIST_STORAGE_KEY,
      JSON.stringify([
        {
          symbol: 'CN:REMOTE',
          addedAt: '2026-01-01',
          positionPct: 15,
          costPrice: 9.5,
        },
      ]),
    );
    vi.mocked(fetch)
      .mockResolvedValueOnce(
        registryResponse([{ symbol: 'CN:REMOTE', addedAt: '2026-06-18' }]) as Response,
      )
      .mockResolvedValueOnce({ ok: true, status: 200, text: async () => '{}' } as Response);

    const result = await hydrateWatchlist();
    expect(result.source).toBe('registry');
    expect(loadWatchlist()[0]?.symbol).toBe('CN:REMOTE');
    expect(loadWatchlist()[0]?.positionPct).toBe(15);
    expect(loadWatchlist()[0]?.costPrice).toBe(9.5);
    expect(fetch).toHaveBeenCalledTimes(2);
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

describe('applyZeroPositionCleanup', () => {
  it('clears cost/max/entry when positionPct is 0', () => {
    const out = applyZeroPositionCleanup({
      symbol: 'CN:000977',
      addedAt: '2026-07-01T00:00:00Z',
      positionPct: 0,
      costPrice: 12.5,
      maxPrice: 13,
      entryDate: '2026-07-01',
      source: 'manual',
    });
    expect(out.costPrice).toBeNull();
    expect(out.maxPrice).toBeNull();
    expect(out.entryDate).toBeNull();
    expect(out.positionPct).toBe(0);
  });

  it('leaves economics intact when positionPct > 0', () => {
    const out = applyZeroPositionCleanup({
      symbol: 'CN:000977',
      addedAt: '2026-07-01T00:00:00Z',
      positionPct: 5,
      costPrice: 12.5,
      maxPrice: 13,
      entryDate: '2026-07-01',
      source: 'manual',
    });
    expect(out.costPrice).toBe(12.5);
    expect(out.entryDate).toBe('2026-07-01');
  });
});

describe('upsertWatchlistOpenTrade', () => {
  it('inserts a new buy at the front with cost, size and entryDate', () => {
    const out = upsertWatchlistOpenTrade([], {
      symbol: 'CN:300413',
      name: '中石能',
      side: 'BUY',
      price: 21.3,
      positionPct: 12.5,
      entryDate: '2026-09-02',
    });
    expect(out).toHaveLength(1);
    expect(out[0]?.symbol).toBe('CN:300413');
    expect(out[0]?.name).toBe('中石能');
    expect(out[0]?.positionPct).toBe(12.5);
    expect(out[0]?.costPrice).toBe(21.3);
    expect(out[0]?.entryDate).toBe('2026-09-02');
    expect(out[0]?.source).toBe('research');
  });

  it('clears cost on a full sell', () => {
    const out = upsertWatchlistOpenTrade(
      [
        {
          symbol: 'CN:300413',
          addedAt: '2026-09-01T00:00:00Z',
          positionPct: 12.5,
          costPrice: 21.3,
          maxPrice: 21.3,
          entryDate: '2026-09-01',
          source: 'research',
        },
      ],
      {
        symbol: 'CN:300413',
        side: 'SELL',
        price: 22,
        positionPct: 12.5,
        entryDate: '2026-09-02',
      },
    );
    expect(out[0]?.positionPct).toBe(0);
    expect(out[0]?.costPrice).toBeNull();
    expect(out[0]?.entryDate).toBeNull();
  });
});
