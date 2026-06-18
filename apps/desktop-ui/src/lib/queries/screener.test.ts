import { beforeEach, describe, expect, it, vi } from 'vitest';

import { apiGetJson } from '@/lib/api/client';

import {
  fetchScreenerSnapshotsMap,
  screenerListQueryKey,
  screenerListQueryOptions,
  screenerSnapshotsQueryKey,
  screenerSnapshotsQueryOptions,
} from './screener';

vi.mock('@/lib/api/client', () => ({
  apiGetJson: vi.fn(),
}));

const mockedApiGetJson = vi.mocked(apiGetJson);

describe('screenerListQueryKey', () => {
  it('returns stable list key', () => {
    expect(screenerListQueryKey()).toEqual(['screener', 'list', 'enabled']);
  });
});

describe('screenerSnapshotsQueryKey', () => {
  it('sorts screener ids for stable cache key', () => {
    expect(screenerSnapshotsQueryKey(['b', 'a', 'c'])).toEqual([
      'screener',
      'snapshots',
      'a,b,c',
    ]);
    expect(screenerSnapshotsQueryKey(['c', 'b', 'a'])).toEqual([
      'screener',
      'snapshots',
      'a,b,c',
    ]);
  });

  it('filters empty ids', () => {
    expect(screenerSnapshotsQueryKey(['a', '', '  '])).toEqual([
      'screener',
      'snapshots',
      'a',
    ]);
  });

  it('uses empty join for no ids', () => {
    expect(screenerSnapshotsQueryKey([])).toEqual(['screener', 'snapshots', '']);
  });
});

describe('screenerListQueryOptions', () => {
  it('uses screenerListQueryKey for queryKey', () => {
    const options = screenerListQueryOptions();
    expect(options.queryKey).toEqual(screenerListQueryKey());
    expect(typeof options.queryFn).toBe('function');
  });
});

describe('screenerSnapshotsQueryOptions', () => {
  it('uses screenerSnapshotsQueryKey for queryKey', () => {
    const ids = ['sc-2', 'sc-1'];
    const options = screenerSnapshotsQueryOptions(ids);
    expect(options.queryKey).toEqual(screenerSnapshotsQueryKey(ids));
    expect(typeof options.queryFn).toBe('function');
  });
});

describe('fetchScreenerSnapshotsMap', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
  });

  it('fetches latest snapshot details in parallel', async () => {
    const inFlight = new Set<string>();
    let maxInFlight = 0;

    mockedApiGetJson.mockImplementation(async (url: string) => {
      inFlight.add(url);
      maxInFlight = Math.max(maxInFlight, inFlight.size);
      await new Promise((resolve) => setTimeout(resolve, 20));
      inFlight.delete(url);

      if (url.includes('/screeners/sc-a/snapshots')) {
        return { items: [{ id: 'snap-a', screenerId: 'sc-a', capturedAt: '2026-06-18T10:00:00Z', rowCount: 1 }] };
      }
      if (url.includes('/screeners/sc-b/snapshots')) {
        return { items: [{ id: 'snap-b', screenerId: 'sc-b', capturedAt: '2026-06-18T10:00:00Z', rowCount: 2 }] };
      }
      if (url.endsWith('/snapshots/snap-a')) {
        return {
          id: 'snap-a',
          screenerId: 'sc-a',
          capturedAt: '2026-06-18T10:00:00Z',
          rowCount: 1,
          screenTitle: 'A',
          filters: [],
          url: 'https://example.com/a',
          headers: ['Ticker'],
          rows: [{ Ticker: 'AAA' }],
        };
      }
      if (url.endsWith('/snapshots/snap-b')) {
        return {
          id: 'snap-b',
          screenerId: 'sc-b',
          capturedAt: '2026-06-18T10:00:00Z',
          rowCount: 2,
          screenTitle: 'B',
          filters: [],
          url: 'https://example.com/b',
          headers: ['Ticker'],
          rows: [{ Ticker: 'BBB' }],
        };
      }
      throw new Error(`Unexpected url: ${url}`);
    });

    const result = await fetchScreenerSnapshotsMap(['sc-a', 'sc-b']);

    expect(mockedApiGetJson).toHaveBeenCalledTimes(4);
    expect(maxInFlight).toBeGreaterThan(1);
    expect(result['sc-a']?.rows[0]?.Ticker).toBe('AAA');
    expect(result['sc-b']?.rows[0]?.Ticker).toBe('BBB');
  });

  it('returns null when screener has no snapshots', async () => {
    mockedApiGetJson.mockResolvedValueOnce({ items: [] });

    const result = await fetchScreenerSnapshotsMap(['sc-empty']);

    expect(mockedApiGetJson).toHaveBeenCalledTimes(1);
    expect(result['sc-empty']).toBeNull();
  });
});
