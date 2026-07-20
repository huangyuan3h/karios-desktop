import { beforeEach, describe, expect, it, vi } from 'vitest';

import { apiGetJson } from '@/lib/api/client';

import {
  fetchScreenerSnapshotsMap,
  screenerListQueryKey,
  screenerSnapshotsQueryKey,
} from './screener';

vi.mock('@/lib/api/client', () => ({
  apiGetJson: vi.fn(),
}));

const mockedApiGetJson = vi.mocked(apiGetJson);

describe('screenerSnapshotsQueryKey', () => {
  it('sorts screener ids for stable cache key', () => {
    expect(screenerSnapshotsQueryKey(['b', 'a'])).toEqual(['screener', 'snapshots', 'a,b']);
  });
});

describe('fetchScreenerSnapshotsMap', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
  });

  it('uses batch latest snapshots endpoint once', async () => {
    mockedApiGetJson.mockResolvedValueOnce({
      items: {
        'sc-a': {
          id: 'snap-a',
          screenerId: 'sc-a',
          capturedAt: '2026-06-18T10:00:00Z',
          rowCount: 1,
          screenTitle: 'A',
          filters: [],
          url: 'https://example.com/a',
          headers: ['Ticker'],
          rows: [{ Ticker: 'AAA' }],
        },
        'sc-b': null,
      },
    });

    const result = await fetchScreenerSnapshotsMap(['sc-a', 'sc-b']);

    expect(mockedApiGetJson).toHaveBeenCalledTimes(1);
    expect(String(mockedApiGetJson.mock.calls[0][0])).toContain(
      '/integrations/tradingview/screeners/snapshots/latest',
    );
    expect(result['sc-a']?.rows[0]?.Ticker).toBe('AAA');
    expect(result['sc-b']).toBeNull();
  });

  it('returns empty map for no ids', async () => {
    const result = await fetchScreenerSnapshotsMap([]);
    expect(result).toEqual({});
    expect(mockedApiGetJson).not.toHaveBeenCalled();
  });
});

describe('screenerListQueryKey', () => {
  it('returns stable list key', () => {
    expect(screenerListQueryKey()).toEqual(['screener', 'list', 'enabled']);
  });
});
