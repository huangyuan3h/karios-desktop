import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { resetTrendOkInflightForTests } from '@/lib/api/trendok';

vi.mock('@/lib/api/client', () => ({
  apiGetJson: vi.fn(),
}));

import { apiGetJson } from '@/lib/api/client';
import {
  fetchWatchlistMarketSnapshot,
  forceRefreshWatchlistBars,
} from '@/lib/watchlist-market';

beforeEach(() => {
  resetTrendOkInflightForTests();
  vi.mocked(apiGetJson).mockReset();
});

afterEach(() => {
  resetTrendOkInflightForTests();
});

describe('forceRefreshWatchlistBars', () => {
  it('requests bars for each CN symbol and counts failures', async () => {
    vi.mocked(apiGetJson)
      .mockResolvedValueOnce({ bars: [] })
      .mockRejectedValueOnce(new Error('500'))
      .mockResolvedValueOnce({ bars: [] })
      .mockResolvedValueOnce({ bars: [] })
      .mockResolvedValueOnce({ bars: [] });

    const result = await forceRefreshWatchlistBars(
      ['CN:600519', 'CN:000001', 'CN:600000', 'HK:00700', 'CN:300750'],
      { concurrency: 4 },
    );

    expect(result.total).toBe(4);
    expect(result.failures).toBe(1);
    expect(apiGetJson).toHaveBeenCalledTimes(4);
    expect(String(vi.mocked(apiGetJson).mock.calls[0]?.[0])).toContain('force=true');
  });

  it('retries once on 429', async () => {
    vi.mocked(apiGetJson)
      .mockRejectedValueOnce(new Error('429 Too Many Requests'))
      .mockResolvedValueOnce({ bars: [] });

    const result = await forceRefreshWatchlistBars(['CN:600519'], { concurrency: 1 });

    expect(result.failures).toBe(0);
    expect(apiGetJson).toHaveBeenCalledTimes(2);
  });
});

describe('fetchWatchlistMarketSnapshot', () => {
  it('forceMarket runs bars before trendok and quotes', async () => {
    const order: string[] = [];
    vi.mocked(apiGetJson).mockImplementation(async (path: string) => {
      if (path.includes('/bars?')) {
        order.push('bars');
        return { bars: [] };
      }
      if (path.startsWith('/market/stocks/trendok')) {
        order.push('trendok');
        return [{ symbol: 'CN:600519', score: 80 }];
      }
      if (path.startsWith('/quote?')) {
        order.push('quote');
        return {
          items: [
            {
              ts_code: '600519.SH',
              price: 100,
              trade_time: '2026-06-18 15:00:00',
            },
          ],
        };
      }
      return {};
    });

    const snap = await fetchWatchlistMarketSnapshot(['CN:600519'], {
      forceMarket: true,
      realtime: true,
    });

    expect(order[0]).toBe('bars');
    expect(order).toContain('trendok');
    expect(order).toContain('quote');
    expect(snap.trend['CN:600519']?.score).toBe(80);
    expect(snap.barSync).toEqual({ failures: 0, total: 1 });
  });

  it('chunks large symbol lists for trendok', async () => {
    const symbols = Array.from({ length: 250 }, (_, i) => `CN:${String(i).padStart(6, '0')}`);
    vi.mocked(apiGetJson).mockImplementation(async (path: string) => {
      if (path.startsWith('/market/stocks/trendok')) return [];
      if (path.startsWith('/quote?')) return { items: [] };
      return {};
    });

    await fetchWatchlistMarketSnapshot(symbols, { forceMarket: false, realtime: false });

    const trendokCalls = vi
      .mocked(apiGetJson)
      .mock.calls.filter(([path]) => String(path).startsWith('/market/stocks/trendok'));
    expect(trendokCalls.length).toBe(2);
  });
});
