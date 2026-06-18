import { beforeEach, describe, expect, it, vi } from 'vitest';

import { apiGetJson } from '@/lib/api/client';

import {
  fetchStockDetail,
  normalizeSymbol,
  stockDetailQueryKey,
  STOCK_DETAIL_STALE_MS,
} from './stock';

vi.mock('@/lib/api/client', () => ({
  apiGetJson: vi.fn(),
}));

const mockedApiGetJson = vi.mocked(apiGetJson);

describe('normalizeSymbol', () => {
  it('maps board prefix to CN symbol', () => {
    expect(normalizeSymbol('主板:000001')).toBe('CN:000001');
  });
});

describe('stockDetailQueryKey', () => {
  it('uses normalized symbol in key', () => {
    expect(stockDetailQueryKey('主板:000001')).toEqual(['stock', 'CN:000001', 'detail']);
  });
});

describe('fetchStockDetail', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
    vi.stubGlobal('localStorage', {
      getItem: vi.fn(() => String(Date.now() - STOCK_DETAIL_STALE_MS - 1)),
      setItem: vi.fn(),
    });
  });

  it('fetches fund-flow and quote in parallel during trading hours', async () => {
    const inflight = new Set<string>();
    let maxInFlight = 0;

    mockedApiGetJson.mockImplementation(async (url: string) => {
      inflight.add(url);
      maxInFlight = Math.max(maxInFlight, inflight.size);
      await new Promise((resolve) => setTimeout(resolve, 20));
      inflight.delete(url);
      if (url.includes('/bars')) {
        return { symbol: 'CN:000001', market: 'CN', ticker: '000001', name: 'Test', currency: 'CNY', bars: [] };
      }
      if (url.includes('/chips')) {
        return { symbol: 'CN:000001', market: 'CN', ticker: '000001', name: 'Test', currency: 'CNY', items: [] };
      }
      if (url.includes('/fund-flow')) {
        return { symbol: 'CN:000001', market: 'CN', ticker: '000001', name: 'Test', currency: 'CNY', items: [] };
      }
      if (url.includes('/quote')) {
        return { ok: true, items: [{ ts_code: '000001.SZ', price: '10' }] };
      }
      throw new Error(`Unexpected url: ${url}`);
    });

    await fetchStockDetail('CN:000001', { force: true, quote: true });

    expect(maxInFlight).toBeGreaterThan(1);
    expect(mockedApiGetJson).toHaveBeenCalled();
  });
});
