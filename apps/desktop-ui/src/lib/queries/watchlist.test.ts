import { describe, expect, it } from 'vitest';

import { watchlistMarketKey, watchlistMarketQueryOptions } from './watchlist';

describe('watchlistMarketKey', () => {
  it('sorts and normalizes symbols', () => {
    expect(watchlistMarketKey(['CN:000001', 'CN:600519'])).toEqual([
      'watchlist',
      'market',
      'CN:000001,CN:600519',
    ]);
    expect(watchlistMarketKey(['CN:600519', 'CN:000001'])).toEqual([
      'watchlist',
      'market',
      'CN:000001,CN:600519',
    ]);
  });

  it('filters empty symbols', () => {
    expect(watchlistMarketKey(['CN:600519', '', '  '])).toEqual([
      'watchlist',
      'market',
      'CN:600519',
    ]);
  });
});

describe('watchlistMarketQueryOptions', () => {
  it('uses watchlistMarketKey for queryKey', () => {
    const symbols = ['CN:600519', 'CN:000001'];
    const options = watchlistMarketQueryOptions(symbols);
    expect(options.queryKey).toEqual(watchlistMarketKey(symbols));
    expect(typeof options.queryFn).toBe('function');
  });
});
