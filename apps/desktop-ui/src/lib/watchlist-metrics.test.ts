import { describe, expect, it } from 'vitest';

import {
  computePnLPct,
  computeVwap,
  formatHotTop3,
  industryDisplayName,
  isHotTop3Industry,
  resolveWatchlistCurrentPrice,
  shouldRequireRealtimeQuote,
} from './watchlist-metrics';

describe('computePnLPct', () => {
  it('returns null without cost', () => {
    expect(computePnLPct(null, 10)).toBeNull();
    expect(computePnLPct(0, 10)).toBeNull();
  });

  it('computes percentage gain and loss', () => {
    expect(computePnLPct(100, 105)).toBeCloseTo(5);
    expect(computePnLPct(100, 95)).toBeCloseTo(-5);
  });
});

describe('computeVwap', () => {
  it('computes realtime vwap from amount and volume lots', () => {
    expect(computeVwap(101200, 100, 'realtime')).toBeCloseTo(10.12);
  });

  it('computes daily vwap with thousand-yuan amount', () => {
    expect(computeVwap(1012, 100, 'daily')).toBeCloseTo(101.2);
  });

  it('returns null for invalid inputs', () => {
    expect(computeVwap(null, 100)).toBeNull();
    expect(computeVwap(1000, 0)).toBeNull();
  });
});

describe('resolveWatchlistCurrentPrice', () => {
  const today = '2026-05-28';

  it('uses realtime quote during CN session when trend is for today', () => {
    expect(
      resolveWatchlistCurrentPrice({
        tradingTime: true,
        todaySh: today,
        symbol: 'CN:600000',
        trendAsOfDate: today,
        quotePrice: 10.5,
        quoteTradeTime: `${today} 14:30:00`,
        trendClose: 10.0,
      }),
    ).toBe(10.5);
  });

  it('prefers daily close after hours even if stale quote remains in state', () => {
    expect(
      resolveWatchlistCurrentPrice({
        tradingTime: false,
        todaySh: today,
        symbol: 'CN:600000',
        trendAsOfDate: today,
        quotePrice: 10.2,
        quoteTradeTime: `${today} 15:00:00`,
        trendClose: 10.8,
      }),
    ).toBe(10.8);
  });

  it('falls back to close when realtime quote is missing during session', () => {
    expect(
      resolveWatchlistCurrentPrice({
        tradingTime: true,
        todaySh: today,
        symbol: 'CN:600000',
        trendAsOfDate: today,
        quotePrice: null,
        quoteTradeTime: null,
        trendClose: 9.9,
      }),
    ).toBe(9.9);
  });
});

describe('shouldRequireRealtimeQuote', () => {
  it('is false outside session and when trend bar is not today', () => {
    expect(
      shouldRequireRealtimeQuote({
        tradingTime: false,
        symbol: 'CN:600000',
        trendAsOfDate: '2026-05-28',
        todaySh: '2026-05-28',
      }),
    ).toBe(false);
    expect(
      shouldRequireRealtimeQuote({
        tradingTime: true,
        symbol: 'CN:600000',
        trendAsOfDate: '2026-05-27',
        todaySh: '2026-05-28',
      }),
    ).toBe(false);
  });
});

describe('industry helpers', () => {
  it('prefers emIndustry over tushare industry', () => {
    expect(industryDisplayName({ emIndustry: '集成电路封测', industry: '元器件' })).toBe('集成电路封测');
  });

  it('detects HotTop3 from industryFlowReasons', () => {
    const t = { values: { industryFlowReasons: ['hotspots_today_top3'] } };
    expect(isHotTop3Industry(t)).toBe(true);
    expect(formatHotTop3(t)).toBe('✓');
  });
});
