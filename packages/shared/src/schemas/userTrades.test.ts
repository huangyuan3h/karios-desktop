import { describe, expect, it } from 'vitest';
import {
  UserTradePatchSchema,
  UserTradeRequestSchema,
  UserTradeSchema,
  UserTradesStatsSchema,
  UserTradeSideSchema,
} from './userTrades';

describe('UserTradeSideSchema', () => {
  it('accepts BUY / ADD / SELL', () => {
    expect(UserTradeSideSchema.parse('BUY')).toBe('BUY');
    expect(UserTradeSideSchema.parse('ADD')).toBe('ADD');
    expect(UserTradeSideSchema.parse('SELL')).toBe('SELL');
  });

  it('rejects unknown side', () => {
    expect(() => UserTradeSideSchema.parse('HOLD')).toThrow();
  });
});

describe('UserTradeSchema', () => {
  it('validates a SELL leg from the API', () => {
    const trade = UserTradeSchema.parse({
      id: 'abc',
      symbol: 'CN:600000',
      side: 'SELL',
      tradeDate: '2026-08-08',
      price: 11,
      positionPct: 5,
      costBasis: 10,
      entryDate: '2026-08-01',
      pnlPct: 10,
      holdingDays: 7,
      source: 'ALPHA',
      market: 'CN',
    });
    expect(trade.pnlPct).toBe(10);
    expect(trade.holdingDays).toBe(7);
  });

  it('allows BUY legs without cost basis', () => {
    const trade = UserTradeSchema.parse({
      id: 'abc',
      symbol: 'CN:600000',
      side: 'BUY',
      tradeDate: '2026-08-08',
      price: 10,
      positionPct: 5,
    });
    expect(trade.costBasis).toBeUndefined();
    expect(trade.pnlPct).toBeUndefined();
  });

  it('accepts an alpha snapshot (§19.3)', () => {
    const trade = UserTradeSchema.parse({
      id: 'abc',
      symbol: 'CN:600000',
      side: 'BUY',
      tradeDate: '2026-08-13',
      price: 10,
      positionPct: 5,
      alphaSnapshot: {
        asOf: '2026-08-13',
        windowDays: 14,
        nEvents: 2,
        hasSA: true,
        maxConfidence: 0.95,
        riskStatuses: ['active'],
        events: [
          {
            trend: 'x',
            grade: 'S',
            confidence: 0.95,
            daysAgo: 1,
            riskStatus: 'active',
            focus: 'y',
          },
        ],
      },
    });
    expect(trade.alphaSnapshot?.hasSA).toBe(true);
    expect(trade.alphaSnapshot?.nEvents).toBe(2);
  });

  it('accepts a leg without alpha snapshot', () => {
    const trade = UserTradeSchema.parse({
      id: 'abc',
      symbol: 'CN:600000',
      side: 'SELL',
      tradeDate: '2026-08-13',
      price: 11,
      positionPct: 5,
      alphaSnapshot: null,
    });
    expect(trade.alphaSnapshot).toBeNull();
  });

  it('defaults leg to s3 and accepts parking (OPT-149)', () => {
    const core = UserTradeSchema.parse({
      id: 'a',
      symbol: 'CN:600000',
      side: 'BUY',
      tradeDate: '2026-09-09',
      price: 10,
      positionPct: 10,
    });
    expect(core.leg).toBe('s3');
    expect(core.strategyMode).toBe('legacy_unknown');
    const parking = UserTradeSchema.parse({
      id: 'b',
      symbol: 'ETF:518880',
      side: 'BUY',
      tradeDate: '2026-09-07',
      price: 7.2,
      positionPct: 10,
      leg: 'parking',
    });
    expect(parking.leg).toBe('parking');
    const satellite = UserTradeRequestSchema.parse({
      symbol: 'CN:002128',
      side: 'BUY',
      price: 27.7,
      positionPct: 25,
      leg: 'satellite',
    });
    expect(satellite.leg).toBe('satellite');
    expect(UserTradeRequestSchema.parse({
      symbol: 'CN:002128',
      side: 'BUY',
      price: 27.7,
      positionPct: 25,
      leg: 'b3',
      strategyMode: 'starship_robust',
    }).strategyMode).toBe('starship_robust');
    expect(() =>
      UserTradeRequestSchema.parse({
        symbol: 'ETF:518880',
        side: 'BUY',
        price: 7.2,
        positionPct: 10,
        leg: 'sat',
      }),
    ).toThrow();
  });

  it('validates patch bodies (OPT-150)', () => {
    expect(UserTradePatchSchema.parse({ leg: 'parking' }).leg).toBe('parking');
    expect(UserTradePatchSchema.parse({ strategyMode: 'harbor' }).strategyMode).toBe('harbor');
    expect(UserTradePatchSchema.parse({ positionPct: 12.5 }).positionPct).toBe(12.5);
    expect(() => UserTradePatchSchema.parse({ leg: 'x' })).toThrow();
    expect(() => UserTradePatchSchema.parse({ positionPct: -1 })).toThrow();
  });
});

describe('UserTradesStatsSchema', () => {
  it('validates stats payload with bySource', () => {
    const stats = UserTradesStatsSchema.parse({
      count: 2,
      wins: 1,
      losses: 1,
      winRate: 0.5,
      avgWinPct: 6,
      avgLossPct: 5,
      expectancyPct: 0.5,
      netExpectancyPct: 0.2,
      profitFactor: 1.2,
      avgHoldingDays: 2.8,
      total: 2,
      roundTripCostPct: 0.3,
      bySource: {
        ALPHA: {
          count: 1,
          wins: 1,
          losses: 0,
          winRate: 1,
          avgWinPct: 10,
          avgLossPct: null,
          expectancyPct: 10,
          netExpectancyPct: 9.7,
          profitFactor: null,
          avgHoldingDays: 5,
        },
      },
      bySymbol: {},
    });
    expect(stats.roundTripCostPct).toBe(0.3);
    expect(stats.bySource.ALPHA.winRate).toBe(1);
  });

  it('accepts empty stats', () => {
    const stats = UserTradesStatsSchema.parse({
      count: 0,
      wins: 0,
      losses: 0,
      winRate: null,
      avgWinPct: null,
      avgLossPct: null,
      expectancyPct: null,
      netExpectancyPct: null,
      profitFactor: null,
      avgHoldingDays: null,
      total: 0,
      roundTripCostPct: 0.3,
      bySource: {},
      bySymbol: {},
    });
    expect(stats.count).toBe(0);
  });
});
