import fs from 'node:fs';
import path from 'node:path';

import { z } from 'zod';
import { describe, expect, it } from 'vitest';

import {
  ExecutionChangeListResponseSchema,
  ExecutionSnapshotListResponseSchema,
} from '@karios/shared';
import { TrendOkResultSchema } from '@karios/shared';

const fixture = (name: string): unknown => {
  const p = path.join(__dirname, '__fixtures__', name);
  return JSON.parse(fs.readFileSync(p, 'utf-8')) as unknown;
};

const QuoteSchema = z.object({
  price: z.number().nullable(),
  preClose: z.number().nullable(),
  pctChg: z.number().nullable(),
  tradeTime: z.string().nullable(),
  amount: z.number().nullable(),
  volume: z.number().nullable(),
});

const WatchlistMarketContractSchema = z.object({
  trend: z.record(z.string(), TrendOkResultSchema.nullable().optional()),
  quotes: z.record(z.string(), QuoteSchema),
});

const MarketSentimentContractSchema = z.object({
  asOfDate: z.string(),
  days: z.number(),
  items: z.array(
    z.object({
      date: z.string(),
      upCount: z.number(),
      downCount: z.number(),
      flatCount: z.number(),
      upDownRatio: z.number(),
      marketTurnoverCny: z.number(),
      riskMode: z.string(),
      yesterdayLimitUpPremium: z.number().nullable(),
      failedLimitUpRate: z.number().nullable(),
    }),
  ),
  srvIndex: z
    .object({
      asOfDate: z.string(),
      score: z.number().nullable(),
      overlapCount: z.number().nullable(),
      level: z.string().nullable(),
      labelZh: z.string().nullable().optional(),
    })
    .nullable(),
  executionGate: z
    .object({
      mode: z.string(),
      allowNewEntries: z.boolean(),
      marketRegime: z.string(),
      indexLight: z.string(),
      reasons: z.array(z.string()),
    })
    .nullable(),
});

const DashboardContractSchema = z.object({
  asOfDate: z.string(),
  industryFundFlow: z
    .object({
      asOfDate: z.string().nullable(),
      days: z.number(),
      topK: z.number(),
      dates: z.array(z.string()),
      topByDate: z
        .array(
          z.object({
            date: z.string(),
            top: z.array(z.string()),
          }),
        )
        .optional(),
      flow5d: z
        .object({
          dates: z.array(z.string()),
          top: z.array(z.record(z.string(), z.unknown())),
        })
        .optional(),
    })
    .nullable(),
  marketSentiment: MarketSentimentContractSchema.nullable(),
  macroSnapshot: z.record(z.string(), z.unknown()).nullable().optional(),
  news: z.record(z.string(), z.unknown()).nullable().optional(),
});

describe('contract: dashboard chain (/dashboard/summary vs DashboardContractSchema)', () => {
  it('real fixture passes the contract shape', () => {
    const payload = fixture('dashboard_summary.json');
    const parsed = DashboardContractSchema.parse(payload);
    expect(parsed.asOfDate).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect(parsed.marketSentiment?.items.length).toBeGreaterThan(0);
    expect(parsed.industryFundFlow?.days).toBe(5);
  });

  it('mutation detection: renamed marketSentiment field fails the contract', () => {
    const payload = fixture('dashboard_summary.json') as Record<string, unknown>;
    const mutated = {
      ...payload,
      marketSentiment: { ...(payload.marketSentiment as Record<string, unknown>), as_of_date: 'x' },
    };
    delete (mutated.marketSentiment as Record<string, unknown>).asOfDate;
    expect(() => DashboardContractSchema.parse(mutated)).toThrow();
  });
});

describe('contract: watchlist chain (/market/stocks/trendok + /quote)', () => {
  const golden = {
    trend: {
      'CN:600519': {
        symbol: 'CN:600519',
        asOfDate: '2026-08-07',
        trendOk: true,
        score: 92,
        scoreParts: {},
        buyMode: 'A_pullback',
        buyAction: 'buy',
        buyZoneLow: 10,
        buyZoneHigh: 12,
        stopLossPrice: 1450,
        marketRegime: 'Strong',
        rs: 8.5,
        intradayChgPct: 2.1,
        gapUp: false,
        riskAlerts: [],
        instFlow: null,
        missingData: [],
        values: { close: 1500 },
      },
      'CN:000858': { symbol: 'CN:000858' },
    },
    quotes: {
      'CN:600519': {
        price: 1502.5,
        preClose: 1490,
        pctChg: 0.84,
        tradeTime: '2026-08-07 14:30:00',
        amount: 1_200_000_000,
        volume: 80_000_000,
      },
    },
  };

  it('realistic trend/quote payload passes the contract', () => {
    const parsed = WatchlistMarketContractSchema.parse(golden);
    expect(parsed.trend['CN:600519']?.score).toBe(92);
    expect(parsed.quotes['CN:600519'].price).toBe(1502.5);
  });

  it('drift detection: quote missing price field fails', () => {
    const mutated = {
      ...golden,
      quotes: { 'CN:600519': { ...golden.quotes['CN:600519'], price: undefined } },
    };
    expect(() => WatchlistMarketContractSchema.parse(mutated)).toThrow();
  });
});

describe('contract: execution chain (/execution/snapshots + /changes vs shared Zod)', () => {
  it('real snapshots fixture passes ExecutionSnapshotListResponseSchema', () => {
    const payload = fixture('execution_snapshots.json');
    const parsed = ExecutionSnapshotListResponseSchema.parse(payload);
    expect(parsed.items.length).toBeGreaterThan(0);
    expect(parsed.items[0].id).toBeTruthy();
    expect(parsed.items[0].cards[0].symbol).toBeTruthy();
  });

  it('real changes fixture passes ExecutionChangeListResponseSchema', () => {
    const payload = fixture('execution_changes.json');
    const parsed = ExecutionChangeListResponseSchema.parse(payload);
    expect(parsed.items.length).toBeGreaterThan(0);
    expect(['action', 'why']).toContain(parsed.items[0].field);
  });

  it('drift detection: unknown action enum fails the shared schema', () => {
    const payload = fixture('execution_snapshots.json') as {
      items: Array<{ cards: Array<{ action: string }> }>;
    };
    const mutated = JSON.parse(JSON.stringify(payload)) as typeof payload;
    mutated.items[0].cards[0].action = 'PANIC_SELL_EVERYTHING';
    expect(() => ExecutionSnapshotListResponseSchema.parse(mutated)).toThrow();
  });

  it('drift detection: change missing required field fails the shared schema', () => {
    const payload = fixture('execution_changes.json') as {
      items: Array<Record<string, unknown>>;
    };
    const mutated = JSON.parse(JSON.stringify(payload)) as typeof payload;
    delete mutated.items[0].field;
    expect(() => ExecutionChangeListResponseSchema.parse(mutated)).toThrow();
  });
});
