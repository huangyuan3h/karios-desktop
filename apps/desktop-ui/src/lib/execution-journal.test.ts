import { describe, expect, it } from 'vitest';

import type { ExecutionGate } from '@karios/shared';

import {
  buildExecutionSnapshotPayload,
  filterLatestActionCards,
  symbolsWithLatestActionDeltas,
} from './execution-journal';
import { BUY_SCORE_MIN } from './execution-action';
import type { MainlineAllowSet } from './hot-industry-picks';

const gate: ExecutionGate = {
  mode: 'ATTACK',
  allowNewEntries: true,
  marketRegime: 'Strong',
  indexLight: 'green',
  reasons: [],
};

function allowSet(entries: Array<[string, '5D_TOP3' | 'MOMENTUM']>): MainlineAllowSet {
  const names = new Set(entries.map(([n]) => n));
  const byName = new Map(entries);
  return { ready: true, names, byName };
}

describe('buildExecutionSnapshotPayload', () => {
  it('builds cards from watchlist + trend', () => {
    const payload = buildExecutionSnapshotPayload({
      items: [{ symbol: 'CN:600000', addedAt: '2026-01-01', positionPct: 5, costPrice: 10 }],
      trend: {
        'CN:600000': {
          score: BUY_SCORE_MIN,
          buyAction: 'buy',
          stopLossPrice: 9,
          values: { emIndustry: '半导体' },
          gapUp: false,
          marketRegime: 'Strong',
        } as any,
      },
      quotes: { 'CN:600000': { price: 10.5 } },
      gate,
      mainlineAllow: allowSet([['半导体', '5D_TOP3']]),
      tradingTime: false,
      todaySh: '2026-07-18',
      source: 'manual',
    });
    expect(payload).not.toBeNull();
    expect(payload!.tradeDate).toBe('2026-07-18');
    expect(payload!.source).toBe('manual');
    expect(payload!.cards).toHaveLength(1);
    expect(payload!.cards[0]?.symbol).toBe('CN:600000');
    expect(payload!.cards[0]?.action).toBe('ADD');
    expect(payload!.cards[0]?.industry).toBe('半导体');
  });

  it('returns null without gate', () => {
    expect(
      buildExecutionSnapshotPayload({
        items: [],
        trend: {},
        quotes: {},
        gate: null,
        mainlineAllow: null,
        source: 'poll',
      }),
    ).toBeNull();
  });
});

describe('filterLatestActionCards (delta logging)', () => {
  it('keeps only Action / Trigger / Stop deltas and drops silent WATCH', () => {
    const cards = [
      { symbol: 'CN:A', action: 'WATCH', why: 'WATCH' },
      { symbol: 'CN:B', action: 'BUY', why: 'MAINLINE_OK' },
      { symbol: 'CN:C', action: 'HOLD', why: 'HOLD' },
    ];
    const changes = [
      { scope: 'symbol', symbol: 'CN:B', field: 'action' },
      { scope: 'symbol', symbol: 'CN:C', field: 'hardStop' },
      { scope: 'symbol', symbol: 'CN:A', field: 'why' },
      { scope: 'gate', symbol: null, field: 'mode' },
    ];
    expect([...symbolsWithLatestActionDeltas(changes)].sort()).toEqual(['CN:B', 'CN:C']);
    expect(filterLatestActionCards(cards, changes).map((c) => c.symbol)).toEqual([
      'CN:B',
      'CN:C',
    ]);
  });

  it('returns empty when only silent why/positionPct churn', () => {
    const cards = [{ symbol: 'CN:A', action: 'WATCH' }];
    const changes = [
      { scope: 'symbol', symbol: 'CN:A', field: 'why' },
      { scope: 'symbol', symbol: 'CN:A', field: 'positionPct' },
    ];
    expect(filterLatestActionCards(cards, changes)).toEqual([]);
  });
});
