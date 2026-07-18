import { describe, expect, it } from 'vitest';

import type { ExecutionGate } from '@karios/shared';

import { buildExecutionSnapshotPayload } from './execution-journal';
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
