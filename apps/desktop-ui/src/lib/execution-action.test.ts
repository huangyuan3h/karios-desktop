import { describe, expect, it } from 'vitest';

import type { ExecutionGate } from '@karios/shared';

import {
  BUY_SCORE_MIN,
  deriveActionCard,
  deriveTriggerAndTrail,
  isHeldPosition,
} from './execution-action';

const attackGate: ExecutionGate = {
  mode: 'ATTACK',
  allowNewEntries: true,
  marketRegime: 'Strong',
  indexLight: 'green',
  srvLevel: 'Stable',
  srvOverlapCount: 3,
  downCount: 1000,
  reasons: ['REGIME_STRONG'],
  positionRangeHint: '50%-60%',
  satelliteNote: 'ok',
};

const holdGate: ExecutionGate = {
  ...attackGate,
  mode: 'HOLD_ONLY',
  allowNewEntries: false,
  marketRegime: 'Diverging',
  reasons: ['REGIME_DIVERGING'],
};

describe('deriveTriggerAndTrail', () => {
  it('arms chandelier when pnl >= 10% and has atr/peak', () => {
    const out = deriveTriggerAndTrail({
      hardStop: 10,
      costPrice: 10,
      maxPrice: 12,
      current: 11.5, // +15%
      atr14: 0.5,
    });
    expect(out.trailArmed).toBe(true);
    expect(out.trailStop).toBeCloseTo(11, 6); // 12 - 2*0.5
    expect(out.trigger).toBeCloseTo(11, 6); // max(10, 11)
  });

  it('uses hardStop only when not armed', () => {
    const out = deriveTriggerAndTrail({
      hardStop: 9.5,
      costPrice: 10,
      maxPrice: 10.5,
      current: 10.2, // +2%
      atr14: 0.4,
    });
    expect(out.trailArmed).toBe(false);
    expect(out.trigger).toBe(9.5);
  });
});

describe('deriveActionCard', () => {
  it('marks BUY when attack + buy + score', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.3 },
      },
      position: { symbol: 'CN:600000' },
      currentPrice: 10,
    });
    expect(card.action).toBe('BUY');
    expect(isHeldPosition({ symbol: 'CN:600000' })).toBe(false);
  });

  it('downgrades BUY to WATCH when gate blocks new entries', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: holdGate,
      trendok: { score: 90, buyAction: 'buy', stopLossPrice: 9 },
      position: { symbol: 'CN:600000' },
      currentPrice: 10,
    });
    expect(card.action).toBe('WATCH');
    expect(card.why).toBe('GATE_BLOCK_NEW');
  });

  it('EXIT on exit_now', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: 70,
        buyAction: 'avoid',
        stopLossPrice: 9,
        stopLossParts: { exit_now: true, atr14: 0.2 },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11 },
      currentPrice: 10.5,
    });
    expect(card.action).toBe('EXIT');
    expect(card.why).toBe('EXIT_NOW');
  });

  it('ADD when held + attack + buy', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: 85,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.2 },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 10.5 },
      currentPrice: 10.2,
    });
    expect(card.action).toBe('ADD');
  });

  it('TRIM on warn_reduce_half', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: 70,
        buyAction: 'wait',
        stopLossPrice: 9,
        stopLossParts: { warn_reduce_half: true, atr14: 0.2 },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 8, maxPrice: 11 },
      currentPrice: 10.5,
    });
    expect(card.action).toBe('TRIM');
  });
});
