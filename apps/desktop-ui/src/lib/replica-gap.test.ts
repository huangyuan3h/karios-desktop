import { describe, expect, it } from 'vitest';

import {
  classifyHoldingSymbol,
  detectReplicaGaps,
  holdingMatchesPick,
} from './replica-gap';

describe('replica-gap', () => {
  it('classifies symbols', () => {
    expect(classifyHoldingSymbol('CN:600519')).toBe('STOCK');
    expect(classifyHoldingSymbol('HK:00700')).toBe('STOCK');
    expect(classifyHoldingSymbol('ETF:513100')).toBe('ETF');
  });

  it('matches pick aliases', () => {
    expect(holdingMatchesPick('ETF:513110', 'NASDAQ')).toBe(true);
    expect(holdingMatchesPick('ETF:518880', 'GOLD')).toBe(true);
    expect(holdingMatchesPick('CN:600519', 'STOCK')).toBe(true);
    expect(holdingMatchesPick('CN:600519', 'NASDAQ')).toBe(false);
  });

  it('flags stock while ETF pick as block', () => {
    const r = detectReplicaGaps({
      pick: 'NASDAQ',
      holdings: [
        { symbol: 'CN:600519', positionPct: 40 },
        { symbol: 'ETF:513110', positionPct: 30 },
      ],
    });
    expect(r.verdict).toBe('diverged');
    expect(r.reasons.some((x) => x.id === 'stock_while_etf')).toBe(true);
    expect(r.targetWeightPct).toBe(30);
    expect(r.reasons.some((x) => x.id === 'timing_1430')).toBe(true);
    expect(r.reasons.some((x) => x.id === 'conditional_orders')).toBe(true);
  });

  it('aligned when mostly on target ETF', () => {
    const r = detectReplicaGaps({
      pick: 'GOLD',
      holdings: [{ symbol: 'ETF:518880', positionPct: 95 }],
    });
    expect(r.verdict).toBe('aligned');
    expect(r.reasons.every((x) => x.severity === 'info')).toBe(true);
  });

  it('repo with risk holdings is diverged', () => {
    const r = detectReplicaGaps({
      pick: 'REPO',
      holdings: [{ symbol: 'ETF:513100', positionPct: 50 }],
    });
    expect(r.verdict).toBe('diverged');
    expect(r.reasons.some((x) => x.id === 'should_be_cash')).toBe(true);
  });
});
