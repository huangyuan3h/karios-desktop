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

  it('twin_star sat-active: CN stocks + 50% core ETF is aligned, not stock_while_etf', () => {
    const r = detectReplicaGaps({
      pick: 'OIL',
      mode: 'twin_star',
      coreTargetPct: 50,
      holdings: [
        { symbol: 'ETF:513350', positionPct: 50 },
        { symbol: 'CN:600519', positionPct: 12.5 },
        { symbol: 'CN:000001', positionPct: 12.5 },
        { symbol: 'CN:000002', positionPct: 12.5 },
        { symbol: 'CN:000003', positionPct: 12.5 },
      ],
    });
    expect(r.verdict).toBe('aligned');
    expect(r.reasons.some((x) => x.id === 'stock_while_etf')).toBe(false);
    expect(r.coreTargetPct).toBe(50);
    expect(r.reasons.some((x) => x.id === 'clip4_structure')).toBe(true);
    expect(r.reasons.some((x) => x.id === 'not_full_switch')).toBe(false);
  });

  it('twin_star does not treat 100% core ETF as the live instruction when sat is active', () => {
    const r = detectReplicaGaps({
      pick: 'NASDAQ',
      mode: 'twin_star',
      coreTargetPct: 50,
      holdings: [{ symbol: 'ETF:513110', positionPct: 100 }],
    });
    expect(r.verdict).toBe('partial');
    expect(r.reasons.some((x) => x.id === 'core_over_sat_empty')).toBe(true);
    expect(r.reasons.some((x) => x.id === 'stock_while_etf')).toBe(false);
  });

  it('twin_star idle day with leftover sat stocks is warn, not 100% hard-switch block', () => {
    const r = detectReplicaGaps({
      pick: 'GOLD',
      mode: 'twin_star',
      coreTargetPct: 100,
      holdings: [
        { symbol: 'ETF:518880', positionPct: 80 },
        { symbol: 'CN:600519', positionPct: 20 },
      ],
    });
    expect(r.verdict).toBe('partial');
    expect(r.reasons.some((x) => x.id === 'leftover_sat_idle')).toBe(true);
    expect(r.reasons.some((x) => x.id === 'stock_while_etf')).toBe(false);
  });
});
