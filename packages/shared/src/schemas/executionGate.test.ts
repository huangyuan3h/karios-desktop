import { describe, expect, it } from 'vitest';

import { ExecutionGateSchema, ExecutionActionCardSchema } from './executionGate';

describe('ExecutionGateSchema', () => {
  it('parses ATTACK gate', () => {
    const parsed = ExecutionGateSchema.parse({
      mode: 'ATTACK',
      allowNewEntries: true,
      marketRegime: 'Strong',
      indexLight: 'green',
      srvLevel: 'Stable',
      srvOverlapCount: 3,
      downCount: 1000,
      reasons: ['REGIME_STRONG', 'SRV_STABLE'],
      positionRangeHint: '50%-60%',
      satelliteNote: '允许开新仓与加仓；遵守单票上限与吊灯止盈',
    });
    expect(parsed.mode).toBe('ATTACK');
    expect(parsed.allowNewEntries).toBe(true);
  });

  it('parses WEAK_ATTACK overflow gate', () => {
    const parsed = ExecutionGateSchema.parse({
      mode: 'WEAK_ATTACK',
      allowNewEntries: true,
      marketRegime: 'Strong',
      indexLight: 'green',
      srvLevel: 'Extreme_High',
      srvOverlapCount: 0,
      upCount: 4100,
      reasons: ['SRV_EXTREME_HIGH', 'INTRADAY_OVERFLOW_OVERRIDE'],
      overflowSector: '电子',
      overflowInflowYi: 726,
      satelliteNote: '极端资金流豁免；允许 5% 先锋仓试探',
    });
    expect(parsed.mode).toBe('WEAK_ATTACK');
    expect(parsed.overflowSector).toBe('电子');
    expect(parsed.overflowInflowYi).toBe(726);
  });

  it('rejects unknown mode', () => {
    expect(() =>
      ExecutionGateSchema.parse({
        mode: 'YOLO',
        allowNewEntries: true,
        marketRegime: 'Strong',
        indexLight: 'green',
        reasons: [],
      }),
    ).toThrow();
  });

  it('parses per-market cnGate/hkGate', () => {
    const parsed = ExecutionGateSchema.parse({
      mode: 'HOLD_ONLY',
      allowNewEntries: false,
      marketRegime: 'Diverging',
      indexLight: 'red',
      reasons: ['REGIME_DIVERGING'],
      positionRangeHint: '30%',
      cnGate: {
        mode: 'HOLD_ONLY',
        allowNewEntries: false,
        marketRegime: 'Diverging',
        indexLight: 'red',
        reasons: ['REGIME_DIVERGING'],
        positionRangeHint: '30%',
      },
      hkGate: {
        mode: 'ATTACK',
        allowNewEntries: true,
        marketRegime: 'Strong',
        indexLight: 'green',
        reasons: ['REGIME_STRONG'],
        positionRangeHint: '50%-60%',
      },
    });
    expect(parsed.cnGate?.mode).toBe('HOLD_ONLY');
    expect(parsed.hkGate?.mode).toBe('ATTACK');
    expect(parsed.hkGate?.positionRangeHint).toBe('50%-60%');
  });
});

describe('ExecutionActionCardSchema', () => {
  it('parses EXIT card with trail', () => {
    const parsed = ExecutionActionCardSchema.parse({
      symbol: 'CN:600000',
      action: 'EXIT',
      trailArmed: true,
      peak: 12.5,
      hardStop: 11.0,
      trailStop: 11.2,
      trigger: 11.2,
      exitStop: 11.2,
      distPct: -0.5,
      why: 'CHANDELIER',
      mainlineOk: false,
      mainlineTag: null,
    });
    expect(parsed.trigger).toBe(11.2);
    expect(parsed.exitStop).toBe(11.2);
    expect(parsed.mainlineOk).toBe(false);
  });

  it('parses PURGE card with entryTrigger', () => {
    const parsed = ExecutionActionCardSchema.parse({
      symbol: 'CN:603019',
      action: 'PURGE',
      trailArmed: false,
      entryTrigger: 55.2,
      trigger: 55.2,
      distPct: 3.1,
      why: 'PURGE_GC',
      mainlineOk: false,
    });
    expect(parsed.action).toBe('PURGE');
    expect(parsed.entryTrigger).toBe(55.2);
  });

  it('parses WATCH_SILENT card', () => {
    const parsed = ExecutionActionCardSchema.parse({
      symbol: 'CN:002230',
      action: 'WATCH_SILENT',
      trailArmed: false,
      why: 'ALPHA_S_WATCH',
      mainlineOk: false,
    });
    expect(parsed.action).toBe('WATCH_SILENT');
    expect(parsed.why).toBe('ALPHA_S_WATCH');
  });

  it('parses BUY card with mainline tag', () => {
    const parsed = ExecutionActionCardSchema.parse({
      symbol: 'CN:600519',
      action: 'BUY',
      trailArmed: false,
      why: 'MAINLINE_5D_TOP3',
      mainlineOk: true,
      mainlineTag: '5D_TOP3',
    });
    expect(parsed.mainlineTag).toBe('5D_TOP3');
  });

  it('parses source attribution field (TIP-011)', () => {
    const parsed = ExecutionActionCardSchema.parse({
      symbol: 'CN:600519',
      action: 'BUY',
      trailArmed: false,
      why: 'MAINLINE_OK',
      source: 'TV',
    });
    expect(parsed.source).toBe('TV');
    const alpha = ExecutionActionCardSchema.parse({
      symbol: 'CN:600519',
      action: 'ADD',
      trailArmed: false,
      source: 'ALPHA',
    });
    expect(alpha.source).toBe('ALPHA');
    const none = ExecutionActionCardSchema.parse({
      symbol: 'CN:600519',
      action: 'BUY',
      trailArmed: false,
      source: null,
    });
    expect(none.source).toBeNull();
  });

  it('parses size suggestion fields', () => {
    const parsed = ExecutionActionCardSchema.parse({
      symbol: 'CN:600519',
      action: 'BUY',
      trailArmed: false,
      why: 'MAINLINE_OK',
      suggestAddPct: 5,
      suggestSizeNote: 'clip',
    });
    expect(parsed.suggestAddPct).toBe(5);
    expect(parsed.suggestSizeNote).toBe('clip');
  });

  it('parses ETF_FALLBACK ruleType', () => {
    const parsed = ExecutionActionCardSchema.parse({
      symbol: 'ETF:515880',
      action: 'TRIM',
      trailArmed: false,
      why: 'ETF_FALLBACK_TRIM',
      ruleType: 'ETF_FALLBACK',
    });
    expect(parsed.ruleType).toBe('ETF_FALLBACK');
    const plain = ExecutionActionCardSchema.parse({
      symbol: 'CN:600519',
      action: 'EXIT',
      trailArmed: false,
      ruleType: null,
    });
    expect(plain.ruleType).toBeNull();
  });
});
