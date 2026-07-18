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
      distPct: -0.5,
      why: 'CHANDELIER',
      mainlineOk: false,
      mainlineTag: null,
    });
    expect(parsed.trigger).toBe(11.2);
    expect(parsed.mainlineOk).toBe(false);
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
});
