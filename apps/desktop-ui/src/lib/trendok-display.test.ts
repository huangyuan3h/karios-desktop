import { describe, expect, it } from 'vitest';

import type { TrendOkResult } from '@/lib/api/types';

import { trendOkRuleLines, trendOkSummary } from './trendok-display';

describe('trendOkSummary', () => {
  it('returns checkmark when trendOk is true', () => {
    expect(trendOkSummary({ trendOk: true } as TrendOkResult)).toBe('✅');
  });

  it('returns dash when no data', () => {
    expect(trendOkSummary(null)).toBe('—');
    expect(trendOkSummary(undefined)).toBe('—');
  });

  it('lists failed rules when checks are present', () => {
    const t = {
      symbol: 'CN:600000',
      trendOk: false,
      checks: {
        emaOrder: false,
        macdPositive: true,
        macdHistExpanding: true,
        closeNear20dHigh: true,
        rsiInRange: true,
        volumeSurge: true,
      },
    } as TrendOkResult;
    expect(trendOkSummary(t)).toContain('EMA order broken');
  });

  it('returns cross when trendOk false without check details', () => {
    expect(trendOkSummary({ trendOk: false } as TrendOkResult)).toBe('❌');
  });
});

describe('trendOkRuleLines', () => {
  it('returns six rules', () => {
    expect(trendOkRuleLines()).toHaveLength(6);
  });
});
