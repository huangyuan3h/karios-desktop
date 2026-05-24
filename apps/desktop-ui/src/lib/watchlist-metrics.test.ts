import { describe, expect, it } from 'vitest';

import {
  computePnLPct,
  computeVwap,
  formatHotTop3,
  industryDisplayName,
  isHotTop3Industry,
} from './watchlist-metrics';

describe('computePnLPct', () => {
  it('returns null without cost', () => {
    expect(computePnLPct(null, 10)).toBeNull();
    expect(computePnLPct(0, 10)).toBeNull();
  });

  it('computes percentage gain and loss', () => {
    expect(computePnLPct(100, 105)).toBeCloseTo(5);
    expect(computePnLPct(100, 95)).toBeCloseTo(-5);
  });
});

describe('computeVwap', () => {
  it('computes realtime vwap from amount and volume lots', () => {
    expect(computeVwap(101200, 100, 'realtime')).toBeCloseTo(10.12);
  });

  it('computes daily vwap with thousand-yuan amount', () => {
    expect(computeVwap(1012, 100, 'daily')).toBeCloseTo(101.2);
  });

  it('returns null for invalid inputs', () => {
    expect(computeVwap(null, 100)).toBeNull();
    expect(computeVwap(1000, 0)).toBeNull();
  });
});

describe('industry helpers', () => {
  it('prefers emIndustry over tushare industry', () => {
    expect(industryDisplayName({ emIndustry: '集成电路封测', industry: '元器件' })).toBe('集成电路封测');
  });

  it('detects HotTop3 from industryFlowReasons', () => {
    const t = { values: { industryFlowReasons: ['hotspots_today_top3'] } };
    expect(isHotTop3Industry(t)).toBe(true);
    expect(formatHotTop3(t)).toBe('✓');
  });
});
