import { describe, expect, it } from 'vitest';
import {
  blendAddCost,
  holdingDays,
  isAddOnOpenPosition,
  isOpenPosition,
  sellPnLPct,
} from './trade-recording';

describe('blendAddCost', () => {
  it('blends weighted average cost', () => {
    const r = blendAddCost(10, 10, 12, 5);
    expect(r.blendedCost).toBeCloseTo(10.667, 2);
    expect(r.newPositionPct).toBe(15);
    expect(r.addPct).toBe(5);
  });

  it('handles zero old pct as fresh entry', () => {
    const r = blendAddCost(10, 0, 12, 5);
    expect(r.blendedCost).toBe(12);
    expect(r.newPositionPct).toBe(5);
  });

  it('keeps same cost when add price equals old cost', () => {
    const r = blendAddCost(10, 10, 10, 5);
    expect(r.blendedCost).toBe(10);
  });
});

describe('sellPnLPct', () => {
  it('computes gross pnl pct', () => {
    expect(sellPnLPct(11, 10)).toBe(10);
    expect(sellPnLPct(9, 10)).toBe(-10);
    expect(sellPnLPct(10.5, 10)).toBe(5);
  });

  it('guards zero/negative cost basis', () => {
    expect(sellPnLPct(10, 0)).toBe(0);
    expect(sellPnLPct(10, -1)).toBe(0);
  });
});

describe('holdingDays', () => {
  it('computes calendar-day holding period', () => {
    expect(holdingDays('2026-08-01', '2026-08-08')).toBe(7);
    expect(holdingDays('2026-08-08', '2026-08-08')).toBe(0);
    expect(holdingDays('2026-08-08', '2026-08-01')).toBe(0);
  });
});

describe('isOpenPosition / isAddOnOpenPosition', () => {
  const held = {
    symbol: 'CN:600000',
    addedAt: '',
    color: '#ffffff',
    positionPct: 10,
    costPrice: 10,
  } as const;

  it('detects open position', () => {
    expect(isOpenPosition({ ...held })).toBe(true);
    expect(isOpenPosition({ ...held, positionPct: 0 })).toBe(false);
    expect(isOpenPosition({ ...held, positionPct: null })).toBe(false);
  });

  it('detects add on open position when price differs', () => {
    expect(isAddOnOpenPosition({ ...held }, 12)).toBe(true);
    expect(isAddOnOpenPosition({ ...held }, 10)).toBe(false);
    expect(isAddOnOpenPosition({ ...held }, null)).toBe(false);
    expect(isAddOnOpenPosition({ ...held, positionPct: 0 }, 12)).toBe(false);
  });
});
