import { describe, expect, it } from 'vitest';

import { lotRuleFor, rankSizeBoost, roundToLot, slotLotShares, suggestLotSizing } from './lot-sizing';

describe('lotRuleFor', () => {
  it('沪深主板/创业板：100 股整数倍', () => {
    expect(lotRuleFor('CN:600519')).toMatchObject({ min: 100, step: 100, unit: '股' });
    expect(lotRuleFor('CN:300750')).toMatchObject({ min: 100, step: 100 });
    expect(lotRuleFor('CN:002594')).toMatchObject({ min: 100, step: 100 });
  });

  it('科创板：200 股起，1 股递增', () => {
    expect(lotRuleFor('CN:688111')).toMatchObject({ min: 200, step: 1, unit: '股' });
  });

  it('北交所：100 股起，1 股递增', () => {
    expect(lotRuleFor('CN:832000')).toMatchObject({ min: 100, step: 1 });
    expect(lotRuleFor('CN:920001')).toMatchObject({ min: 100, step: 1 });
  });

  it('ETF：100 份整数倍', () => {
    expect(lotRuleFor('ETF:513350')).toMatchObject({ min: 100, step: 100, unit: '份' });
  });

  it('港股：静态每手表 + 默认 100 估算', () => {
    expect(lotRuleFor('HK:00700')).toMatchObject({ min: 100, source: 'table' });
    expect(lotRuleFor('HK:00939')).toMatchObject({ min: 1000, source: 'table' });
    expect(lotRuleFor('HK:09618')).toMatchObject({ min: 50, source: 'table' });
    expect(lotRuleFor('HK:01234')).toMatchObject({ min: 100, source: 'default' });
  });
});

describe('roundToLot', () => {
  it('nearest board lot (round half up)', () => {
    const a = lotRuleFor('CN:600519');
    expect(roundToLot(149, a)).toBe(100);
    expect(roundToLot(150, a)).toBe(200);
    expect(roundToLot(34, a)).toBe(0);
  });

  it('科创板 200 起 + 1 股递增', () => {
    const star = lotRuleFor('CN:688111');
    expect(roundToLot(243.4, star)).toBe(243);
    expect(roundToLot(150, star)).toBe(200);
    expect(roundToLot(60, star)).toBe(0);
  });

  it('港股整手（1000 股/手）', () => {
    expect(roundToLot(2600, lotRuleFor('HK:00939'))).toBe(3000);
  });
});

describe('suggestLotSizing', () => {
  it('10% × 50 万 @72.5 → 最接近的 100 股整数倍', () => {
    const s = suggestLotSizing({
      symbol: 'CN:600519',
      price: 72.5,
      targetPct: 10,
      capital: 500_000,
    });
    expect(s).not.toBeNull();
    expect(s!.shares).toBe(700);
    expect(s!.actualPct).toBeCloseTo(10.15, 2);
    expect(s!.warning).toBeUndefined();
  });

  it('排名 #1 加成 ×1.2（并受 15% 上限约束）', () => {
    const s = suggestLotSizing({
      symbol: 'CN:600519',
      price: 72.5,
      targetPct: 10,
      capital: 500_000,
      rank: 1,
    });
    expect(s!.boost).toBe(1.2);
    expect(s!.targetPct).toBeCloseTo(12, 5);
    expect(s!.shares).toBe(800);
    const capped = suggestLotSizing({
      symbol: 'CN:600519',
      price: 72.5,
      targetPct: 14,
      capital: 500_000,
      rank: 1,
    });
    expect(capped!.targetPct).toBe(15);
  });

  it('港股按汇率换成 HKD 再取整手', () => {
    const s = suggestLotSizing({
      symbol: 'HK:00700',
      price: 320,
      targetPct: 10,
      capital: 500_000,
      fxRate: 0.92,
    });
    expect(s!.localCurrency).toBe('HKD');
    expect(s!.shares).toBe(200); // 50000/0.92/320 = 169.8 → 200 (lot 100)
    expect(s!.actualPct).toBeCloseTo(11.776, 2);
  });

  it('ETF 100 份整数倍（100% 资金）', () => {
    const s = suggestLotSizing({
      symbol: 'ETF:513350',
      price: 1.417,
      targetPct: 100,
      capital: 500_000,
    });
    expect(s!.shares).toBe(352_900);
    expect(s!.rule.unit).toBe('份');
  });

  it('不足 1 手时返回 0 + 警告', () => {
    const s = suggestLotSizing({
      symbol: 'CN:600519',
      price: 72.5,
      targetPct: 0.5,
      capital: 500_000,
    });
    expect(s!.shares).toBe(0);
    expect(s!.warning).toContain('不足 1 手');
  });

  it('参数不合法返回 null', () => {
    expect(
      suggestLotSizing({ symbol: 'CN:600519', price: 0, targetPct: 10, capital: 500_000 }),
    ).toBeNull();
    expect(
      suggestLotSizing({ symbol: 'CN:600519', price: 10, targetPct: 10, capital: 0 }),
    ).toBeNull();
  });
});

describe('slotLotShares (satellite equal-weight slots)', () => {
  it('sizes a 25% slot in 100-lots without the 15% stock cap', () => {
    // ¥1,000,000 * 25% = ¥250,000; / 27.7 = 9025 → 9,000 shares.
    const r = slotLotShares({ symbol: 'CN:002128', price: 27.7, capital: 1_000_000, slotPct: 25 });
    expect(r).not.toBeNull();
    expect(r?.shares).toBe(9000);
    expect(r?.valueCny).toBe(250_000);
  });

  it('respects the STAR (200 + 1) rule', () => {
    const r = slotLotShares({ symbol: 'CN:688008', price: 210.28, capital: 1_000_000, slotPct: 25 });
    // 250,000 / 210.28 = 1188.9 → round → 1189 (no 100-step)
    expect(r?.shares).toBe(1189);
  });

  it('returns null without capital or price', () => {
    expect(slotLotShares({ symbol: 'CN:002128', price: 27.7, capital: 0, slotPct: 25 })).toBeNull();
    expect(slotLotShares({ symbol: 'CN:002128', price: 0, capital: 1_000_000, slotPct: 25 })).toBeNull();
  });
});

describe('rankSizeBoost', () => {
  it('top ranks tilt mildly, others unchanged', () => {
    expect(rankSizeBoost(1)).toBe(1.2);
    expect(rankSizeBoost(2)).toBe(1.1);
    expect(rankSizeBoost(3)).toBe(1.05);
    expect(rankSizeBoost(4)).toBe(1);
    expect(rankSizeBoost(null)).toBe(1);
  });
});
