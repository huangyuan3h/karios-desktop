import { describe, expect, it } from 'vitest';

import {
  SAT_MAX_POS,
  buildTwinStarTradePlan,
  satConclusionLine,
  type TwinStarTradePlanInput,
} from './twin-star-trade-plan';

function base(over: Partial<TwinStarTradePlanInput> = {}): TwinStarTradePlanInput {
  return {
    coreTargetPct: 50,
    satTargetPct: 50,
    gateOpen: true,
    afterSatWindow: true,
    satHoldings: [],
    satExitsDue: [],
    satCandidates: [{ ts: '000712.SZ', amp: 1, gapPct: 5, close: 10 }],
    pickKey: 'OIL',
    pickSymbol: 'ETF:513350',
    pickName: '原油 ETF',
    cnCandidates: [],
    hkCandidates: [],
    cnAllowBuys: false,
    hkAllowBuys: false,
    suggestedSizePct: 10,
    s3GateBlocksNew: false,
    etfHoldings: [],
    ...over,
  };
}

describe('buildTwinStarTradePlan', () => {
  it('sizes each satellite slot as 10% of the sat sleeve (5% of NAV at 50/50)', () => {
    const plan = buildTwinStarTradePlan(base());
    expect(plan.satSlotNavPct).toBe(5);
    expect(plan.buys.some((r) => r.sleeve === 'sat' && r.symbol === 'CN:000712' && r.navPct === 5)).toBe(true);
    expect(satConclusionLine(plan, true)).toMatch(/买入 000712\.SZ · 每只总资产 5%/);
  });

  it('does not advertise gap candidates when the 15-slot book is full', () => {
    const holdings = Array.from({ length: SAT_MAX_POS }, (_, i) => ({
      ts: `60000${i}.SH`,
      daysLeft: 2,
    }));
    const plan = buildTwinStarTradePlan(
      base({
        satHoldings: holdings,
        satCandidates: [
          { ts: '600352.SH', amp: 1, gapPct: 2, close: 10 },
          { ts: '603339.SH', amp: 1, gapPct: 2, close: 10 },
        ],
      }),
    );
    expect(plan.satFreeSlots).toBe(0);
    expect(plan.buys.filter((r) => r.sleeve === 'sat')).toHaveLength(0);
    expect(plan.holds.filter((r) => r.sleeve === 'sat')).toHaveLength(SAT_MAX_POS);
    expect(plan.holds[0]?.navPct).toBe(5);
    expect(plan.satHeadline).toMatch(/持仓簿满 15\/15 · 今日不买新票/);
    expect(satConclusionLine(plan, true)).not.toMatch(/600352/);
  });

  it('opens slots for new buys when the book is full but names exit today', () => {
    const holdings = Array.from({ length: SAT_MAX_POS }, (_, i) => ({
      ts: `60010${i}.SH`,
      daysLeft: i === 0 ? 1 : 2,
    }));
    const plan = buildTwinStarTradePlan(
      base({
        satHoldings: holdings,
        satExitsDue: [holdings[0]!],
        satCandidates: [{ ts: '600352.SH', amp: 1, gapPct: 2, close: 10 }],
      }),
    );
    expect(plan.satFreeSlots).toBe(1);
    expect(plan.buys.some((r) => r.symbol === 'CN:600352')).toBe(true);
    expect(plan.sells.some((r) => r.symbol === 'CN:600100')).toBe(true);
  });

  it('uses the next fillable name when the top gap is limit-up', () => {
    const plan = buildTwinStarTradePlan(
      base({
        satCandidates: [],
        satBlocked: [{ ts: '600003.SH', amp: 1, gapPct: 9, close: 11, limitLocked: true }],
        satAlternates: [{ ts: '000712.SZ', amp: 2, gapPct: 4, close: 10 }],
      }),
    );
    const buy = plan.buys.find((r) => r.sleeve === 'sat');
    expect(buy?.symbol).toBe('CN:000712');
    expect(buy?.swapFrom).toBe('600003.SH');
  });

  it('hides satellite buys before 14:30 even when the gate is open', () => {
    const plan = buildTwinStarTradePlan(base({ afterSatWindow: false }));
    expect(plan.buys.filter((r) => r.sleeve === 'sat')).toHaveLength(0);
    expect(plan.satHeadline).toMatch(/14:20 拉当日行情/);
  });

  it('does not dump ETFs when pick=STOCK has zero executable names', () => {
    const plan = buildTwinStarTradePlan(
      base({
        pickKey: 'STOCK',
        pickSymbol: 'STOCK',
        pickName: '股票篮',
        cnCandidates: [],
        cnAllowBuys: true,
        etfHoldings: [
          { symbol: 'ETF:513110', key: 'NASDAQ', positionPct: 48.6 },
          { symbol: 'ETF:513350', key: 'OIL', positionPct: 42 },
        ],
      }),
    );
    expect(plan.coreBuyable).toBe(false);
    expect(plan.buys.filter((r) => r.sleeve === 'core')).toHaveLength(0);
    expect(plan.sells.filter((r) => r.sleeve === 'core')).toHaveLength(0);
    expect(plan.coreHeadline).toMatch(/不要为 STOCK 清空 ETF/);
  });

  it('lists core STOCK buys as % of total NAV and sells non-pick ETFs only then', () => {
    const plan = buildTwinStarTradePlan(
      base({
        pickKey: 'STOCK',
        pickSymbol: 'STOCK',
        cnAllowBuys: true,
        cnCandidates: [{ symbol: 'CN:600111', name: '北方稀土', score: 71 }],
        etfHoldings: [{ symbol: 'ETF:513350', key: 'OIL', positionPct: 42 }],
      }),
    );
    expect(plan.coreBuyable).toBe(true);
    expect(plan.buys.find((r) => r.symbol === 'CN:600111')?.navPct).toBe(5);
    expect(plan.sells.some((r) => r.symbol === 'ETF:513350')).toBe(true);
  });

  it('blocks core STOCK buys when the S-3 execution gate is closed', () => {
    const plan = buildTwinStarTradePlan(
      base({
        pickKey: 'STOCK',
        s3GateBlocksNew: true,
        cnAllowBuys: true,
        cnCandidates: [{ symbol: 'CN:600111', name: '北方稀土', score: 71 }],
        etfHoldings: [{ symbol: 'ETF:513110', key: 'NASDAQ', positionPct: 48 }],
      }),
    );
    expect(plan.buys.filter((r) => r.sleeve === 'core')).toHaveLength(0);
    expect(plan.sells.filter((r) => r.sleeve === 'core')).toHaveLength(0);
    expect(plan.coreHeadline).toMatch(/闸门关闭/);
  });

  it('targets the pick ETF at coreTargetPct of NAV', () => {
    const plan = buildTwinStarTradePlan(base({ etfHoldings: [] }));
    const buy = plan.buys.find((r) => r.sleeve === 'core');
    expect(buy?.symbol).toBe('ETF:513350');
    expect(buy?.navPct).toBe(50);
  });
});
