import { describe, expect, it } from 'vitest';

import {
  SAT_MAX_POS,
  SAT_SLOT_OF_SLEEVE,
  TWIN_STAR_LIVE_RECIPE,
  allocateSatFundTrims,
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
    etfHoldings: [],
    liveStockHoldings: [],
    ...over,
  };
}

function recipeBook(n = SAT_MAX_POS) {
  return Array.from({ length: n }, (_, i) => ({
    ts: `60000${i}.SH`,
    daysLeft: 2,
  }));
}

describe('buildTwinStarTradePlan', () => {
  it('locks live sizing to the frozen S-gap engine constants', () => {
    expect(TWIN_STAR_LIVE_RECIPE.maxPos).toBe(15);
    expect(TWIN_STAR_LIVE_RECIPE.slotOfSleeve).toBe(0.1);
    expect(TWIN_STAR_LIVE_RECIPE.bucketQ).toBe(3);
    expect(TWIN_STAR_LIVE_RECIPE.body).toBe(3);
    expect(SAT_MAX_POS).toBe(TWIN_STAR_LIVE_RECIPE.maxPos);
    expect(SAT_SLOT_OF_SLEEVE).toBe(TWIN_STAR_LIVE_RECIPE.slotOfSleeve);
  });

  it('drains the weaker ETF before touching the core park ETF', () => {
    const trims = allocateSatFundTrims({
      holdings: [
        { symbol: 'ETF:513110', key: 'NASDAQ', positionPct: 48.6 },
        { symbol: 'ETF:513350', key: 'OIL', positionPct: 42 },
      ],
      trimTotal: 25,
      parkKey: 'NASDAQ',
      momByKey: { NASDAQ: 5.16, OIL: 4.98 },
    });
    expect(trims.map((t) => ({ key: t.holding.key, cut: t.cut }))).toEqual([{ key: 'OIL', cut: 25 }]);
  });
  it('sizes each satellite slot as 10% of the sat sleeve (5% of NAV at 50/50)', () => {
    const plan = buildTwinStarTradePlan(base());
    expect(plan.satSlotNavPct).toBe(5);
    expect(plan.buys.some((r) => r.sleeve === 'sat' && r.symbol === 'CN:000712' && r.navPct === 5)).toBe(true);
    expect(satConclusionLine(plan, true)).toMatch(/买入 000712\.SZ · 每只总资产 5%/);
  });

  it('still fills today\'s gaps when the recipe replay is 15/15 but live satellite is empty', () => {
    const plan = buildTwinStarTradePlan(
      base({
        satHoldings: recipeBook(),
        satCandidates: [
          { ts: '600352.SH', amp: 1, gapPct: 2, close: 10 },
          { ts: '603339.SH', amp: 1, gapPct: 2, close: 10 },
        ],
        liveStockHoldings: [],
        etfHoldings: [
          { symbol: 'ETF:513110', key: 'NASDAQ', positionPct: 48.6 },
          { symbol: 'ETF:513350', key: 'OIL', positionPct: 42 },
        ],
      }),
    );
    expect(plan.recipeSatHeld).toBe(15);
    expect(plan.satHeld).toBe(0);
    expect(plan.satFreeSlots).toBe(15);
    expect(plan.buys.filter((r) => r.kind === 'stock').map((r) => r.symbol)).toEqual(['CN:600352', 'CN:603339']);
    expect(plan.holds.filter((r) => r.kind === 'stock')).toHaveLength(0);
    expect(plan.bookNote).toMatch(/模拟仓，不是券商持仓/);
    expect(plan.etfSparePct).toBe(40.6);
  });

  it('does not advertise gap names when the user already holds 15 satellite stocks', () => {
    const holdings = recipeBook();
    const plan = buildTwinStarTradePlan(
      base({
        satHoldings: holdings,
        satCandidates: [
          { ts: '600352.SH', amp: 1, gapPct: 2, close: 10 },
          { ts: '603339.SH', amp: 1, gapPct: 2, close: 10 },
        ],
        liveStockHoldings: holdings.map((h) => ({ symbol: `CN:${h.ts.slice(0, 6)}` })),
      }),
    );
    expect(plan.satFreeSlots).toBe(0);
    expect(plan.buys.filter((r) => r.sleeve === 'sat')).toHaveLength(0);
    expect(plan.holds.filter((r) => r.sleeve === 'sat')).toHaveLength(SAT_MAX_POS);
    expect(plan.satHeadline).toMatch(/你卫星仓满 15\/15 · 今日不买新票/);
    expect(satConclusionLine(plan, true)).not.toMatch(/600352/);
  });

  it('opens slots for new buys when a live satellite name exits today', () => {
    const holdings = Array.from({ length: SAT_MAX_POS }, (_, i) => ({
      ts: `60010${i}.SH`,
      daysLeft: i === 0 ? 1 : 2,
    }));
    const plan = buildTwinStarTradePlan(
      base({
        satHoldings: holdings,
        satExitsDue: [holdings[0]!],
        satCandidates: [{ ts: '600352.SH', amp: 1, gapPct: 2, close: 10 }],
        liveStockHoldings: holdings.map((h) => ({ symbol: `CN:${h.ts.slice(0, 6)}` })),
      }),
    );
    expect(plan.satFreeSlots).toBe(1);
    expect(plan.buys.some((r) => r.symbol === 'CN:600352')).toBe(true);
    expect(plan.sells.some((r) => r.symbol === 'CN:600100')).toBe(true);
  });

  it('does not sell a recipe exit the user does not hold', () => {
    const plan = buildTwinStarTradePlan(
      base({
        satHoldings: recipeBook(),
        satExitsDue: [{ ts: '600000.SH', daysLeft: 0, exitDue: '2026-09-01' }],
        liveStockHoldings: [],
      }),
    );
    expect(plan.sells.filter((r) => r.kind === 'stock')).toHaveLength(0);
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

  it('hides satellite buys until a market snapshot exists', () => {
    const plan = buildTwinStarTradePlan(base({ afterSatWindow: false }));
    expect(plan.buys.filter((r) => r.sleeve === 'sat')).toHaveLength(0);
    expect(plan.satHeadline).toMatch(/等待全市场快照/);
  });

  it('cuts the weaker ETF first and parks the core in the strongest ETF', () => {
    const plan = buildTwinStarTradePlan(
      base({
        pickKey: 'STOCK',
        pickSymbol: 'STOCK',
        pickName: '股票篮',
        cnCandidates: [],
        cnAllowBuys: true,
        satCandidates: [
          { ts: '600352.SH', amp: 1, gapPct: 2, close: 10 },
          { ts: '603339.SH', amp: 1, gapPct: 2, close: 10 },
        ],
        coreParkEtfKey: 'NASDAQ',
        etfMomByKey: { NASDAQ: 5.16, OIL: 4.98, GOLD: 1, BOND10: 0.5, STOCK: 6.88 },
        etfHoldings: [
          { symbol: 'ETF:513110', key: 'NASDAQ', name: '纳指', positionPct: 48.6 },
          { symbol: 'ETF:513350', key: 'OIL', name: '原油', positionPct: 42 },
        ],
      }),
    );
    expect(plan.stockBuyNavPct).toBe(10);
    expect(plan.etfTrimPct).toBe(10);
    const funds = plan.sells.filter((r) => r.purpose === 'sat-fund');
    expect(funds).toHaveLength(1);
    expect(funds[0]?.symbol).toBe('ETF:513350');
    expect(funds[0]?.navPct).toBe(10);
    expect(plan.sells.some((r) => r.symbol === 'ETF:513110' && r.purpose === 'sat-fund')).toBe(false);
    expect(plan.etfHeadline).toMatch(/先砍弱 ETF/);
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

  it('does not use the S-3 execution gate — core STOCK still lists basket names', () => {
    const plan = buildTwinStarTradePlan(
      base({
        pickKey: 'STOCK',
        cnAllowBuys: true,
        cnCandidates: [{ symbol: 'CN:600111', name: '北方稀土', score: 71 }],
        etfHoldings: [{ symbol: 'ETF:513110', key: 'NASDAQ', positionPct: 48 }],
      }),
    );
    expect(plan.buys.find((r) => r.symbol === 'CN:600111')?.navPct).toBe(5);
    expect(plan.coreHeadline).not.toMatch(/闸门/);
  });

  it('targets the pick ETF at coreTargetPct of NAV', () => {
    const plan = buildTwinStarTradePlan(base({ etfHoldings: [] }));
    const buy = plan.buys.find((r) => r.sleeve === 'core');
    expect(buy?.symbol).toBe('ETF:513350');
    expect(buy?.navPct).toBe(50);
    expect(buy?.kind).toBe('etf');
  });
});
