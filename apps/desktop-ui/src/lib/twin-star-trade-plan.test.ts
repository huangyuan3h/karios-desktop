import { describe, expect, it } from 'vitest';

import {
  SAT_MAX_POS,
  SAT_SLOT_NAV_PCT,
  SAT_SLOT_OF_SLEEVE,
  TWIN_STAR_LIVE_RECIPE,
  allocateSatFundTrims,
  buildTwinStarTradePlan,
  isLiveSatelliteStock,
  satBodyProgress,
  satConclusionLine,
  satConditionalLine,
  satNameTsFromAction,
  satProtectStop,
  twinStarDayFlow,
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
    expect(TWIN_STAR_LIVE_RECIPE.maxPos).toBe(4);
    expect(TWIN_STAR_LIVE_RECIPE.slotOfSleeve).toBe(0.25);
    expect(TWIN_STAR_LIVE_RECIPE.bucketQ).toBe(3);
    expect(TWIN_STAR_LIVE_RECIPE.body).toBe(3);
    expect(SAT_MAX_POS).toBe(TWIN_STAR_LIVE_RECIPE.maxPos);
    expect(SAT_SLOT_OF_SLEEVE).toBe(TWIN_STAR_LIVE_RECIPE.slotOfSleeve);
    expect(SAT_SLOT_NAV_PCT).toBe(12.5);
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
  it('sizes each satellite slot as 25% of the sat sleeve (12.5% of NAV at 50/50)', () => {
    const plan = buildTwinStarTradePlan(base());
    expect(plan.satSlotNavPct).toBe(12.5);
    expect(plan.buys.some((r) => r.sleeve === 'sat' && r.symbol === 'CN:000712' && r.navPct === 12.5)).toBe(true);
    expect(satConclusionLine(plan, true)).toMatch(/买入 000712\.SZ · 每只总资产 12\.5%/);
  });

  it('uses the Chinese name on satellite buy rows when the API sends it', () => {
    const plan = buildTwinStarTradePlan(
      base({
        satCandidates: [{ ts: '000712.SZ', name: '锦江投资', amp: 1, gapPct: 5, close: 10 }],
      }),
    );
    expect(plan.buys[0]?.name).toBe('锦江投资');
    expect(satConclusionLine(plan, true)).toMatch(/买入 锦江投资 · 每只总资产 12\.5%/);
  });

  it('still fills today\'s gaps when the recipe replay is 4/4 but live satellite is empty', () => {
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
    expect(plan.recipeSatHeld).toBe(SAT_MAX_POS);
    expect(plan.satHeld).toBe(0);
    expect(plan.satFreeSlots).toBe(SAT_MAX_POS);
    expect(plan.buys.filter((r) => r.kind === 'stock').map((r) => r.symbol)).toEqual(['CN:600352', 'CN:603339']);
    expect(plan.holds.filter((r) => r.kind === 'stock')).toHaveLength(0);
    expect(plan.bookNote).toMatch(/对照，不是券商仓/);
    expect(plan.etfSparePct).toBe(40.6);
  });

  it('does not advertise gap names when the user already holds 4 satellite stocks', () => {
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
    expect(plan.satHeadline).toMatch(/你卫星仓满 4\/4 · 今日不买新票/);
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

  it('does not auto-buy alternates when the bucket top is limit-up (strict, no refill)', () => {
    const plan = buildTwinStarTradePlan(
      base({
        satCandidates: [],
        satBlocked: [{ ts: '600003.SH', amp: 1, gapPct: 9, close: 11, limitLocked: true }],
        satAlternates: [{ ts: '000712.SZ', amp: 2, gapPct: 4, close: 10 }],
      }),
    );
    expect(plan.buys.filter((r) => r.sleeve === 'sat')).toHaveLength(0);
    expect(plan.satHeadline).toMatch(/涨停跳过 1 只，strict 不补/);
  });

  it('buys only the strict primary list, not the next-amp alternates', () => {
    const plan = buildTwinStarTradePlan(
      base({
        satCandidates: [
          { ts: '300413.SZ', amp: 6.24, gapPct: 5.5, close: 21 },
          { ts: '603318.SH', amp: 6.43, gapPct: 3.2, close: 10 },
        ],
        satBlocked: [{ ts: '003005.SZ', amp: 0, gapPct: 10, close: 20, limitLocked: true }],
        satAlternates: [
          { ts: '600871.SH', amp: 7.14, gapPct: 3.06, close: 2.24 },
          { ts: '301012.SZ', amp: 7.16, gapPct: 3.6, close: 25 },
        ],
      }),
    );
    expect(plan.buys.filter((r) => r.sleeve === 'sat').map((r) => r.symbol)).toEqual([
      'CN:300413',
      'CN:603318',
    ]);
    expect(plan.buys.some((r) => r.swapFrom)).toBe(false);
    expect(plan.buys.some((r) => r.symbol === 'CN:600871')).toBe(false);
  });

  it('does not sell a 0% leftover ETF after the user already sold it', () => {
    const plan = buildTwinStarTradePlan(
      base({
        pickKey: 'OIL',
        pickSymbol: 'ETF:513350',
        pickName: '原油 ETF',
        etfHoldings: [
          { symbol: 'ETF:513110', key: 'NASDAQ', name: '纳指', positionPct: 0 },
          { symbol: 'ETF:513350', key: 'OIL', name: '原油', positionPct: 51.5 },
        ],
      }),
    );
    expect(plan.sells.some((r) => r.symbol === 'ETF:513110')).toBe(false);
    expect(plan.holds.some((r) => r.symbol === 'ETF:513350')).toBe(true);
    expect(plan.etfTotalPct).toBe(51.5);
  });

  it('hides satellite buys until 14:30 even when a snapshot already exists', () => {
    const plan = buildTwinStarTradePlan(base({ afterSatWindow: false }));
    expect(plan.buys.filter((r) => r.sleeve === 'sat')).toHaveLength(0);
    expect(plan.satHeadline).toMatch(/候选 14:30 后公布/);
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
    expect(plan.stockBuyNavPct).toBe(25);
    expect(plan.etfTrimPct).toBe(25);
    const funds = plan.sells.filter((r) => r.purpose === 'sat-fund');
    expect(funds).toHaveLength(1);
    expect(funds[0]?.symbol).toBe('ETF:513350');
    expect(funds[0]?.navPct).toBe(25);
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

  it('treats all CN holdings as satellite when the core pick is not STOCK', () => {
    const plan = buildTwinStarTradePlan(
      base({
        pickKey: 'OIL',
        satCandidates: [{ ts: '603221.SH', name: '爱玛科技', amp: 1, gapPct: 2, close: 10 }],
        liveStockHoldings: [
          { symbol: 'CN:300413', name: '芒果超媒', positionPct: 12.5 },
          { symbol: 'CN:603318', name: '水发燃气', positionPct: 12.5 },
          { symbol: 'CN:600540', name: '新赛股份', positionPct: 12.5 },
          { symbol: 'CN:301012', name: '扬电科技', positionPct: 12.5 },
        ],
      }),
    );
    expect(plan.satHeld).toBe(4);
    expect(plan.satFreeSlots).toBe(0);
    expect(plan.satHeldSymbols.sort()).toEqual(['CN:300413', 'CN:301012', 'CN:600540', 'CN:603318']);
    expect(plan.buys.filter((r) => r.sleeve === 'sat')).toHaveLength(0);
    expect(plan.holds.map((h) => h.name).sort()).toEqual(['扬电科技', '新赛股份', '水发燃气', '芒果超媒']);
    expect(plan.satHeadline).toMatch(/你卫星仓满 4\/4 · 今日不买新票/);
    expect(satConclusionLine(plan, true)).not.toMatch(/603221/);
  });

  it('keeps unmatched CN names in the S-3 basket only when pick=STOCK', () => {
    const plan = buildTwinStarTradePlan(
      base({
        pickKey: 'STOCK',
        pickSymbol: 'STOCK',
        pickName: '股票篮',
        cnAllowBuys: true,
        satCandidates: [{ ts: '000712.SZ', amp: 1, gapPct: 5, close: 10 }],
        liveStockHoldings: [
          { symbol: 'CN:000712', name: '锦江投资', positionPct: 12.5 },
          { symbol: 'CN:600111', name: '北方稀土', positionPct: 10 },
        ],
      }),
    );
    expect(plan.satHeldSymbols).toEqual(['CN:000712']);
    expect(plan.satHeld).toBe(1);
    expect(plan.satFreeSlots).toBe(3);
    expect(plan.holds.some((r) => r.symbol === 'CN:600111')).toBe(false);
  });

  it('does not print engine-book occupancy as 15/4', () => {
    const plan = buildTwinStarTradePlan(
      base({
        satHoldings: Array.from({ length: 15 }, (_, i) => ({
          ts: `6000${String(i).padStart(2, '0')}.SH`,
          daysLeft: 2,
        })),
        liveStockHoldings: [
          { symbol: 'CN:300413', name: '芒果超媒', positionPct: 12.5 },
          { symbol: 'CN:603318', name: '水发燃气', positionPct: 12.5 },
          { symbol: 'CN:600540', name: '新赛股份', positionPct: 12.5 },
          { symbol: 'CN:301012', name: '扬电科技', positionPct: 12.5 },
        ],
      }),
    );
    expect(plan.recipeSatHeld).toBe(15);
    expect(plan.satHeld).toBe(4);
    expect(plan.bookNote).not.toMatch(/15\/4/);
    expect(plan.bookNote).toMatch(/你卫星仓 4\/4/);
    expect(plan.bookNote).toMatch(/引擎模拟 15 只为对照/);
  });

  it('counts body=3 from the live entry date and prints a -5% broker stop', () => {
    expect(satBodyProgress('2026-09-02', '2026-09-02')).toEqual({
      heldDays: 1,
      daysLeft: 2,
      exitDue: '2026-09-04',
      due: false,
      missingEntry: false,
    });
    expect(satProtectStop(20)).toBe(19);
    const plan = buildTwinStarTradePlan(
      base({
        asOfDate: '2026-09-02',
        satCandidates: [],
        liveStockHoldings: [
          {
            symbol: 'CN:300413',
            name: '芒果超媒',
            positionPct: 12.5,
            costPrice: 20,
            entryDate: '2026-09-02',
            lastClose: 21,
            pnlPct: 5,
          },
        ],
      }),
    );
    const row = plan.holds.find((r) => r.symbol === 'CN:300413');
    expect(row?.heldDays).toBe(1);
    expect(row?.daysLeft).toBe(2);
    expect(row?.exitDue).toBe('2026-09-04');
    expect(row?.protectStop).toBe(19);
    expect(row?.side).toBe('HOLD');
    expect(satConditionalLine(row!)).toBe('芒果超媒 300413.SZ 止损19 到期2026-09-04 持有');
  });

  it('sells a live satellite name on the 3rd weekday', () => {
    const plan = buildTwinStarTradePlan(
      base({
        asOfDate: '2026-09-02',
        satCandidates: [],
        liveStockHoldings: [
          {
            symbol: 'CN:300413',
            name: '芒果超媒',
            positionPct: 12.5,
            costPrice: 20,
            entryDate: '2026-08-31',
            lastClose: 21,
          },
        ],
      }),
    );
    expect(plan.sells.some((r) => r.symbol === 'CN:300413' && r.purpose === 'sat-exit')).toBe(true);
    expect(plan.satHeadline).toMatch(/卖 芒果超媒/);
  });

  it('flags a protective stop breach without waiting for body=3', () => {
    const plan = buildTwinStarTradePlan(
      base({
        asOfDate: '2026-09-02',
        satCandidates: [],
        liveStockHoldings: [
          {
            symbol: 'CN:300413',
            name: '芒果超媒',
            positionPct: 12.5,
            costPrice: 20,
            entryDate: '2026-09-02',
            lastClose: 18.9,
          },
        ],
      }),
    );
    const row = plan.sells.find((r) => r.symbol === 'CN:300413');
    expect(row?.stopBreached).toBe(true);
    expect(row?.reason).toMatch(/保护止损已破 19/);
  });

  it('targets the pick ETF at coreTargetPct of NAV', () => {
    const plan = buildTwinStarTradePlan(base({ etfHoldings: [] }));
    const buy = plan.buys.find((r) => r.sleeve === 'core');
    expect(buy?.symbol).toBe('ETF:513350');
    expect(buy?.navPct).toBe(50);
    expect(buy?.kind).toBe('etf');
  });
});

describe('isLiveSatelliteStock / day flow', () => {
  it('treats every CN name as satellite when pick is not STOCK', () => {
    expect(isLiveSatelliteStock('CN:300413', { pickKey: 'OIL', satNameTs: new Set() })).toBe(true);
    expect(isLiveSatelliteStock('HK:00700', { pickKey: 'OIL', satNameTs: new Set() })).toBe(false);
  });

  it('on STOCK days only recipe/candidate ts codes are satellite', () => {
    const satNameTs = satNameTsFromAction({
      candidates: [{ ts: '000712.SZ' }],
      book: { holdings: [{ ts: '300413.SZ' }] },
    });
    expect(satNameTs.has('000712.SZ')).toBe(true);
    expect(satNameTs.has('300413.SZ')).toBe(true);
    expect(isLiveSatelliteStock('CN:000712', { pickKey: 'STOCK', satNameTs })).toBe(true);
    expect(isLiveSatelliteStock('CN:300413', { pickKey: 'STOCK', satNameTs })).toBe(true);
    expect(isLiveSatelliteStock('CN:600111', { pickKey: 'STOCK', satNameTs })).toBe(false);
  });

  it('orders the Watchlist day script core-then-sat and hides names before 14:30', () => {
    const plan = buildTwinStarTradePlan(
      base({
        afterSatWindow: false,
        liveStockHoldings: [
          {
            symbol: 'CN:300413',
            name: '芒果超媒',
            positionPct: 12.5,
            costPrice: 20,
            entryDate: '2026-08-31',
            lastClose: 21,
          },
        ],
      }),
    );
    const waiting = twinStarDayFlow({
      plan,
      afterSatWindow: false,
      snapshotFailed: false,
      gateOpen: true,
    });
    expect(waiting.map((s) => s.id)).toEqual(['remind', 'names', 'core', 'sat-sell', 'sat-buy']);
    expect(waiting.find((s) => s.id === 'names')?.detail).toMatch(/候选未公布/);
    expect(waiting.find((s) => s.id === 'sat-buy')?.status).toBe('wait');
    expect(waiting.find((s) => s.id === 'core')?.title).toBe('先调核心');

    const blocked = twinStarDayFlow({
      plan,
      afterSatWindow: true,
      snapshotFailed: true,
      gateOpen: true,
    });
    expect(blocked.find((s) => s.id === 'names')?.status).toBe('blocked');
    expect(blocked.find((s) => s.id === 'sat-buy')?.detail).toMatch(/名单不可用/);
  });
});
