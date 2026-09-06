import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import type { TwinStarTradePlan } from '@/lib/twin-star-trade-plan';
import { TWIN_STAR_RECIPE_VERSION } from '@karios/shared';

import { TwinStarTradePlanPanel } from './TwinStarTradePlanPanel';

const PLAN = {
  coreTargetPct: 50,
  satTargetPct: 50,
  satSlotNavPct: 12.5,
  satHeld: 0,
  recipeSatHeld: 0,
  satFreeSlots: 4,
  satHeldSymbols: [],
  coreBuyable: true,
  satHeadline: '卫星开闸',
  coreHeadline: '核心 GOLD',
  etfHeadline: '',
  bookNote: '',
  etfTotalPct: 50,
  etfSparePct: 50,
  stockBuyNavPct: 12.5,
  etfTrimPct: 0,
  recipeNames: [],
  buys: [{ kind: 'stock', sleeve: 'sat', symbol: 'CN:000001' }],
  holds: [],
  sells: [],
} as unknown as TwinStarTradePlan;

describe('TwinStarTradePlanPanel', () => {
  it('shows the frozen recipe badge', () => {
    render(<TwinStarTradePlanPanel plan={PLAN} />);
    expect(screen.getByText(TWIN_STAR_RECIPE_VERSION)).toBeDefined();
    expect(screen.getByText(/买股票 1 只/)).toBeDefined();
  });
});
