import { describe, expect, it } from 'vitest';

import type { TimelineRow } from './queries/backtest';
import {
  buildTwinStarNavPoints,
  circuitRuns,
  extremeSentimentRuns,
  hasFlowLayer,
  satActiveRuns,
  satOccupancyLine,
} from './twin-star-nav-series';

function row(over: Partial<TimelineRow>): TimelineRow {
  return {
    date: '2026-08-01',
    deployedPct: 100,
    idlePct: 0,
    positions: 0,
    cnPositions: 0,
    hkPositions: 0,
    stockMarket: '',
    stockSymbols: [],
    stockMom: null,
    pick: 'GOLD',
    pickTs: '518880.SH',
    navBase: 1,
    navSleeve: null,
    navSingle: 1.1,
    navMulti: 1.1,
    navBaseReturnPct: 0,
    navSingleReturnPct: 10,
    navMultiReturnPct: 10,
    coreNav: 1.08,
    coreNavReturnPct: 8,
    satNav: 1.02,
    satNavReturnPct: 2,
    satPositions: 1,
    satSlots: 1,
    satActive: true,
    exits: [],
    ...over,
  };
}

describe('twin-star-nav-series', () => {
  it('keeps twin / core / sat as separate series', () => {
    const pts = buildTwinStarNavPoints([
      row({ date: '2026-08-01' }),
      row({
        date: '2026-08-04',
        navSingleReturnPct: 12,
        coreNavReturnPct: 9,
        satNavReturnPct: 1,
        satActive: false,
        satSlots: 0,
      }),
    ]);
    expect(pts[0]).toMatchObject({ twinPct: 10, corePct: 8, satPct: 2, satActive: true });
    expect(pts[1].satActive).toBe(false);
    expect(satActiveRuns(pts)).toEqual([{ start: 0, end: 1 }]);
    expect(satOccupancyLine(pts)).toBe('开闸占用 1/2 日 · 均 1.0 槽');
  });

  it('carries OPT-152 sim curve when present', () => {
    const pts = buildTwinStarNavPoints([
      row({ navSimMultiReturnPct: 4.5, navSimReturnPct: 3.2 }),
      row({ navSimMultiReturnPct: 100.8, navSimReturnPct: 102.3 }),
    ]);
    expect(pts[0]).toMatchObject({ twinSimPct: 4.5, coreSimPct: 3.2 });
    expect(pts[1]).toMatchObject({ twinSimPct: 100.8, coreSimPct: 102.3, twinPct: 10 });
  });

  it('falls back to null sim series without sim fields', () => {
    const pts = buildTwinStarNavPoints([row({})]);
    expect(pts[0].twinSimPct).toBeNull();
    expect(pts[0].coreSimPct).toBeNull();
  });

  it('flags circuit + extreme-sentiment runs', () => {
    const pts = buildTwinStarNavPoints([
      row({ date: '2026-08-03', cnCircuit: true }),
      row({ date: '2026-08-04', cnCircuit: true, sentiment: 'extreme_caution' }),
      row({ date: '2026-08-05', sentiment: 'extreme_caution' }),
      row({ date: '2026-08-06' }),
    ]);
    expect(circuitRuns(pts)).toEqual([{ start: 0, end: 2 }]);
    expect(extremeSentimentRuns(pts)).toEqual([{ start: 1, end: 3 }]);
  });

  it('defaults circuit/sentiment off when absent', () => {
    const pts = buildTwinStarNavPoints([row({})]);
    expect(pts[0].cnCircuit).toBe(false);
    expect(pts[0].hkCircuit).toBe(false);
    expect(pts[0].sentiment).toBeNull();
    expect(circuitRuns(pts)).toEqual([]);
    expect(extremeSentimentRuns(pts)).toEqual([]);
  });

  it('carries TIP-017 flow layer and reports availability', () => {
    const flow = { etfShareD20Pct: 1.2, marginD20Pct: -0.5, northD20: 320.5, smNetPct: 2.1 };
    const pts = buildTwinStarNavPoints([
      row({ date: '2026-08-01', flow }),
      row({ date: '2026-08-04' }),
    ]);
    expect(pts[0].flow).toEqual(flow);
    expect(pts[1].flow).toBeNull();
    expect(hasFlowLayer(pts)).toBe(true);
    expect(hasFlowLayer(buildTwinStarNavPoints([row({})]))).toBe(false);
  });
});
