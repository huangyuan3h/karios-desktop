import { describe, expect, it } from 'vitest';

import type { TimelineRow } from './queries/backtest';
import {
  buildHarborNavPoints,
  circuitRuns,
  extremeSentimentRuns,
  hasFlowLayer,
  hasSimCurve,
} from './harbor-nav-series';

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
    exits: [],
    ...over,
  };
}

describe('harbor-nav-series', () => {
  it('maps harbor + baseline series from the timeline rows', () => {
    const pts = buildHarborNavPoints([
      row({ date: '2026-08-01' }),
      row({ date: '2026-08-04', navSingleReturnPct: 12, navBaseReturnPct: 3 }),
    ]);
    expect(pts[0]).toMatchObject({ harborPct: 10, basePct: 0, hold: '黄金 518880' });
    expect(pts[1]).toMatchObject({ harborPct: 12, basePct: 3 });
  });

  it('carries OPT-152 sim curve when present', () => {
    const pts = buildHarborNavPoints([
      row({ navSimReturnPct: 3.2 }),
      row({ navSimReturnPct: 102.3 }),
    ]);
    expect(pts[0].harborSimPct).toBe(3.2);
    expect(pts[1].harborSimPct).toBe(102.3);
    expect(hasSimCurve(pts)).toBe(true);
  });

  it('falls back to null sim series without sim fields', () => {
    const pts = buildHarborNavPoints([row({})]);
    expect(pts[0].harborSimPct).toBeNull();
    expect(hasSimCurve(pts)).toBe(false);
  });

  it('flags circuit + extreme-sentiment runs', () => {
    const pts = buildHarborNavPoints([
      row({ date: '2026-08-03', cnCircuit: true }),
      row({ date: '2026-08-04', cnCircuit: true, sentiment: 'extreme_caution' }),
      row({ date: '2026-08-05', sentiment: 'extreme_caution' }),
      row({ date: '2026-08-06' }),
    ]);
    expect(circuitRuns(pts)).toEqual([{ start: 0, end: 2 }]);
    expect(extremeSentimentRuns(pts)).toEqual([{ start: 1, end: 3 }]);
  });

  it('defaults circuit/sentiment off when absent', () => {
    const pts = buildHarborNavPoints([row({})]);
    expect(pts[0].cnCircuit).toBe(false);
    expect(pts[0].hkCircuit).toBe(false);
    expect(pts[0].sentiment).toBeNull();
    expect(circuitRuns(pts)).toEqual([]);
    expect(extremeSentimentRuns(pts)).toEqual([]);
  });

  it('carries TIP-017 flow layer and reports availability', () => {
    const flow = { etfShareD20Pct: 1.2, marginD20Pct: -0.5, northD20: 320.5, smNetPct: 2.1 };
    const pts = buildHarborNavPoints([
      row({ date: '2026-08-01', flow }),
      row({ date: '2026-08-04' }),
    ]);
    expect(pts[0].flow).toEqual(flow);
    expect(pts[1].flow).toBeNull();
    expect(hasFlowLayer(pts)).toBe(true);
    expect(hasFlowLayer(buildHarborNavPoints([row({})]))).toBe(false);
  });
});
