import { describe, expect, it } from 'vitest';

import type { TimelineRow } from './queries/backtest';
import { buildTwinStarNavPoints, satActiveRuns, satOccupancyLine } from './twin-star-nav-series';

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
});
