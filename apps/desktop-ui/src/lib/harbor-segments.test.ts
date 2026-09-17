import { describe, expect, it } from 'vitest';

import type { TimelineRow } from './queries/backtest';
import {
  buildHarborSegments,
  buildParkingStrip,
  buildRiskStrip,
  estimateLabelPx,
  fitSegmentLabel,
  harborHoldLine,
  hasBaseLeg,
  hasParkedLeg,
  hasRiskLeg,
  isSatelliteRow,
} from './harbor-segments';

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

const ETF: Partial<TimelineRow> = { positions: 0, stockSymbols: [], stockMarket: '' };

describe('harbor-segments', () => {
  it('merges consecutive same-pick days into one block', () => {
    const segs = buildHarborSegments([
      row({ date: '2026-08-03', ...ETF, pick: 'GOLD', navSingleReturnPct: 10 }),
      row({ date: '2026-08-04', ...ETF, pick: 'GOLD', navSingleReturnPct: 11 }),
      row({ date: '2026-08-05', ...ETF, pick: 'GOLD', navSingleReturnPct: 12 }),
      row({ date: '2026-08-06', ...ETF, pick: 'REPO', pickTs: null, navSingleReturnPct: 12 }),
    ]);
    expect(segs).toHaveLength(2);
    expect(segs[0]).toMatchObject({
      pick: 'GOLD',
      mode: 'PARK',
      ident: 'PARK:GOLD:518880',
      start: '2026-08-03',
      end: '2026-08-05',
      days: 3,
      code: '518880',
    });
    expect(segs[0].labels.short).toBe('黄金 518880');
    expect(segs[0].labels.medium).toBe('518880');
    expect(segs[0].labels.full).toBe('黄金 518880 · 3天');
    expect(segs[1]).toMatchObject({ pick: 'REPO', start: '2026-08-06', days: 1, code: 'GC001' });
    expect(segs[1].retPct).toBe(0);
  });

  it('labels STOCK phases with basket size and top holdings in the tooltip', () => {
    const segs = buildHarborSegments([
      row({
        date: '2026-08-03',
        pick: 'STOCK',
        pickTs: null,
        positions: 10,
        stockMarket: 'A股',
        stockSymbols: ['600519', '000858'],
        navSingleReturnPct: 10,
      }),
      row({
        date: '2026-08-04',
        pick: 'STOCK',
        pickTs: null,
        positions: 10,
        stockMarket: 'A股',
        stockSymbols: ['600519', '300750'],
        exits: ['000858'],
        navSingleReturnPct: 13,
      }),
    ]);
    expect(segs).toHaveLength(1);
    expect(segs[0].mode).toBe('STOCK');
    expect(segs[0].labels.full).toBe('股票 10票 · 2天');
    expect(segs[0].labels.tiny).toBe('股');
    expect(segs[0].stockSymbols).toEqual(['600519', '300750']);
    expect(segs[0].exits).toEqual(['000858']);
    expect(segs[0].title).toContain('持有(600519×2 000858×1 300750×1)');
    expect(segs[0].title).toContain('卖出 000858');
  });

  it('marks partial stock phases as MIXED with the parking leg in the label', () => {
    const segs = buildHarborSegments([
      row({
        date: '2026-08-03',
        pick: 'GOLD',
        pickTs: '518880.SH',
        positions: 5,
        idlePct: 62.5,
        deployedPct: 37.5,
        stockMarket: 'A股',
        stockSymbols: ['600519'],
        navSingleReturnPct: 10,
      }),
      row({
        date: '2026-08-04',
        pick: 'GOLD',
        pickTs: '518880.SH',
        positions: 5,
        idlePct: 62.5,
        deployedPct: 37.5,
        stockMarket: 'A股',
        stockSymbols: ['600519'],
        navSingleReturnPct: 11,
      }),
    ]);
    expect(segs).toHaveLength(1);
    expect(segs[0].mode).toBe('MIXED');
    expect(segs[0].ident).toBe('MIXED:GOLD:518880');
    expect(segs[0].labels.full).toBe('股票 5票+黄金 518880 · 2天');
    expect(segs[0].labels.short).toBe('股票 5票+黄金');
    expect(segs[0].labels.medium).toBe('股5+金');
    expect(segs[0].title).toContain('闲置62.5%停 黄金 518880');
  });

  it('uses the actual NASDAQ alias from pickTs', () => {
    const segs = buildHarborSegments([
      row({ date: '2026-08-03', ...ETF, pick: 'NASDAQ', pickTs: '513110.SH' }),
    ]);
    expect(segs[0].code).toBe('513110');
    expect(segs[0].labels.short).toBe('纳指 513110');
  });

  it('splits a STOCK phase from the following ETF phase and keeps segment returns', () => {
    const segs = buildHarborSegments([
      row({ date: '2026-08-03', pick: 'STOCK', pickTs: null, positions: 8, navSingleReturnPct: 0 }),
      row({ date: '2026-08-04', pick: 'STOCK', pickTs: null, positions: 8, navSingleReturnPct: 5 }),
      row({ date: '2026-08-05', ...ETF, pick: 'OIL', pickTs: '513350.SH', navSingleReturnPct: 9 }),
    ]);
    expect(segs.map((s) => s.pick)).toEqual(['STOCK', 'OIL']);
    expect(segs[1].retPct).toBe(4);
  });

  it('builds the per-day hold line for stock, mixed and parking states', () => {
    expect(
      harborHoldLine(
        row({
          pick: 'NASDAQ',
          pickTs: '513100.SH',
          positions: 10,
          idlePct: 0,
          stockSymbols: ['600519', '000858'],
        }),
      ),
    ).toBe('股票 10票 600519 000858');
    expect(
      harborHoldLine(row({ pick: 'GOLD', pickTs: '518880.SH', positions: 5, idlePct: 62.5 })),
    ).toBe('股票 5票 +停车 黄金 518880');
    expect(harborHoldLine(row({ pick: 'GOLD', pickTs: '518880.SH' }))).toBe('黄金 518880');
  });

  it('renders satellite (S-GAP) rows as slot phases with gate state', () => {
    const sat = (over: Partial<TimelineRow>) =>
      row({
        pick: 'S-GAP',
        pickTs: '',
        positions: 0,
        idlePct: 0,
        navSingle: 1,
        navMulti: 1,
        navSingleReturnPct: 0,
        navMultiReturnPct: 0,
        satNav: 1,
        satNavReturnPct: 0,
        ...over,
      });
    const rows = [
      sat({
        date: '2026-08-20',
        satPositions: 0,
        satSlots: 0,
        idleSlots: 4,
        satActive: false,
        gateOpen: false,
      }),
      sat({
        date: '2026-08-21',
        satPositions: 1,
        satSlots: 1,
        idleSlots: 3,
        satActive: true,
        gateOpen: true,
        filledToday: 1,
      }),
      sat({
        date: '2026-08-24',
        satPositions: 1,
        satSlots: 1,
        idleSlots: 3,
        satActive: true,
        gateOpen: false,
        navSingleReturnPct: 0.4,
        satNavReturnPct: 0.4,
      }),
      sat({
        date: '2026-08-25',
        satPositions: 4,
        satSlots: 5,
        idleSlots: 0,
        satActive: true,
        gateOpen: true,
        filledToday: 4,
        navSingleReturnPct: 0.9,
        satNavReturnPct: 0.9,
      }),
    ];
    const segs = buildHarborSegments(rows);
    expect(segs.map((s) => s.ident)).toEqual(['SAT:0:x', 'SAT:1:g', 'SAT:1:x', 'SAT:4:g']);
    expect(segs[0].mode).toBe('SAT');
    expect(segs[0].labels.medium).toBe('空仓');
    expect(segs[0].title).toContain('卫星空仓（4槽空闲） · 闸关');
    expect(segs[1].sat).toMatchObject({
      positions: 1,
      capacity: 4,
      idleSlots: 3,
      gateOpen: true,
    });
    expect(segs[1].labels.short).toBe('卫星 1/4仓');
    expect(segs[1].labels.medium).toBe('1/4');
    expect(segs[1].labels.full).toBe('卫星 1/4仓（空3槽）· 1天 +0.0%');
    expect(segs[1].title).toContain('卫星 1/4仓 · 空3槽');
    expect(segs[2].labels.full).toBe('卫星 1/4仓（空3槽）· 1天 +0.4% · 闸关');
    expect(segs[3].title).toContain('卫星 4/4仓 · 空0槽');
    expect(segs[3].title).toContain('卫星 0.90%');
    expect(harborHoldLine(rows[1])).toBe('卫星 1/4仓 · 空3槽 · 成交1');
    expect(harborHoldLine(rows[0])).toBe('卫星 0/4仓 · 空4槽 · 闸关');
  });

  it('fits the longest label variant into the block width', () => {
    const labels = {
      full: '黄金 518880 · 23天 +9.3%',
      short: '黄金 518880',
      medium: '518880',
      tiny: '金',
    };
    expect(fitSegmentLabel(labels, estimateLabelPx(labels.full) + 1)).toBe(labels.full);
    expect(fitSegmentLabel(labels, estimateLabelPx(labels.short) + 1)).toBe(labels.short);
    expect(fitSegmentLabel(labels, estimateLabelPx(labels.medium) + 1)).toBe(labels.medium);
    expect(fitSegmentLabel(labels, estimateLabelPx(labels.tiny) + 1)).toBe(labels.tiny);
    expect(fitSegmentLabel(labels, 2)).toBe('');
  });

  it('detects base / satellite / parked / risk legs per row', () => {
    const base = row({ positions: 3, stockSymbols: ['a', 'b', 'c'] });
    expect(hasBaseLeg(base)).toBe(true);
    expect(isSatelliteRow(base)).toBe(false);
    const overlay = row({
      positions: 3,
      stockSymbols: ['a'],
      satPositions: 2,
      satSlots: 4,
      gateOpen: true,
    });
    expect(hasBaseLeg(overlay)).toBe(true);
    expect(isSatelliteRow(overlay)).toBe(true);
    const satOnly = row({ pick: 'S-GAP', satPositions: 1 });
    expect(hasBaseLeg(satOnly)).toBe(false);
    expect(isSatelliteRow(satOnly)).toBe(true);
    expect(hasParkedLeg(row({ parkedPick: 'GOLD' }))).toBe(true);
    expect(hasParkedLeg(row({}))).toBe(false);
    expect(hasRiskLeg(row({ riskTop: '510300.SH', riskTopW: 0.3 }))).toBe(true);
    expect(hasRiskLeg(row({}))).toBe(false);
  });

  it('builds the base strip for overlay rows with ignoreSat', () => {
    const rows = [
      row({ date: '2026-08-03', positions: 2, stockSymbols: ['a', 'b'], satPositions: 4 }),
      row({ date: '2026-08-04', positions: 2, stockSymbols: ['a', 'b'], satPositions: 4 }),
    ];
    // Default: satellite wins (the old overlay-blind behavior for the main bar).
    expect(buildHarborSegments(rows).map((s) => s.mode)).toEqual(['SAT']);
    // Base strip: the stock leg survives next to the satellite strip.
    const base = buildHarborSegments(rows, { ignoreSat: true });
    expect(base.map((s) => s.mode)).toEqual(['STOCK']);
    expect(base[0]).toMatchObject({ days: 2, positions: 2 });
    expect(base[0].title).toContain('股票 2票');
  });

  it('builds the parking strip keyed by parkedPick', () => {
    const rows = [
      row({ date: '2026-08-03', pick: 'S-GAP', satPositions: 2, parkedPick: 'GOLD', parkedTs: '518880.SH' }),
      row({ date: '2026-08-04', pick: 'S-GAP', satPositions: 2, parkedPick: 'GOLD', parkedTs: '518880.SH' }),
      row({ date: '2026-08-05', pick: 'S-GAP', satPositions: 0, parkedPick: 'REPO', parkedTs: 'GC001' }),
    ];
    const strip = buildParkingStrip(rows);
    expect(strip).toHaveLength(2);
    expect(strip[0]).toMatchObject({ mode: 'PARK', pick: 'GOLD', days: 2, code: '518880' });
    expect(strip[0].labels.short).toBe('黄金 518880');
    expect(strip[1]).toMatchObject({ mode: 'PARK', pick: 'REPO', days: 1 });
    expect(buildParkingStrip([row({})])).toEqual([]);
  });

  it('builds the B3 strip keyed by riskTop with universe names', () => {
    const universe = [
      { ts: '510300.SH', name: '沪深300' },
      { ts: '518880.SH', name: '黄金' },
    ];
    const rows = [
      row({ date: '2026-08-03', riskTop: '518880.SH', riskTopW: 0.3 }),
      row({ date: '2026-08-04', riskTop: '518880.SH', riskTopW: 0.31 }),
      row({ date: '2026-08-05', riskTop: '510300.SH', riskTopW: 0.28 }),
    ];
    const strip = buildRiskStrip(rows, universe);
    expect(strip).toHaveLength(2);
    expect(strip[0]).toMatchObject({ mode: 'PARK', pick: '518880.SH', days: 2 });
    expect(strip[0].labels.short).toBe('黄金 518880');
    expect(strip[1].labels.short).toBe('沪深300 510300');
  });

  it('names the parking ETF and appends the base leg on overlay hold lines', () => {
    const satParked = row({
      pick: 'S-GAP',
      satPositions: 4,
      idleSlots: 0,
      gateOpen: true,
      filledToday: 2,
      parkedPick: 'GOLD',
      parkedWeight: 1,
    });
    expect(harborHoldLine(satParked)).toBe('卫星 4/4仓 · 空0槽 · 成交2 · 停车 黄金 100%');
    const overlay = row({
      positions: 2,
      stockSymbols: ['a', 'b'],
      satPositions: 1,
      idleSlots: 3,
      gateOpen: true,
    });
    expect(harborHoldLine(overlay)).toContain('卫星 1/4仓 · 空3槽');
    expect(harborHoldLine(overlay)).toContain('股票 2票 a b');
    const risk = row({ riskTop: '510300.SH', riskTopW: 0.25 });
    expect(harborHoldLine(risk, { '510300.SH': '沪深300' })).toContain('B3 沪深300 25%');
  });
});
