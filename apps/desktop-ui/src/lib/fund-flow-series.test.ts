import { describe, expect, it } from 'vitest';

import type { FundFlowRow } from '@/lib/queries/backtest';
import { flowLatest, flowSpanWindow, flowTickDates, flowTotalDaily20, fmtSigned, gateOnRuns } from '@/lib/fund-flow-series';

function row(over: Partial<FundFlowRow>): FundFlowRow {
  return {
    date: '2026-01-01',
    etfShareYi: null,
    etfShareD20Pct: null,
    gateOn: null,
    marginTrillion: null,
    marginD20Pct: null,
    northDailyYi: null,
    northD20Yi: null,
    smNetPct: null,
    natDailyYi: null,
    natD20Yi: null,
    marginDailyYi: null,
    marginD20Yi: null,
    ...over,
  };
}

describe('fund-flow-series', () => {
  it('computes span windows', () => {
    const now = new Date('2026-09-10T04:00:00Z');
    expect(flowSpanWindow('6m', now)).toEqual({ start: '2026-03-11', end: '2026-09-10' });
    expect(flowSpanWindow('1y', now).start).toBe('2025-09-10');
    expect(flowSpanWindow('3y', now).start).toBe('2023-09-11');
  });

  it('finds gateOn runs for banding', () => {
    const rows = [
      row({ date: '2026-01-01' }),
      row({ date: '2026-01-02', gateOn: true }),
      row({ date: '2026-01-03', gateOn: true }),
      row({ date: '2026-01-04' }),
      row({ date: '2026-01-05', gateOn: true }),
    ];
    expect(gateOnRuns(rows)).toEqual([
      { start: 1, end: 3 },
      { start: 4, end: 5 },
    ]);
  });

  it('picks the latest row with a published level', () => {
    const rows = [
      row({ date: '2026-01-01' }),
      row({ date: '2026-01-02', etfShareYi: 441.9 }),
      row({ date: '2026-01-03' }),
      row({ date: '2026-01-04', etfShareYi: 488.1, marginTrillion: 2.6 }),
    ];
    expect(flowLatest(rows)?.etfShareYi).toBe(488.1);
    expect(flowLatest([row({})])).toBeUndefined();
  });

  it('spaces ticks evenly', () => {
    const rows = Array.from({ length: 10 }, (_, i) => row({ date: `2026-01-0${i + 1}` }));
    expect(flowTickDates(rows, 5)).toEqual(['2026-01-01', '2026-01-03', '2026-01-05', '2026-01-07', '2026-01-09']);
  });

  it('formats signed values', () => {
    expect(fmtSigned(1.234, 1, '%')).toBe('+1.2%');
    expect(fmtSigned(-0.5, 2, '%')).toBe('-0.50%');
    expect(fmtSigned(null)).toBe('—');
  });

  it('sums the 1:1:1 20-day trio (null-safe)', () => {
    expect(
      flowTotalDaily20(row({ natD20Yi: -931.5, marginD20Yi: 1709.1, northD20Yi: 781.9 })),
    ).toBe(1559.5);
    expect(flowTotalDaily20(row({ natD20Yi: 10, marginD20Yi: null, northD20Yi: 5 }))).toBe(15);
    expect(flowTotalDaily20(row({}))).toBeNull();
  });
});
