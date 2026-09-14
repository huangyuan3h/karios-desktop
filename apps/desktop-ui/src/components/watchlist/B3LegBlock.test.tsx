import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { B3LegBlock } from './B3LegBlock';

const STATE = {
  ok: true,
  asOf: '2026-09-11',
  rebalanceDate: '2026-09-01',
  month: '2026-09',
  universe: [
    { symbol: '510300.SH', name: '沪深300', targetPct: 24.1, driftPct: 25.3, deltaPct: -1.2 },
    { symbol: '518880.SH', name: '黄金', targetPct: 22.0, driftPct: 22.0, deltaPct: 0.0 },
  ],
  trades: [{ symbol: '510300.SH', name: '沪深300', side: 'SELL', deltaPct: -1.2 }],
  note: 'B3 腿内部权重（占组合 50%）；月频再平衡、5bp/边；paper/实盘记账未接线（OPT-186）',
};

const stateRef: { value: unknown } = { value: STATE };

vi.mock('@/lib/queries/backtest', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/queries/backtest')>();
  return { ...actual, useB3StateQuery: () => ({ data: stateRef.value }) };
});

describe('B3LegBlock', () => {
  it('renders the monthly rebalance list and target weights', () => {
    stateRef.value = STATE;
    render(<B3LegBlock />);
    expect(screen.getByText('B3 月再平衡')).toBeDefined();
    expect(screen.getByText(/本月基准 2026-09-01/)).toBeDefined();
    expect(screen.getByText(/卖 沪深300 1\.2%/)).toBeDefined();
    expect(screen.getByText(/沪深300 24\.1%/)).toBeDefined();
    expect(screen.getByText(/OPT-186/)).toBeDefined();
  });

  it('renders nothing while loading and fails open on error', () => {
    stateRef.value = undefined;
    const { container } = render(<B3LegBlock />);
    expect(container.firstChild).toBeNull();

    stateRef.value = { ok: false, error: 'panel missing' };
    render(<B3LegBlock />);
    expect(screen.getByText(/B3 腿：panel missing/)).toBeDefined();
  });
});
