import { fireEvent, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it } from 'vitest';

import { LotSizeHint } from './LotSizeHint';

const CAPITAL_KEY = 'karios.accountCapital.v1';

describe('LotSizeHint', () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  it('asks for the account capital inline and then shows the integer order', () => {
    render(<LotSizeHint symbol="CN:600519" price={100} targetPct={10} />);
    expect(screen.getByText(/总资金（元）/)).toBeDefined();
    fireEvent.change(screen.getByPlaceholderText('500000'), { target: { value: '500000' } });
    fireEvent.click(screen.getByText('设好'));
    expect(screen.getByText(/≈ 500 股/)).toBeDefined();
    expect(screen.getByText(/沪深：100 股整数倍/)).toBeDefined();
  });

  it('renders the lot rule, rank tilt and HK conversion with capital set', () => {
    window.localStorage.setItem(CAPITAL_KEY, JSON.stringify(500000));
    render(<LotSizeHint symbol="HK:00700" price={320} targetPct={10} rank={1} />);
    expect(screen.getByText(/≈ 200 股/)).toBeDefined();
    expect(screen.getByText(/排名 #1 加成 ×1.2/)).toBeDefined();
    expect(screen.getByText(/HK\$320/)).toBeDefined();
  });

  it('warns when the target is below one lot', () => {
    window.localStorage.setItem(CAPITAL_KEY, JSON.stringify(500000));
    render(<LotSizeHint symbol="CN:600519" price={72.5} targetPct={0.5} />);
    expect(screen.getByText(/不足 1 手/)).toBeDefined();
  });
});
