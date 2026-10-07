import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { MultiAssetHealthBlock } from './MultiAssetHealthBlock';

const SLEEVE = {
  action: 'ROTATE',
  idlePct: 26.1,
  parkPct: 100,
  pick: { key: 'OIL', symbol: 'ETF:513350' },
} as never;

const HOLDINGS = [
  { symbol: 'ETF:511260', key: 'BOND10', positionPct: 69.3 },
  { symbol: 'ETF:513100', key: 'NASDAQ', positionPct: 3.6 },
] as never;

describe('MultiAssetHealthBlock', () => {
  it('harbor view tags non-pick parking ETFs 卖出', () => {
    render(<MultiAssetHealthBlock holdings={HOLDINGS} sleeve={SLEEVE} />);
    expect(screen.getAllByText('卖出')).toHaveLength(2);
    expect(screen.queryByText('B3 腿')).toBeNull();
  });

  it('M30 view tags B3 ETFs as the B3 leg, never 卖出', () => {
    render(<MultiAssetHealthBlock holdings={HOLDINGS} sleeve={SLEEVE} m30 />);
    expect(screen.queryByText('卖出')).toBeNull();
    expect(screen.getAllByText('B3 腿')).toHaveLength(2);
    expect(screen.getByText(/母港M30：港湾腿（70%）的停车部分/)).toBeDefined();
  });
});
