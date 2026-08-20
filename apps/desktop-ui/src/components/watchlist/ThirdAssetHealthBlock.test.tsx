import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { ThirdAssetHealthBlock } from './ThirdAssetHealthBlock';

describe('ThirdAssetHealthBlock', () => {
  it('renders nothing when no sleeve ETF is held', () => {
    const { container } = render(<ThirdAssetHealthBlock holding={null} />);
    expect(container.firstChild).toBeNull();
  });

  it('renders the held ETF with HOLD status above the 200d MA', () => {
    render(
      <ThirdAssetHealthBlock
        holding={{
          active: true,
          symbol: 'ETF:513110',
          name: '华泰柏瑞纳斯达克100ETF(QDII)',
          price: 2.459,
          ma200: 2.232,
          aboveMa200: true,
          positionPct: 23.61,
          pnlPct: 0.37,
          action: 'HOLD',
          label: '持有（站上200日线）',
          message: '站上200日线 → 持有，破线或 A 股有买点时卖出',
          asOfDate: '2026-08-19',
        }}
      />,
    );
    expect(screen.getByText(/华泰柏瑞纳斯达克100ETF/)).toBeInTheDocument();
    expect(screen.getByText(/持有（站上200日线）/)).toBeInTheDocument();
    expect(screen.getByText(/站上 200 日线/)).toBeInTheDocument();
    expect(screen.getByText(/现价 2.459/)).toBeInTheDocument();
    expect(screen.getByText(/MA200 2.232/)).toBeInTheDocument();
    expect(screen.getByText(/盈亏 \+0.37%/)).toBeInTheDocument();
  });

  it('renders the sell status when the ETF breaks the 200d MA', () => {
    render(
      <ThirdAssetHealthBlock
        holding={{
          active: true,
          symbol: 'ETF:513110',
          name: '华泰柏瑞纳斯达克100ETF(QDII)',
          price: 2.1,
          ma200: 2.4,
          aboveMa200: false,
          action: 'SELL_TO_REPO',
          label: '卖出 513100 · 转逆回购',
          message: '跌破200日线 → 卖出转逆回购',
        }}
      />,
    );
    expect(screen.getByText(/跌破 200 日线/)).toBeInTheDocument();
    expect(screen.getByText(/卖出 513100/)).toBeInTheDocument();
    expect(screen.getByText(/跌破200日线/)).toBeInTheDocument();
  });
});