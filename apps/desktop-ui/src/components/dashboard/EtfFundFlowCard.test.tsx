import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { EtfFundFlowCard } from './EtfFundFlowCard';

const LIVE_ITEM = {
  name: '沪深300ETF',
  symbol: 'SH:510300',
  live: true,
  netFlow1d: 1_230_000_000,
  superLargeNetInflow: 800_000_000,
  largeNetInflow: 400_000_000,
  netFlow3d: 3_000_000_000,
  tradeTime: '2026-08-07 14:30:00',
  source: 'eastmoney',
  flowStatus: 'Live',
  signal: 'Accumulate',
  signalDisplay: 'Accumulate',
};

const CLOSED_ITEM = {
  ...LIVE_ITEM,
  name: '恒生ETF',
  symbol: 'HK:02800',
  live: false,
  flowStatus: 'MarketClosed',
  signal: '—',
};

describe('EtfFundFlowCard', () => {
  it('hides title when showTitle=false', () => {
    render(<EtfFundFlowCard etfFundFlow={{}} showTitle={false} />);
    expect(screen.queryByText('ETF资金流 (持仓关注)')).not.toBeInTheDocument();
  });

  it('shows shareLag warning with intradaySafe=false suffix', () => {
    render(<EtfFundFlowCard etfFundFlow={{ shareLag: true, intradaySafe: false }} />);
    expect(screen.getByText(/东方财富实时资金流不完整/)).toBeInTheDocument();
    expect(screen.getByText(/盘中决策不可用/)).toBeInTheDocument();
  });

  it('shows shareLag warning without suffix when intradaySafe', () => {
    render(<EtfFundFlowCard etfFundFlow={{ shareLag: true }} />);
    expect(screen.getByText(/东方财富实时资金流不完整/)).toBeInTheDocument();
    expect(screen.queryByText(/盘中决策不可用/)).not.toBeInTheDocument();
  });

  it('renders a live row with formatted flows', () => {
    render(<EtfFundFlowCard etfFundFlow={{ items: [LIVE_ITEM] }} />);
    expect(screen.getByText('沪深300ETF')).toBeInTheDocument();
    expect(screen.getByText('SH:510300')).toBeInTheDocument();
    expect(screen.getByText('+12.30亿')).toBeInTheDocument();
    expect(screen.getByText('+8.00亿/+4.00亿')).toBeInTheDocument();
    expect(screen.getByText('Live')).toBeInTheDocument();
  });

  it('renders MarketClosed status as 已收盘', () => {
    render(<EtfFundFlowCard etfFundFlow={{ items: [CLOSED_ITEM] }} />);
    expect(screen.getByText('已收盘')).toBeInTheDocument();
    expect(screen.queryByText('MarketClosed')).not.toBeInTheDocument();
  });

  it('renders stale flow when no live data but a lagged value exists', () => {
    render(
      <EtfFundFlowCard
        etfFundFlow={{
          items: [{ ...LIVE_ITEM, live: false, flowStatus: 'Stale', netFlow1d: null, flowAsOfDate: '2026-08-06' }],
        }}
      />,
    );
    expect(screen.getByText('— (stale)')).toBeInTheDocument();
  });

  it('marks Data Lag signal rows as muted', () => {
    render(
      <EtfFundFlowCard
        etfFundFlow={{ items: [{ ...LIVE_ITEM, signal: 'Data Lag', signalDisplay: 'Data Lag' }] }}
      />,
    );
    expect(screen.getByText('Data Lag')).toBeInTheDocument();
  });

  it('shows empty-state message when no items', () => {
    render(<EtfFundFlowCard etfFundFlow={{}} />);
    expect(screen.getByText(/暂无ETF资金流数据/)).toBeInTheDocument();
  });
});
