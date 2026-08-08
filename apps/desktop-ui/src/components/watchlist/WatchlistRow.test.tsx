import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { buildWatchlistRowMetrics } from '@/lib/watchlist-metrics';

import { WatchlistRow, type WatchlistRowProps } from './WatchlistRow';

const TREND = {
  symbol: 'CN:600519',
  asOfDate: '2026-08-07',
  trendOk: true,
  score: 92,
  scoreParts: {},
  stopLossPrice: 1450,
  buyMode: 'A_pullback',
  buyAction: 'buy',
  buyZoneLow: 10,
  buyZoneHigh: 12,
  buyWhy: 'pullback',
  buyChecks: {},
  checks: { rs_leader: true },
  marketRegime: 'Strong',
  rs: 8.5,
  intradayChgPct: 2.1,
  gapUp: false,
  riskAlerts: [],
  instFlow: null,
  missingData: [],
  values: { close: 1500, rsValue: 8.5 },
};

const ITEM = {
  symbol: 'CN:600519',
  name: '贵州茅台',
  addedAt: '2026-08-01T08:00:00+08:00',
  source: 'manual',
  color: '#ff0000',
  positionPct: 10,
  costPrice: 1500.5,
  maxPrice: null,
  entryDate: '2026-08-01',
};

const baseProps = (over: Partial<WatchlistRowProps> = {}): WatchlistRowProps => {
  const trend = TREND as never;
  const metrics = buildWatchlistRowMetrics({
    symbol: ITEM.symbol,
    trend,
    quote: undefined,
    tradingTime: false,
    todaySh: '',
  });
  return {
    item: ITEM as never,
    trend,
    quote: undefined,
    rowMetrics: metrics,
    tradingTime: false,
    todaySh: '',
    costPriceDraft: undefined,
    positionPctDraft: undefined,
    executionGate: null,
    mainlineAllow: null,
    sectorExposureByIndustry: null,
    sleeveExposurePct: 0,
    showTooltip: vi.fn(),
    hideTooltip: vi.fn(),
    showColorPicker: vi.fn(),
    setItemPositionPct: vi.fn(),
    setItemPositionPctDraft: vi.fn(),
    commitItemPositionPctDraft: vi.fn(),
    setItemCostPriceDraft: vi.fn(),
    setItemCostPriceValue: vi.fn(),
    commitItemCostPriceDraft: vi.fn(),
    onRemove: vi.fn(),
    onAddReference: vi.fn(),
    ...over,
  };
};

describe('WatchlistRow', () => {
  it('renders symbol, name and color flag', () => {
    render(<table><tbody><WatchlistRow {...baseProps()} /></tbody></table>);
    expect(screen.getByText('CN:600519')).toBeInTheDocument();
    expect(screen.getByText('贵州茅台')).toBeInTheDocument();
    const flag = screen.getByLabelText('Set color flag');
    expect(flag.firstElementChild).toHaveStyle({ backgroundColor: '#ff0000' });
  });

  it('opens stock on symbol click', () => {
    const onOpenStock = vi.fn();
    render(<table><tbody><WatchlistRow {...baseProps({ onOpenStock })} /></tbody></table>);
    fireEvent.click(screen.getByLabelText('Open CN:600519'));
    expect(onOpenStock).toHaveBeenCalledWith('CN:600519');
  });

  it('shows color picker on flag click', () => {
    const showColorPicker = vi.fn();
    render(<table><tbody><WatchlistRow {...baseProps({ showColorPicker })} /></tbody></table>);
    fireEvent.click(screen.getByLabelText('Set color flag'));
    expect(showColorPicker).toHaveBeenCalledWith(expect.anything(), 'CN:600519');
  });

  it('edits position pct and commits on blur', () => {
    const setItemPositionPctDraft = vi.fn();
    const commitItemPositionPctDraft = vi.fn();
    render(
      <table><tbody><WatchlistRow
        {...baseProps({ setItemPositionPctDraft, commitItemPositionPctDraft })}
      /></tbody></table>,
    );
    const pos = screen.getByDisplayValue('10');
    fireEvent.change(pos, { target: { value: '12.5' } });
    expect(setItemPositionPctDraft).toHaveBeenCalledWith('CN:600519', '12.5');
    fireEvent.blur(pos);
    expect(commitItemPositionPctDraft).toHaveBeenCalledWith('CN:600519');
  });

  it('rejects invalid position pct input', () => {
    const setItemPositionPctDraft = vi.fn();
    render(
      <table><tbody><WatchlistRow
        {...baseProps({ setItemPositionPctDraft })}
      /></tbody></table>,
    );
    const pos = screen.getByDisplayValue('10');
    fireEvent.change(pos, { target: { value: 'abc' } });
    expect(setItemPositionPctDraft).not.toHaveBeenCalled();
  });

  it('edits cost price and updates value on change', () => {
    const setItemCostPriceDraft = vi.fn();
    const setItemCostPriceValue = vi.fn();
    render(
      <table><tbody><WatchlistRow
        {...baseProps({ setItemCostPriceDraft, setItemCostPriceValue })}
      /></tbody></table>,
    );
    const cost = screen.getByDisplayValue('1500.500');
    fireEvent.change(cost, { target: { value: '1490' } });
    expect(setItemCostPriceDraft).toHaveBeenCalledWith('CN:600519', '1490');
    expect(setItemCostPriceValue).toHaveBeenCalledWith('CN:600519', 1490);
  });

  it('clears cost price value when emptied', () => {
    const setItemCostPriceValue = vi.fn();
    render(
      <table><tbody><WatchlistRow {...baseProps({ setItemCostPriceValue })} /></tbody></table>,
    );
    const cost = screen.getByDisplayValue('1500.500');
    fireEvent.change(cost, { target: { value: '' } });
    expect(setItemCostPriceValue).toHaveBeenCalledWith('CN:600519', null);
  });

  it('shows trade buttons: 加仓/卖出 for held positions, 买入 otherwise', () => {
    const onOpenTradeDialog = vi.fn();
    render(
      <table><tbody><WatchlistRow {...baseProps({ onOpenTradeDialog })} /></tbody></table>,
    );
    expect(screen.getByText('加仓')).toBeInTheDocument();
    expect(screen.getByText('卖出')).toBeInTheDocument();
    fireEvent.click(screen.getByText('卖出'));
    expect(onOpenTradeDialog).toHaveBeenCalledWith('sell', expect.objectContaining({ symbol: 'CN:600519' }));
  });

  it('shows 买入 for non-held positions', () => {
    const onOpenTradeDialog = vi.fn();
    render(
      <table><tbody><WatchlistRow
        {...baseProps({ onOpenTradeDialog, item: { ...ITEM, positionPct: null, entryDate: null, costPrice: null } as never })}
      /></tbody></table>,
    );
    expect(screen.getByText('买入')).toBeInTheDocument();
    fireEvent.click(screen.getByText('买入'));
    expect(onOpenTradeDialog).toHaveBeenCalledWith('buy', expect.objectContaining({ symbol: 'CN:600519' }));
  });

  it('fires reference and remove actions', () => {
    const onAddReference = vi.fn();
    const onRemove = vi.fn();
    render(
      <table><tbody><WatchlistRow {...baseProps({ onAddReference, onRemove })} /></tbody></table>,
    );
    fireEvent.click(screen.getByLabelText('Reference to chat'));
    expect(onAddReference).toHaveBeenCalled();
    fireEvent.click(screen.getByLabelText('Remove'));
    expect(onRemove).toHaveBeenCalledWith('CN:600519');
  });

  it('renders green tone class for strong buy setup', () => {
    render(<table><tbody><WatchlistRow {...baseProps()} /></tbody></table>);
    expect(screen.getByText('CN:600519').closest('tr')).toHaveClass('bg-emerald-50/60');
  });

  it('renders red tone when avoid action or blocking alerts', () => {
    const avoid = { ...TREND, buyAction: 'avoid' };
    render(
      <table><tbody><WatchlistRow
        {...baseProps({ trend: avoid as never })}
      /></tbody></table>,
    );
    expect(screen.getByText('CN:600519').closest('tr')).toHaveClass('bg-red-50/60');
  });
});
