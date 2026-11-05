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
    // 份数/金额只在操作清单里算：这里只给入口，不再引用已下线的跟仓卡，
    // 也不再断言“B3 腿 ETF 不卖”（B3 腿按月频目标会产生买卖单）。
    expect(screen.getByText(/份数\/金额见上方操作清单/)).toBeDefined();
    expect(screen.queryByText(/母港M30跟仓/)).toBeNull();
    expect(screen.queryByText(/B3 腿 ETF 不卖/)).toBeNull();
  });

  it('a pending trail exit shows Must-act 待卖 instead of 持有/加仓', () => {
    const pendingSleeve = {
      action: 'SELL_TO_REPO',
      label: '卖出转repo(止损)',
      message: 'ETF:513350峰值回撤8.5% ≥8% → 次日开盘卖转逆回购（信号日 2026-11-03）',
      symbol: 'ETF:513350',
      pendingOpenSell: true,
      signalDate: '2026-11-03',
      executeHint: '待开盘卖出转逆回购（信号已触发，未执行）',
      idlePct: 0,
      parkPct: 63.5,
      pick: { key: 'OIL', symbol: 'ETF:513350' },
    } as never;
    const holdings = [
      {
        symbol: 'ETF:513350',
        key: 'OIL',
        positionPct: 63.5,
        marketData: { close: 1.28, ma200: 1.5, above: false },
      },
    ] as never;
    render(<MultiAssetHealthBlock holdings={holdings} sleeve={pendingSleeve} m30 />);
    expect(screen.getByTestId('parking-must-act').textContent).toMatch(/卖出 513350 → 逆回购/);
    expect(screen.getByText('待卖 · 止损')).toBeDefined();
    // The leg holds only one row: no 加仓/持有 on the exiting ETF.
    expect(screen.queryByText('加仓')).toBeNull();
    expect(screen.queryByText('持有')).toBeNull();
    expect(screen.getByText('待卖')).toBeDefined();
    // No "park today's idle cash" banner while an exit is pending.
    expect(screen.queryByText(/母港M30：港湾腿（70%）的停车部分/)).toBeNull();
  });

  it('a 14:30 现价触线 shows the conditional Must-watch, not 持有', () => {
    const warnSleeve = {
      action: 'HOLD',
      label: '持有',
      message: '港湾停车：持有 ETF:513350（mom60 4.98%）',
      idlePct: 0,
      parkPct: 70,
      pick: { key: 'OIL', symbol: 'ETF:513350' },
      intradayWarn: {
        active: true,
        status: 'breached',
        symbol: 'ETF:513350',
        px: 1.281,
        thresholdPx: 1.289,
        peak: 1.401,
        drawdownPct: 8.6,
        message: 'ETF:513350现价1.281 ≤ 门槛价1.289（峰值1.401 回撤8.6%）→ 现价已达卖出条件',
        executeHint: '若收盘仍低于门槛价 → 明日开盘卖出转逆回购（不盘中卖）',
      },
    } as never;
    const holdings = [
      {
        symbol: 'ETF:513350',
        key: 'OIL',
        positionPct: 40,
        marketData: { close: 1.281, ma200: 1.5, above: true },
      },
    ] as never;
    render(<MultiAssetHealthBlock holdings={holdings} sleeve={warnSleeve} m30 />);
    const warn = screen.getByTestId('parking-intraday-warn');
    expect(warn.textContent).toMatch(/现价已达卖出条件/);
    expect(warn.textContent).toMatch(/门槛价 1.289/);
    expect(warn.textContent).toMatch(/明日开盘卖出转逆回购/);
    expect(screen.getByText('已触线')).toBeDefined();
    expect(screen.queryByText('加仓')).toBeNull();
    expect(screen.queryByText('持有')).toBeNull();
    // No "park the idle cash into OIL" banner while the print is at/under.
    expect(screen.queryByText(/母港M30：港湾腿（70%）的停车部分/)).toBeNull();
  });
});
