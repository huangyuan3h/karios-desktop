import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { RegimeAutoCard } from './RegimeAutoCard';

const BULL = {
  ok: true,
  asOf: '2026-11-13',
  confirmedAt: '2026-10-30',
  close: 4100.1,
  ma200: 3900.2,
  distancePct: 5.12,
  state: 'bull' as const,
  stateLabel: '牛市 / risk-on',
  target: 'starship_robust' as const,
  targetLabel: '稳健星舰 H2-a25',
  sgapExposure: 'high' as const,
  nextReview: '2026-11-30',
  rule: '月末确认：000300 > MA200 → 稳健星舰 H2-a25；否则 → 星港 0.2',
  note: '自动跟随为只读标签；手动策略选择在 Watchlist 页已关闭。',
};

describe('RegimeAutoCard', () => {
  it('shows the auto target and explicitly disables manual selection', () => {
    render(<RegimeAutoCard data={BULL} />);
    const card = screen.getByTestId('regime-auto-card');
    expect(card.getAttribute('data-target')).toBe('starship_robust');
    expect(screen.getByText('手动策略选择已关闭')).toBeDefined();
    expect(screen.getByText('稳健星舰 H2-a25')).toBeDefined();
    expect(screen.getByText('牛市 / risk-on')).toBeDefined();
    expect(screen.getByText(/S-gap 暴露：高/)).toBeDefined();
    expect(screen.getByText(/2026-10-30/)).toBeDefined();
    expect(screen.getByText(/2026-11-30/)).toBeDefined();
  });

  it('renders the fail-open state', () => {
    render(<RegimeAutoCard data={{ ok: false, error: 'no data' }} />);
    expect(screen.getByText(/regime 读取失败/)).toBeDefined();
  });
});
