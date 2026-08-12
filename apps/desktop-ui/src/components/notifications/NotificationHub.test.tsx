import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { NotificationHub } from './NotificationHub';

const { apiGetJson } = vi.hoisted(() => ({ apiGetJson: vi.fn() }));
vi.mock('@/lib/api/client', () => ({ apiGetJson }));

function renderHub() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <NotificationHub />
    </QueryClientProvider>,
  );
}

const ITEMS = [
  {
    id: 'near:CN:ETF:513180:stop',
    type: 'near_line',
    severity: 'high',
    title: '接近止损线 · 华夏恒生科技ETF(QDII)',
    detail: 'ETF:513180 距止损线 1.07pt（现 -0.49% / 线 0.582）',
    anchor: 'holdings',
    createdAt: '2026-08-12T08:00:00Z',
  },
  {
    id: 'recon:2026-08-07:HK',
    type: 'recon_missing',
    severity: 'medium',
    title: '回测口径 · 港股缺 19 只持仓',
    detail: '回测应持 19 · 实持 0',
    anchor: 'recon',
    createdAt: '2026-08-12T08:00:00Z',
  },
];

beforeEach(() => {
  localStorage.clear();
  apiGetJson.mockReset();
  apiGetJson.mockResolvedValue({ ok: true, items: ITEMS });
});

describe('NotificationHub', () => {
  it('shows a toast for new high-severity items and jumps to watchlist on click', async () => {
    renderHub();
    expect(await screen.findByText(/接近止损线/)).toBeDefined();
    expect(screen.getByText(/点击查看/)).toBeDefined();

    fireEvent.click(screen.getByText(/接近止损线/));
    expect(window.location.hash).toBe('#/watchlist');
    await waitFor(() => expect(screen.queryByText(/点击查看/)).toBeNull());
  });

  it('bell badge counts unread and panel lists items; opening marks all read', async () => {
    renderHub();
    await waitFor(() => expect(screen.getByText('2')).toBeDefined());
    expect(screen.getByText('2')).toBeDefined();

    fireEvent.click(screen.getByTitle(/提醒/));
    expect(await screen.findByText(/回测口径 · 港股缺 19 只持仓/)).toBeDefined();
    expect(screen.getByText(/点击跳 watchlist/)).toBeDefined();
    expect(screen.getByText(/→ watchlist · 回测缺票/)).toBeDefined();

    fireEvent.click(screen.getByTitle(/提醒/));
    await waitFor(() => expect(screen.queryByText('2')).toBeNull());
    const seen = JSON.parse(localStorage.getItem('karios_notifications_seen') ?? '[]');
    expect(seen).toContain('recon:2026-08-07:HK');
  });

  it('merges local buy reminders into the panel', async () => {
    localStorage.setItem(
      'karios_buy_reminders',
      JSON.stringify([
        { symbol: 'HK:02099', name: '中国黄金国际', targetPrice: 88.5, note: '等回踩', createdAt: '2026-08-12T00:00:00Z' },
      ]),
    );
    renderHub();
    fireEvent.click(await screen.findByTitle(/提醒/));
    expect(await screen.findByText(/买入提醒 · 中国黄金国际/)).toBeDefined();
    expect(screen.getByText(/HK:02099 · 目标价 88.5 · 等回踩/)).toBeDefined();
  });
});
