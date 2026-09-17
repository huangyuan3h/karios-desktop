import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { describe, expect, it, vi } from 'vitest';

import { H2ShadowFollowCard } from './H2ShadowFollowCard';

const { apiGetJson } = vi.hoisted(() => ({ apiGetJson: vi.fn() }));
vi.mock('@/lib/api/client', () => ({ apiGetJson }));

function renderCard() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <H2ShadowFollowCard />
    </QueryClientProvider>,
  );
}

const row = {
  date: '2026-09-16',
  navLive: 1.02,
  navH2: 1.01,
  peakLive: 1.02,
  peakH2: 1.015,
  dayLivePct: 2.0,
  dayH2Pct: 1.0,
  spreadPt: -1.0,
  ddLivePct: 0,
  ddH2Pct: 0.49,
  mddGapPt: 0.49,
  pickLive: 'OIL',
  pickH2: 'GOLD',
  actionH2: 'rotate',
  idlePct: 100,
  status: 'watch',
};

const report = {
  ok: true,
  inception: '2026-09-16',
  generatedAt: '2026-09-16T10:35:00+00:00',
  rows: [row],
  latest: row,
  appended: 1,
  thresholds: {
    spreadWatchPt: -1.0,
    spreadRollbackPt: -2.0,
    mddGapWatchPt: 0.5,
    mddGapRollbackPt: 1.0,
  },
  note: 'shadow',
};

describe('H2ShadowFollowCard', () => {
  it('shows spread, action and the daily follow checklist', async () => {
    apiGetJson.mockResolvedValueOnce(report);
    renderCard();
    expect(await screen.findByText(/港湾H2影子账本/)).toBeDefined();
    expect(await screen.findByText('观察')).toBeDefined();
    expect(await screen.findByText(/-1\.00pt/)).toBeDefined();
    expect(await screen.findByText(/换仓 · GOLD/)).toBeDefined();
    expect(await screen.findByText(/每日三步/)).toBeDefined();
    expect(await screen.findByText(/H2 永不下单/)).toBeDefined();
  });

  it('shows the not-generated state before the first 18:35 run', async () => {
    apiGetJson.mockRejectedValueOnce(new Error('Request failed with status 404'));
    renderCard();
    expect(await screen.findByText(/影子账本尚未生成/)).toBeDefined();
    expect(await screen.findByText(/18:35/)).toBeDefined();
  });
});
