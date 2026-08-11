import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { BacktestReconCard } from './BacktestReconCard';

const { apiGetJson } = vi.hoisted(() => ({ apiGetJson: vi.fn() }));
vi.mock('@/lib/api/client', () => ({ apiGetJson }));

function renderCard() {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={qc}>
      <BacktestReconCard />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  apiGetJson.mockReset();
});

describe('BacktestReconCard', () => {
  it('renders empty state when no snapshot exists', async () => {
    apiGetJson.mockResolvedValue({ ok: true, items: [] });
    renderCard();
    expect(await screen.findByText(/暂无对账快照/)).toBeTruthy();
  });

  it('renders clean snapshot with checkmark', async () => {
    apiGetJson.mockResolvedValue({
      ok: true,
      items: [
        {
          reconDate: '2026-08-07',
          market: 'CN',
          window: 'valid',
          expected: 0,
          actual: 0,
          aligned: 0,
          missing: 0,
          extra: 0,
        },
      ],
    });
    renderCard();
    expect(await screen.findByText('2026-08-07')).toBeTruthy();
    expect(screen.getByText('A股')).toBeTruthy();
    expect(screen.getByText('回测 0 · 实持 0 · 一致 0')).toBeTruthy();
  });

  it('flags drift rows with missing/extra', async () => {
    apiGetJson.mockResolvedValue({
      ok: true,
      items: [
        {
          reconDate: '2026-08-07',
          market: 'HK',
          window: 'valid',
          expected: 19,
          actual: 0,
          aligned: 0,
          missing: 19,
          extra: 0,
        },
      ],
    });
    renderCard();
    expect(await screen.findByText('港股')).toBeTruthy();
    expect(screen.getByText('回测 19 · 实持 0 · 一致 0')).toBeTruthy();
    expect(screen.getByText('缺 19 · 多 0')).toBeTruthy();
  });
});
