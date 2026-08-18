import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { WeeklyReviewCard } from './WeeklyReviewCard';

const { apiGetJson } = vi.hoisted(() => ({ apiGetJson: vi.fn() }));
vi.mock('@/lib/api/client', () => ({ apiGetJson }));

function renderCard() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <WeeklyReviewCard />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  apiGetJson.mockReset();
  apiGetJson.mockImplementation(async (path: string) => {
    if (String(path).includes('/weekly-review')) {
      return {
        ok: true,
        week: { start: '2026-08-03', end: '2026-08-07' },
        decisionVolume: { total: 3, bySource: { TV: 3 } },
        paper: { closed: 1, wins: 1, winRate: 1, avgNetPnlPct: 2.5, byReason: {} },
        exitAttribution: { withForward: 0, earlyRate: null, wellRate: null, avgFwdPct: null },
        funnel: { runs: 5, screenerAdded: 0 },
        registry: { total: 10, held: 2 },
        markdown: '# Karios 周度决策质量报告（2026-08-03 ~ 2026-08-07）',
      };
    }
    if (String(path).includes('/weekly-plan')) {
      return { ok: true, plan: { brief_date: '2026-08-17', markdown: '# 下周行动计划（旧）' } };
    }
    throw new Error(`unexpected: ${path}`);
  });
});

describe('WeeklyReviewCard', () => {
  it('renders the weekly report and stored plan', async () => {
    renderCard();
    expect(await screen.findByText(/周度复盘（L3-P4 · 周一 07:40 自动生成）/)).toBeDefined();
    expect(screen.getByText(/下周行动计划（决策 Agent 自动产出/)).toBeDefined();
    expect(await screen.findByText(/下周行动计划（旧）/)).toBeDefined();
    expect(
      screen.getAllByText((_, el) => Boolean(el?.textContent?.includes('周度决策质量报告'))).length,
    ).toBeGreaterThanOrEqual(1);
  });

  it('generates a new plan and stores it', async () => {
    const fetchMock = vi.fn(async (url: string) => {
      if (String(url).includes('/weekly-plan')) {
        return new Response(
          JSON.stringify({ ok: true, plan: '# 下周行动计划（新）\n\n### 买入\n- HK:02343' }),
          { status: 200 },
        );
      }
      return new Response(JSON.stringify({ ok: true }), { status: 200 });
    });
    vi.stubGlobal('fetch', fetchMock);
    renderCard();
    await screen.findByText(/下周行动计划（旧）/);

    fireEvent.click(screen.getByText(/生成计划|重新生成/));
    await waitFor(() => expect(screen.getByText(/下周行动计划（新）/)).toBeDefined());
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining('/weekly-plan'),
      expect.objectContaining({ method: 'POST' }),
    );
    vi.unstubAllGlobals();
  });
});
