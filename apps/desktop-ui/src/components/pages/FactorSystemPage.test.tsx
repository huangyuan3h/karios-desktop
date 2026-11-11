import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { FactorSystemPage } from './FactorSystemPage';

const { apiGetJson, apiPostJson } = vi.hoisted(() => ({
  apiGetJson: vi.fn(),
  apiPostJson: vi.fn(),
}));
vi.mock('@/lib/api/client', () => ({ apiGetJson, apiPostJson }));

const PAYLOAD = {
  available: true,
  generatedAt: '2026-11-11',
  adviceOnly: true,
  freshness: {
    sourceGeneratedAt: '2026-10-28',
    historyLatest: '2026-10-28',
    tradeBlotterLastEntry: '2026-10-03',
    ageDays: 14,
    stale: true,
    reason:
      '因子库生成于 2026-10-28，距今 14 天；交易流水止于 2026-10-03，距今 39 天；交易流水中断，灯不会自行变化',
  },
  summary: { nFactors: 27, nCold: 22, nWatch: 4, nRevived: 1 },
  policy: {
    action: 'wait_in_baseline',
    baseline: 'homeport_m30',
    baselineLabel: '母港 M30',
    suggestedWeight: 0,
    suggestedLiveWeight: 0,
    maxLiveWeight: 20,
    liveBlocked: true,
    liveBlockedReason:
      'paper 4/20 未满、holdout -19.8% 未收复至 -10.0%；卫星实盘权重保持 0%，复活灯只作观察。',
    reason:
      '相关卫星因子为冷库/观察，未复活；坐在母港 M30 基线，不自动跟星舰，也不把参考灯当成下单指令。',
    decayedFactors: [],
    revivedFactors: [],
    preconditions: {
      paper20: {
        required: 20,
        completed: 4,
        pass: false,
        asOfDate: '2026-10-03',
        source: 'documented 2026-10-03 audit; no live blotter append in this slice',
        stale: true,
      },
      holdoutRecovery: {
        currentPct: -19.8,
        recoverToPct: -10,
        pass: false,
        source: 'factor vault starship_b holdout net (frozen)',
      },
      filteredValidPositive: {
        currentPct: -1.21,
        thresholdPct: 0,
        pass: false,
        source: 'factor vault starship_cap50 valid net (frozen)',
      },
      allPass: false,
    },
  },
  focus: [
    {
      id: 'harbor',
      name: '港湾 (参考)',
      family: 'E1',
      status: 'revived',
      badge: '复活',
      role: 'reference',
      position: 0,
      percentile: 99.8,
      actionable: false,
      note: '参考灯：复活只代表参考，不是下单指令；实盘仍 0%。',
    },
    {
      id: 'starship_b',
      name: '星舰B默认',
      family: '星舰B族',
      status: 'cold',
      badge: '冷库',
      role: 'satellite',
      position: 0,
      percentile: 10.9,
      actionable: false,
      note: 'S-gap 冷库：坐在母港 M30，不自动跟星舰。',
    },
  ],
  recentTransitions: [
    {
      date: '2026-10-28',
      kind: 'status',
      eventType: 'revive',
      factorId: 'harbor',
      name: '港湾 (参考)',
      from: 'watch',
      to: 'revived',
    },
  ],
};

const UNAVAILABLE = {
  available: false,
  error: 'factor_vault.json: [Errno 2] No such file or directory',
  generatedAt: '2026-11-11',
  freshness: {
    sourceGeneratedAt: null,
    stale: true,
    reason: '因子库文件缺失或不可读，无法判断新鲜度。',
  },
  summary: { nFactors: 0, nCold: 0, nWatch: 0, nRevived: 0 },
  policy: {
    action: 'wait_in_baseline',
    baseline: 'homeport_m30',
    baselineLabel: '母港 M30',
    suggestedWeight: 0,
    suggestedLiveWeight: 0,
    maxLiveWeight: 20,
    liveBlocked: true,
    liveBlockedReason:
      '因子库不可用（factor_vault.json: missing）；按母港 M30 基线等待，不自动跟任何卫星。',
    reason: '因子库不可用（factor_vault.json: missing）；按母港 M30 基线等待，不自动跟任何卫星。',
    decayedFactors: [],
    revivedFactors: [],
    preconditions: {},
  },
  focus: [],
  recentTransitions: [],
  adviceOnly: true,
};

function renderPage() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <FactorSystemPage />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  apiGetJson.mockReset();
  apiPostJson.mockReset();
  apiGetJson.mockResolvedValue(PAYLOAD);
});

describe('FactorSystemPage', () => {
  it('calls the factor-system contract endpoint once and shows the advice-only banner', async () => {
    renderPage();
    const banner = await screen.findByTestId('factor-system-advice-banner');
    expect(banner.textContent).toContain('仅建议');
    expect(banner.textContent).toContain('不下单');
    expect(apiGetJson).toHaveBeenCalledTimes(1);
    expect(apiGetJson).toHaveBeenCalledWith('/v1/agent/factor-system');
  });

  it('renders the wait-in-baseline policy with summary, preconditions and fresh-stale reason', async () => {
    renderPage();

    expect((await screen.findByTestId('factor-system-action')).textContent).toContain(
      '母港 M30 等待',
    );
    expect(screen.getByTestId('factor-system-policy').textContent).toContain('母港 M30');
    expect(screen.getByTestId('factor-system-weight').textContent).toContain('0%');
    expect(screen.getByTestId('factor-system-live-weight').textContent).toContain('0%');
    expect(screen.getByTestId('factor-system-live-blocked').textContent).toContain('受阻');
    expect(screen.getByTestId('factor-system-live-reason').textContent).toContain('paper 4/20');

    const summary = screen.getByTestId('factor-system-summary');
    expect(summary.textContent).toContain('27');
    expect(summary.textContent).toContain('22');
    expect(summary.textContent).toContain('4');

    expect(screen.getByTestId('factor-system-gate-paper20').textContent).toContain('4/20');
    expect(screen.getByTestId('factor-system-gate-holdoutRecovery').textContent).toContain('未过');
    expect(screen.getByTestId('factor-system-gate-filteredValidPositive').textContent).toContain(
      '未过',
    );

    const stale = screen.getByTestId('factor-system-stale');
    expect(stale.textContent).toContain('灯不会自行变化');
    expect(stale.textContent).toContain('交易流水止于 2026-10-03');
  });

  it('shows a revived reference light and its note without an order action', async () => {
    renderPage();

    const row = await screen.findByTestId('factor-system-row-harbor');
    expect(row.textContent).toContain('港湾 (参考)');
    expect(row.textContent).toContain('复活');
    expect(row.textContent).toContain('参考灯');

    const action = screen.getByTestId('factor-system-action-harbor');
    expect(action.textContent).toContain('非下单指令');
    expect(action.textContent).not.toContain('可跟随');

    // Read-only page: no button can place an order or write settings.
    expect(screen.queryAllByRole('button')).toHaveLength(0);

    const starship = screen.getByTestId('factor-system-row-starship_b');
    expect(starship.textContent).toContain('冷库');
    expect(starship.textContent).toContain('母港 M30');
  });

  it('renders recent transitions', async () => {
    renderPage();
    const table = await screen.findByTestId('factor-system-transitions');
    expect(table.textContent).toContain('2026-10-28');
    expect(table.textContent).toContain('watch → revived');
  });

  it('fails open to wait-in-M30 when the contract reports available: false', async () => {
    apiGetJson.mockResolvedValue(UNAVAILABLE);
    renderPage();

    const card = await screen.findByTestId('factor-system-unavailable');
    expect(card.textContent).toContain('因子库不可用');
    expect(card.textContent).toContain('母港 M30');
    expect(card.textContent).toContain('factor_vault.json');
    expect(screen.getByTestId('factor-system-action').textContent).toContain('母港 M30 等待');
    expect(screen.queryByTestId('factor-system-focus')).toBeNull();
  });

  it('fails open to wait-in-M30 when the request itself errors', async () => {
    apiGetJson.mockRejectedValue(new Error('500 Internal Server Error'));
    renderPage();

    const card = await screen.findByTestId('factor-system-unavailable');
    expect(card.textContent).toContain('因子库不可用');
    expect(card.textContent).toContain('500 Internal Server Error');
    expect(screen.getByTestId('factor-system-advice-banner')).toBeDefined();
  });
});
