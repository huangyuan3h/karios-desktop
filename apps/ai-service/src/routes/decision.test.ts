import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { queryHoldingsHealth, searchArchive } from './decision';

const HEALTH = {
  tradeDate: '2026-08-07',
  regime: 'Weak',
  sentiment: 'normal',
  panicCooldown: { lastPanicDate: '2026-07-30', cooldownEndDate: '2026-08-04', active: false },
  s3Candidates: [],
  s3Rules: {
    entryScore: 65,
    rsMin: 0.5,
    stopLossPct: -5.0,
    trailingStopPct: -8.0,
    maxHoldDays: 60,
    pyramidTriggerPct: 2.5,
    pyramidAddScale: 0.5,
  },
  holdings: [
    {
      symbol: 'HK:00700',
      name: '腾讯控股',
      positionPct: 6.3,
      pnlPct: 0.6,
      drawdownFromPeakPct: -2.7,
      holdingDays: 11,
      stopLossLine: 452.2,
      trailingLine: 452.8,
      pyramidTriggerLine: 487.9,
      pyramidAdded: false,
      expireDate: '2026-09-27',
      action: 'HOLD',
    },
  ],
};

function mockFetchOk(body: unknown) {
  vi.stubGlobal(
    'fetch',
    vi.fn(async () => new Response(JSON.stringify(body), { status: 200 })),
  );
}

beforeEach(() => {
  vi.restoreAllMocks();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('queryHoldingsHealth', () => {
  it('renders market state, holdings and weak-regime note', async () => {
    mockFetchOk(HEALTH);
    const md = await queryHoldingsHealth();
    expect(md).toContain('S-3 决策体检（2026-08-07）');
    expect(md).toContain('regime=Weak');
    expect(md).toContain('腾讯控股');
    expect(md).toContain('✅ 持有');
    expect(md).toContain('止损线 452.2');
    expect(md).toContain('移动线 452.8');
    expect(md).toContain('金字塔触发线 487.9');
    expect(md).toContain('未加仓');
    expect(md).toContain('到期 2026-09-27');
    expect(md).toContain('今日 **无开仓候选**（regime=Weak');
  });

  it('flags EXIT holdings with the trigger reason', async () => {
    mockFetchOk({
      ...HEALTH,
      holdings: [{ ...HEALTH.holdings[0], action: 'EXIT', reason: 'trailing_stop' }],
    });
    const md = await queryHoldingsHealth();
    expect(md).toContain('🔴 建议退出');
    expect(md).toContain('触发：trailing_stop');
  });

  it('renders candidates list when present', async () => {
    mockFetchOk({
      ...HEALTH,
      regime: 'Strong',
      s3Candidates: [
        { symbol: 'CN:600111', name: '北方稀土', industry: '有色', score: 71.0, rs: 0.62 },
      ],
    });
    const md = await queryHoldingsHealth();
    expect(md).toContain('北方稀土');
    expect(md).toContain('score=71');
    expect(md).toContain('建议仓位 ~5%');
  });

  it('falls back gracefully when the service is unreachable', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response('', { status: 503 })));
    const md = await queryHoldingsHealth();
    expect(md).toContain('暂不可用');
  });
});

describe('searchArchive', () => {
  it('renders hits and fired counts', async () => {
    mockFetchOk({
      ok: true,
      hits: [
        { date: '2026-08-01', status: 'open', matches: ['BUY 腾讯'], outcome: { fired: [{ symbol: 'HK:00700' }] } },
      ],
    });
    const md = await searchArchive('HK:00700');
    expect(md).toContain('HK:00700');
    expect(md).toContain('2026-08-01');
    expect(md).toContain('开火 1');
  });

  it('reports no hits', async () => {
    mockFetchOk({ ok: true, hits: [] });
    const md = await searchArchive('XXX');
    expect(md).toContain('无命中');
  });
});
