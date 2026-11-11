import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  buildDecisionContext,
  buildDecisionSystemPrompt,
  queryHoldingsHealth,
  searchArchive,
  shouldIncludeS3Rules,
} from './decision.js';

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
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => new Response('', { status: 503 })),
    );
    const md = await queryHoldingsHealth();
    expect(md).toContain('暂不可用');
  });
});

describe('searchArchive', () => {
  it('renders hits and fired counts', async () => {
    mockFetchOk({
      ok: true,
      hits: [
        {
          date: '2026-08-01',
          status: 'open',
          matches: ['BUY 腾讯'],
          outcome: { fired: [{ symbol: 'HK:00700' }] },
        },
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

// ---------------------------------------------------------------------------
// mode-aware decision context (2026-11-11)
// ---------------------------------------------------------------------------

function briefFor(mode: string): Record<string, unknown> {
  const follow =
    mode === 'starship_b'
      ? {
          followable: false,
          status: 'cold',
          reason: 'starship_b=cold(badge 冷库, net -14.79, pos 0%)',
          recommendedMode: 'homeport',
          recommendedModeLabel: '母港 M30',
          recommendedTimeline: 'homeport_m30',
          adviceOnly: true,
        }
      : {
          followable: true,
          status: 'not_gated',
          reason: 'not a starship-family mode; no cold-factor gate',
          recommendedMode: null,
          recommendedTimeline: null,
          adviceOnly: true,
        };
  return {
    briefVersion: '2026-11-11.2',
    contract: {
      contractId: `dc:2026-11-11:${mode}:HOLD:BOND10`,
      strategy: {
        mode,
        timelineStrategy: mode === 'homeport' ? 'homeport_m30' : mode,
        label: mode === 'homeport' ? '母港 M30' : '星舰 B',
        timelineLabel: mode === 'homeport' ? '母港 M30' : '星舰 B',
        mappingNote: "settings mode 'homeport' is the product M30 tier",
        followability: follow,
        pathMismatch: {
          active: mode === 'starship_b',
          message: 'settings 是 星舰 B，但停车腿仍是 港湾 HOLD BOND10。',
        },
      },
      sleeve: {
        action: 'HOLD',
        pickKey: 'BOND10',
        trailExit: false,
        pendingOpenSell: false,
      },
      fillClock: {
        kind: 'hold',
        label: '无需操作',
        detail: '未达阈值/无新腿',
        basis: 'close',
        tradableNow: false,
      },
      whip: { context: { sameKeyRebuy: false } },
    },
    judgment: { settingsMode: mode, priceIntegrity: 'ok' },
    integrity: { status: 'ok', warnings: [] },
    eventTags: {
      count: 1,
      tags: [
        {
          tag: 'oil_political_calendar',
          effect: 'advise_defer_same_key_rebuy',
          confidence: 0.8,
          relatedAssets: ['OIL', 'ETF:513350'],
          validFrom: '2026-10-15',
          validUntil: '2026-11-20',
        },
      ],
      strategyAdvice: {
        advice: 'switch_after_confirmation',
        currentMode: mode,
        recommendedMode: 'homeport',
      },
      neverVetoesExits: true,
    },
    geoRegime: { active: true, label: 'oil_political', notMarketRegime: true },
    gate: {
      cn: { mode: 'DEFEND', allowNewEntries: false, marketRegime: 'Weak' },
      hk: { mode: 'HOLD_ONLY', allowNewEntries: false },
    },
    execution: {
      attention: {
        counts: { mustAct: 1, exits: 1, trims: 0, fires: 0, blockedFires: 0 },
        mustAct: [
          { symbol: 'ETF:513350', action: 'EXIT', why: 'trail_exit', fill: { label: '明日开盘卖' } },
        ],
      },
    },
    backtest: {
      currentMode: { mode, timelineStrategy: mode === 'homeport' ? 'homeport_m30' : mode },
      comparison: [
        {
          timelineStrategy: 'homeport_m30',
          name: '母港 M30',
          statusLabel: '基线 · M30',
          isCurrentMode: mode === 'homeport',
          windows: { long: { total: 146.6, mdd: -16.4, sharpe: 1.06 } },
        },
      ],
    },
  };
}

function stubBriefFetch(brief: Record<string, unknown> | null, health: unknown = HEALTH) {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: unknown) => {
      const url = String(input);
      if (url.includes('/v1/agent/judgment-brief')) {
        return brief
          ? new Response(JSON.stringify(brief), { status: 200 })
          : new Response('', { status: 503 });
      }
      if (url.includes('/v1/agent/portfolio-health')) {
        return new Response(JSON.stringify(health), { status: 200 });
      }
      return new Response('', { status: 404 });
    }),
  );
}

describe('shouldIncludeS3Rules', () => {
  it('is true only for S-3 modes or explicit S-3 questions', () => {
    expect(shouldIncludeS3Rules('homeport', '今天怎么做？')).toBe(true);
    expect(shouldIncludeS3Rules('starship_b', '现在该跟哪个档？')).toBe(false);
    expect(shouldIncludeS3Rules('starship_b', 'S-3 持仓要怎么处理？')).toBe(true);
    expect(shouldIncludeS3Rules(null, 'B3 腿怎么调？')).toBe(true);
  });
});

describe('buildDecisionContext', () => {
  it('builds starship_b context from the brief without defaulting to S-3', async () => {
    stubBriefFetch(briefFor('starship_b'));
    const ctx = await buildDecisionContext({ question: '现在该跟哪个档？' });

    expect(ctx.mode).toBe('starship_b');
    expect(ctx.includeS3Rules).toBe(false);
    expect(ctx.system).toContain('settings mode: `starship_b`');
    expect(ctx.system).toContain('followability');
    expect(ctx.system).toContain('homeport_m30');
    expect(ctx.system).toContain('可成交时钟');
    expect(ctx.system).toContain('事件/地缘标签');
    expect(ctx.system).toContain('不否决卖出');
    expect(ctx.system).not.toContain('S-3 回测纪律');
    // Only the brief was fetched — no S-3 health round.
    expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);
  });

  it('includes S-3 knowledge for homeport because the mode uses the S-3/B3 sleeve', async () => {
    stubBriefFetch(briefFor('homeport'));
    const ctx = await buildDecisionContext({ question: '今天持仓怎么办？' });

    expect(ctx.includeS3Rules).toBe(true);
    expect(ctx.system).toContain('S-3 回测纪律');
    expect(ctx.system).toContain('S-3 决策体检（2026-08-07）');
    expect(ctx.system).toContain('settings mode: `homeport`');
    expect(vi.mocked(fetch)).toHaveBeenCalledTimes(2);
  });

  it('includes S-3 knowledge for an explicit S-3 question on a starship mode', async () => {
    stubBriefFetch(briefFor('starship_b'));
    const ctx = await buildDecisionContext({ question: 'S-3 持仓的止损线在哪？' });
    expect(ctx.includeS3Rules).toBe(true);
    expect(ctx.system).toContain('S-3 回测纪律');
  });

  it('degrades clearly when the brief is unavailable', async () => {
    stubBriefFetch(null);
    const ctx = await buildDecisionContext({ question: '现在怎么办？' });
    expect(ctx.brief).toBeNull();
    expect(ctx.system).toContain('判断简报服务暂不可用');
    expect(ctx.system).toContain('S-3 决策体检');
  });

  it('buildDecisionSystemPrompt never asks the model to override an exit', () => {
    const system = buildDecisionSystemPrompt({
      brief: briefFor('starship_b'),
      includeS3: false,
    });
    expect(system).toContain('不能否决');
    expect(system).toContain('frozen engine exits > 事件标签');
    expect(system).toContain('CN gate = DEFEND / Weak');
  });
});
