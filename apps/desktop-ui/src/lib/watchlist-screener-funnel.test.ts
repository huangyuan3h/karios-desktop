import { describe, expect, it } from 'vitest';

import {
  emptyScreenerFunnel,
  formatScreenerFunnel,
} from '@/lib/watchlist-screener-import';
import { formatAutomationSummary, type AutomationRun } from '@/lib/watchlist-automation';

describe('formatScreenerFunnel', () => {
  it('formats TIP-002 funnel counts', () => {
    expect(
      formatScreenerFunnel({
        tvHit: 40,
        passPullback: 8,
        passTrendOk: 3,
        addedNew: 2,
        droppedByPullback: 32,
      }),
    ).toBe('TV 40 → pullback 8 → TrendOK 3 → +2');
  });

  it('empty funnel helper starts at zeros', () => {
    expect(emptyScreenerFunnel()).toEqual({
      tvHit: 0,
      passPullback: 0,
      passTrendOk: 0,
      addedNew: 0,
      droppedByPullback: 0,
    });
  });
});

describe('formatAutomationSummary funnel', () => {
  it('appends funnel from result', () => {
    const run: AutomationRun = {
      runId: 'r1',
      createdAt: '2026-07-22T10:00:00.000Z',
      trigger: 'manual',
      remove: [{ symbol: 'CN:1' }],
      alphaAdd: [],
    };
    const summary = formatAutomationSummary(run, {
      removed: 1,
      screenerAdded: 2,
      alphaAdded: 0,
      funnel: {
        tvHit: 10,
        passPullback: 4,
        passTrendOk: 2,
        addedNew: 2,
        droppedByPullback: 6,
      },
    });
    expect(summary).toContain('funnel TV 10 → pullback 4 → TrendOK 2 → +2');
  });

  it('reads funnel from meta when result omitted', () => {
    const run: AutomationRun = {
      runId: 'r2',
      createdAt: '2026-07-22T10:00:00.000Z',
      trigger: 'scheduled',
      remove: [],
      alphaAdd: [],
      screenerAdded: 1,
      meta: {
        funnel: {
          tvHit: 5,
          passPullback: 2,
          passTrendOk: 1,
          addedNew: 1,
          droppedByPullback: 3,
        },
        alphaRejected: { defense_sector: 2 },
      },
    };
    const summary = formatAutomationSummary(run);
    expect(summary).toContain('funnel TV 5 → pullback 2 → TrendOK 1 → +1');
    expect(summary).toContain('alphaReject defense_sector:2');
  });
});
