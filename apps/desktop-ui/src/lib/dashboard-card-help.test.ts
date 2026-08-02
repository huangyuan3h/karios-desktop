import { describe, expect, it } from 'vitest';

import {
  DASHBOARD_HELP,
  buildDashboardHelpTooltipBody,
  getDashboardHelp,
} from './dashboard-card-help';

describe('DASHBOARD_HELP registry', () => {
  it('covers every dashboard card section that uses tooltips', () => {
    // Mirror the headers referenced in DashboardPage.tsx (2026-08-01 audit).
    const expectedIds = [
      'sentiment5d.date',
      'sentiment5d.ratio',
      'sentiment5d.turnover',
      'sentiment5d.premiumPct',
      'sentiment5d.failedPct',
      'sentiment5d.risk',
      'etf.name',
      'etf.symbol',
      'etf.mainFlow',
      'etf.superLarge',
      'etf.flow3d',
      'etf.realtimeAsOf',
      'etf.source',
      'etf.status',
      'etf.signal',
      'idxRule.title',
      'idxRule.posRange',
      'risk.symbol',
      'risk.name',
      'risk.intradayPct',
      'risk.vr',
      'risk.gap',
      'risk.alerts',
      'screener.name',
      'screener.capturedAt',
      'screener.rows',
      'screener.filters',
      'sync.step',
      'sync.ok',
      'sync.duration',
      'sync.message',
      'news.brief',
      'news.asOf',
    ];
    const missing = expectedIds.filter((id) => !DASHBOARD_HELP[id]);
    expect(missing, `Missing help entries: ${missing.join(', ')}`).toEqual([]);
  });

  it('every entry has non-empty label, short, detail', () => {
    for (const [id, h] of Object.entries(DASHBOARD_HELP)) {
      expect(h.id, id).toBe(id);
      expect(h.label.length, id).toBeGreaterThan(0);
      expect(h.short.length, id).toBeGreaterThan(10);
    }
  });

  it('numeric columns declare a unit', () => {
    const numericIds = [
      'sentiment5d.premiumPct',
      'sentiment5d.failedPct',
      'risk.intradayPct',
      'risk.vr',
    ];
    for (const id of numericIds) {
      expect(DASHBOARD_HELP[id]?.unit, `${id} should have a unit`).toBeTruthy();
    }
  });
});

describe('getDashboardHelp', () => {
  it('returns known entry', () => {
    expect(getDashboardHelp('etf.mainFlow').id).toBe('etf.mainFlow');
  });

  it('returns fallback for unknown id', () => {
    const out = getDashboardHelp('nope');
    expect(out.id).toBe('nope');
  });
});

describe('buildDashboardHelpTooltipBody', () => {
  it('renders ReactNode', () => {
    const node = buildDashboardHelpTooltipBody(getDashboardHelp('idxRule.title'));
    expect(node).toBeTruthy();
  });
});
