import { describe, expect, it } from 'vitest';

import { buildDashboardSummaryPath, dashboardSummaryQueryKey } from './dashboard';

describe('dashboardSummaryQueryKey', () => {
  it('distinguishes macro full vs lite', () => {
    expect(dashboardSummaryQueryKey(true)).toEqual(['dashboard', 'summary', 'full']);
    expect(dashboardSummaryQueryKey(false)).toEqual(['dashboard', 'summary', 'lite']);
  });
});

describe('buildDashboardSummaryPath', () => {
  it('omits macro when includeMacro is false', () => {
    expect(buildDashboardSummaryPath(false)).toBe('/dashboard/summary?include_macro=false');
  });

  it('uses full path when includeMacro is true', () => {
    expect(buildDashboardSummaryPath(true)).toBe('/dashboard/summary');
  });
});
