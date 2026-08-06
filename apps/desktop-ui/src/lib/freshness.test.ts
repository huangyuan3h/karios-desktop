import { describe, expect, it } from 'vitest';

import { buildDataFreshnessMarkdown, type DataSourceFreshness } from './freshness';

const sources: DataSourceFreshness[] = [
  {
    source: 'market',
    label: '行情',
    lastSyncedAt: '2026-08-05T09:10:01.691359+00:00',
    ageMinutes: 1031,
    thresholdMinutes: 1440,
    stale: false,
  },
  {
    source: 'news',
    label: '新闻',
    lastSyncedAt: '2026-08-06T02:09:17.150002+00:00',
    ageMinutes: 12,
    thresholdMinutes: 360,
    stale: false,
  },
  {
    source: 'research',
    label: '研报',
    lastSyncedAt: '2026-08-05T17:00:00.000000+00:00',
    ageMinutes: 1500,
    thresholdMinutes: 1440,
    stale: true,
  },
];

describe('buildDataFreshnessMarkdown', () => {
  it('lists each source with age and timestamp', () => {
    const md = buildDataFreshnessMarkdown(sources);
    expect(md).toContain('## Data freshness');
    expect(md).toContain('行情: 2026-08-05T09:10:01.691359+00:00 (17.2h ago)');
    expect(md).toContain('新闻: 2026-08-06T02:09:17.150002+00:00 (12m ago)');
  });

  it('flags stale sources and adds a header warning', () => {
    const md = buildDataFreshnessMarkdown(sources);
    expect(md).toContain('⚠ STALE');
    expect(md).toContain('⚠ WARNING: 1 data source(s) stale at copy time.');
  });

  it('handles empty and never-synced sources', () => {
    expect(buildDataFreshnessMarkdown([])).toContain('- unavailable');
    const never = buildDataFreshnessMarkdown([
      {
        source: 'macro',
        label: '宏观',
        lastSyncedAt: null,
        ageMinutes: null,
        thresholdMinutes: 2880,
        stale: true,
      },
    ]);
    expect(never).toContain('宏观: never (unknown ago)');
    expect(never).toContain('⚠ STALE');
  });
});
