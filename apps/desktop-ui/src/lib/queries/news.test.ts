import { describe, expect, it, vi } from 'vitest';

import {
  buildDashboardSummaryPath,
  dashboardLiteQueryKey,
  dashboardSummaryQueryKey,
  DASHBOARD_LITE_INCLUDES,
  DASHBOARD_NEWS_INCLUDES,
  DASHBOARD_SENTIMENT_INCLUDES,
} from './dashboard';
import { dashboardNewsQueryKey, newsItemsQueryKey } from './news';
import { dashboardSentimentQueryKey } from './sentiment';

vi.mock('@/lib/market-hours', () => ({
  isShanghaiSyncWindow: () => true,
}));

describe('buildDashboardSummaryPath', () => {
  it('builds lite path without macro, sentiment, or news', () => {
    expect(buildDashboardSummaryPath(DASHBOARD_LITE_INCLUDES)).toBe(
      '/dashboard/summary?include_macro=false&include_sentiment=false&include_news=false',
    );
  });

  it('omits macro when includeMacro is false', () => {
    expect(buildDashboardSummaryPath(false)).toBe(
      '/dashboard/summary?include_macro=false',
    );
  });

  it('uses full path when all blocks are included', () => {
    expect(buildDashboardSummaryPath(true)).toBe('/dashboard/summary');
  });
});

describe('dashboardSummaryQueryKey', () => {
  it('distinguishes full vs lite vs partial variants', () => {
    expect(dashboardSummaryQueryKey(true)).toEqual(['dashboard', 'summary', 'full']);
    expect(dashboardSummaryQueryKey(false)).toEqual(['dashboard', 'summary', 'no-macro']);
    expect(dashboardLiteQueryKey()).toEqual(['dashboard', 'summary', 'lite']);
  });
});

describe('sub query keys', () => {
  it('uses stable sentiment and news keys', () => {
    expect(dashboardSentimentQueryKey()).toEqual(['dashboard', 'summary', 'sentiment']);
    expect(dashboardNewsQueryKey()).toEqual(['dashboard', 'summary', 'news']);
    expect(newsItemsQueryKey(24, 100)).toEqual(['news', 'items', 24, 100]);
  });
});

describe('partial include presets', () => {
  it('defines sentiment-only and news-only presets', () => {
    expect(DASHBOARD_SENTIMENT_INCLUDES.includeSentiment).toBe(true);
    expect(DASHBOARD_NEWS_INCLUDES.includeNews).toBe(true);
    expect(DASHBOARD_LITE_INCLUDES.includeSentiment).toBe(false);
  });
});
