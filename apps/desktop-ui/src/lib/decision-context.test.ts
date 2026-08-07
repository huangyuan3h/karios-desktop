import { describe, expect, it } from 'vitest';

import {
  activeLayerToMarkdown,
  buildNewsMarkdown,
  estimateTokens,
  type DecisionActiveLayer,
  type DecisionBlock,
} from './decision-context';

describe('estimateTokens', () => {
  it('estimates zh-heavy text at ~1.8 chars/token', () => {
    const zh = estimateTokens('这是一段中文内容用于测试token估算');
    expect(zh).toBeGreaterThan(0);
    expect(zh).toBeLessThan(15);
  });

  it('estimates empty as 0', () => {
    expect(estimateTokens('')).toBe(0);
  });
});

describe('buildNewsMarkdown', () => {
  it('renders top items with relevance and time', () => {
    const md = buildNewsMarkdown({
      news: {
        hours: 24,
        total: 2,
        items: [
          { title: '半导体板块拉升', relevanceScore: 92, publishedAt: '2026-08-06T09:30:00Z' },
          { title: '美联储纪要', relevanceScore: 60, publishedAt: '2026-08-06T08:00:00Z' },
        ],
      },
    });
    expect(md).toContain('## 最近新闻');
    expect(md).toContain('半导体板块拉升 (rel=92)');
    expect(md).toContain('美联储纪要');
  });

  it('returns empty for no items', () => {
    expect(buildNewsMarkdown({})).toBe('');
  });

  it('drops low-relevance non-whitelist noise and caps at 5', () => {
    const items = Array.from({ length: 8 }, (_, i) => ({
      title: `无关娱乐新闻${i}`,
      relevanceScore: 10,
      publishedAt: '2026-08-06T09:00:00Z',
    }));
    items.push({ title: '央行宣布降准', relevanceScore: 5, publishedAt: '2026-08-06T09:00:00Z' });
    const md = buildNewsMarkdown({ news: { hours: 24, total: items.length, items } });
    expect(md).toContain('央行宣布降准');
    expect(md).not.toContain('无关娱乐新闻');
  });
});

describe('activeLayerToMarkdown', () => {
  it('serializes blocks in order under the layer heading', () => {
    const p0: DecisionBlock = { id: 'p0-watchlist', label: '操作表', tier: 'P0', content: '## Watchlist\nrow', tokens: 10 };
    const p1: DecisionBlock = { id: 'p1-news', label: '最近新闻', tier: 'P1', content: '## 最近新闻\n- x', tokens: 5 };
    const layer: DecisionActiveLayer = {
      blocks: [p0, p1],
      totalTokens: 15,
      generatedAt: '2026-08-06T00:00:00Z',
      freshness: [],
    };
    const md = activeLayerToMarkdown(layer);
    expect(md).toContain('# 决策活跃层');
    expect(md).toContain('## Watchlist');
    expect(md).toContain('## 最近新闻');
  });
});
