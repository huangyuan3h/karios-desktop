import type { QueryClient } from '@tanstack/react-query';

import { parseExecutionGate } from '@/lib/execution-action';
import {
  buildMainlineAllowSet,
  isSectorOutflowBlock,
} from '@/lib/hot-industry-picks';
import {
  buildDataFreshnessMarkdown,
  fetchDataSourcesHealth,
  type DataSourceFreshness,
} from '@/lib/freshness';
import { buildWatchlistMarkdown } from '@/lib/dashboard-export';
import { buildIndustryMarkdown, buildMarketAndMacroMarkdown } from '@/lib/dashboard-export';
import { formatExecutionGateMarkdown } from '@/lib/dashboard-format';
import { fetchDashboardSummaryPartial } from '@/lib/queries/dashboard';

export type DecisionBlockTier = 'P0' | 'P1' | 'P2';

export type DecisionBlock = {
  id: string;
  label: string;
  tier: DecisionBlockTier;
  content: string;
  tokens: number;
};

export type DecisionActiveLayer = {
  blocks: DecisionBlock[];
  totalTokens: number;
  generatedAt: string;
  freshness: DataSourceFreshness[];
};

/** Rough token estimate: zh-heavy mixed text ~2 chars/token, EN ~4. */
export function estimateTokens(text: string): number {
  if (!text) return 0;
  let zh = 0;
  let other = 0;
  for (const ch of text) {
    if (/[\u4e00-\u9fff]/.test(ch)) zh += 1;
    else other += 1;
  }
  return Math.ceil(zh / 1.8 + other / 4);
}

export const DECISION_BLOCK_DEFS: Array<{
  id: string;
  label: string;
  tier: DecisionBlockTier;
  description: string;
}> = [
  {
    id: 'p0-watchlist',
    label: '操作表（Watchlist + Gate）',
    tier: 'P0',
    description: '监控票表格 + 执行闸门状态，决策必须项',
  },
  {
    id: 'p1-freshness',
    label: '数据新鲜度',
    tier: 'P1',
    description: '各数据源最后同步时间 + STALE 警告（TIP-013）',
  },
  {
    id: 'p1-news',
    label: '最近新闻',
    tier: 'P1',
    description: '高相关新闻 Top 10（标题 + 相关度）',
  },
  {
    id: 'p2-macro',
    label: '市场 & 宏观',
    tier: 'P2',
    description: '指数红绿灯 + 宏观商品/汇率，可折叠',
  },
  {
    id: 'p2-industry',
    label: '行业资金流',
    tier: 'P2',
    description: '行业资金流表，可折叠',
  },
];

export function buildNewsMarkdown(s: Record<string, unknown>): string {
  const news: any = (s as any)?.news ?? {};
  const items: any[] = Array.isArray(news?.items) ? news.items : [];
  if (!items.length) return '';
  const lines = ['## 最近新闻（按相关度）', ''];
  for (const it of items.slice(0, 10)) {
    const title = String(it?.title ?? '—').slice(0, 80);
    const rel = it?.relevanceScore ?? '';
    const time = String(it?.publishedAt ?? it?.published_at ?? '');
    lines.push(`- ${title}${rel !== '' ? ` (rel=${rel})` : ''}${time ? ` · ${time.slice(0, 16)}` : ''}`);
  }
  return lines.join('\n');
}

async function fetchSummaryWithNewsAndSentiment(): Promise<Record<string, unknown>> {
  const s = await fetchDashboardSummaryPartial({
    includeMacro: true,
    includeSentiment: true,
    includeNews: true,
    includeIndustry: true,
    includeScreeners: false,
  });
  return (s ?? {}) as Record<string, unknown>;
}

/**
 * Build the Layer 1 active decision context (TIP-015 M2).
 * Blocks are assembled live (not cached) so the agent always sees fresh
 * data; forceFresh bypasses the react-query cache for quotes/screeners.
 */
export async function buildDecisionActiveLayer(opts: {
  queryClient: QueryClient;
  forceFresh?: boolean;
  include?: Record<string, boolean>;
}): Promise<DecisionActiveLayer> {
  const { queryClient, forceFresh = false, include = {} } = opts;
  const enabled = (id: string, tier: DecisionBlockTier) => {
    if (id in include) return include[id] === true;
    return tier === 'P0' || tier === 'P1';
  };

  const [summary, health] = await Promise.all([
    fetchSummaryWithNewsAndSentiment(),
    fetchDataSourcesHealth().catch(() => ({ sources: [] as DataSourceFreshness[] })),
  ]);

  const gate = parseExecutionGate((summary as any)?.marketSentiment?.executionGate);
  const mainlineAllow = buildMainlineAllowSet(summary);
  const sectorOutflowBlock = isSectorOutflowBlock(summary);

  const blockPromises: Array<Promise<DecisionBlock | null>> = [
    enabled('p0-watchlist', 'P0')
      ? (async () => {
          const watchlistMd = await buildWatchlistMarkdown(
            queryClient,
            gate,
            mainlineAllow,
            sectorOutflowBlock,
            forceFresh,
          );
          const gateMd = gate ? formatExecutionGateMarkdown(gate, '###') : '';
          const content = [gateMd, watchlistMd.trim()].filter(Boolean).join('\n\n');
          return { id: 'p0-watchlist', label: '操作表', tier: 'P0', content, tokens: 0 } as DecisionBlock;
        })()
      : Promise.resolve(null),
    enabled('p1-freshness', 'P1')
      ? Promise.resolve({
          id: 'p1-freshness',
          label: '数据新鲜度',
          tier: 'P1',
          content: buildDataFreshnessMarkdown(Array.isArray(health?.sources) ? health.sources : []),
          tokens: 0,
        } as DecisionBlock)
      : Promise.resolve(null),
    enabled('p1-news', 'P1')
      ? Promise.resolve({
          id: 'p1-news',
          label: '最近新闻',
          tier: 'P1',
          content: buildNewsMarkdown(summary),
          tokens: 0,
        } as DecisionBlock)
      : Promise.resolve(null),
    enabled('p2-macro', 'P2')
      ? Promise.resolve({
          id: 'p2-macro',
          label: '市场 & 宏观',
          tier: 'P2',
          content: buildMarketAndMacroMarkdown(summary, '##'),
          tokens: 0,
        } as DecisionBlock)
      : Promise.resolve(null),
    enabled('p2-industry', 'P2')
      ? Promise.resolve({
          id: 'p2-industry',
          label: '行业资金流',
          tier: 'P2',
          content: buildIndustryMarkdown(summary, '##'),
          tokens: 0,
        } as DecisionBlock)
      : Promise.resolve(null),
  ];

  const blocks = (await Promise.all(blockPromises)).filter(
    (b): b is DecisionBlock => b !== null && b.content.trim().length > 0,
  );
  for (const b of blocks) b.tokens = estimateTokens(b.content);
  const totalTokens = blocks.reduce((acc, b) => acc + b.tokens, 0);
  return {
    blocks,
    totalTokens,
    generatedAt: new Date().toISOString(),
    freshness: Array.isArray(health?.sources) ? health.sources : [],
  };
}

/** Serialize an active layer into a single system message chunk. */
export function activeLayerToMarkdown(layer: DecisionActiveLayer): string {
  const lines = ['# 决策活跃层（实时数据，复制时点快照的替代）', ''];
  for (const b of layer.blocks) {
    lines.push(b.content.trim());
    lines.push('');
  }
  return lines.join('\n').trim();
}
