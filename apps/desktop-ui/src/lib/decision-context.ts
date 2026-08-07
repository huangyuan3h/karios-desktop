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
import {
  DEFAULT_CATALYST_MAX_AGE_DAYS,
  fetchAlphaRadarTrendsForCopy,
} from '@/lib/alpha-radar-catalyst';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import { isNewsTitleWhitelisted } from '@/lib/news-keyword-whitelist';
import { loadWatchlist } from '@/lib/watchlist-storage';

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
  {
    id: 'p2-alpha',
    label: 'Alpha 催化（Watchlist 交集）',
    tier: 'P2',
    description: 'Alpha Radar 趋势中命中持仓/观察票的 S/A 级催化，一行一个',
  },
];

/**
 * Watchlist news keys for filtering: 6-digit CN/ETF codes, HK codes, names.
 * Falls back to empty when storage is unavailable (e.g. tests).
 */
export function buildWatchlistNewsKeys(): { codes: Set<string>; names: Set<string> } {
  const codes = new Set<string>();
  const names = new Set<string>();
  try {
    for (const it of loadWatchlist()) {
      const sym = String(it?.symbol ?? '').trim().toUpperCase();
      const cn = sym.match(/^(?:CN|ETF):(\d{6})$/);
      if (cn) codes.add(cn[1]);
      const hk = sym.match(/^HK:(\d{4,5})$/);
      if (hk) codes.add(hk[1]);
      const name = String(it?.name ?? '').trim();
      if (name) names.add(name);
    }
  } catch {
    // storage unavailable
  }
  return { codes, names };
}

/**
 * Compact, decision-relevant news: keep only items that hit the watchlist
 * (ticker/name), are highly relevant (rel≥50), or are macro-critical
 * (keyword whitelist). Top 5 — news is a scan input, not a context hog.
 */
export function buildNewsMarkdown(s: Record<string, unknown>): string {
  const news: any = (s as any)?.news ?? {};
  const items: any[] = Array.isArray(news?.items) ? news.items : [];
  if (!items.length) return '';
  const keys = buildWatchlistNewsKeys();
  const hits = items
    .filter((it) => {
      const title = String(it?.title ?? '');
      const rel = Number(it?.relevanceScore) || 0;
      const tickers = Array.isArray(it?.tickers) ? (it.tickers as string[]) : [];
      const tickerHit = tickers.some((t) => {
        const code = String(t ?? '').split('.')[0] ?? '';
        return code.length > 0 && keys.codes.has(code);
      });
      const nameHit = keys.names.size > 0 && [...keys.names].some((n) => title.includes(n));
      const relHit = rel >= 50;
      const kwHit = isNewsTitleWhitelisted(title);
      return tickerHit || nameHit || relHit || kwHit;
    })
    .slice(0, 5);
  if (!hits.length) return '';
  const lines = ['## 最近新闻（watchlist 相关 + 高相关 + 宏观，Top 5）', ''];
  for (const it of hits) {
    const title = String(it?.title ?? '—').slice(0, 80);
    const rel = it?.relevanceScore ?? '';
    const time = String(it?.publishedAt ?? it?.published_at ?? '');
    lines.push(`- ${title}${rel !== '' ? ` (rel=${rel})` : ''}${time ? ` · ${time.slice(0, 16)}` : ''}`);
  }
  return lines.join('\n');
}

/** Alpha Radar trends intersecting the watchlist — why a watched symbol is S/A grade. */
export async function buildWatchlistAlphaMarkdown(): Promise<string> {
  const keys = buildWatchlistNewsKeys();
  if (keys.codes.size === 0) return '';
  const { items } = await fetchAlphaRadarTrendsForCopy(
    DATA_SYNC_BASE_URL,
    20,
    DEFAULT_CATALYST_MAX_AGE_DAYS,
  ).catch(() => ({ items: [] as Array<Record<string, unknown>> }));
  const lines = ['## Alpha 催化（Watchlist 交集）', ''];
  let count = 0;
  for (const t of Array.isArray(items) ? items : []) {
    const cnSymbols: any[] = Array.isArray((t as any)?.cnSymbols) ? (t as any).cnSymbols : [];
    for (const s of cnSymbols) {
      const code = String(s?.symbol ?? '');
      const code6 = code.replace(/\D/g, '').slice(-6) ?? '';
      if (!code6 || !keys.codes.has(code6)) continue;
      const theme = String((t as any)?.macroTheme ?? (t as any)?.trendName ?? '—');
      const grade = String((t as any)?.catalystGrade ?? (t as any)?.urgencyLevel ?? 'B');
      const conf = Math.round(Number(s?.confidence) * 100);
      const rationale = String(s?.rationale ?? '').slice(0, 60);
      lines.push(
        `- ${String(s?.name ?? code)} (${code}) · ${theme} · ${grade}${Number.isFinite(conf) ? ` · ${conf}%` : ''}${rationale ? ` · ${rationale}` : ''}`,
      );
      count += 1;
      break; // one line per trend
    }
    if (count >= 5) break;
  }
  if (!count) return '';
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
    enabled('p2-alpha', 'P2')
      ? buildWatchlistAlphaMarkdown().then((content) =>
          content
            ? ({ id: 'p2-alpha', label: 'Alpha 催化', tier: 'P2', content, tokens: 0 } as DecisionBlock)
            : null,
        )
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
