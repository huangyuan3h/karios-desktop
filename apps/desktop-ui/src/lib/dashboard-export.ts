/* eslint-disable @typescript-eslint/no-explicit-any */
import type { QueryClient } from '@tanstack/react-query';

import { apiGetJson } from '@/lib/api/client';
import type { TrendOkResult } from '@/lib/api/types';
import {
  buildAutoQaMarkdown,
  buildCatalystPurgeMap,
  buildCatalystStocksMarkdown,
  buildAlphaRadarTrendsMarkdown,
  DEFAULT_CATALYST_MAX_AGE_DAYS,
  fetchAlphaRadarTrendsForCopy,
  fetchAutoQaStats,
  fetchCatalystStocks,
  normalizeCatalystSymbol,
  type CatalystCopyContext,
  type CatalystStocksResponse,
} from '@/lib/alpha-radar-catalyst';
import { fetchTrendOkMap } from '@/lib/api/trendok';
import { chunk } from '@/lib/chunk';
import {
  buildTopByDateMap,
  dedupeShownDates,
  fmtAmountCn,
  fmtSignedAmountCn,
  formatSrvIndexLine,
  formatExecutionGateMarkdown,
  mdTable,
  escapeMarkdownCell,
  buildHotIndustriesMarkdown,
} from '@/lib/dashboard-format';
import {
  formatCondOrderDraftMarkdown,
  formatSinceLastCopyMarkdown,
  readLastCopyAt,
  type CondOrderCard,
  type CondOrderQuoteHint,
} from '@/lib/copy-ai-brief';
import {
  buildExecAttentionQueue,
  formatExecAttentionMarkdown,
  resolveAttentionCards,
} from '@/lib/exec-attention';
import { parseExecutionGate } from '@/lib/execution-action';
import {
  buildExecutionSnapshotPayload,
  fetchExecutionJournalMarkdown,
} from '@/lib/execution-journal';
import {
  fetchSourceContext,
  fetchSourceStats,
  formatSourceAttributionMarkdown,
  type SourceContext,
} from '@/lib/execution-source';
import { buildPositionsExecutionMarkdown, fetchPanicCooldown } from '@/lib/execution-markdown';
import {
  buildMainlineAllowSet,
  isSectorOutflowBlock,
  type MainlineAllowSet,
} from '@/lib/hot-industry-picks';
import { applyWatchlistPurgeAfterReport } from '@/lib/watchlist-purge';
import { buildDataFreshnessMarkdown, fetchDataSourcesHealth } from '@/lib/freshness';
import type {
  ExecutionChangeListResponse,
  ExecutionGate,
  ExecutionSnapshotListResponse,
} from '@karios/shared';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import {
  getShanghaiTodayIso,
  isShanghaiQuoteWindow,
  isShanghaiSyncWindow,
  isShanghaiTradingTime,
} from '@/lib/market-hours';
import { toTsCodeFromSymbol } from '@/lib/symbols';
import {
  dashboardLiteQueryKey,
  fetchDashboardLiteSummary,
  fetchDashboardSummaryPartial,
} from '@/lib/queries/dashboard';
import { watchlistMarketQueryOptions } from '@/lib/queries/watchlist';
import { fetchPortfolioHealth } from '@/lib/queries/portfolioHealth';
import { fetchWatchlistMarketSnapshot, fetchQuoteChunkWithRetry, type WatchlistMarketSnapshot } from '@/lib/watchlist-market';
import {
  parseQuoteNumber,
  shouldRequireRealtimeQuote,
  tradeDateFromTradeTime,
} from '@/lib/watchlist-metrics';
import { copyBlockingMissingData } from '@/lib/watchlist-export';
import { loadWatchlist, ensureWatchlistHydrated, type WatchlistItem } from '@/lib/watchlist-storage';

type DashboardSummary = any;


type QuoteResp = {
  ok: boolean;
  error?: string;
  items: Array<{
    ts_code: string;
    price?: string | number | null;
    pre_close?: string | number | null;
    pct_chg?: string | number | null;
    amount?: string | number | null;
    volume?: string | number | null;
    trade_time?: string | null;
  }>;
};

function parseDashboardQuoteItem(it: QuoteResp['items'][number]): {
  price: number | null;
  tradeTime: string | null;
  amount: number | null;
  volume: number | null;
  preClose: number | null;
  pctChg: number | null;
} {
  const p = it.price != null ? Number(it.price) : NaN;
  const pre = it.pre_close != null ? Number(it.pre_close) : NaN;
  const pct = it.pct_chg != null ? Number(it.pct_chg) : NaN;
  return {
    price: Number.isFinite(p) ? p : null,
    tradeTime: typeof it.trade_time === 'string' ? it.trade_time : null,
    amount: parseQuoteNumber(it.amount),
    volume: parseQuoteNumber(it.volume),
    preClose: Number.isFinite(pre) ? pre : null,
    pctChg: Number.isFinite(pct) ? pct : null,
  };
}

function loadWatchlistSymbols(): Set<string> {
  const itemsRaw = loadWatchlist();
  const items = (Array.isArray(itemsRaw) ? itemsRaw : [])
    .filter((x) => x && typeof x.symbol === 'string' && String(x.symbol).trim())
    .map((x) => String(x.symbol).trim().toUpperCase());
  return new Set(items);
}

function symbolsWithMissingTrendInputs(
  symbols: string[],
  trend: Record<string, TrendOkResult>,
): string[] {
  const missing: string[] = [];
  for (const sym of symbols) {
    const row = trend[sym];
    const blocking = copyBlockingMissingData(row?.missingData);
    if (blocking.length) missing.push(sym);
  }
  return missing;
}

async function fetchWatchlistSnapshotForCopy(
  symbols: string[],
  queryClient?: QueryClient,
  forceFresh = false,
): Promise<WatchlistMarketSnapshot> {
  if (!queryClient) {
    return fetchWatchlistMarketSnapshot(symbols, {
      forceMarket: false,
      realtime: isShanghaiQuoteWindow(),
    });
  }

  const options = watchlistMarketQueryOptions(symbols);
  const snapshot = forceFresh
    ? await queryClient.fetchQuery({ ...options, staleTime: 0 })
    : await queryClient.fetchQuery(options);
  const missingInputs = symbolsWithMissingTrendInputs(symbols, snapshot.trend);
  if (!missingInputs.length) return snapshot;

  const refreshed = await fetchWatchlistMarketSnapshot(symbols, {
    forceMarket: true,
    realtime: isShanghaiQuoteWindow(),
  });
  queryClient.setQueryData(options.queryKey, refreshed);
  return refreshed;
}

export function buildIndustryMarkdown(s: DashboardSummary | null, heading = '##'): string {
  const summary2: any = s ?? {};
  const ind: any = summary2?.industryFundFlow ?? {};
  const asOfDate = String(ind?.asOfDate ?? summary2?.asOfDate ?? '').trim();

  const datesAll: string[] = Array.isArray(ind?.dates) ? ind.dates : [];
  const rawShownDates = datesAll.slice(-5);
  const byDate = buildTopByDateMap(summary2);
  const { dedupedDates } = dedupeShownDates(rawShownDates, byDate);

  const lines: string[] = [];
  lines.push(`${heading} Industry fund flow`);
  if (asOfDate) lines.push(`- asOfDate: ${asOfDate}`);
  lines.push('');

  if (dedupedDates.length) {
    const headers1 = ['#', ...dedupedDates.map((d) => String(d).slice(5))];
    const rows1: unknown[][] = Array.from({ length: 5 }).map((_, i) => [
      i + 1,
      ...dedupedDates.map((d) => String((byDate[d] || [])[i] ?? '')),
    ]);
    lines.push(`${heading}# Top5×Date hotspots (names only)`);
    lines.push('');
    lines.push(mdTable(headers1, rows1));
    lines.push('');
  }

  const buildFlow = (block: any, title: string) => {
    const topRows: any[] = Array.isArray(block?.top) ? block.top : [];
    if (!topRows.length || !dedupedDates.length) return;
    const cols = dedupedDates;
    const headers = ['Industry', 'Sum(5D)', ...cols.map((d) => String(d).slice(5))];
    const rows: unknown[][] = topRows.slice(0, 10).map((r: any) => {
      const seriesArr: any[] = Array.isArray(r?.series) ? r.series : [];
      const m2: Record<string, number> = {};
      for (const p of seriesArr) {
        const dd = String(p?.date ?? '');
        const nv = Number(p?.netInflow ?? 0);
        if (dd) m2[dd] = Number.isFinite(nv) ? nv : 0;
      }
      return [
        String(r?.industryName ?? ''),
        fmtAmountCn(r?.sum5d),
        ...cols.map((d) => fmtAmountCn(m2[d] ?? 0)),
      ];
    });
    lines.push(`${heading}# ${title}`);
    lines.push('');
    lines.push(mdTable(headers, rows));
    lines.push('');
  };

  buildFlow(ind?.flow5d ?? null, '5D net inflow (Top by 5D sum)');
  buildFlow(ind?.flow5dOut ?? null, '5D net outflow (Top by 5D sum)');

  return lines.join('\n').trim() + '\n';
}

export function buildMarketAndMacroMarkdown(
  s: DashboardSummary | null,
  heading = '##',
): string {
  const summary2: any = s ?? {};
  const ms: any = summary2?.marketSentiment ?? {};
  const indexSignals: any[] = Array.isArray(ms?.indexSignals) ? ms.indexSignals : [];
  const macroSnapshot: any = summary2?.macroSnapshot ?? {};
  const macroItems: any[] = Array.isArray(macroSnapshot?.macro) ? macroSnapshot.macro : [];

  if (!indexSignals.length && !macroItems.length) return '';

  const lines: string[] = [];
  lines.push(`${heading} Market & Macro overview`);
  lines.push('');
  lines.push(
    '- note: 指数红绿灯 + 宏观商品/汇率 + 300ETF Put IV 一张表；避免散落重复',
  );

  const headers = ['Name', 'Kind', 'Signal', 'Pos', 'Chg%', 'Close', 'MA5', 'MA20', 'AsOfDate', 'Source'];
  const rows: unknown[][] = [];

  // B5: flag rows whose data is much older than the newest row (stale > 2 days).
  const allDates = [...indexSignals, ...macroItems]
    .map((it) => String(it?.asOfDate ?? '').slice(0, 10))
    .filter(Boolean)
    .sort();
  const latestAsOf = allDates[allDates.length - 1] ?? '';
  const staleDays = (asOf: string): number => {
    const a = Date.parse(`${asOf.slice(0, 10)}T00:00:00Z`);
    const b = Date.parse(`${latestAsOf}T00:00:00Z`);
    if (!Number.isFinite(a) || !Number.isFinite(b)) return 0;
    return Math.round((b - a) / 86_400_000);
  };
  let staleCount = 0;

  for (const it of indexSignals) {
    const pc = it?.pctChg;
    const chg =
      typeof pc === 'number' && Number.isFinite(pc)
        ? `${pc >= 0 ? '+' : ''}${pc.toFixed(2)}%`
        : '—';
    const name = String(it?.name ?? it?.tsCode ?? '');
    const asOf = String(it?.asOfDate ?? '').slice(0, 10);
    const days = staleDays(asOf);
    const staleTag = days > 2 ? ` ⚠️${days}d` : '';
    if (days > 2) staleCount += 1;
    rows.push([
      it?.featured === true ? `★ ${name}` : name,
      'Index',
      String(it?.signal ?? ''),
      String(it?.positionRange ?? '—'),
      chg,
      Number.isFinite(it?.close) ? Number(it.close).toFixed(2) : '—',
      Number.isFinite(it?.ma5) ? Number(it.ma5).toFixed(2) : '—',
      Number.isFinite(it?.ma20) ? Number(it.ma20).toFixed(2) : '—',
      `${asOf}${staleTag}`,
      String(it?.source ?? '—'),
    ]);
  }

  for (const it of macroItems) {
    const pct = it?.pctChg;
    const chg =
      typeof pct === 'number' && Number.isFinite(pct)
        ? `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`
        : '—';
    const isVol = it?.category === 'volatility';
    const closeStr = Number.isFinite(it?.close)
      ? isVol
        ? `${Number(it.close).toFixed(1)}%`
        : Number(it.close).toFixed(2)
      : '—';
    const signalStr = it?.signalLabel
      ? it.signalLabel
      : it?.signal
        ? String(it.signal)
        : '—';
    const kind = isVol ? 'Vol (IV)' : 'Macro';
    const asOf = String(it?.asOfDate ?? '').slice(0, 10);
    const days = staleDays(asOf);
    const staleTag = days > 2 ? ` ⚠️${days}d` : '';
    if (days > 2) staleCount += 1;
    rows.push([
      String(it?.name ?? it?.seriesId ?? ''),
      kind,
      signalStr,
      '—',
      chg,
      closeStr,
      Number.isFinite(it?.ma5) ? Number(it.ma5).toFixed(2) : '—',
      Number.isFinite(it?.ma20) ? Number(it.ma20).toFixed(2) : '—',
      `${asOf}${staleTag}`,
      String(it?.source ?? ''),
    ]);
  }

  if (staleCount > 0) {
    lines.push(`- note: ⚠️n = 数据比最新行旧 ${staleCount} 行，慎用（上次同步 ${latestAsOf}）`);
  }

  lines.push(mdTable(headers, rows));
  lines.push('');
  return lines.join('\n').trim() + '\n';
}

export function buildSentimentMarkdown(
  s: DashboardSummary | null,
  heading = '##',
  compact = false,
): string {
  const summary2: any = s ?? {};
  const ms: any = summary2?.marketSentiment ?? {};
  const items: any[] = Array.isArray(ms?.items) ? ms.items : [];
  const latest = items.length ? items[items.length - 1] : null;
  const asOfDate = String(ms?.asOfDate ?? summary2?.asOfDate ?? '').trim();
  const indexSignals: any[] = Array.isArray(ms?.indexSignals) ? ms.indexSignals : [];

  const lines: string[] = [];
  const envZh = String(summary2?.marketEnvironmentZh ?? '').trim();
  if (envZh) {
    lines.push(`${heading} 市场环境摘要`);
    lines.push('');
    lines.push(envZh);
    lines.push('');
  }
  lines.push(`${heading} Market sentiment`);
  if (asOfDate) lines.push(`- asOfDate: ${asOfDate}`);
  if (latest) {
    const risk = String(latest?.riskMode ?? '');
    if (risk) lines.push(`- risk: ${risk}`);
    if (risk === 'confirmed_uptrend') {
      lines.push('- ftd: triggered (右侧确立，死锁解除)');
    }
    const rulesArr: string[] = Array.isArray(latest?.rules)
      ? latest.rules.map((x: any) => String(x)).filter(Boolean)
      : [];
    if (
      risk === 'extreme_caution' ||
      rulesArr.some((r) => r.includes('macro_override_lock') || r.includes('breadth_panic'))
    ) {
      lines.push('- macroLock: active');
    }
    lines.push(`- ${formatSrvIndexLine(ms?.srvIndex)}`);
    const up = Number(latest?.upCount ?? 0);
    const down = Number(latest?.downCount ?? 0);
    if (up > 0 || down > 0) {
      lines.push(
        `- Market Breadth: ${up.toLocaleString()} Up / ${down.toLocaleString()} Down`,
      );
    }
    const total = fmtAmountCn(latest?.marketTurnoverCny);
    if (total && total !== '—') lines.push(`- totalTurnover: ${total}`);
    const rules = rulesArr;
    if (rules.length)
      lines.push(`- rules: ${rules.slice(0, 6).join(' • ')}${rules.length > 6 ? '…' : ''}`);
  }
  lines.push('');

  if (indexSignals.length) {
    // 2026-08-01: Index traffic lights moved into the unified Market & Macro
    // overview table (see buildMarketAndMacroMarkdown). Skip duplicate here.
  }

  const last5 = (items || []).slice(-5);
  const headers = ['date', 'up', 'down', 'flat', 'ratio', 'turnover', 'premium%', 'failed%', 'risk'];
  const rows: unknown[][] = last5.map((it: any) => [
    String(it?.date ?? ''),
    Number(it?.upCount ?? 0).toLocaleString(),
    Number(it?.downCount ?? 0).toLocaleString(),
    Number(it?.flatCount ?? 0).toLocaleString(),
    Number.isFinite(it?.upDownRatio) ? Number(it.upDownRatio).toFixed(2) : '—',
    fmtAmountCn(it?.marketTurnoverCny),
    Number.isFinite(it?.yesterdayLimitUpPremium)
      ? `${Number(it.yesterdayLimitUpPremium).toFixed(2)}%`
      : '—',
    Number.isFinite(it?.failedLimitUpRate) ? `${Number(it.failedLimitUpRate).toFixed(1)}%` : '—',
    String(it?.riskMode ?? ''),
  ]);
  lines.push(mdTable(headers, rows));
  lines.push('');

  const etfFlow: any = ms?.etfFundFlow ?? {};
  const etfFlowSignal: any = ms?.etfFlowSignal ?? null;
  const etfItems: any[] = Array.isArray(etfFlow?.items) ? etfFlow.items : [];
  if (etfItems.length) {
    if (etfFlowSignal && typeof etfFlowSignal === 'object') {
      const verdict = String(etfFlowSignal?.verdict ?? 'neutral');
      const broad = String(etfFlowSignal?.broadDirection ?? 'neutral');
      const sector = String(etfFlowSignal?.sectorDirection ?? 'neutral');
      lines.push(
        `- ETF flow confirmation: ${verdict} (broad=${broad}, sector=${sector}${etfFlowSignal?.incomplete ? ', incomplete' : ''})`,
      );
    }
    if (etfFlow?.shareLag) {
      lines.push(
        `- ETF realtime flow incomplete; missing rows are excluded from intraday signals (intradaySafe: ${String(etfFlow?.intradaySafe ?? false)})`,
      );
    }
    const etfHeaders = [
      'ETF Name',
      'Symbol',
      'Main Flow',
      '3D Net Flow',
      'Realtime AsOf',
      'Source',
      'Status',
      'Signal',
    ];
    const etfRows: unknown[][] = etfItems.map((it: any) => {
      const status = String(it?.flowStatus ?? (it?.live === true ? 'Live' : '—'));
      const live = it?.live === true || status === 'Live';
      const isMarketClosed = status === 'MarketClosed';
      const flow1dStale =
        !live &&
        !isMarketClosed &&
        it?.netFlow1d == null &&
        (it?.flowAsOfDate != null || it?.netFlow1dLagged != null);
      const flow1d = flow1dStale ? '— (stale)' : fmtSignedAmountCn(it?.netFlow1d);
      return [
        String(it?.name ?? ''),
        String(it?.symbol ?? ''),
        flow1d,
        fmtSignedAmountCn(it?.netFlow3d),
        String(it?.tradeTime ?? it?.flowAsOfDate ?? etfFlow?.asOfDate ?? '—'),
        String(it?.source ?? '—'),
        isMarketClosed ? 'Market Closed' : status,
        String(it?.signalDisplay ?? it?.signal ?? '—'),
      ];
    });
    const etfShown = compact ? etfRows.slice(0, 4) : etfRows;
    lines.push(`${heading} ETF Fund Flow (Top by 资金流，非仅持仓)`);
    lines.push('');
    if (compact && etfRows.length > etfShown.length) {
      lines.push('- note: compact mode — 仅保留 Top 4 ETF 资金流');
    }
    lines.push(mdTable(etfHeaders, etfShown));
    lines.push('');
  }

  const macroSnapshot: any = summary2?.macroSnapshot ?? {};
  const macroItems: any[] = Array.isArray(macroSnapshot?.macro) ? macroSnapshot.macro : [];
  void macroItems;
  // 2026-08-01: 300ETF Put IV moved into the unified Market & Macro overview table
  // (see buildMarketAndMacroMarkdown). Skip duplicate here.

  return lines.join('\n').trim() + '\n';
}

async function fetchBacktestOverviewDash(): Promise<{ cnBaseline?: { tag?: string; windows?: Record<string, { totalNetPnlPct?: number; winRate?: number; maxDrawdownPct?: number; sharpe?: number; trades?: number }> }; longWindowCN?: { window?: string; totalNetPnlPct?: number; maxDrawdownPct?: number; sharpe?: number; trades?: number } } | null> {
  try {
    const res = await fetch(`${DATA_SYNC_BASE_URL}/api/backtest/overview`, { cache: 'no-store' });
    if (!res.ok) return null;
    return (await res.json()) as never;
  } catch {
    return null;
  }
}
function buildStrategyAppendixDash(overview: Awaited<ReturnType<typeof fetchBacktestOverviewDash>>): string {
  const lines: string[] = [];
  lines.push('## 策略体系（固化口径 · 可复现）');
  lines.push('- S-3 定案（`docs/modules/strategy-params.md §1` · `service/paper_s3.py`）：score≥65 · RS前50% · regime非Weak · 主线白名单 · 移动止损-8%（Strong ATR×2） · 持有60天 · 不止盈 · 恐慌冷却2天 · 回撤熔断-25%（CN） · 单票10%×10=100% mp10 · 入场次日开盘（回测） · 创业板300排除');
  lines.push('- 港股 S-3（HK线）：regime闸 · RS前40% · trail-12% · 其余同A股；A/H独立');
  if (overview?.cnBaseline?.windows) {
    const w = overview.cnBaseline.windows as Record<string, { totalNetPnlPct?: number; winRate?: number; maxDrawdownPct?: number; sharpe?: number; trades?: number }>;
    const fmt = (k: string) => {
      const v = w[k];
      if (!v) return `${k} —`;
      return `${k} ${v.totalNetPnlPct?.toFixed(1) ?? '—'}% / DD${v.maxDrawdownPct?.toFixed(1) ?? '—'}% / 胜率${v.winRate != null ? (v.winRate * 100).toFixed(1) + '%' : '—'} / ${v.trades ?? '—'}笔`;
    };
    lines.push(`- 三窗（OOS2/train/valid · 100%现金≤1.0+0.7亿流动性 · ${overview.cnBaseline.tag ?? ''}）：${fmt('OOS2')} · ${fmt('train')} · ${fmt('valid')}`);
  }
  if (overview?.longWindowCN) {
    const l = overview.longWindowCN;
    lines.push(`- 长窗 ${l.window ?? '2021-08~2026-08'}：${l.totalNetPnlPct ?? '—'}% / DD${l.maxDrawdownPct ?? '—'}% / 夏普${l.sharpe ?? '—'} / ${l.trades ?? '—'}笔`);
  }
  lines.push('- 择强单轨定案（`docs/modules/pick-strong-track.md` · `GET /api/backtest/timeline` mode=mom_compare）：STOCK篮∪金/油/纳/债同权比t-1 mom60（ETF≥MA200），argmax 100%硬切，空档GC001；LB60·MA200·hold1');
  lines.push('- 多资产 sleeve：GOLD 518880·OIL 513350·NASDAQ 513100·BOND10 511260 · 多头轮动·可1.4×杠杆');
  lines.push('- 形态：strong_scoop_exhaustion 勺型耗尽顶 89-92%（ret60>0.4+放量）· 方向判别层');
  lines.push('');
  return lines.join('\n');
}

export function buildMacroMarkdown(s: DashboardSummary | null, heading = '##'): string {
  const summary2: any = s ?? {};
  const macroSnapshot: any = summary2?.macroSnapshot ?? {};
  const macroItems: any[] = Array.isArray(macroSnapshot?.macro) ? macroSnapshot.macro : [];

  if (!macroItems.length) return '';

  const lines: string[] = [];
  lines.push(`${heading} Macro indices`);
  lines.push('');

  const headers = ['Name', 'Close', 'Chg%', 'Signal', 'MA5', 'MA20', 'AsOfDate', 'Source'];
  const rows: unknown[][] = macroItems.map((it: any) => {
    const pct = it?.pctChg;
    const chg =
      typeof pct === 'number' && Number.isFinite(pct)
        ? `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`
        : '—';
    const isVol = it?.category === 'volatility';
    const closeStr = Number.isFinite(it?.close)
      ? isVol
        ? `${Number(it.close).toFixed(1)}%`
        : Number(it.close).toFixed(2)
      : '—';
    const signalStr = it?.signalLabel
      ? it.signalLabel
      : it?.signal
        ? String(it.signal)
        : '—';
    return [
      String(it?.name ?? it?.seriesId ?? ''),
      closeStr,
      chg,
      signalStr,
      Number.isFinite(it?.ma5) ? Number(it.ma5).toFixed(2) : '—',
      Number.isFinite(it?.ma20) ? Number(it.ma20).toFixed(2) : '—',
      String(it?.asOfDate ?? ''),
      String(it?.source ?? ''),
    ];
  });

  lines.push(mdTable(headers, rows));
  lines.push('');
  return lines.join('\n').trim() + '\n';
}

/** C8: compress long screener filters (e.g. the 18-sector whitelist) to save context. */
export async function buildWatchlistMarkdown(
  queryClient?: QueryClient,
  gate?: ExecutionGate | null,
  mainlineAllow?: MainlineAllowSet | null,
  sectorOutflowBlock = false,
  forceFresh = false,
): Promise<string> {
  const itemsRaw = loadWatchlist();
  const items: WatchlistItem[] = (Array.isArray(itemsRaw) ? itemsRaw : [])
    .filter((x) => x && typeof x.symbol === 'string' && String(x.symbol).trim())
    .map((x) => ({ ...x, symbol: String(x.symbol).trim().toUpperCase() }));

  const heading = '##';
  if (!items.length) {
    return `${heading} Combat Positions & Watchlist (Unified)\n\n- No watchlist items.\n`;
  }

  const syms = items.map((x) => x.symbol);
  const tradingTime = isShanghaiTradingTime();
  const quoteWindow = isShanghaiSyncWindow();
  const todaySh = getShanghaiTodayIso();

  let trend: Record<string, TrendOkResult>;
  let quotes: Record<
    string,
    {
      price: number | null;
      tradeTime: string | null;
      amount: number | null;
      volume: number | null;
      preClose: number | null;
      pctChg: number | null;
    }
  >;

  if (queryClient) {
    const snapshot = await fetchWatchlistSnapshotForCopy(syms, queryClient, forceFresh);
    trend = snapshot.trend;
    quotes = snapshot.quotes;
  } else {
    const byTsCode = new Map<string, string>();
    const tsCodes = syms
      .map((s) => {
        const t = toTsCodeFromSymbol(s);
        if (t) byTsCode.set(t, s);
        return t;
      })
      .filter(Boolean) as string[];
    const tsCodesChunks = chunk(tsCodes, 50);

    const [trendMap, quoteResults] = await Promise.all([
      fetchTrendOkMap(syms, { realtime: quoteWindow }),
      Promise.all(
        tsCodesChunks.map((part) => fetchQuoteChunkWithRetry(part.join(','))),
      ),
    ]);

    trend = Object.fromEntries(trendMap);
    quotes = {};
    for (const r of quoteResults) {
      for (const it of r?.items ?? []) {
        const sym = byTsCode.get(it.ts_code);
        if (!sym) continue;
        quotes[sym] = parseDashboardQuoteItem(it);
      }
    }
  }

  const sorted = [...items];
  sorted.sort((a, b) => {
    const sa = trend[a.symbol]?.score;
    const sb = trend[b.symbol]?.score;
    const va = typeof sa === 'number' && Number.isFinite(sa) ? sa : null;
    const vb = typeof sb === 'number' && Number.isFinite(sb) ? sb : null;
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    return vb - va;
  });

  const missingRealtime: string[] = [];
  const missingTrend: string[] = [];
  const missingHistory: string[] = [];
  for (const it of sorted) {
    const sym = it.symbol;
    const t = trend[sym];
    if (!t) {
      missingTrend.push(sym);
      continue;
    }
    const blockingMd = copyBlockingMissingData(t.missingData);
    if (blockingMd.length) {
      missingHistory.push(`${sym} (${blockingMd.join(', ')})`);
    }
    if (
      shouldRequireRealtimeQuote({
        tradingTime,
        symbol: sym,
        trendAsOfDate: t?.asOfDate ?? null,
        todaySh,
      })
    ) {
      const q = quotes[sym];
      const qDate = tradeDateFromTradeTime(q?.tradeTime ?? null);
      if (!(q && typeof q.price === 'number' && Number.isFinite(q.price) && qDate === todaySh)) {
        missingRealtime.push(sym);
      }
    }
  }
  if (missingTrend.length || missingHistory.length || missingRealtime.length) {
    const parts: string[] = [];
    if (missingRealtime.length)
      parts.push(
        `missing realtime quote (today): ${missingRealtime.slice(0, 6).join(', ')}${missingRealtime.length > 6 ? '…' : ''}`,
      );
    if (missingHistory.length)
      parts.push(
        `missing history/indicators: ${missingHistory.slice(0, 6).join(', ')}${missingHistory.length > 6 ? '…' : ''}`,
      );
    if (missingTrend.length)
      parts.push(
        `missing TrendOK result: ${missingTrend.slice(0, 6).join(', ')}${missingTrend.length > 6 ? '…' : ''}`,
      );
    throw new Error(`Copy aborted: ${parts.join(' | ')}`);
  }

  // Unified combat table only — fat Watchlist dump removed for LLM SNR.
  const catalystBySymbol = await fetchCatalystStocks(
    DATA_SYNC_BASE_URL,
    50,
    DEFAULT_CATALYST_MAX_AGE_DAYS,
  )
    .then((resp) => buildCatalystPurgeMap(resp))
    .catch(() => null);
  const rsRanks = await fetchRsRanks(sorted.map((i) => i.symbol)).catch(() => null);
  const panicCooldown = await fetchPanicCooldown();
  const health = await fetchPortfolioHealth().catch(() => null);
  const { markdown, purgeSymbols } = buildPositionsExecutionMarkdown(
    sorted,
    trend,
    quotes,
    gate ?? null,
    heading,
    mainlineAllow ?? null,
    tradingTime,
    todaySh,
    sectorOutflowBlock,
    catalystBySymbol,
    rsRanks,
    panicCooldown,
    health,
  );
  // Report still lists PURGE rows; remove them from storage for the next copy.
  if (purgeSymbols.length) {
    await applyWatchlistPurgeAfterReport(purgeSymbols).catch(() => 0);
  }
  return markdown.trim() + '\n';
}

export async function buildAlphaRadarCopyContext(
  s: DashboardSummary,
  catalystResp: CatalystStocksResponse,
): Promise<CatalystCopyContext> {
  const watchlistSymbols = loadWatchlistSymbols();
  // 2026-08-12: TV retired — no screener funnel symbols anymore.
  const catalystSymbols = catalystResp.items.map((row) => normalizeCatalystSymbol(row.symbol));
  const allSymbols = [
    ...new Set<string>([...watchlistSymbols, ...catalystSymbols]),
  ];

  const trendMapRaw = await fetchTrendOkMap(allSymbols, {
    realtime: isShanghaiTradingTime(),
  });

  const trendMap: CatalystCopyContext['trendMap'] = new Map();
  for (const [sym, trend] of trendMapRaw) {
    trendMap.set(sym, {
      symbol: sym,
      trendOk: trend.trendOk,
      score: trend.score ?? null,
    });
  }

  const watchlistScores = new Map<string, number>();
  for (const sym of watchlistSymbols) {
    const score = trendMap.get(sym)?.score;
    if (typeof score === 'number' && Number.isFinite(score)) {
      watchlistScores.set(sym, score);
    }
  }

  return {
    watchlistSymbols,
    watchlistScores,
    trendMap,
  };
}

export async function buildCompactCatalystMarkdown(s: DashboardSummary): Promise<string> {
  try {
    const resp = await fetchCatalystStocks(DATA_SYNC_BASE_URL, 10, DEFAULT_CATALYST_MAX_AGE_DAYS);
    const ctx = await buildAlphaRadarCopyContext(s, resp);
    return buildCatalystStocksMarkdown(resp, {
      headingLevel: '##',
      mode: 'compact',
      context: ctx,
    });
  } catch {
    return '## Alpha Radar · Top Catalyst Stocks\n\n- Alpha Radar: unavailable\n';
  }
}

export type DashboardCopyAllOptions = {
  summary: DashboardSummary | null;
  newsSummary?: string | null;
  newsSummaryUpdatedAt?: string | null;
  newsFallback?: string | null;
  queryClient?: QueryClient;
  /** Copy mode (2026-08-01): 'compact' trims ~80% of secondary sections for fast scan. */
  mode?: 'full' | 'compact';
  /** TIP-014: bypass react-query cache and re-fetch market/screener data before building. */
  forceFresh?: boolean;
};

type ExecutionCopyBundle = {
  attentionMd: string;
  cards: CondOrderCard[];
  quotes: Record<string, CondOrderQuoteHint>;
  sourceStatsMd: string;
};

async function buildExecutionCopyBundle(opts: {
  gate: ExecutionGate | null;
  mainlineAllow: MainlineAllowSet | null;
  sectorOutflowBlock?: boolean;
  queryClient?: QueryClient;
  forceFresh?: boolean;
}): Promise<ExecutionCopyBundle> {
  const { gate, mainlineAllow, sectorOutflowBlock = false, queryClient, forceFresh = false } = opts;
  const itemsRaw = loadWatchlist();
  const items: WatchlistItem[] = (Array.isArray(itemsRaw) ? itemsRaw : [])
    .filter((x) => x && typeof x.symbol === 'string' && String(x.symbol).trim())
    .map((x) => ({ ...x, symbol: String(x.symbol).trim().toUpperCase() }));

  let liveCards: CondOrderCard[] | null = null;
  let quotes: Record<string, CondOrderQuoteHint> = {};
  let sourceContext: SourceContext | null = null;
  let trendMap: Record<string, unknown> = {};
  if (gate && items.length) {
    try {
      const symbols = items.map((i) => i.symbol);
      const market = await fetchWatchlistSnapshotForCopy(symbols, queryClient, forceFresh);
      trendMap = market.trend;
      const nameBySym = new Map(items.map((i) => [i.symbol, i.name ?? null]));
      quotes = {};
      for (const sym of symbols) {
        const q = market.quotes[sym];
        quotes[sym] = {
          preClose: q?.preClose ?? null,
          name: nameBySym.get(sym) ?? null,
        };
      }
      const catalystBySymbol = await fetchCatalystStocks(
        DATA_SYNC_BASE_URL,
        50,
        DEFAULT_CATALYST_MAX_AGE_DAYS,
      )
        .then((resp) => buildCatalystPurgeMap(resp))
        .catch(() => null);
      sourceContext = await fetchSourceContext(DATA_SYNC_BASE_URL).catch(() => null);
      const payload = buildExecutionSnapshotPayload({
        items,
        trend: market.trend,
        quotes: market.quotes,
        gate,
        mainlineAllow,
        sectorOutflowBlock,
        catalystBySymbol,
        sourceContext,
        source: 'poll',
      });
      liveCards = (payload?.cards as CondOrderCard[] | undefined) ?? null;
    } catch {
      liveCards = null;
      quotes = {};
    }
  } else if (gate && !items.length) {
    liveCards = [];
  }

  let snapshotCards: CondOrderCard[] = [];
  try {
    const td = getShanghaiTodayIso();
    const snaps = await apiGetJson<ExecutionSnapshotListResponse>(
      `/execution/snapshots?trade_date=${encodeURIComponent(td)}&limit=1`,
    );
    const latest = snaps?.items?.[0];
    if (latest && Array.isArray(latest.cards)) {
      snapshotCards = latest.cards as CondOrderCard[];
    }
  } catch {
    // ignore
  }

  let changes: ExecutionChangeListResponse['items'] = [];
  try {
    const td = getShanghaiTodayIso();
    const ch = await apiGetJson<ExecutionChangeListResponse>(
      `/execution/changes?trade_date=${encodeURIComponent(td)}&limit=100`,
    );
    changes = Array.isArray(ch?.items) ? ch.items : [];
  } catch {
    changes = [];
  }

  const { cards, source } = resolveAttentionCards({ liveCards, snapshotCards });
  const queue = buildExecAttentionQueue({
    gate,
    watchlistItems: items.map((it) => ({
      ...it,
      trendok: trendMap[it.symbol] ?? null,
    })),
    cards,
    changes,
  });
  let sourceStatsMd = '';
  try {
    sourceStatsMd = formatSourceAttributionMarkdown(
      await fetchSourceStats(DATA_SYNC_BASE_URL, 30),
      { heading: '##' },
    );
  } catch {
    sourceStatsMd = '';
  }
  return {
    attentionMd: formatExecAttentionMarkdown(queue, { heading: '##', source }),
    cards: cards as CondOrderCard[],
    quotes,
    sourceStatsMd,
  };
}

async function buildSinceLastCopyMarkdown(): Promise<string> {
  const lastAt = readLastCopyAt();
  const td = getShanghaiTodayIso();
  const params = new URLSearchParams();
  params.set('trade_date', td);
  params.set('limit', '100');
  if (lastAt) params.set('since', lastAt);
  let changes: ExecutionChangeListResponse['items'] = [];
  try {
    const ch = await apiGetJson<ExecutionChangeListResponse>(
      `/execution/changes?${params.toString()}`,
    );
    changes = Array.isArray(ch?.items) ? ch.items : [];
  } catch {
    changes = [];
  }
  return formatSinceLastCopyMarkdown(changes, { lastAt, heading: '##' });
}

export async function buildDashboardCopyAllMarkdown(
  options: DashboardCopyAllOptions,
): Promise<string> {
  await ensureWatchlistHydrated();
  let { summary: s } = options;
  const { newsSummary, newsSummaryUpdatedAt, newsFallback, queryClient, mode = 'full', forceFresh = false } = options;
  const compact = mode === 'compact';
  if (!s) {
    throw new Error('No data available. Please refresh first.');
  }
  // TIP-014: force fresh dashboard summary when requested (bypasses cache).
  if (forceFresh && queryClient) {
    try {
      s = await queryClient.fetchQuery({
        queryKey: dashboardLiteQueryKey(),
        queryFn: () => fetchDashboardLiteSummary(),
        staleTime: 0,
      });
    } catch {
      // keep provided summary on refresh failure
    }
  }
  const macroItems = (s as any)?.macroSnapshot?.macro;
  if (!Array.isArray(macroItems) || macroItems.length === 0) {
    try {
      const macroPartial = await fetchDashboardSummaryPartial({
        includeMacro: true,
        includeSentiment: false,
        includeNews: false,
        includeIndustry: false,
        includeScreeners: false,
      });
      if (macroPartial?.macroSnapshot) {
        s = { ...s, macroSnapshot: macroPartial.macroSnapshot };
      }
    } catch {
      // keep existing summary
    }
  }
  const sentimentItems = (s as any)?.marketSentiment?.items;
  if (!Array.isArray(sentimentItems) || sentimentItems.length === 0) {
    try {
      const sentimentPartial = await fetchDashboardSummaryPartial({
        includeMacro: false,
        includeSentiment: true,
        includeNews: false,
        includeIndustry: false,
        includeScreeners: false,
      });
      if (sentimentPartial?.marketSentiment) {
        s = { ...s, marketSentiment: sentimentPartial.marketSentiment };
      }
    } catch {
      // keep existing summary
    }
  }
  const generatedAt = new Date().toLocaleString('zh-CN', {
    timeZone: 'Asia/Shanghai',
    hour12: false,
  });
  const executionGate = parseExecutionGate((s as any)?.marketSentiment?.executionGate);
  const mainlineAllow = buildMainlineAllowSet(s);
  const sectorOutflowBlock = isSectorOutflowBlock(s);
  const tradingTime = isShanghaiTradingTime();
  const [watchlistMd, catalystMd, alphaTrendsMd, execBundle, sinceLastMd, strategyMd] =
    await Promise.all([
      buildWatchlistMarkdown(queryClient, executionGate, mainlineAllow, sectorOutflowBlock, forceFresh),
      buildCompactCatalystMarkdown(s),
      fetchAlphaRadarTrendsForCopy(DATA_SYNC_BASE_URL, 20, DEFAULT_CATALYST_MAX_AGE_DAYS)
        .then(({ items, scope }) =>
          buildAlphaRadarTrendsMarkdown(items, {
            headingLevel: '##',
            mode: compact ? 'compact' : 'full',
            scopeNote:
              scope === 'recent'
                ? `recent ${DEFAULT_CATALYST_MAX_AGE_DAYS}d (latest batch empty)`
                : 'latest batch',
          }),
        )
        .catch(() => '## Alpha Radar · Structured Trends\n\n- Alpha Radar trends: unavailable\n'),
      buildExecutionCopyBundle({
        gate: executionGate,
        mainlineAllow,
        sectorOutflowBlock,
        queryClient,
        forceFresh,
      }).catch(
        (): ExecutionCopyBundle => ({
          attentionMd: formatExecAttentionMarkdown(
            buildExecAttentionQueue({
              gate: executionGate,
              watchlistItems: [],
              cards: [],
              changes: [],
            }),
            { source: 'none' },
          ),
          cards: [],
          quotes: {},
          sourceStatsMd: '',
        }),
      ),
      buildSinceLastCopyMarkdown().catch(() =>
        formatSinceLastCopyMarkdown([], { lastAt: readLastCopyAt() }),
      ),
      fetchBacktestOverviewDash()
        .then((ov) => buildStrategyAppendixDash(ov))
        .catch(() => ''),
    ]);
  const autoQaMd = buildAutoQaMarkdown(
    await fetchAutoQaStats(DATA_SYNC_BASE_URL, 7, 20),
  );
  const attentionMd = execBundle.attentionMd;
  const condOrderMd = formatCondOrderDraftMarkdown(execBundle.cards, {
    heading: '##',
    allowNewEntries: executionGate?.allowNewEntries === true,
    tradingTime,
    phase: tradingTime ? 'Open' : 'Closed',
    quotes: execBundle.quotes,
  });
  const lines: string[] = [];
  lines.push(`# Copy all (Dashboard)`);
  lines.push(`- generatedAt: ${generatedAt}`);
  lines.push(`- asOfDate: ${String((s as any)?.asOfDate ?? '')}`);
  lines.push('');
  // 策略体系置顶：外部 agent 第一眼即知回测口径与择强规则
  if (strategyMd.trim()) {
    lines.push(strategyMd.trim());
    lines.push('');
  }
  // TIP-013: per-source freshness header (stale sources flagged for the agent).
  try {
    const health = await fetchDataSourcesHealth();
    const freshnessMd = buildDataFreshnessMarkdown(
      Array.isArray(health?.sources) ? health.sources : [],
    );
    if (freshnessMd.trim()) {
      lines.push(freshnessMd.trim());
      lines.push('');
    }
  } catch {
    lines.push('## Data freshness');
    lines.push('- unavailable (health endpoint not reachable)');
    lines.push('');
  }
  // AI behavior lives in System Prompt (docs/modules/downstream-ai-prompt.md); payload stays data-only.
  lines.push(sinceLastMd.trim());
  lines.push('');
  lines.push(
    formatExecutionGateMarkdown(
      parseExecutionGate((s as any)?.marketSentiment?.executionGate) ??
        (s as any)?.marketSentiment?.executionGate,
      '##',
    ).trim(),
  );
  lines.push('');
  lines.push(attentionMd.trim());
  lines.push('');
  lines.push(condOrderMd.trim());
  lines.push('');
  try {
    const journalMd = await fetchExecutionJournalMarkdown({
      tradeDate: getShanghaiTodayIso(),
      days: 5,
    });
    if (journalMd.trim()) {
      lines.push(journalMd.trim());
      lines.push('');
    }
  } catch {
    lines.push('## Decision Journal');
    lines.push('- note: unavailable (capture snapshots via Dashboard Sync All or Snapshot now)');
    lines.push('');
  }
  if (execBundle.sourceStatsMd.trim()) {
    lines.push(execBundle.sourceStatsMd.trim());
    lines.push('');
  }
  // 持仓与执行是 agent 最关心的“现在该做什么”，紧跟 Gate/Attention 之后
  lines.push(watchlistMd.trim());
  lines.push('');
  if (!compact) {
    lines.push(buildIndustryMarkdown(s, '##').trim());
    lines.push('');
    lines.push(buildHotIndustriesMarkdown(s, '##').trim());
    lines.push('');
  } else {
    // Compact mode: keep only the headline industry fund flow summary (no Top5×Date grid).
    lines.push(
      buildIndustryMarkdown(s, '##')
        .split('\n')
        .slice(0, 6)
        .concat(['- note: compact mode — full industry tables omitted', ''])
        .join('\n'),
    );
    lines.push('');
    lines.push('- note: compact mode — Hot industries workflow omitted');
    lines.push('');
  }
  lines.push(buildSentimentMarkdown(s, '##', compact).trim());
  lines.push('');
  lines.push(buildMarketAndMacroMarkdown(s, '##').trim());
  lines.push('');
  lines.push('## News brief');
  lines.push('');
  lines.push(`- hours: ${String((s as any)?.news?.hours ?? 24)}`);
  const newsItemsCount = Array.isArray((s as any)?.news?.items)
    ? (s as any).news.items.length
    : 0;
  const newsTotal = Number((s as any)?.news?.total ?? 0);
  const usingFallback = !newsSummary?.trim() && newsFallback?.trim();
  if (usingFallback) {
    lines.push(`- total: ${String(newsItemsCount || newsTotal || 0)}`);
    lines.push(
      `- note: AI 摘要未生成/未更新（${newsSummaryUpdatedAt ? `上次 ${newsSummaryUpdatedAt}` : '无'}）— 以下为原始标题回退`,
    );
  } else {
    const summaryItemCount = (newsSummary?.match(/^\s*\d+\./gm) ?? []).length;
    lines.push(`- total: ${String(summaryItemCount || newsTotal || 0)}`);
  }
  lines.push(
    '- note: 关键词白名单过滤（AI/算力/半导体/美联储/降准降息/原油/关税/…）；其他娱乐/边缘政治新闻剔除',
  );
  if (newsSummaryUpdatedAt) lines.push(`- summaryUpdatedAt: ${newsSummaryUpdatedAt}`);
  lines.push('');
  let newsBody = '';
  if (newsSummary?.trim()) newsBody = newsSummary.trim();
  else if (newsFallback?.trim()) newsBody = newsFallback.trim();
  if (compact && newsBody) {
    const numbered = newsBody.split('\n').filter((l) => /^\s*\d+\./.test(l));
    if (numbered.length > 8) {
      newsBody = numbered.slice(0, 8).join('\n');
      lines.push('- note: compact mode — 仅保留前 8 条新闻，完整列表见 News 页');
    }
  }
  if (newsBody) lines.push(newsBody);
  else lines.push('No summary yet. Last news records are included above.');
  lines.push('');
  lines.push(alphaTrendsMd.trim());
  lines.push('');
  if (autoQaMd) {
    lines.push(autoQaMd.trim());
    lines.push('');
  }
  lines.push(catalystMd.trim());
  lines.push('');
  return lines.join('\n').trim() + '\n';
}

/**
 * Incremental snapshot for the Decision Agent: only what changed since the last
 * reference (freshness + since-last-copy + Gate + Exec Attention + Cond orders +
 * Journal). Full tables (watchlist / macro / news / industry) are intentionally
 * omitted — the live active layer (L1) injects them on every LLM call.
 */
export async function buildCopyDeltaMarkdown(opts: {
  summary: DashboardSummary | null;
  queryClient?: QueryClient;
  forceFresh?: boolean;
}): Promise<string> {
  const { summary: s, queryClient, forceFresh = false } = opts;
  const executionGate = parseExecutionGate((s as any)?.marketSentiment?.executionGate);
  const mainlineAllow = buildMainlineAllowSet(s);
  const sectorOutflowBlock = isSectorOutflowBlock(s);
  const tradingTime = isShanghaiTradingTime();
  const [sinceLastMd, journalMd, freshnessMd] = await Promise.all([
    buildSinceLastCopyMarkdown().catch(() =>
      formatSinceLastCopyMarkdown([], { lastAt: readLastCopyAt() }),
    ),
    fetchExecutionJournalMarkdown({ tradeDate: getShanghaiTodayIso(), days: 5 }).catch(
      () => '',
    ),
    fetchDataSourcesHealth()
      .then((h) =>
        buildDataFreshnessMarkdown(Array.isArray(h?.sources) ? h.sources : []),
      )
      .catch(() => ''),
  ]);
  const execBundle = await buildExecutionCopyBundle({
    gate: executionGate,
    mainlineAllow,
    sectorOutflowBlock,
    queryClient,
    forceFresh,
  }).catch(() => null);

  const lines: string[] = [];
  lines.push('# 增量快照（自上次引用以来的变更）');
  lines.push(
    '- note: 完整操作表 / 宏观 / 新闻等全量数据由「决策活跃层」在每次对话时实时注入，此处只附增量与待办，不重复全量',
  );
  lines.push('');
  if (freshnessMd.trim()) {
    lines.push(freshnessMd.trim());
    lines.push('');
  }
  if (sinceLastMd.trim()) {
    lines.push(sinceLastMd.trim());
    lines.push('');
  }
  lines.push(
    formatExecutionGateMarkdown(
      executionGate ??
        ((s as any)?.marketSentiment?.executionGate as ExecutionGate | null) ??
        null,
      '##',
    ).trim(),
  );
  lines.push('');
  if (execBundle) {
    if (execBundle.attentionMd.trim()) {
      lines.push(execBundle.attentionMd.trim());
      lines.push('');
    }
    lines.push(
      formatCondOrderDraftMarkdown(execBundle.cards, {
        heading: '##',
        allowNewEntries: executionGate?.allowNewEntries === true,
        tradingTime,
        phase: tradingTime ? 'Open' : 'Closed',
        quotes: execBundle.quotes,
      }).trim(),
    );
    lines.push('');
    if (execBundle.sourceStatsMd.trim()) {
      lines.push(execBundle.sourceStatsMd.trim());
      lines.push('');
    }
  }
  if (journalMd.trim()) {
    lines.push(journalMd.trim());
    lines.push('');
  }
  return lines.join('\n').trim();
}

async function fetchRsRanks(symbols: string[]): Promise<Record<string, number> | null> {
  if (!symbols.length) return null;
  try {
    const q = encodeURIComponent(symbols.join(','));
    const res = await fetch(`${DATA_SYNC_BASE_URL}/watchlist/rs-ranks?symbols=${q}`, { cache: 'no-store' });
    if (!res.ok) return null;
    const d = (await res.json()) as { ranks?: Record<string, number> };
    return d.ranks ?? null;
  } catch {
    return null;
  }
}
