import type { QueryClient } from '@tanstack/react-query';

import {
  buildCatalystPurgeMap,
  DEFAULT_CATALYST_MAX_AGE_DAYS,
  fetchCatalystStocks,
} from '@/lib/alpha-radar-catalyst';
import type { TrendOkResult, WatchlistQuote } from '@/lib/api/types';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import { buildPositionsExecutionMarkdown } from '@/lib/execution-markdown';
import { fetchPanicCooldown } from '@/lib/execution-markdown';
import type { MainlineAllowSet } from '@/lib/hot-industry-picks';
import { getShanghaiTodayIso, isShanghaiTradingTime } from '@/lib/market-hours';
import { fetchPortfolioHealth } from '@/lib/queries/portfolioHealth';
import { refetchWatchlistMarket } from '@/lib/queries/watchlist';
import {
  shouldRequireRealtimeQuote,
  tradeDateFromTradeTime,
} from '@/lib/watchlist-metrics';
import { applyWatchlistPurgeAfterReport } from '@/lib/watchlist-purge';
import type { WatchlistItem } from '@/lib/watchlist-storage';
import type { ExecutionGate } from '@karios/shared';
import type { CatalystPurgeHint } from '@/lib/execution-action';

async function loadCatalystPurgeMap(): Promise<Map<string, CatalystPurgeHint> | null> {
  try {
    const resp = await fetchCatalystStocks(
      DATA_SYNC_BASE_URL,
      50,
      DEFAULT_CATALYST_MAX_AGE_DAYS,
    );
    return buildCatalystPurgeMap(resp);
  } catch {
    return null;
  }
}

const COPY_BLOCKING_MISSING_DATA = new Set([
  'no_bars',
  'bars_lt_60',
  'insufficient_indicators',
  'unsupported_market',
  'no_result',
]);

export function copyBlockingMissingData(missingData: string[] | undefined | null): string[] {
  const md = Array.isArray(missingData) ? missingData.filter(Boolean) : [];
  return md.filter((reason) => COPY_BLOCKING_MISSING_DATA.has(reason));
}

export type WatchlistCopyValidationError = {
  ok: false;
  message: string;
};

export type WatchlistCopyBuildResult = {
  ok: true;
  markdown: string;
};

export type WatchlistCopyResult = WatchlistCopyValidationError | WatchlistCopyBuildResult;

export function validateWatchlistCopyData(options: {
  sortedItems: WatchlistItem[];
  trendSnap: Record<string, TrendOkResult>;
  quotesSnap: Record<string, WatchlistQuote>;
  tradingTime: boolean;
  todaySh: string;
}): WatchlistCopyValidationError | null {
  const { sortedItems, trendSnap, quotesSnap, tradingTime, todaySh } = options;
  const missingRealtime: string[] = [];
  const missingTrend: string[] = [];
  const missingHistory: string[] = [];

  for (const it of sortedItems) {
    const sym = it.symbol;
    const t = trendSnap[sym];
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
      const q = quotesSnap[sym];
      const qDate = tradeDateFromTradeTime(q?.tradeTime ?? null);
      if (!(q && typeof q.price === 'number' && Number.isFinite(q.price) && qDate === todaySh)) {
        missingRealtime.push(sym);
      }
    }
  }

  if (!missingTrend.length && !missingHistory.length && !missingRealtime.length) {
    return null;
  }

  const parts: string[] = [];
  if (missingRealtime.length) {
    parts.push(
      `missing realtime quote (today): ${missingRealtime.slice(0, 6).join(', ')}${
        missingRealtime.length > 6 ? '…' : ''
      }`,
    );
  }
  if (missingHistory.length) {
    parts.push(
      `missing history/indicators: ${missingHistory.slice(0, 6).join(', ')}${
        missingHistory.length > 6 ? '…' : ''
      }`,
    );
  }
  if (missingTrend.length) {
    parts.push(
      `missing TrendOK result: ${missingTrend.slice(0, 6).join(', ')}${
        missingTrend.length > 6 ? '…' : ''
      }`,
    );
  }
  return { ok: false, message: `Copy aborted: ${parts.join(' | ')}` };
}

async function fetchBacktestOverview(): Promise<{
  cnBaseline?: { windows?: Record<string, { totalNetPnlPct?: number; winRate?: number; maxDrawdownPct?: number; sharpe?: number; trades?: number }>; tag?: string };
  longWindowCN?: { window?: string; totalNetPnlPct?: number; maxDrawdownPct?: number; sharpe?: number; trades?: number };
} | null> {
  try {
    const res = await fetch(`${DATA_SYNC_BASE_URL}/api/backtest/overview`, { cache: 'no-store' });
    if (!res.ok) return null;
    const j = (await res.json()) as { cnBaseline?: unknown; longWindowCN?: unknown };
    return j as never;
  } catch {
    return null;
  }
}

function buildSystemAppendix(overview: Awaited<ReturnType<typeof fetchBacktestOverview>>): string {
  const lines: string[] = [];
  lines.push('## 策略体系（固化口径 · 可复现）');
  lines.push('- S-3 定案（`docs/modules/strategy-params.md §1` · `service/paper_s3.py`）：score≥65 · RS前50% · regime非Weak · 主线白名单 · 移动止损-8%（Strong日ATR×2） · 持有60天 · 不止盈 · 恐慌冷却2天 · 回撤熔断-25%（CN） · 单票10%×10=100%（mp10） · 入场次日开盘（回测） · 创业板300排除');
  lines.push('- 港股 S-3（HK线）：regime闸 · RS前40% · trail-12% · 其余同A股；A/H独立核算');
  if (overview?.cnBaseline?.windows) {
    const w = overview.cnBaseline.windows as Record<string, { totalNetPnlPct?: number; winRate?: number; maxDrawdownPct?: number; sharpe?: number; trades?: number }>;
    const fmt = (k: string) => {
      const v = w[k];
      if (!v) return `${k} —`;
      return `${k} ${v.totalNetPnlPct?.toFixed(1) ?? '—'}% / DD${v.maxDrawdownPct?.toFixed(1) ?? '—'}% / 胜率${v.winRate != null ? (v.winRate * 100).toFixed(1) + '%' : '—'} / ${v.trades ?? '—'}笔`;
    };
    lines.push(`- 三窗（OOS2/train/valid · 100%现金≤1.0 +0.7亿流动性 · walk_forward_baseline.json ${overview.cnBaseline.tag ?? ''}）：${fmt('OOS2')} · ${fmt('train')} · ${fmt('valid')}`);
  }
  if (overview?.longWindowCN) {
    const l = overview.longWindowCN;
    lines.push(`- 长窗 ${l.window ?? '2021-08~2026-08'}：${l.totalNetPnlPct ?? '—'}% / DD${l.maxDrawdownPct ?? '—'}% / 夏普${l.sharpe ?? '—'} / ${l.trades ?? '—'}笔（全市场 5226 · 含回撤熔断）`);
  }
  lines.push('- 择强单轨定案（`docs/modules/pick-strong-track.md` · `GET /api/backtest/timeline` mode=mom_compare）：STOCK篮 ∪ 金518880/油513350/纳指513100·513110/债511260 同权比 t-1 mom60（ETF须≥MA200），argmax 100%硬切，空档 GC001；LB60·MA200·hold1');
  lines.push('- S-3 = 股票腿生成器（非终局产品）；多资产腿规则在 `multi_asset_sleeve.py`，live pick 同 mom_compare');
  lines.push('- 形态因子（`ml_forecast/morphology.py strong_scoop_exhaustion`）：强股勺型耗尽顶≥80%（ret60>0.4+放量 89-92%胜率）· 方向判别层，不改S-3');
  lines.push('- 数据：Postgres + Alembic（`alembic upgrade head`）· score全市场日更17:30/10:30/14:00 · TrendOK=信号真值');
  lines.push('');
  return lines.join('\n');
}

export async function buildWatchlistMarkdown(options: {
  sortedItems: WatchlistItem[];
  trendSnap: Record<string, TrendOkResult>;
  quotesSnap: Record<string, WatchlistQuote>;
  trendUpdatedAt: string | null;
  tradingTime: boolean;
  todaySh: string;
  executionGate?: ExecutionGate | null;
  mainlineAllow?: MainlineAllowSet | null;
  sectorOutflowBlock?: boolean;
}): Promise<string> {
  const {
    sortedItems,
    trendSnap,
    quotesSnap,
    tradingTime,
    todaySh,
    executionGate = null,
    mainlineAllow = null,
    sectorOutflowBlock = false,
  } = options;
  // Same unified combat table as Dashboard Copy all (no separate fat Watchlist dump).
  // Sync helper: no catalyst fetch (PURGE exemption only on Copy / Sync&Copy paths).
  const rsRanks = await fetchRsRanks(sortedItems.map((i) => i.symbol));
  const panicCooldown = await fetchPanicCooldown();
  const health = await fetchPortfolioHealth().catch(() => null);
  const overview = await fetchBacktestOverview().catch(() => null);
  const { markdown } = buildPositionsExecutionMarkdown(
    sortedItems,
    trendSnap,
    quotesSnap,
    executionGate ?? null,
    '##',
    mainlineAllow ?? null,
    tradingTime,
    todaySh,
    sectorOutflowBlock,
    null,
    rsRanks,
    panicCooldown,
    health,
  );
  const appendix = buildSystemAppendix(overview);
  return (markdown.trim() + '\n\n' + appendix).trim() + '\n';
}

export async function copyWatchlistMarkdown(options: {
  queryClient: QueryClient;
  sortedItems: WatchlistItem[];
  trend: Record<string, TrendOkResult>;
  quotes: Record<string, WatchlistQuote>;
  trendUpdatedAt: string | null;
  executionGate?: ExecutionGate | null;
  mainlineAllow?: MainlineAllowSet | null;
  sectorOutflowBlock?: boolean;
}): Promise<WatchlistCopyResult> {
  const {
    queryClient,
    sortedItems,
    trend,
    quotes,
    trendUpdatedAt,
    executionGate = null,
    mainlineAllow = null,
    sectorOutflowBlock = false,
  } = options;
  if (!sortedItems.length) {
    return { ok: false, message: 'No items to copy.' };
  }

  const tradingTime = isShanghaiTradingTime();
  const todaySh = getShanghaiTodayIso();
  const syms = sortedItems.map((x) => x.symbol);
  let trendSnap: Record<string, TrendOkResult>;
  let quotesSnap: Record<string, WatchlistQuote>;

  try {
    const fresh = await refetchWatchlistMarket(queryClient, syms, { forceMarket: false });
    trendSnap = fresh.trend;
    quotesSnap = fresh.quotes;
  } catch (e) {
    console.warn('Watchlist copy refresh failed, using cached data:', e);
    trendSnap = trend;
    quotesSnap = quotes;
  }

  const validationError = validateWatchlistCopyData({
    sortedItems,
    trendSnap,
    quotesSnap,
    tradingTime,
    todaySh,
  });
  if (validationError) return validationError;

  const catalystBySymbol = await loadCatalystPurgeMap();
  const panicCooldown = await fetchPanicCooldown();
  const overview = await fetchBacktestOverview().catch(() => null);
  const appendix = buildSystemAppendix(overview);
  const { markdown, purgeSymbols } = buildPositionsExecutionMarkdown(
    sortedItems,
    trendSnap,
    quotesSnap,
    executionGate ?? null,
    '##',
    mainlineAllow ?? null,
    tradingTime,
    todaySh,
    sectorOutflowBlock,
    catalystBySymbol,
    null,
    panicCooldown,
  );
  if (purgeSymbols.length) {
    await applyWatchlistPurgeAfterReport(purgeSymbols).catch(() => 0);
  }
  const full = (markdown.trim() + '\n\n' + appendix).trim() + '\n';
  return { ok: true, markdown: full };
}

/** Fetch whole-market RS percentiles for the S-3 candidate block. */
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
