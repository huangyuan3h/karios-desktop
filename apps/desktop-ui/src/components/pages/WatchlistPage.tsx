'use client';

import * as React from 'react';
import { useQueryClient } from '@tanstack/react-query';
import {
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  CircleX,
  ExternalLink,
  Play,
  RefreshCw,
  Trash2,
} from 'lucide-react';
import { createPortal } from 'react-dom';

import { Button } from '@/components/ui/button';
import { Switch } from '@/components/ui/switch';
import { apiGetJson } from '@/lib/api/client';
import type { TrendOkResult, WatchlistQuote } from '@/lib/api/types';
import {
  getShanghaiTodayIso,
  isShanghaiTradingTime,
} from '@/lib/market-hours';
import {
  refetchWatchlistMarket,
  useWatchlistMarketQuery,
  watchlistMarketKey,
} from '@/lib/queries/watchlist';
import {
  fetchAutomationLatest,
  formatAutomationSummary,
  runManualAutomation,
  type AutomationRun,
} from '@/lib/watchlist-automation';
import { importFromScreener } from '@/lib/watchlist-screener-import';
import {
  ensureWatchlistHydrated,
  loadWatchlist,
  saveWatchlist,
  WATCHLIST_UPDATED_EVENT,
  type WatchlistItem,
} from '@/lib/watchlist-storage';
import { useChatStore } from '@/lib/chat/store';
import {
  WATCHLIST_MD_HEADERS,
  buildWatchlistRowMetrics,
  collectWatchlistRiskAlerts,
  computePnLPct,
  formatGapUp,
  formatHotTop3,
  formatIntradayChgPct,
  formatPnLPct,
  formatRiskAlerts,
  formatVwap,
  hasBlockingWatchlistRisk,
  industryDisplayName,
  isIntradaySurge,
  resolveWatchlistCurrentPrice,
  rowHasWatchlistRiskHighlight,
  shouldRequireRealtimeQuote,
  tradeDateFromTradeTime,
  tushareIndustryTooltip,
  type WatchlistRiskAlert,
} from '@/lib/watchlist-metrics';


const COST_PRICE_RE = /^\d+(\.\d{0,2})?$/;

const FLAG_COLORS: Array<{ label: string; hex: string }> = [
  { label: 'White', hex: '#ffffff' },
  { label: 'Red', hex: '#fee2e2' },
  { label: 'Orange', hex: '#ffedd5' },
  { label: 'Yellow', hex: '#fef9c3' },
  { label: 'Green', hex: '#dcfce7' },
  { label: 'Blue', hex: '#dbeafe' },
  { label: 'Purple', hex: '#f3e8ff' },
  { label: 'Gray', hex: '#f4f4f5' },
];

type WatchlistRowTone = 'green' | 'red' | 'none';
type WatchlistStickyColumn = 'score' | 'trendOk' | 'action';

const WATCHLIST_STICKY_COLUMN_LAYOUT: Record<
  WatchlistStickyColumn,
  { width: number; right: number; zHeader: number; zBody: number }
> = {
  score: { width: 80, right: 168, zHeader: 23, zBody: 13 },
  trendOk: { width: 80, right: 88, zHeader: 22, zBody: 12 },
  action: { width: 88, right: 0, zHeader: 25, zBody: 15 },
};

function watchlistStickyRowBg(tone: WatchlistRowTone, header = false): string {
  if (header) return 'bg-[var(--k-surface)]';
  // Sticky cells must be fully opaque so scrolled columns do not show through.
  if (tone === 'green') {
    return 'bg-emerald-50 group-hover:bg-emerald-100 dark:bg-emerald-950 dark:group-hover:bg-emerald-900';
  }
  if (tone === 'red') {
    return 'bg-red-50 group-hover:bg-red-100 dark:bg-red-950 dark:group-hover:bg-red-900';
  }
  return 'bg-[var(--k-surface)] group-hover:bg-[var(--k-surface-2)]';
}

function watchlistStickyCellClass(
  column: WatchlistStickyColumn,
  opts: { header?: boolean; tone?: WatchlistRowTone; extra?: string } = {},
): string {
  const tone = opts.tone ?? 'none';
  const parts = [
    'sticky',
    watchlistStickyRowBg(tone, opts.header),
    'px-3 py-2',
    column === 'score' ? 'shadow-[-4px_0_8px_rgba(0,0,0,0.06)]' : '',
    opts.extra ?? '',
  ];
  return parts.filter(Boolean).join(' ');
}

function watchlistStickyCellStyle(
  column: WatchlistStickyColumn,
  opts: { header?: boolean } = {},
): React.CSSProperties {
  const layout = WATCHLIST_STICKY_COLUMN_LAYOUT[column];
  return {
    right: layout.right,
    minWidth: layout.width,
    width: layout.width,
    zIndex: opts.header ? layout.zHeader : layout.zBody,
  };
}

function escapeMarkdownCell(value: string): string {
  return String(value ?? '')
    .replace(/\|/g, '\\|')
    .replace(/\r?\n/g, '<br>')
    .trim();
}

function mdBool(v: boolean | null | undefined): string {
  if (v == null) return '—';
  return v ? '✅' : '❌';
}

function mdNum(v: number | null | undefined, digits = 2): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return v.toFixed(digits);
}

function mdScore(v: number | null | undefined): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return String(Math.round(v));
}

function mdPrice(v: number | null | undefined): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return v.toFixed(2);
}

function mdLines(items: string[]): string {
  return items.filter((x) => String(x || '').trim()).join('\n');
}

function VisibilitySection({
  visible,
  className,
  children,
}: {
  visible: boolean;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <div
      className={className}
      style={{ display: visible ? 'block' : 'none' }}
      aria-hidden={!visible}
    >
      {children}
    </div>
  );
}

const TREND_OK_CHECKS: Array<{ key: keyof TrendOkChecks; failText: string }> = [
  { key: 'emaOrder', failText: 'EMA order broken (Close <= EMA20 or EMA20 <= EMA60)' },
  { key: 'macdPositive', failText: 'MACD <= 0' },
  { key: 'macdHistExpanding', failText: 'MACD hist <= 0' },
  { key: 'closeNear20dHigh', failText: 'Close < 0.90 * High(20)' },
  { key: 'rsiInRange', failText: 'RSI(14) out of 50..90' },
  { key: 'volumeSurge', failText: 'AvgVol(5) < 0.9 * AvgVol(30)' },
];

function trendOkSummary(t?: TrendOkResult | null): string {
  if (!t) return '—';
  if (t.trendOk === true) return '✅';
  const checks = t.checks ?? null;
  if (!checks || typeof checks !== 'object') return t.trendOk === false ? '❌' : '—';
  const failed: string[] = [];
  for (const rule of TREND_OK_CHECKS) {
    const val = (checks as TrendOkChecks)[rule.key];
    if (val === false) failed.push(rule.failText);
  }
  if (failed.length) return failed.join('; ');
  return t.trendOk === false ? '❌' : '—';
}

function trendOkRuleLines(): string[] {
  return [
    '- Close > EMA20 and EMA20 > EMA60',
    '- MACD line > 0',
    '- MACD histogram > 0',
    '- Close >= 0.90 * High(20)',
    '- RSI(14) in [50, 90]',
    '- AvgVol(5) >= 0.9 * AvgVol(30)',
  ];
}

/**
 * Score (0–100) rules for UI / Markdown; aligned with
 * `services/data-sync-service/.../trendok.py` (`_trendok_one` score block).
 */
function scoreExplainZhLines(): string[] {
  return [
    'Score 为 0～100 的确定性公式分（A 股日线、无 LLM）。先算「基础分」并限制在 0～100；若有行业资金流上下文，再累加行业调整 delta，再次限制在 0～100。',
    '基础分 = 五项加权子分之和 + EMA20 五日正斜率奖励 − Anti-Spike 剥离惩罚。每项子分先把信号压到 0～1，再乘以「100 × 该项权重」。',
    '权重：EMA 趋势连贯 40%；MACD 动能稳定 20%；量能一致性 20%；突破平滑 10%；RSI 舒适带 10%。',
    'EMA：EMA5>EMA20（0.4）+ EMA20>EMA60（0.4）+ EMA20 日斜率>0.1%（0.2），合计 0～1 后乘 40 分。',
    'MACD：MACD 线 <0 时该项为 0；否则需 MACD 柱连续 2 日为正且今日柱>昨日柱，满分映射后乘 20 分。',
    'Breakout：收盘价 ÷ 近 20 日最高价，从约 0.85～1.0 线性映射到 0～1（clip）后乘 10 分。',
    'RSI：以 RSI=65 为最高分，随 |RSI−65| 增大线性衰减（15 点尺度 clip）后乘 10 分；RSI>80 额外加速衰减。',
    'Volume：AvgVol5÷AvgVol30，[1.2, 2.0] 满分 20；<1.0 按比例衰减；>3.0 子分为 0。',
    '右侧加分：EMA20 连续 5 日上升 → +5（scoreParts 中 bonus_ema20_slope_5d）。',
    'Anti-Spike 剥离：① 日内涨幅>6% → −20（penalty_intraday_spike）。② ATR14/收盘价>5% 起按 (ratio−0.05)×1000  steep 扣分（penalty_volatility_atr）。③ 当日量/AvgVol30>3 → −15（penalty_volume_climax）。④ 收盘<EMA20 → −30（penalty_below_ema20）。',
    '行业资金流（可选）：如 5 日净流入行业 Top3 +10、当日热点 Top3 +5、Top4–5 +3；5 日弱势榜等可 −10～−20；细节以返回的 scoreParts 与 industryFlowReasons 为准。',
  ];
}

type MarketStockBasicRow = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
};

type TrendOkChecks = {
  emaOrder?: boolean | null;
  macdPositive?: boolean | null;
  macdHistExpanding?: boolean | null;
  closeNear20dHigh?: boolean | null;
  rsiInRange?: boolean | null;
  volumeSurge?: boolean | null;
};

type TrendOkValues = {
  close?: number | null;
  ema5?: number | null;
  ema20?: number | null;
  ema60?: number | null;
  macd?: number | null;
  macdSignal?: number | null;
  macdHist?: number | null;
  macdHist4?: number[];
  rsi14?: number | null;
  high20?: number | null;
  avgVol5?: number | null;
  avgVol30?: number | null;
  industry?: string | null;
  emIndustry?: string | null;
  industryFlowReasons?: string[];
};

type ScreenerImportDebugState = {
  updatedAt: string | null;
  scanned: number;
  trendOkCount: number;
  rows: TrendOkResult[];
};

function normalizeSymbolInput(input: string): { symbol: string } | { error: string } {
  const raw = (input || '').trim().toUpperCase();
  if (!raw) return { error: 'Empty input' };

  // Accept already-normalized market prefix forms.
  // Examples: "CN:600000", "HK:0700"
  if (/^(CN|HK):[0-9A-Z.\-]{1,16}$/.test(raw)) {
    return { symbol: raw };
  }

  // CN A-share ticker (6 digits)
  if (/^\d{6}$/.test(raw)) {
    return { symbol: `CN:${raw}` };
  }

  // HK ticker (4-5 digits), allow leading zeros
  if (/^\d{4,5}$/.test(raw)) {
    return { symbol: `HK:${raw.padStart(4, '0')}` };
  }

  return {
    error:
      'Unsupported code format. Use 6-digit CN ticker, 4-5 digit HK ticker, or CN:/HK: prefixed symbol.',
  };
}

function fmtPrice(v: number | null | undefined): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return v.toFixed(2);
}

function fmtScore(v: number | null | undefined): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return String(Math.round(v));
}

function fmtNum(v: unknown, digits = 2): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return v.toFixed(digits);
}

function isGreenZoneSignal(x: unknown): boolean {
  const s = String(x ?? '').trim();
  return s === 'green' || s === 'light_green' || s === 'deep_green';
}

function isHotspotTop3Industry(t: TrendOkResult | undefined | null): boolean {
  if (!t) return false;
  const reasonsRaw = (t.values as Record<string, unknown> | undefined)?.industryFlowReasons;
  const reasons = Array.isArray(reasonsRaw) ? reasonsRaw.map((x) => String(x ?? '')) : [];
  if (reasons.includes('hotspots_today_top3')) return true;
  const parts = t.scoreParts as Record<string, unknown> | null | undefined;
  const v = parts?.hotspots_today_top3;
  return typeof v === 'number' && Number.isFinite(v) && v > 0;
}

function isGreenZoneMarket(t: TrendOkResult | undefined | null): boolean {
  if (!t) return false;
  // TrendOK service always returns marketRegime (Strong/Diverging/Weak).
  const regime = String(
    (t as TrendOkResult & { marketRegime?: unknown }).marketRegime ?? '',
  ).trim();
  if (regime === 'Strong') return true;

  // Forward-compatible fallback in case backend later passes raw index signals in buyChecks.
  const checks = t.buyChecks as Record<string, unknown> | null | undefined;
  if (!checks || typeof checks !== 'object') return false;
  const directCandidates = [
    checks.marketSignal,
    checks.signal,
    checks.sseSignal,
    checks.cybSignal,
    checks.sse_signal,
    checks.cyb_signal,
  ];
  if (directCandidates.some((x) => isGreenZoneSignal(x))) return true;
  const indexSignals = checks.indexSignals;
  if (Array.isArray(indexSignals)) {
    for (const it of indexSignals) {
      if (isGreenZoneSignal((it as Record<string, unknown>)?.signal)) return true;
    }
  }
  return false;
}

function shouldForceNoAPullbackWait(t: TrendOkResult | undefined | null): boolean {
  if (!t) return false;
  const score = typeof t.score === 'number' && Number.isFinite(t.score) ? t.score : null;
  if (score == null || score <= 90) return false;
  if (t.buyMode !== 'A_pullback' || t.buyAction !== 'wait') return false;
  if (!isGreenZoneMarket(t)) return false;
  if (!isHotspotTop3Industry(t)) return false;
  return true;
}

function fmtBuyCell(t: TrendOkResult | undefined | null): {
  text: string;
  tone: 'buy' | 'wait' | 'avoid' | 'none';
  forced?: boolean;
  forcedReason?: string;
} {
  if (!t || !t.buyMode || !t.buyAction) return { text: '—', tone: 'none' };
  if (shouldForceNoAPullbackWait(t)) {
    return {
      text: 'B 买 强势热点',
      tone: 'buy',
      forced: true,
      forcedReason: 'Green zone + score>90 + industry in today top3: A_pullback/wait is blocked.',
    };
  }
  if (t.buyAction === 'avoid') return { text: '回避', tone: 'avoid' };
  const zl = typeof t.buyZoneLow === 'number' ? t.buyZoneLow : null;
  const zh = typeof t.buyZoneHigh === 'number' ? t.buyZoneHigh : null;
  const zone =
    zl != null && zh != null
      ? `${zl.toFixed(2)}–${zh.toFixed(2)}`
      : zl != null
        ? `${zl.toFixed(2)}`
        : '—';
  if (t.buyMode === 'A_pullback') {
    const prefix = t.buyAction === 'buy' ? 'A 买' : 'A 等';
    return { text: `${prefix} 回踩 ${zone}`, tone: t.buyAction === 'buy' ? 'buy' : 'wait' };
  }
  if (t.buyMode === 'B_momentum') {
    const prefix = t.buyAction === 'buy' ? 'B 买' : 'B 等';
    return { text: `${prefix} 新高 ${zone}`, tone: t.buyAction === 'buy' ? 'buy' : 'wait' };
  }
  return { text: '无', tone: 'none' };
}

function rowTone(
  t: TrendOkResult | undefined | null,
  alerts: WatchlistRiskAlert[] = [],
): 'green' | 'red' | 'none' {
  if (!t) return 'none';
  const stopParts = t.stopLossParts as Record<string, unknown> | null | undefined;
  const exitNow = Boolean(stopParts && typeof stopParts === 'object' && stopParts['exit_now']);
  if (exitNow || t.buyAction === 'avoid' || hasBlockingWatchlistRisk(alerts)) return 'red';
  if (rowHasWatchlistRiskHighlight(alerts)) return 'red';
  const score = typeof t.score === 'number' && Number.isFinite(t.score) ? t.score : null;
  const buy = fmtBuyCell(t);
  const buyModeOk = t.buyMode === 'A_pullback' || t.buyMode === 'B_momentum';
  if (t.trendOk === true && buy.tone === 'buy' && buyModeOk && score != null && score >= 85) {
    return 'green';
  }
  return 'none';
}

export function WatchlistPage({ onOpenStock }: { onOpenStock?: (symbol: string) => void } = {}) {
  const { addReference } = useChatStore();
  const queryClient = useQueryClient();
  const [items, setItems] = React.useState<WatchlistItem[]>([]);
  const [watchlistHydrating, setWatchlistHydrating] = React.useState(true);
  const [code, setCode] = React.useState('');
  const [error, setError] = React.useState<string | null>(null);
  const symbols = React.useMemo(
    () => items.map((x) => x.symbol).filter(Boolean),
    [items],
  );
  const marketQuery = useWatchlistMarketQuery(symbols);
  const trend = marketQuery.data?.trend ?? {};
  const quotes = marketQuery.data?.quotes ?? {};
  const trendBusy = marketQuery.isFetching;
  const trendUpdatedAt = marketQuery.dataUpdatedAt
    ? new Date(marketQuery.dataUpdatedAt).toISOString()
    : null;
  const [syncBusy, setSyncBusy] = React.useState(false);
  const [syncMsg, setSyncMsg] = React.useState<string | null>(null);
  const [syncStage, setSyncStage] = React.useState<string | null>(null);
  const [syncProgress, setSyncProgress] = React.useState<{ cur: number; total: number } | null>(
    null,
  );
  const [syncLogs, setSyncLogs] = React.useState<string[]>([]);
  const [automationBusy, setAutomationBusy] = React.useState(false);
  const [automationStage, setAutomationStage] = React.useState<string | null>(null);
  const [automationLogs, setAutomationLogs] = React.useState<string[]>([]);
  const [automationMsg, setAutomationMsg] = React.useState<string | null>(null);
  const [latestAutomation, setLatestAutomation] = React.useState<AutomationRun | null>(null);
  const [automationSkipRun, setAutomationSkipRun] = React.useState<AutomationRun | null>(null);
  const [copyMdStatus, setCopyMdStatus] = React.useState<{ ok: boolean; text: string } | null>(
    null,
  );
  const [copyMdBusy, setCopyMdBusy] = React.useState(false);
  const copyMdTimerRef = React.useRef<number | null>(null);

  // Keep the last screener import inspection table visible for manual follow-ups.
  const [importDebugOpen, setImportDebugOpen] = React.useState(true);
  const [importDebugFilter, setImportDebugFilter] = React.useState('');
  const [importDebugScoreSortDir, setImportDebugScoreSortDir] = React.useState<'desc' | 'asc'>(
    'desc',
  );
  const [importDebug, setImportDebug] = React.useState<ScreenerImportDebugState>({
    updatedAt: null,
    scanned: 0,
    trendOkCount: 0,
    rows: [],
  });

  const [scoreSortDir, setScoreSortDir] = React.useState<'desc' | 'asc'>('desc');
  const [scoreSortEnabled, setScoreSortEnabled] = React.useState(true);
  const [costPriceDrafts, setCostPriceDrafts] = React.useState<Record<string, string>>({});
  const nameBySymbol = React.useMemo(() => {
    const map = new Map<string, string>();
    for (const it of items) {
      if (it.symbol) map.set(it.symbol, it.name || '');
    }
    return map;
  }, [items]);
  const [tooltip, setTooltip] = React.useState<{
    open: boolean;
    x: number;
    y: number;
    w: number;
    placement: 'top-end' | 'bottom-end';
    content: React.ReactNode;
  }>({ open: false, x: 0, y: 0, w: 0, placement: 'top-end', content: null });

  const [colorPicker, setColorPicker] = React.useState<{
    open: boolean;
    x: number;
    y: number;
    placement: 'top-end' | 'bottom-end';
    symbol: string | null;
  }>({ open: false, x: 0, y: 0, placement: 'bottom-end', symbol: null });

  React.useEffect(() => {
    void fetchAutomationLatest()
      .then((run) => {
        if (run) setLatestAutomation(run);
      })
      .catch(() => {
        // ignore
      });
  }, []);

  React.useEffect(() => {
    function onExternalUpdate() {
      setItems(loadWatchlist());
    }
    window.addEventListener(WATCHLIST_UPDATED_EVENT, onExternalUpdate);
    return () => window.removeEventListener(WATCHLIST_UPDATED_EVENT, onExternalUpdate);
  }, []);

  React.useEffect(() => {
    let cancelled = false;
    void ensureWatchlistHydrated()
      .then(() => {
        if (!cancelled) setItems(loadWatchlist());
      })
      .finally(() => {
        if (!cancelled) setWatchlistHydrating(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  React.useEffect(
    () => () => {
      if (copyMdTimerRef.current) window.clearTimeout(copyMdTimerRef.current);
    },
    [],
  );

  function persist(next: WatchlistItem[]) {
    setItems(next);
    void saveWatchlist(next);
  }

  React.useEffect(() => {
    let cancelled = false;
    async function resolveMissingNames() {
      const missing = items
        .filter((x) => !x.name && x.nameStatus !== 'not_found')
        .map((x) => x.symbol);
      if (!missing.length) return;

      try {
        const sp = new URLSearchParams();
        for (const s of missing) sp.append('symbols', s);
        const rows = await apiGetJson<MarketStockBasicRow[]>(
          `/market/stocks/resolve?${sp.toString()}`,
        );
        if (cancelled) return;
        const bySym = new Map<string, MarketStockBasicRow>();
        for (const r of Array.isArray(rows) ? rows : []) bySym.set(r.symbol, r);

        const next = items.map((it) => {
          if (it.name || it.nameStatus === 'resolved') return it;
          const hit = bySym.get(it.symbol);
          if (hit) return { ...it, name: hit.name, nameStatus: 'resolved' as const };
          if (missing.includes(it.symbol)) return { ...it, nameStatus: 'not_found' as const };
          return it;
        });
        persist(next);
      } catch (e) {
        // If Market is not synced or service is unavailable, keep silent; user can still manage codes.
        if (!cancelled) console.warn('Watchlist name resolve failed:', e);
      }
    }
    void resolveMissingNames();
    return () => {
      cancelled = true;
    };
  }, [items]);

  React.useEffect(() => {
    if (!marketQuery.data) return;
    const nextQuotes = marketQuery.data.quotes;
    const next = marketQuery.data.trend;
    const nextItems = items.map((it) => {
      if (!(it.positionPct && it.positionPct > 0)) return it;
      if (!it.costPrice) return it;
      const q = nextQuotes[it.symbol];
      const price =
        typeof q?.price === 'number' && Number.isFinite(q.price)
          ? q.price
          : typeof next[it.symbol]?.values?.close === 'number'
            ? next[it.symbol]?.values?.close
            : null;
      if (price == null) return it;
      const maxPrice = typeof it.maxPrice === 'number' ? it.maxPrice : 0;
      if (price > maxPrice) return { ...it, maxPrice: price };
      if (!it.maxPrice) return { ...it, maxPrice: price };
      return it;
    });
    if (nextItems.some((x, i) => x.maxPrice !== items[i]?.maxPrice)) {
      persist(nextItems);
    }
  }, [marketQuery.data, items]);

  async function onManualRefreshTrend() {
    if (!symbols.length) return;
    setSyncMsg(null);
    try {
      const snapshot = await refetchWatchlistMarket(queryClient, symbols, { forceMarket: true });
      if (snapshot.barSync && snapshot.barSync.failures > 0) {
        setSyncMsg(
          `Network sync failed for ${snapshot.barSync.failures}/${snapshot.barSync.total} symbols; using cached data.`,
        );
      }
      setError(null);
    } catch (e) {
      console.warn('Watchlist trendok load failed:', e);
    }
  }

  function addSymbolToWatchlist(symRaw: string) {
    setError(null);
    setSyncMsg(null);
    const parsed = normalizeSymbolInput(symRaw);
    if ('error' in parsed) {
      setError(parsed.error);
      return;
    }
    const sym = parsed.symbol;
    if (items.some((x) => x.symbol === sym)) return;
    const next: WatchlistItem[] = [
      {
        symbol: sym,
        name: null,
        addedAt: new Date().toISOString(),
        color: '#ffffff',
      },
      ...items,
    ];
    persist(next);
  }

  function onAdd() {
    setError(null);
    setSyncMsg(null);
    const parsed = normalizeSymbolInput(code);
    if ('error' in parsed) {
      setError(parsed.error);
      return;
    }
    const sym = parsed.symbol;
    if (items.some((x) => x.symbol === sym)) {
      setError('Already in watchlist.');
      return;
    }
    const next: WatchlistItem[] = [
      {
        symbol: sym,
        name: null,
        addedAt: new Date().toISOString(),
        color: '#ffffff',
      },
      ...items,
    ];
    persist(next);
    setCode('');
  }

  function onRemove(sym: string) {
    setSyncMsg(null);
    persist(items.filter((x) => x.symbol !== sym));
  }

  async function onSyncFromScreener() {
    setError(null);
    setSyncMsg(null);
    setSyncBusy(true);
    setSyncStage('Loading enabled screeners');
    setSyncProgress(null);
    setSyncLogs([]);
    setImportDebugFilter('');

    const pushLog = (line: string) => {
      setSyncLogs((prev) => [...prev, line].slice(-6));
    };
    try {
      const result = await importFromScreener({
        existingItems: items,
        onStage: (label, cur, total) => {
          setSyncStage(label);
          if (typeof cur === 'number' && typeof total === 'number') {
            setSyncProgress({ cur, total });
          } else {
            setSyncProgress(null);
          }
          pushLog(
            label +
              (typeof cur === 'number' && typeof total === 'number' ? ` (${cur}/${total})` : ''),
          );
        },
      });
      setImportDebug(result.debug as ScreenerImportDebugState);
      setSyncMsg(result.message);
      if (result.addedCount > 0) {
        setItems(loadWatchlist());
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSyncBusy(false);
      setSyncStage(null);
      setSyncProgress(null);
    }
  }

  async function onRunAutomation(force = true) {
    setError(null);
    setAutomationMsg(null);
    setAutomationBusy(true);
    setAutomationStage('Starting automation…');
    setAutomationLogs([]);
    const pushLog = (line: string) => {
      setAutomationLogs((prev) => [...prev, line].slice(-6));
    };
    try {
      const { run, result } = await runManualAutomation({
        force,
        onStage: (label) => {
          setAutomationStage(label);
          pushLog(label);
        },
      });
      setLatestAutomation(run);
      if (run.skipped) {
        setAutomationMsg(`Skipped: ${run.skipReason || 'unknown'}`);
        setAutomationSkipRun(run);
        return;
      }
      setAutomationSkipRun(null);
      setItems(loadWatchlist());
      void queryClient.invalidateQueries({ queryKey: watchlistMarketKey(symbols) });
      const summary = formatAutomationSummary(run, result ?? null);
      setAutomationMsg(summary);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setAutomationBusy(false);
      setAutomationStage(null);
    }
  }

  async function onForceAutomationFromSkip() {
    if (!automationSkipRun) return;
    void onRunAutomation(true);
  }

  function showTooltip(el: HTMLElement, content: React.ReactNode, width = 360) {
    // Render via portal to avoid clipping, but anchor near the hovered element.
    // Place the tooltip at the element's top-right corner (top-end). If there isn't
    // enough room above, flip to bottom-end.
    const r = el.getBoundingClientRect();
    const pad = 12;
    const w = Math.min(width, Math.max(240, window.innerWidth - pad * 2));
    const x = Math.max(pad, Math.min(window.innerWidth - w - pad, r.right - w));
    const preferTop = r.top > 140;
    const placement: 'top-end' | 'bottom-end' = preferTop ? 'top-end' : 'bottom-end';
    const y = preferTop
      ? Math.max(pad, r.top - 8)
      : Math.min(window.innerHeight - pad, r.bottom + 8);
    setTooltip({ open: true, x, y, w, placement, content });
  }

  function hideTooltip() {
    setTooltip((prev) => (prev.open ? { ...prev, open: false } : prev));
  }

  function showColorPicker(el: HTMLElement, sym: string) {
    // Anchor near the clicked button, but clamp within viewport.
    // Flip to open upward when near the bottom to keep all items clickable.
    const r = el.getBoundingClientRect();
    const pad = 10;
    const panelW = 220;
    const panelH = 220; // heuristic, enough for header + 2 rows of 4 color buttons

    const x0 = r.right - panelW;
    const x = Math.max(pad, Math.min(window.innerWidth - panelW - pad, x0));

    const shouldOpenDown = r.bottom + 8 + panelH <= window.innerHeight - pad;
    const placement: 'top-end' | 'bottom-end' = shouldOpenDown ? 'bottom-end' : 'top-end';

    // y is the anchor point. For top-end, we use translateY(-100%) so y refers to the bottom edge.
    let y = placement === 'bottom-end' ? r.bottom + 8 : r.top - 8;
    if (placement === 'bottom-end') {
      y = Math.max(pad, Math.min(window.innerHeight - panelH - pad, y));
    } else {
      // Ensure y is not so small that the panel would go above the viewport when translated.
      y = Math.max(pad + panelH, Math.min(window.innerHeight - pad, y));
    }

    setColorPicker({ open: true, x, y, placement, symbol: sym });
  }

  function hideColorPicker() {
    setColorPicker((prev) => (prev.open ? { ...prev, open: false, symbol: null } : prev));
  }

  function setItemColor(symbol: string, color: string) {
    const next = items.map((it) => (it.symbol === symbol ? { ...it, color } : it));
    persist(next);
  }

  function setItemPositionPct(symbol: string, value: string) {
    const raw = value.trim();
    const num = raw === '' ? null : Number(raw);
    const nextVal =
      typeof num === 'number' && Number.isFinite(num) ? Math.max(0, Math.min(100, num)) : null;
    const next = items.map((it) => (it.symbol === symbol ? { ...it, positionPct: nextVal } : it));
    persist(next);
  }

  function setItemCostPriceValue(symbol: string, value: number | null) {
    const nextVal =
      typeof value === 'number' && Number.isFinite(value) ? Math.round(value * 100) / 100 : null;
    const next = items.map((it) =>
      it.symbol === symbol ? { ...it, costPrice: nextVal, maxPrice: nextVal ?? it.maxPrice } : it,
    );
    persist(next);
  }

  function setItemCostPriceDraft(symbol: string, value: string) {
    setCostPriceDrafts((prev) => ({ ...prev, [symbol]: value }));
  }

  function commitItemCostPriceDraft(symbol: string) {
    const raw = costPriceDrafts[symbol];
    setCostPriceDrafts((prev) => {
      const next = { ...prev };
      delete next[symbol];
      return next;
    });
    if (raw == null) return;
    const trimmed = raw.trim();
    if (!trimmed) {
      setItemCostPriceValue(symbol, null);
      return;
    }
    const num = Number(trimmed);
    if (Number.isFinite(num)) {
      setItemCostPriceValue(symbol, num);
    }
  }

  React.useEffect(() => {
    if (!colorPicker.open) return;
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === 'Escape') hideColorPicker();
    }
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [colorPicker.open]);

  function checkLine(label: string, ok: boolean | null | undefined, detail: string) {
    if (ok == null) return { label, state: '—', detail };
    return { label, state: ok ? '✅' : '❌', detail };
  }

  function renderTrendOkCell(sym: string) {
    const t = trend[sym];
    const ok = t?.trendOk ?? null;
    const icon = ok == null ? '—' : ok ? '✅' : '❌';
    const rsiNow =
      typeof t?.values?.rsi14 === 'number' && Number.isFinite(t.values.rsi14)
        ? t.values.rsi14
        : null;
    const h4 =
      Array.isArray(t?.values?.macdHist4) && t?.values?.macdHist4?.length === 4
        ? t.values.macdHist4
        : null;
    const hpos = h4 ? h4.map((x) => Math.max(0, Number(x))) : null;
    const d1 = hpos ? hpos[1] > hpos[0] : null;
    const d2 = hpos ? hpos[2] > hpos[1] : null;
    const d3 = hpos ? hpos[3] > hpos[2] : null;
    const hLastPos = hpos ? hpos[3] > 0 : null;
    const macdHistDetail = h4
      ? `need h_last>0: ${hLastPos ? '✅' : '❌'}; d1 ${d1 ? '✅' : '❌'}; d2 ${
          d2 ? '✅' : '❌'
        }; d3 ${d3 ? '✅' : '❌'} (h: ${h4
          .map((x) => (Number.isFinite(Number(x)) ? Number(x).toFixed(3) : '—'))
          .join(', ')})`
      : 'need last 4 histogram values';
    const lines = [
      checkLine('EMA trend', t?.checks?.emaOrder ?? null, 'Close > EMA(20) AND EMA(20) > EMA(60)'),
      checkLine('MACD > 0', t?.checks?.macdPositive ?? null, 'macdLine > 0'),
      checkLine(
        'MACD hist',
        t?.checks?.macdHistExpanding ?? null,
        `histogram > 0 (red bar above zero axis). Expansion is scored separately; ${macdHistDetail}`,
      ),
      checkLine('Near 20D high', t?.checks?.closeNear20dHigh ?? null, 'Close >= 0.90 * High(20)'),
      checkLine(
        'RSI(14)',
        t?.checks?.rsiInRange ?? null,
        `50 <= RSI <= 90${rsiNow == null ? '' : ` (now: ${rsiNow.toFixed(1)})`}`,
      ),
      checkLine('Volume', t?.checks?.volumeSurge ?? null, 'AvgVol(5) > 0.9 * AvgVol(30)'),
    ];
    const missing = (t?.missingData ?? []).filter(Boolean);
    const tip = (
      <>
        <div className="mb-2 flex items-center justify-between">
          <div className="font-medium">TrendOK checks</div>
          <div className="font-mono text-[var(--k-muted)]">{sym}</div>
        </div>
        <div className="space-y-1">
          {lines.map((x) => (
            <div key={x.label} className="flex items-start justify-between gap-3">
              <div className="text-[var(--k-muted)]">{x.label}</div>
              <div className="flex-1 text-right">
                <span className="font-mono">{x.state}</span>{' '}
                <span className="text-[var(--k-muted)]">{x.detail}</span>
              </div>
            </div>
          ))}
        </div>
        {missing.length ? (
          <div className="mt-2 text-[var(--k-muted)]">
            Missing: <span className="font-mono">{missing.join(', ')}</span>
          </div>
        ) : null}
      </>
    );
    return (
      <button
        type="button"
        className="inline-flex items-center"
        onMouseEnter={(e) => showTooltip(e.currentTarget, tip, 360)}
        onMouseLeave={hideTooltip}
        onFocus={(e) => showTooltip(e.currentTarget, tip, 360)}
        onBlur={hideTooltip}
        aria-label="TrendOK details"
      >
        <span className="font-mono">{icon}</span>
      </button>
    );
  }

  function renderStopLossCell(sym: string) {
    const t = trend[sym];
    const p = t?.stopLossPrice ?? null;
    const parts = t?.stopLossParts ?? null;
    const get = (k: string) =>
      parts && typeof parts === 'object' ? (parts as Record<string, unknown>)[k] : undefined;
    const exitNow = Boolean(get('exit_now'));
    const exitDisplay =
      typeof get('exit_display') === 'string' ? String(get('exit_display')) : null;
    const warnHalf = Boolean(get('warn_reduce_half'));
    const warnDisplay =
      typeof get('warn_display') === 'string' ? String(get('warn_display')) : null;
    const usedStoredHigher = Boolean(get('used_stored_higher'));
    const computedStopLoss = get('computed_stop_loss');
    const exitChecks = {
      ema5_lt_ema20: Boolean(get('exit_check_ema5_lt_ema20')),
      close_lt_ema20: Boolean(get('exit_check_close_lt_ema20')),
      momentum_exhaustion: Boolean(get('exit_check_momentum_exhaustion')),
      volume_dry: Boolean(get('exit_check_volume_dry')),
    };
    // Semantics: ✅ means "NOT triggered" (safe), ❌ means "triggered" (exit-now condition hit).
    const ok = (triggered: boolean) => (triggered ? '❌' : '✅');
    const exitMomAndVol = Boolean(exitChecks.momentum_exhaustion && exitChecks.volume_dry);
    const tip = (
      <>
        <div className="mb-2 flex items-center justify-between">
          <div className="font-medium">StopLoss</div>
          <div className="font-mono text-[var(--k-muted)]">{sym}</div>
        </div>
        {exitNow ? (
          <div className="mb-2 rounded border border-red-500/30 bg-red-500/10 px-2 py-1 text-red-600">
            {exitDisplay || '立刻离场'}
          </div>
        ) : warnHalf ? (
          <div className="mb-2 rounded border border-amber-500/30 bg-amber-500/10 px-2 py-1 text-amber-700">
            {warnDisplay || '警告：MACD柱缩小但未转负，建议至少卖出一半'}
          </div>
        ) : null}
        <div className="text-[var(--k-muted)]">
          Formula: max(final_support - atr_k×ATR14, hard_stop)
        </div>
        <div className="mt-2 rounded border border-[var(--k-border)] bg-[var(--k-surface-2)] px-2 py-1">
          <div className="mb-1 font-medium">立刻离场检查</div>
          <div className="text-[10px] text-[var(--k-muted)]">
            ✅ 安全 / ❌ 触发。任一条为 ❌ 即“立刻离场”（止损价=当前价）。
          </div>
          <div className="mt-1 grid grid-cols-2 gap-x-3 gap-y-1">
            <div className="flex items-center justify-between gap-2">
              <span className="text-[var(--k-muted)]">EMA5 &lt; EMA20</span>
              <span className="font-mono">{ok(exitChecks.ema5_lt_ema20)}</span>
            </div>
            <div className="flex items-center justify-between gap-2">
              <span className="text-[var(--k-muted)]">收盘价 &lt; EMA20</span>
              <span className="font-mono">{ok(exitChecks.close_lt_ema20)}</span>
            </div>
            <div className="flex items-center justify-between gap-2">
              <span className="text-[var(--k-muted)]">动能衰竭 + 量能萎缩</span>
              <span className="font-mono">{ok(exitMomAndVol)}</span>
            </div>
          </div>
        </div>
        <div className="mt-2 space-y-1">
          <div className="flex items-center justify-between">
            <div className="text-[var(--k-muted)]">StopLoss</div>
            <div className="font-mono">{fmtPrice(p)}</div>
          </div>
          {usedStoredHigher ? (
            <div className="mb-1 rounded border border-blue-500/30 bg-blue-500/10 px-2 py-1 text-blue-700">
              使用存储的历史止损价（高于计算值）
            </div>
          ) : null}
          {typeof computedStopLoss === 'number' && computedStopLoss !== p ? (
            <div className="flex items-center justify-between">
              <div className="text-[var(--k-muted)]">computed_stop_loss</div>
              <div className="font-mono">{fmtNum(computedStopLoss, 2)}</div>
            </div>
          ) : null}
          <div className="flex items-center justify-between">
            <div className="text-[var(--k-muted)]">final_support</div>
            <div className="font-mono">{fmtNum(get('final_support'), 2)}</div>
          </div>
          <div className="flex items-center justify-between">
            <div className="text-[var(--k-muted)]">buffer</div>
            <div className="font-mono">{fmtNum(get('buffer'), 3)}</div>
          </div>
          <div className="flex items-center justify-between">
            <div className="text-[var(--k-muted)]">hard_stop</div>
            <div className="font-mono">{fmtNum(get('hard_stop'), 2)}</div>
          </div>
        </div>
      </>
    );
    return (
      <button
        type="button"
        className="inline-flex items-center"
        onMouseEnter={(e) => showTooltip(e.currentTarget, tip, 380)}
        onMouseLeave={hideTooltip}
        onFocus={(e) => showTooltip(e.currentTarget, tip, 380)}
        onBlur={hideTooltip}
        aria-label="StopLoss details"
      >
        {exitNow ? (
          <span className="inline-flex items-center gap-1 font-mono text-red-600">
            <CircleX className="h-4 w-4" aria-hidden />
            {fmtPrice(p)}
          </span>
        ) : warnHalf ? (
          <span className="inline-flex items-center gap-1 font-mono text-amber-700">
            <span aria-hidden>⚠︎</span>
            {fmtPrice(p)}
          </span>
        ) : (
          <span className="font-mono">{fmtPrice(p)}</span>
        )}
      </button>
    );
  }

  function renderScoreCell(sym: string) {
    const t = trend[sym];
    const score = t?.score ?? null;
    const parts = t?.scoreParts ?? null;
    const entries =
      parts && typeof parts === 'object'
        ? Object.entries(parts).filter(([, v]) => typeof v === 'number' && Number.isFinite(v))
        : [];
    entries.sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]));
    const tip = (
      <>
        <div className="mb-2 flex items-center justify-between">
          <div className="font-medium">Score (0–100)</div>
          <div className="font-mono text-[var(--k-muted)]">{sym}</div>
        </div>
        <div className="text-[var(--k-muted)]">
          Deterministic formula (CN daily, no LLM). Higher means better short-horizon setup.
        </div>
        <div className="mt-2 space-y-1">
          <div className="flex items-center justify-between">
            <div className="text-[var(--k-muted)]">Total</div>
            <div className="font-mono">{fmtScore(score)}</div>
          </div>
          {entries.length ? (
            <div className="mt-2">
              {entries.map(([k, v]) => (
                <div key={k} className="flex items-center justify-between gap-3">
                  <div className="text-[var(--k-muted)]">{k}</div>
                  <div className="font-mono">{v > 0 ? `+${v.toFixed(1)}` : v.toFixed(1)}</div>
                </div>
              ))}
            </div>
          ) : (
            <div className="mt-2 text-[var(--k-muted)]">
              No breakdown available (insufficient data).
            </div>
          )}
        </div>
      </>
    );
    return (
      <button
        type="button"
        className="inline-flex items-center"
        onMouseEnter={(e) => showTooltip(e.currentTarget, tip, 360)}
        onMouseLeave={hideTooltip}
        onFocus={(e) => showTooltip(e.currentTarget, tip, 360)}
        onBlur={hideTooltip}
        aria-label="Score details"
      >
        <span className="font-mono">{fmtScore(score)}</span>
      </button>
    );
  }

  function renderBuyCell(sym: string) {
    const t = trend[sym];
    const { text, tone, forced, forcedReason } = fmtBuyCell(t);
    const why = typeof t?.buyWhy === 'string' ? t.buyWhy : null;
    const tip = (
      <>
        <div className="mb-2 flex items-center justify-between">
          <div className="font-medium">买入</div>
          <div className="font-mono text-[var(--k-muted)]">{sym}</div>
        </div>
        <div className="text-[var(--k-muted)]">{why || '—'}</div>
        <div className="mt-2 flex items-center justify-between">
          <div className="text-[var(--k-muted)]">建议</div>
          <div className="font-mono">{text}</div>
        </div>
        {forced ? (
          <div className="mt-2 text-emerald-700">
            {forcedReason || 'A_pullback/wait was overridden by hard rule.'}
          </div>
        ) : null}
      </>
    );
    return (
      <button
        type="button"
        className="inline-flex items-center"
        onMouseEnter={(e) => showTooltip(e.currentTarget, tip, 380)}
        onMouseLeave={hideTooltip}
        onFocus={(e) => showTooltip(e.currentTarget, tip, 380)}
        onBlur={hideTooltip}
        aria-label="Buy details"
      >
        <span
          className={
            tone === 'buy'
              ? 'font-mono text-emerald-700'
              : tone === 'avoid'
                ? 'font-mono text-red-600'
                : tone === 'wait'
                  ? 'font-mono text-[var(--k-muted)]'
                  : 'font-mono'
          }
        >
          {text}
        </span>
      </button>
    );
  }

  const sortedItems = React.useMemo(() => {
    if (!scoreSortEnabled) return items;
    const arr = [...items];
    arr.sort((a, b) => {
      const sa = trend[a.symbol]?.score;
      const sb = trend[b.symbol]?.score;
      const va = typeof sa === 'number' && Number.isFinite(sa) ? sa : null;
      const vb = typeof sb === 'number' && Number.isFinite(sb) ? sb : null;
      if (va == null && vb == null) return 0;
      if (va == null) return 1; // push unknown to bottom
      if (vb == null) return -1;
      const d = va - vb;
      return scoreSortDir === 'asc' ? d : -d;
    });
    return arr;
  }, [items, trend, scoreSortEnabled, scoreSortDir]);

  function referenceTable() {
    const capturedAt = new Date().toISOString();
    const rows = sortedItems.slice(0, 50).map((it) => {
      const t = trend[it.symbol];
      return {
        symbol: it.symbol,
        name: it.name ?? null,
        asOfDate: t?.asOfDate ?? null,
        close: t?.values?.close ?? null,
        trendOk: t?.trendOk ?? null,
        score: t?.score ?? null,
        stopLossPrice: t?.stopLossPrice ?? null,
        buyMode: t?.buyMode ?? null,
        buyAction: t?.buyAction ?? null,
        buyZoneLow: t?.buyZoneLow ?? null,
        buyZoneHigh: t?.buyZoneHigh ?? null,
      };
    });
    addReference({
      kind: 'watchlistTable',
      refId: `${capturedAt}:${sortedItems.length}`,
      capturedAt,
      total: sortedItems.length,
      items: rows,
    });
  }

  function toastCopyMd(ok: boolean, text: string) {
    setCopyMdStatus({ ok, text });
    if (copyMdTimerRef.current) window.clearTimeout(copyMdTimerRef.current);
    copyMdTimerRef.current = window.setTimeout(() => setCopyMdStatus(null), 2400);
  }

  async function copyWatchlistMarkdown() {
    if (!sortedItems.length) {
      toastCopyMd(false, 'No items to copy.');
      return;
    }
    setCopyMdBusy(true);
    try {
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
        const md = Array.isArray(t.missingData) ? t.missingData.filter(Boolean) : [];
        if (md.length) {
          missingHistory.push(sym);
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
      if (missingTrend.length || missingHistory.length || missingRealtime.length) {
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
        toastCopyMd(false, `Copy aborted: ${parts.join(' | ')}`);
        return;
      }
      const generatedAt = new Date().toISOString();
      const lines: string[] = [];
      lines.push('## Watchlist');
      lines.push(`- generatedAt: ${generatedAt}`);
      lines.push(`- items: ${sortedItems.length}`);
      lines.push(
        `- scoresUpdatedAt: ${trendUpdatedAt ? new Date(trendUpdatedAt).toLocaleString() : '—'}`,
      );
      lines.push(`- shanghaiToday: ${todaySh}`);
      lines.push(`- tradingTime: ${tradingTime ? 'true' : 'false'}`);
      lines.push('');

      lines.push('### TrendOK rules');
      lines.push(mdLines(trendOkRuleLines()));
      lines.push('');
      lines.push('### Score（0–100）计分说明');
      lines.push(
        mdLines(
          scoreExplainZhLines().map((line) => (line.startsWith('-') ? line : `- ${line}`)),
        ),
      );
      lines.push('');

      const headers = [...WATCHLIST_MD_HEADERS];
      const blockAlerts: string[] = [];
      lines.push(`| ${headers.join(' | ')} |`);
      lines.push(`| ${headers.map(() => '---').join(' | ')} |`);
      for (const it of sortedItems) {
        const t = trendSnap[it.symbol];
        const buy = fmtBuyCell(t).text;
        const q = quotesSnap[it.symbol];
        const rowMetrics = buildWatchlistRowMetrics({
          symbol: it.symbol,
          trend: t,
          quote: q,
          tradingTime,
          todaySh,
        });
        const pnl = computePnLPct(it.costPrice ?? null, rowMetrics.current);
        const qDate = tradeDateFromTradeTime(q?.tradeTime ?? null);
        const asOf = qDate === todaySh ? qDate : String(t?.asOfDate ?? '');
        const values = (t?.values ?? {}) as Record<string, unknown>;
        const intradayCell = isIntradaySurge(rowMetrics.intradayChgPct)
          ? `⚠️ ${formatIntradayChgPct(rowMetrics.intradayChgPct)}`
          : formatIntradayChgPct(rowMetrics.intradayChgPct);
        const gapCell =
          rowMetrics.gapUp === true
            ? `⚠️ ${formatGapUp(true)}`
            : formatGapUp(rowMetrics.gapUp);
        const alertsCell = formatRiskAlerts(rowMetrics.alerts);
        for (const alert of rowMetrics.alerts) {
          if (alert.severity === 'block') {
            blockAlerts.push(`${it.symbol}: ${alert.message}`);
          }
        }
        const row = [
          escapeMarkdownCell(it.symbol),
          escapeMarkdownCell(it.name || '—'),
          escapeMarkdownCell(industryDisplayName(values)),
          escapeMarkdownCell(formatHotTop3(t)),
          escapeMarkdownCell(
            typeof it.positionPct === 'number' && Number.isFinite(it.positionPct)
              ? it.positionPct.toFixed(1)
              : '—',
          ),
          escapeMarkdownCell(mdPrice(it.costPrice ?? null)),
          escapeMarkdownCell(mdPrice(rowMetrics.current)),
          escapeMarkdownCell(formatVwap(rowMetrics.vwap)),
          escapeMarkdownCell(intradayCell),
          escapeMarkdownCell(gapCell),
          escapeMarkdownCell(alertsCell),
          escapeMarkdownCell(formatPnLPct(pnl)),
          escapeMarkdownCell(mdScore(t?.score ?? null)),
          escapeMarkdownCell(trendOkSummary(t)),
          escapeMarkdownCell(buy),
          escapeMarkdownCell(mdPrice(t?.stopLossPrice ?? null)),
          escapeMarkdownCell(asOf),
        ];
        lines.push(`| ${row.join(' | ')} |`);
      }
      lines.push('');
      if (blockAlerts.length) {
        lines.push('### Risk alerts');
        lines.push(mdLines(blockAlerts.map((line) => `- ${line}`)));
        lines.push('');
      }

      const md = lines.join('\n').trim() + '\n';
      try {
        await navigator.clipboard.writeText(md);
        toastCopyMd(true, 'Copied Markdown.');
      } catch {
        toastCopyMd(false, 'Copy failed. Please allow clipboard access.');
      }
    } finally {
      setCopyMdBusy(false);
    }
  }

  const watchlistSet = React.useMemo(() => new Set(items.map((x) => x.symbol)), [items]);
  const importDebugRows = React.useMemo(() => {
    const q = importDebugFilter.trim().toUpperCase();
    const base = (importDebug.rows || []).filter((r) => {
      if (!q) return true;
      const sym = String(r?.symbol || '').toUpperCase();
      const name = String(r?.name || '').toUpperCase();
      return sym.includes(q) || name.includes(q);
    });
    const arr = [...base];
    arr.sort((a, b) => {
      const sa = a?.score;
      const sb = b?.score;
      const va = typeof sa === 'number' && Number.isFinite(sa) ? sa : null;
      const vb = typeof sb === 'number' && Number.isFinite(sb) ? sb : null;
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      const d = va - vb;
      return importDebugScoreSortDir === 'asc' ? d : -d;
    });
    return arr;
  }, [importDebug.rows, importDebugFilter, importDebugScoreSortDir]);

  const headerTip = (
    <>
      <div className="mb-2 font-medium">Definition (CN daily)</div>
      <div className="space-y-1 text-[var(--k-muted)]">
        <div>✅ only when ALL rules are satisfied.</div>
        <div>— when data/indicators are insufficient.</div>
      </div>
      <div className="mt-2 space-y-1">
        <div>1) Close &gt; EMA(20) and EMA(20) &gt; EMA(60)</div>
        <div>2) MACD line &gt; 0</div>
        <div>3) MACD histogram &gt; 0</div>
        <div>4) Close ≥ 0.90 × High(20)</div>
        <div>5) RSI(14) in [50, 90]</div>
        <div>6) AvgVol(5) &gt; 0.9 × AvgVol(30)</div>
      </div>
    </>
  );

  return (
    <div className="box-border min-w-0 w-full max-w-full overflow-x-hidden p-6">
      {watchlistHydrating ? (
        <div className="text-sm text-[var(--k-muted)]">Loading watchlist…</div>
      ) : null}
      <div className={watchlistHydrating ? 'hidden' : undefined}>
      <div className="mb-6 flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="text-lg font-semibold">Watchlist</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            Manage the stocks you are watching.
          </div>
          <div className="mt-1 text-xs text-[var(--k-muted)]">
            Names are resolved from Market cache. If names are missing, go to Market and click Sync
            once.
          </div>
          <div className="mt-1 text-xs text-[var(--k-muted)]">
            {trendUpdatedAt
              ? `Scores updated at ${new Date(trendUpdatedAt).toLocaleString()} (auto refresh: 10 min)`
              : 'Scores not loaded yet.'}
          </div>
          <div className="mt-1 text-xs text-[var(--k-muted)]">
            {formatAutomationSummary(latestAutomation) ?? 'Last automation: —'}
            {' · '}
            Next scheduled: weekdays 17:30 (Asia/Shanghai)
          </div>
          {syncBusy && syncStage ? (
            <div className="mt-2 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] p-2 text-xs">
              <div className="flex items-center justify-between gap-2">
                <div className="font-medium">Import from screener</div>
                <div className="text-[var(--k-muted)]">
                  {syncProgress ? `${syncProgress.cur}/${syncProgress.total}` : '…'}
                </div>
              </div>
              <div className="mt-1 text-[var(--k-muted)]">{syncStage}</div>
              {syncProgress && syncProgress.total > 0 ? (
                <div className="mt-2 h-2 w-full overflow-hidden rounded bg-[var(--k-surface-2)]">
                  <div
                    className="h-full bg-[var(--k-accent)]"
                    style={{
                      width: `${Math.max(
                        0,
                        Math.min(100, (syncProgress.cur / Math.max(1, syncProgress.total)) * 100),
                      ).toFixed(1)}%`,
                    }}
                  />
                </div>
              ) : null}
              {syncLogs.length ? (
                <div className="mt-2 space-y-0.5 text-[var(--k-muted)]">
                  {syncLogs.slice(-4).map((l, i) => (
                    <div key={i} className="truncate">
                      {l}
                    </div>
                  ))}
                </div>
              ) : null}
            </div>
          ) : null}

          {automationBusy && automationStage ? (
            <div className="mt-2 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] p-2 text-xs">
              <div className="flex items-center justify-between gap-2">
                <div className="font-medium">Run automation</div>
                <div className="text-[var(--k-muted)]">…</div>
              </div>
              <div className="mt-1 text-[var(--k-muted)]">{automationStage}</div>
              {automationLogs.length ? (
                <div className="mt-2 space-y-0.5 text-[var(--k-muted)]">
                  {automationLogs.slice(-4).map((l, i) => (
                    <div key={i} className="truncate">
                      {l}
                    </div>
                  ))}
                </div>
              ) : null}
            </div>
          ) : null}

          {automationMsg ? (
            <div className="mt-2 text-xs text-[var(--k-muted)]">{automationMsg}</div>
          ) : null}
          {automationSkipRun ? (
            <div className="mt-2 flex items-center gap-2 text-xs">
              <span className="text-[var(--k-muted)]">
                Automation skipped ({automationSkipRun.skipReason || 'unknown'}).
              </span>
              <Button size="sm" variant="secondary" onClick={() => void onForceAutomationFromSkip()}>
                Force run
              </Button>
            </div>
          ) : null}

          {syncMsg ? <div className="mt-2 text-xs text-[var(--k-muted)]">{syncMsg}</div> : null}
          {copyMdStatus ? (
            <div className="mt-2 text-xs">
              <span className={copyMdStatus.ok ? 'text-emerald-600' : 'text-red-600'}>
                {copyMdStatus.text}
              </span>
            </div>
          ) : null}
          {error ? <div className="mt-2 text-sm text-red-600">{error}</div> : null}
        </div>
        <div className="flex shrink-0 flex-wrap items-center justify-end gap-2">
          <Button
            size="sm"
            variant="secondary"
            onClick={() => void onManualRefreshTrend()}
            disabled={trendBusy || !items.length}
            className="gap-2"
            aria-label="Refresh watchlist scores"
            title="Fetch latest daily bars from network and recompute"
          >
            <RefreshCw className={trendBusy ? 'h-4 w-4 animate-spin' : 'h-4 w-4'} />
            Refresh
          </Button>
          <Button
            size="sm"
            variant="secondary"
            onClick={() => referenceTable()}
            disabled={!sortedItems.length}
            className="gap-2"
          >
            <ExternalLink className="h-4 w-4" />
            Reference table
          </Button>
          <Button
            size="sm"
            variant="secondary"
            onClick={() => void copyWatchlistMarkdown()}
            disabled={!sortedItems.length || copyMdBusy}
          >
            {copyMdBusy ? 'Copying…' : 'Copy Markdown'}
          </Button>
          <Button
            size="sm"
            variant="secondary"
            onClick={() => void onSyncFromScreener()}
            disabled={syncBusy || automationBusy}
            className="gap-2"
          >
            <RefreshCw className={syncBusy ? 'h-4 w-4 animate-spin' : 'h-4 w-4'} />
            Import from screener
          </Button>
          <Button
            size="sm"
            variant="secondary"
            onClick={() => void onRunAutomation(true)}
            disabled={automationBusy || syncBusy}
            className="gap-2"
            title="Run watchlist automation (remove weak, screener import, Alpha Radar S append)"
          >
            <Play className={automationBusy ? 'h-4 w-4 animate-pulse' : 'h-4 w-4'} />
            Run automation
          </Button>
        </div>
      </div>

      <div className="mb-4 min-w-0 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] p-2 text-xs">
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <div className="font-medium">Import debug table</div>
            <Switch
              checked={importDebugOpen}
              onCheckedChange={setImportDebugOpen}
              aria-label="Toggle import debug table"
            />
          </div>
          <div className="text-[var(--k-muted)]">
            {importDebug.updatedAt
              ? new Date(importDebug.updatedAt).toLocaleString()
              : 'No import yet'}
          </div>
        </div>
        <div className="mt-1 flex flex-wrap items-center justify-between gap-2">
          <div className="text-[var(--k-muted)]">
            Scanned {importDebug.scanned} • TrendOK ✅ {importDebug.trendOkCount} • Showing{' '}
            {importDebugRows.length}
          </div>
          <div className="flex items-center gap-2">
            <input
              className="h-8 w-[220px] rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-2 font-mono text-xs outline-none"
              placeholder="Filter (symbol/name)"
              value={importDebugFilter}
              onChange={(e) => setImportDebugFilter(e.target.value)}
            />
            <Button
              size="sm"
              variant="secondary"
              onClick={() => setImportDebugFilter('')}
              disabled={!importDebugFilter.trim()}
            >
              Clear
            </Button>
          </div>
        </div>

        <VisibilitySection
          visible={importDebugOpen}
          className="mt-2 max-h-[520px] min-w-0 overflow-auto rounded border border-[var(--k-border)]"
        >
          <table className="w-full border-collapse text-sm">
            <thead className="sticky top-0 bg-[var(--k-surface)] text-[var(--k-muted)]">
              <tr className="text-left">
                <th className="px-3 py-2 w-[150px]">Symbol</th>
                <th className="px-3 py-2 w-[140px]">Name</th>
                <th className="px-3 py-2 w-[80px]">TrendOK</th>
                <th className="px-3 py-2 w-[90px]">
                  <button
                    type="button"
                    className="inline-flex items-center gap-1 hover:text-[var(--k-text)]"
                    onClick={() =>
                      setImportDebugScoreSortDir((d) => (d === 'desc' ? 'asc' : 'desc'))
                    }
                    aria-label="Sort by score"
                    title="Sort by score"
                  >
                    <span>Score</span>
                    {importDebugScoreSortDir === 'desc' ? (
                      <ArrowDown className="h-3.5 w-3.5" />
                    ) : (
                      <ArrowUp className="h-3.5 w-3.5" />
                    )}
                  </button>
                </th>
                <th className="px-3 py-2 w-[180px]">Buy</th>
                <th className="px-3 py-2 w-[80px]">Intraday%</th>
                <th className="px-3 py-2 w-[52px]">Gap</th>
                <th className="px-3 py-2 w-[180px]">Alerts</th>
                <th className="px-3 py-2 w-[110px]">StopLoss</th>
                <th className="px-3 py-2 w-[120px]">Action</th>
                <th className="px-3 py-2 min-w-[320px]">Notes</th>
              </tr>
            </thead>
            <tbody>
              {importDebugRows.length ? (
                importDebugRows.map((r) => {
                  const sym = String(r?.symbol || '');
                  const ok = r?.trendOk ?? null;
                  const icon = ok == null ? '—' : ok ? '✅' : '❌';
                  const buy = fmtBuyCell(r);
                  const importAlerts = collectWatchlistRiskAlerts({
                    intradayChgPct: r?.intradayChgPct,
                    gapUp: r?.gapUp,
                    marketRegime: r?.marketRegime,
                    serverAlerts: r?.riskAlerts,
                  });
                  const notes =
                    (typeof r?.buyWhy === 'string' && r.buyWhy) ||
                    (Array.isArray(r?.missingData) && r.missingData.length
                      ? r.missingData.join(', ')
                      : '');
                  const inWl = sym ? watchlistSet.has(sym) : false;
                  return (
                    <tr key={sym} className="border-t border-[var(--k-border)]">
                      <td className="px-3 py-2 font-mono">
                        <button
                          type="button"
                          className="hover:underline"
                          onClick={() => {
                            setCode(sym);
                            setError(null);
                          }}
                          title="Fill the Add input with this symbol"
                        >
                          {sym || '—'}
                        </button>
                      </td>
                      <td className="px-3 py-2">
                        <div className="truncate" title={String(r?.name || '')}>
                          {r?.name || '—'}
                        </div>
                      </td>
                      <td className="px-3 py-2 font-mono">{icon}</td>
                      <td className="px-3 py-2 font-mono">{fmtScore(r?.score ?? null)}</td>
                      <td
                        className={
                          buy.tone === 'buy'
                            ? 'px-3 py-2 font-mono text-emerald-700'
                            : buy.tone === 'avoid'
                              ? 'px-3 py-2 font-mono text-red-600'
                              : buy.tone === 'wait'
                                ? 'px-3 py-2 font-mono text-[var(--k-muted)]'
                                : 'px-3 py-2 font-mono'
                        }
                      >
                        {buy.text}
                      </td>
                      <td
                        className={`px-3 py-2 font-mono ${
                          isIntradaySurge(r?.intradayChgPct) ? 'text-red-600 font-semibold' : ''
                        }`}
                      >
                        {formatIntradayChgPct(r?.intradayChgPct ?? null)}
                      </td>
                      <td
                        className={`px-3 py-2 font-mono ${
                          r?.gapUp === true ? 'text-red-600 font-semibold' : ''
                        }`}
                      >
                        {formatGapUp(r?.gapUp ?? null)}
                      </td>
                      <td className="px-3 py-2 text-xs">
                        {importAlerts.length ? (
                          <div className="truncate" title={formatRiskAlerts(importAlerts)}>
                            {importAlerts.map((alert) => (
                              <div
                                key={alert.code}
                                className={
                                  alert.severity === 'block' ? 'text-red-600' : 'text-amber-700'
                                }
                              >
                                {alert.message}
                              </div>
                            ))}
                          </div>
                        ) : (
                          '—'
                        )}
                      </td>
                      <td className="px-3 py-2 font-mono">{fmtPrice(r?.stopLossPrice ?? null)}</td>
                      <td className="px-3 py-2">
                        {inWl ? (
                          <span className="text-[var(--k-muted)]">In watchlist</span>
                        ) : (
                          <Button
                            size="sm"
                            variant="secondary"
                            onClick={() => sym && addSymbolToWatchlist(sym)}
                            disabled={!sym}
                          >
                            Add
                          </Button>
                        )}
                      </td>
                      <td className="px-3 py-2 text-[var(--k-muted)]">
                        <div className="truncate" title={notes}>
                          {notes || '—'}
                        </div>
                      </td>
                    </tr>
                  );
                })
              ) : (
                <tr>
                  <td className="px-3 py-3 text-[var(--k-muted)]" colSpan={11}>
                    No import results yet.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </VisibilitySection>
      </div>

      <section className="mb-4 min-w-0 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="mb-2 text-sm font-medium">Add</div>
        <div className="grid gap-2 md:grid-cols-12">
          <input
            className="h-9 md:col-span-10 rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
            placeholder="Ticker (e.g. 600000 / 0700 / CN:600000)"
            value={code}
            onChange={(e) => setCode(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') onAdd();
            }}
          />
          <div className="md:col-span-2 flex gap-2">
            <Button size="sm" onClick={onAdd} disabled={!code.trim()}>
              Add
            </Button>
            <Button
              size="sm"
              variant="secondary"
              onClick={() => {
                setCode('');
                setError(null);
              }}
              disabled={!code.trim() && !error}
            >
              Clear
            </Button>
          </div>
        </div>
        <div className="mt-2 text-xs text-[var(--k-muted)]">
          Supported inputs: CN 6-digit ticker, HK 4-5 digit ticker, or prefixed symbol (CN:/HK:).
        </div>
      </section>

      <section className="mb-4 min-w-0 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="text-sm font-medium">Score（0–100）计分说明</div>
        <div className="mt-2 space-y-1.5 text-xs leading-relaxed text-[var(--k-text)]">
          {scoreExplainZhLines().map((line, i) => (
            <div key={i}>{line}</div>
          ))}
        </div>
        <div className="mt-3 text-[11px] leading-relaxed text-[var(--k-muted)]">
          鼠标悬停在列表「Score」数字上可查看该股各项得分（ema / macd / breakout / rsi / volume 及加扣分）。
        </div>
      </section>

      <section className="box-border grid min-w-0 w-full grid-cols-1 overflow-hidden rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="mb-2 flex items-center justify-between">
          <div className="text-sm font-medium">List</div>
          <div className="text-xs text-[var(--k-muted)]">{items.length} items</div>
        </div>

        {items.length ? (
          <div className="min-w-0 w-full overflow-hidden rounded border border-[var(--k-border)]">
            <div className="overflow-x-auto overscroll-x-contain">
              <table className="w-max min-w-full border-separate border-spacing-0 text-sm">
              <thead className="bg-[var(--k-surface)] text-[var(--k-muted)]">
                <tr className="text-left">
                  <th className="px-3 py-2 w-[40px]" title="Color flag">
                    <span className="sr-only">Color</span>
                  </th>
                  <th className="px-3 py-2 w-[110px]">Symbol</th>
                  <th className="px-3 py-2 w-[120px] max-w-[120px]">Name</th>
                  <th className="px-3 py-2 w-[120px] max-w-[140px]">Industry</th>
                  <th className="px-2 py-2 w-[58px]">仓位%</th>
                  <th className="px-2 py-2 w-[80px]">成本价</th>
                  <th className="px-2 py-2 w-[72px]">Current</th>
                  <th className="px-2 py-2 w-[80px]">止损</th>
                  <th className="max-w-[130px] px-2 py-2 w-[120px]">买入</th>
                  <th className="px-2 py-2 w-[64px]">HotTop3</th>
                  <th className="px-2 py-2 w-[68px]">VWAP</th>
                  <th className="px-2 py-2 w-[72px]">Intraday%</th>
                  <th className="px-2 py-2 w-[48px]">Gap</th>
                  <th className="px-2 py-2 w-[140px]">Alerts</th>
                  <th className="px-2 py-2 w-[64px]">P&L%</th>
                  <th
                    className={watchlistStickyCellClass('score', { header: true })}
                    style={watchlistStickyCellStyle('score', { header: true })}
                  >
                    <button
                      type="button"
                      className="inline-flex items-center gap-1"
                      onClick={() => {
                        setScoreSortEnabled(true);
                        setScoreSortDir((d) => (d === 'desc' ? 'asc' : 'desc'));
                      }}
                      onContextMenu={(e) => {
                        e.preventDefault();
                        setScoreSortEnabled((v) => !v);
                      }}
                      title="Click to toggle sort. Right-click to enable/disable sorting."
                      aria-label="Sort by score"
                    >
                      <span>Score</span>
                      {scoreSortEnabled ? (
                        scoreSortDir === 'desc' ? (
                          <ArrowDown className="h-3.5 w-3.5" />
                        ) : (
                          <ArrowUp className="h-3.5 w-3.5" />
                        )
                      ) : (
                        <ArrowUpDown className="h-3.5 w-3.5" />
                      )}
                    </button>
                  </th>
                  <th
                    className={watchlistStickyCellClass('trendOk', { header: true })}
                    style={watchlistStickyCellStyle('trendOk', { header: true })}
                  >
                    <button
                      type="button"
                      className="inline-flex items-center hover:text-[var(--k-text)]"
                      onMouseEnter={(e) => showTooltip(e.currentTarget, headerTip, 380)}
                      onMouseLeave={hideTooltip}
                      onFocus={(e) => showTooltip(e.currentTarget, headerTip, 380)}
                      onBlur={hideTooltip}
                      aria-label="TrendOK definition"
                    >
                      TrendOK
                    </button>
                  </th>
                  <th
                    className={watchlistStickyCellClass('action', { header: true, extra: 'text-right' })}
                    style={watchlistStickyCellStyle('action', { header: true })}
                  >
                    Action
                  </th>
                </tr>
              </thead>
              <tbody>
                {sortedItems.map((it) =>
                  (() => {
                    const t = trend[it.symbol];
                    const q = quotes[it.symbol];
                    const tradingTime = isShanghaiTradingTime();
                    const todaySh = getShanghaiTodayIso();
                    const rowMetrics = buildWatchlistRowMetrics({
                      symbol: it.symbol,
                      trend: t,
                      quote: q,
                      tradingTime,
                      todaySh,
                    });
                    const tone = rowTone(t, rowMetrics.alerts);
                    const rowClass =
                      tone === 'green'
                        ? 'group border-t border-[var(--k-border)] bg-emerald-50/60 hover:bg-emerald-100/60'
                        : tone === 'red'
                          ? 'group border-t border-[var(--k-border)] bg-red-50/60 hover:bg-red-100/60'
                          : 'group border-t border-[var(--k-border)] hover:bg-[var(--k-surface-2)]';
                    return (
                      <tr key={it.symbol} className={rowClass}>
                        <td className="px-3 py-2">
                          <button
                            type="button"
                            className="grid h-6 w-6 place-items-center rounded hover:bg-[var(--k-surface-2)]"
                            onClick={(e) => {
                              e.stopPropagation();
                              showColorPicker(e.currentTarget, it.symbol);
                            }}
                            aria-label="Set color flag"
                            title="Set color flag"
                          >
                            <span
                              className="h-3.5 w-3.5 rounded-sm border border-[var(--k-border)]"
                              style={{ backgroundColor: it.color || '#ffffff' }}
                            />
                          </button>
                        </td>
                        <td className="px-3 py-2 font-mono">
                          <button
                            type="button"
                            className="inline-flex items-center rounded px-1 py-0.5 hover:underline"
                            onClick={() => onOpenStock?.(it.symbol)}
                            disabled={!onOpenStock}
                            aria-label={`Open ${it.symbol}`}
                          >
                            {it.symbol}
                          </button>
                        </td>
                        <td className="px-3 py-2 max-w-[120px] truncate" title={it.name || ''}>
                          {it.name || '—'}
                        </td>
                        <td
                          className="px-3 py-2 max-w-[140px] truncate"
                          title={
                            tushareIndustryTooltip(
                              (t?.values ?? null) as Record<string, unknown> | null,
                            ) ?? industryDisplayName((t?.values ?? {}) as Record<string, unknown>)
                          }
                        >
                          {industryDisplayName((t?.values ?? {}) as Record<string, unknown>)}
                        </td>
                        <td className="px-2 py-2">
                          <input
                            className="h-8 w-full min-w-0 max-w-[52px] rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-1.5 font-mono text-xs outline-none"
                            placeholder="0"
                            value={
                              typeof it.positionPct === 'number' && Number.isFinite(it.positionPct)
                                ? String(it.positionPct)
                                : ''
                            }
                            onChange={(e) => setItemPositionPct(it.symbol, e.target.value)}
                          />
                        </td>
                        <td className="px-2 py-2">
                          <input
                            className="h-8 w-full min-w-0 max-w-[72px] rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-1.5 font-mono text-xs outline-none"
                            placeholder="成本"
                            inputMode="decimal"
                            value={
                              costPriceDrafts[it.symbol] ??
                              (typeof it.costPrice === 'number' && Number.isFinite(it.costPrice)
                                ? it.costPrice.toFixed(2)
                                : '')
                            }
                            onChange={(e) => {
                              const raw = e.target.value;
                              if (raw === '' || COST_PRICE_RE.test(raw)) {
                                setItemCostPriceDraft(it.symbol, raw);
                                if (!raw) {
                                  setItemCostPriceValue(it.symbol, null);
                                } else {
                                  const num = Number(raw);
                                  if (Number.isFinite(num)) setItemCostPriceValue(it.symbol, num);
                                }
                              }
                            }}
                            onFocus={() => {
                              if (costPriceDrafts[it.symbol] != null) return;
                              if (
                                typeof it.costPrice === 'number' &&
                                Number.isFinite(it.costPrice)
                              ) {
                                setItemCostPriceDraft(it.symbol, it.costPrice.toFixed(2));
                              }
                            }}
                            onBlur={() => commitItemCostPriceDraft(it.symbol)}
                          />
                        </td>
                        <td
                          className="px-3 py-2 font-mono"
                          title={
                            trend[it.symbol]?.asOfDate
                              ? `as of ${trend[it.symbol]?.asOfDate}`
                              : trend[it.symbol]
                                ? 'as of latest cached daily bar'
                                : '—'
                          }
                        >
                          {(() => {
                            const t = trend[it.symbol];
                            const q = quotes[it.symbol];
                            const close0 = t?.values?.close;
                            const trendClose =
                              typeof close0 === 'number' && Number.isFinite(close0)
                                ? (close0 as number)
                                : null;
                            const current = resolveWatchlistCurrentPrice({
                              tradingTime: isShanghaiTradingTime(),
                              todaySh: getShanghaiTodayIso(),
                              symbol: it.symbol,
                              trendAsOfDate: t?.asOfDate ?? null,
                              quotePrice: q?.price ?? null,
                              quoteTradeTime: q?.tradeTime ?? null,
                              trendClose,
                            });
                            return fmtPrice(current);
                          })()}
                        </td>
                        <td className="px-2 py-2">{renderStopLossCell(it.symbol)}</td>
                        <td className="max-w-[130px] truncate px-2 py-2">
                          {renderBuyCell(it.symbol)}
                        </td>
                        <td className="px-2 py-2 text-center">
                          {formatHotTop3(t) === '✓' ? (
                            <span className="text-emerald-600 font-medium" title="Industry in today fund-flow Top3">
                              ✓
                            </span>
                          ) : (
                            '—'
                          )}
                        </td>
                        <td className="px-3 py-2 font-mono">{formatVwap(rowMetrics.vwap)}</td>
                        <td
                          className={`px-3 py-2 font-mono ${
                            isIntradaySurge(rowMetrics.intradayChgPct)
                              ? 'font-semibold text-red-600'
                              : ''
                          }`}
                        >
                          {formatIntradayChgPct(rowMetrics.intradayChgPct)}
                        </td>
                        <td
                          className={`px-3 py-2 font-mono ${
                            rowMetrics.gapUp === true ? 'font-semibold text-red-600' : ''
                          }`}
                        >
                          {formatGapUp(rowMetrics.gapUp)}
                        </td>
                        <td className="max-w-[140px] px-2 py-2 text-xs">
                          {rowMetrics.alerts.length ? (
                            <div
                              className="truncate"
                              title={formatRiskAlerts(rowMetrics.alerts)}
                            >
                              {rowMetrics.alerts.map((alert) => (
                                <div
                                  key={alert.code}
                                  className={
                                    alert.severity === 'block'
                                      ? 'text-red-600'
                                      : 'text-amber-700'
                                  }
                                >
                                  {alert.message}
                                </div>
                              ))}
                            </div>
                          ) : (
                            '—'
                          )}
                        </td>
                        <td
                          className={`px-3 py-2 font-mono ${
                            (() => {
                              const pnl = computePnLPct(it.costPrice ?? null, rowMetrics.current);
                              if (pnl == null) return '';
                              if (pnl >= 5) return 'text-emerald-600';
                              if (pnl <= 0) return 'text-red-600';
                              return '';
                            })()
                          }`}
                        >
                          {formatPnLPct(computePnLPct(it.costPrice ?? null, rowMetrics.current))}
                        </td>
                        <td
                          className={watchlistStickyCellClass('score', { tone })}
                          style={watchlistStickyCellStyle('score')}
                        >
                          {renderScoreCell(it.symbol)}
                        </td>
                        <td
                          className={watchlistStickyCellClass('trendOk', { tone })}
                          style={watchlistStickyCellStyle('trendOk')}
                        >
                          {renderTrendOkCell(it.symbol)}
                        </td>
                        <td
                          className={watchlistStickyCellClass('action', { tone, extra: 'text-right' })}
                          style={watchlistStickyCellStyle('action')}
                        >
                          <div className="flex justify-end">
                            <Button
                              variant="ghost"
                              size="icon"
                              className="h-8 w-8"
                              onClick={() => {
                                const t = trend[it.symbol];
                                const capturedAt = new Date().toISOString();
                                addReference({
                                  kind: 'watchlistStock',
                                  refId: `${it.symbol}:${capturedAt}`,
                                  symbol: it.symbol,
                                  name: it.name ?? null,
                                  capturedAt,
                                  asOfDate: t?.asOfDate ?? null,
                                  close: t?.values?.close ?? null,
                                  trendOk: t?.trendOk ?? null,
                                  score: t?.score ?? null,
                                  stopLossPrice: t?.stopLossPrice ?? null,
                                  buyMode: t?.buyMode ?? null,
                                  buyAction: t?.buyAction ?? null,
                                  buyZoneLow: t?.buyZoneLow ?? null,
                                  buyZoneHigh: t?.buyZoneHigh ?? null,
                                  buyWhy: t?.buyWhy ?? null,
                                  intradayChgPct: t?.intradayChgPct ?? null,
                                  gapUp: t?.gapUp ?? null,
                                  riskAlerts: t?.riskAlerts ?? [],
                                });
                              }}
                              aria-label="Reference to chat"
                              title="Reference to chat"
                            >
                              <ExternalLink className="h-4 w-4" />
                            </Button>
                            <Button
                              variant="ghost"
                              size="icon"
                              className="h-8 w-8"
                              onClick={() => onRemove(it.symbol)}
                              aria-label="Remove"
                              title="Remove"
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          </div>
                        </td>
                      </tr>
                    );
                  })(),
                )}
              </tbody>
            </table>
            </div>
          </div>
        ) : (
          <div className="text-sm text-[var(--k-muted)]">No items yet. Add a ticker above.</div>
        )}
      </section>

      {tooltip.open
        ? createPortal(
            <div
              className="fixed z-[9999] max-h-[70vh] overflow-auto rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3 text-xs text-[var(--k-text)] shadow-lg"
              style={{
                left: tooltip.x,
                top: tooltip.y,
                width: tooltip.w,
                transform: tooltip.placement === 'top-end' ? 'translateY(-100%)' : undefined,
              }}
            >
              {tooltip.content}
            </div>,
            document.body,
          )
        : null}

      {colorPicker.open
        ? createPortal(
            <div className="fixed inset-0 z-[9999]" onMouseDown={hideColorPicker}>
              <div
                className="fixed w-[220px] rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-2 text-xs text-[var(--k-text)] shadow-lg"
                style={{
                  left: colorPicker.x,
                  top: colorPicker.y,
                  transform: colorPicker.placement === 'top-end' ? 'translateY(-100%)' : undefined,
                }}
                onMouseDown={(e) => e.stopPropagation()}
              >
                <div className="mb-2 flex items-center justify-between">
                  <div className="text-xs font-medium text-[var(--k-muted)]">Color flag</div>
                  <button
                    type="button"
                    className="grid h-7 w-7 place-items-center rounded hover:bg-[var(--k-surface-2)]"
                    onClick={hideColorPicker}
                    aria-label="Close"
                  >
                    <CircleX className="h-4 w-4" />
                  </button>
                </div>
                <div className="grid grid-cols-4 gap-2">
                  {FLAG_COLORS.map((c) => (
                    <button
                      key={c.hex}
                      type="button"
                      className="group flex h-9 items-center justify-center rounded-md border border-[var(--k-border)] hover:bg-[var(--k-surface-2)]"
                      onClick={() => {
                        if (colorPicker.symbol) setItemColor(colorPicker.symbol, c.hex);
                        hideColorPicker();
                      }}
                      aria-label={c.label}
                      title={c.label}
                    >
                      <span
                        className="h-5 w-5 rounded-sm border border-[var(--k-border)]"
                        style={{ backgroundColor: c.hex }}
                      />
                    </button>
                  ))}
                </div>
                <div className="mt-2 text-[11px] text-[var(--k-muted)]">
                  Tip: Press Esc or click outside to close.
                </div>
              </div>
            </div>,
            document.body,
          )
        : null}
      </div>
    </div>
  );
}
