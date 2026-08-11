'use client';

import * as React from 'react';
import { createPortal } from 'react-dom';
import {
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  ChevronDown,
  ChevronUp,
  CircleX,
} from 'lucide-react';

import { Button } from '@/components/ui/button';
import { ColumnHeader } from '@/components/watchlist/ColumnHeader';
import {
  TradeActionDialog,
  type TradeDialogOpenState,
} from '@/components/watchlist/TradeActionDialog';
import { WatchlistRow } from '@/components/watchlist/WatchlistRow';
import { useQueryClient } from '@tanstack/react-query';
import {
  blendAddCost,
  tradeMarketForSymbol,
  tradeSourceForItem,
} from '@/lib/trade-recording';
import {
  invalidateUserTradesQueries,
  recordUserTrade,
} from '@/lib/queries/userTrades';
import {
  clusterExposureForSymbol,
  useCorrelationStatusQuery,
} from '@/lib/queries/backtest';
import { useWatchlistRsRanksQuery } from '@/lib/queries/watchlist';
import type { TrendOkResult, WatchlistQuote } from '@/lib/api/types';
import type { ExecutionGate } from '@karios/shared';
import { buildS3Candidates } from '@/lib/execution-markdown';
import { useChatStore } from '@/lib/chat/store';
import {
  buildDefensiveSleeveExposurePct,
  buildSectorExposureFromWatchlist,
  buildSleeveExposurePct,
  deriveActionCard,
  type CatalystPurgeHint,
} from '@/lib/execution-action';
import type { MainlineAllowSet } from '@/lib/hot-industry-picks';
import { getShanghaiTodayIso, isShanghaiTradingTime } from '@/lib/market-hours';
import {
  buildWatchlistRowMetrics,
} from '@/lib/watchlist-metrics';
import type { WatchlistItem } from '@/lib/watchlist-storage';
import { shouldShowInWatchlistTable } from '@/lib/watchlist-table-filter';

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
type WatchlistStickyColumn = 'score' | 'exec' | 'trendOk' | 'action';

const WATCHLIST_STICKY_COLUMN_LAYOUT: Record<
  WatchlistStickyColumn,
  { width: number; right: number; zHeader: number; zBody: number }
> = {
  // Fixed right group (left→right): score | exec | trendOk | action.
  score: { width: 80, right: 272, zHeader: 23, zBody: 13 },
  exec: { width: 110, right: 168, zHeader: 24, zBody: 14 },
  trendOk: { width: 80, right: 88, zHeader: 22, zBody: 12 },
  action: { width: 88, right: 0, zHeader: 25, zBody: 15 },
};

function watchlistStickyRowBg(tone: WatchlistRowTone, header = false): string {
  if (header) return 'bg-[var(--k-surface)]';
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
    'whitespace-nowrap',
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

export type WatchlistTableProps = {
  sortedItems: WatchlistItem[];
  items: WatchlistItem[];
  trend: Record<string, TrendOkResult>;
  quotes: Record<string, WatchlistQuote>;
  costPriceDrafts: Record<string, string>;
  positionPctDrafts: Record<string, string>;
  scoreSortDir: 'desc' | 'asc';
  scoreSortEnabled: boolean;
  setScoreSortDir: React.Dispatch<React.SetStateAction<'desc' | 'asc'>>;
  setScoreSortEnabled: React.Dispatch<React.SetStateAction<boolean>>;
  showHidden: boolean;
  setShowHidden: React.Dispatch<React.SetStateAction<boolean>>;
  setItemColor: (symbol: string, color: string) => void;
  setItemPositionPct: (symbol: string, value: string) => void;
  setItemPositionPctDraft: (symbol: string, value: string) => void;
  commitItemPositionPctDraft: (symbol: string) => void;
  setItemCostPriceDraft: (symbol: string, value: string) => void;
  setItemCostPriceValue: (symbol: string, value: number | null) => void;
  commitItemCostPriceDraft: (symbol: string) => void;
  onRemove: (sym: string) => void;
  onOpenStock?: (symbol: string) => void;
  executionGate?: ExecutionGate | null;
  mainlineAllow?: MainlineAllowSet | null;
  sectorOutflowBlock?: boolean;
  /** Alpha Radar Max Grade=S → WATCH_SILENT (same map as Copy / Journal). */
  catalystBySymbol?: Map<string, CatalystPurgeHint> | null;
};

export function sortWatchlistItems(
  items: WatchlistItem[],
  trend: Record<string, TrendOkResult>,
  scoreSortEnabled: boolean,
  scoreSortDir: 'desc' | 'asc',
  rsRanks: Record<string, number> | null = null,
): WatchlistItem[] {
  const arr = [...items];
  arr.sort((a, b) => {
    // 2026-08-11: held names (positionPct > 0) always float to the top —
    // the user trades from the watchlist and needs their open positions first.
    const ha = typeof a.positionPct === 'number' && a.positionPct > 0;
    const hb = typeof b.positionPct === 'number' && b.positionPct > 0;
    if (ha !== hb) return ha ? -1 : 1;
    if (!scoreSortEnabled) return 0;
    const sa = trend[a.symbol]?.score;
    const sb = trend[b.symbol]?.score;
    const va = typeof sa === 'number' && Number.isFinite(sa) ? sa : null;
    const vb = typeof sb === 'number' && Number.isFinite(sb) ? sb : null;
    if (va != null && vb != null) {
      const d = va - vb;
      if (d !== 0) return scoreSortDir === 'asc' ? d : -d;
    } else if (va == null && vb != null) {
      return 1;
    } else if (vb == null && va != null) {
      return -1;
    }
    // Score tie (or both missing): RS percentile desc — same number the
    // RS column shows (/watchlist/rs-ranks).
    const ra = rsRanks?.[a.symbol] ?? -1;
    const rb = rsRanks?.[b.symbol] ?? -1;
    return rb - ra;
  });
  return arr;
}

export function WatchlistTable({
  sortedItems,
  items,
  trend,
  quotes,
  costPriceDrafts,
  positionPctDrafts,
  scoreSortDir,
  scoreSortEnabled,
  setScoreSortDir,
  setScoreSortEnabled,
  showHidden,
  setShowHidden,
  setItemColor,
  setItemPositionPct,
  setItemPositionPctDraft,
  commitItemPositionPctDraft,
  setItemCostPriceDraft,
  setItemCostPriceValue,
  commitItemCostPriceDraft,
  onRemove,
  onOpenStock,
  executionGate = null,
  mainlineAllow = null,
  sectorOutflowBlock = false,
  catalystBySymbol = null,
}: WatchlistTableProps) {
  const { addReference } = useChatStore();

  const tradingTime = React.useMemo(() => isShanghaiTradingTime(), []);
  const todaySh = React.useMemo(() => getShanghaiTodayIso(), []);
  const rsRanksQuery = useWatchlistRsRanksQuery(sortedItems.map((i) => i.symbol));

  const rowMetricsBySymbol = React.useMemo(() => {
    const m = new Map<
      string,
      ReturnType<typeof buildWatchlistRowMetrics>
    >();
    for (const it of sortedItems) {
      m.set(
        it.symbol,
        buildWatchlistRowMetrics({
          symbol: it.symbol,
          trend: trend[it.symbol],
          quote: quotes[it.symbol],
          tradingTime,
          todaySh,
        }),
      );
    }
    return m;
  }, [sortedItems, trend, quotes, tradingTime, todaySh]);

  const sectorExposureByIndustry = React.useMemo(
    () => buildSectorExposureFromWatchlist(sortedItems, trend),
    [sortedItems, trend],
  );

  const sleeveExposurePct = React.useMemo(
    () => buildSleeveExposurePct(sortedItems),
    [sortedItems],
  );

  const correlationStatus = useCorrelationStatusQuery(true, true).data;
  const defensiveSleeveExposurePct = React.useMemo(
    () => buildDefensiveSleeveExposurePct(sortedItems, trend),
    [sortedItems, trend],
  );

  const s3Symbols = React.useMemo(() => {
    const cands = buildS3Candidates({
      items: sortedItems,
      trend,
      rsRanks: rsRanksQuery.data?.ranks ?? null,
      gate: executionGate ?? null,
      mainlineAllow: mainlineAllow ?? null,
      sectorOutflowBlock,
      cnCap: null,
      sleeveExposurePct,
    });
    return new Set(cands.map((c) => c.symbol));
  }, [sortedItems, trend, rsRanksQuery.data, executionGate, mainlineAllow, sectorOutflowBlock, sleeveExposurePct]);

  const actionBySymbol = React.useMemo(() => {
    const m = new Map<string, string>();
    for (const it of sortedItems) {
      const t = trend[it.symbol];
      const rowMetrics = rowMetricsBySymbol.get(it.symbol);
      if (!rowMetrics) continue;
      try {
        const card = deriveActionCard({
          symbol: it.symbol,
          gate: executionGate ?? null,
          trendok: t ?? null,
          position: it,
          currentPrice: rowMetrics.current,
          mainlineAllow: mainlineAllow ?? null,
          intradayChgPct: rowMetrics.intradayChgPct,
          gapUp: typeof t?.gapUp === 'boolean' ? t.gapUp : null,
          marketRegime: t?.marketRegime ?? null,
          sectorExposureByIndustry,
          sleeveExposurePct,
          defensiveSleeveExposurePct,
          sectorOutflowBlock,
          catalyst: catalystBySymbol?.get(it.symbol) ?? null,
          todaySh,
          isS3Candidate: s3Symbols.has(it.symbol),
        });
        m.set(it.symbol, card.action);
      } catch {
        m.set(it.symbol, 'WATCH_SILENT');
      }
    }
    return m;
  }, [
    sortedItems,
    trend,
    rowMetricsBySymbol,
    executionGate,
    mainlineAllow,
    sectorExposureByIndustry,
    sleeveExposurePct,
    defensiveSleeveExposurePct,
    sectorOutflowBlock,
    catalystBySymbol,
    todaySh,
  ]);

  const visibleSortedItems = React.useMemo(() => {
    return sortedItems.filter((it) => {
      const t = trend[it.symbol];
      const action = actionBySymbol.get(it.symbol) ?? null;
      if (action === 'PURGE') return true;
      return shouldShowInWatchlistTable(it, t ?? null, action);
    });
  }, [sortedItems, trend, actionBySymbol]);

  const hiddenCount = sortedItems.length - visibleSortedItems.length;

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
  const [tradeDialog, setTradeDialog] = React.useState<TradeDialogOpenState | null>(null);
  const queryClient = useQueryClient();

  const openTradeDialog = React.useCallback(
    (kind: 'buy' | 'add' | 'sell', item: WatchlistItem) => {
      const current = rowMetricsBySymbol.get(item.symbol)?.current ?? null;
      setTradeDialog({ kind, item, currentPrice: current });
    },
    [rowMetricsBySymbol],
  );

  const handleTradeConfirm = React.useCallback(
    async (values: { price: number; positionPct: number; costPrice?: number }) => {
      if (!tradeDialog) return;
      const { kind, item } = tradeDialog;
      const { price, positionPct, costPrice } = values;
      const symbol = item.symbol;
      const source = tradeSourceForItem(item);
      const market = tradeMarketForSymbol(symbol);
      try {
        if (kind === 'buy') {
          await recordUserTrade({ symbol, side: 'BUY', price, positionPct, source, market });
          setItemCostPriceValue(symbol, price);
          setItemPositionPct(symbol, String(positionPct));
        } else if (kind === 'add') {
          const oldCost = item.costPrice ?? price;
          const oldPct = item.positionPct ?? 0;
          const blended = blendAddCost(oldCost, oldPct, price, positionPct);
          await recordUserTrade({ symbol, side: 'ADD', price, positionPct, source, market });
          setItemCostPriceValue(symbol, blended.blendedCost);
          setItemPositionPct(symbol, String(blended.newPositionPct));
        } else {
          // Optional cost fill (2026-08-09): when the holding had no cost
          // price, the dialog can supply one so pnl is computed; the trade
          // is recorded either way.
          const costBasis =
            typeof costPrice === 'number' && costPrice > 0 ? costPrice : item.costPrice;
          if (typeof costBasis === 'number') setItemCostPriceValue(symbol, costBasis);
          await recordUserTrade({
            symbol,
            side: 'SELL',
            price,
            positionPct,
            costBasis: typeof costBasis === 'number' ? costBasis : undefined,
            entryDate: item.entryDate ?? undefined,
            source,
            market,
          });
          const remaining = (item.positionPct ?? 0) - positionPct;
          setItemPositionPct(symbol, String(Math.max(0, remaining)));
        }
        void invalidateUserTradesQueries(queryClient);
      } catch {
        // Trade journal is best-effort; watchlist edits still apply.
      } finally {
        setTradeDialog(null);
      }
    },
    [tradeDialog, queryClient, setItemCostPriceValue, setItemPositionPct],
  );

  const showTooltip = React.useCallback((el: HTMLElement, content: React.ReactNode, width = 360) => {
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
  }, []);

  const hideTooltip = React.useCallback(() => {
    setTooltip((prev) => (prev.open ? { ...prev, open: false } : prev));
  }, []);

  const showColorPicker = React.useCallback((el: HTMLElement, sym: string) => {
    const r = el.getBoundingClientRect();
    const pad = 10;
    const panelW = 220;
    const panelH = 220;
    const x0 = r.right - panelW;
    const x = Math.max(pad, Math.min(window.innerWidth - panelW - pad, x0));
    const shouldOpenDown = r.bottom + 8 + panelH <= window.innerHeight - pad;
    const placement: 'top-end' | 'bottom-end' = shouldOpenDown ? 'bottom-end' : 'top-end';
    let y = placement === 'bottom-end' ? r.bottom + 8 : r.top - 8;
    if (placement === 'bottom-end') {
      y = Math.max(pad, Math.min(window.innerHeight - panelH - pad, y));
    } else {
      y = Math.max(pad + panelH, Math.min(window.innerHeight - pad, y));
    }
    setColorPicker({ open: true, x, y, placement, symbol: sym });
  }, []);

  const hideColorPicker = React.useCallback(() => {
    setColorPicker((prev) => (prev.open ? { ...prev, open: false, symbol: null } : prev));
  }, []);

  const onAddReference = React.useCallback(
    (item: WatchlistItem, tRef: TrendOkResult | undefined) => {
      const capturedAt = new Date().toISOString();
      addReference({
        kind: 'watchlistStock',
        refId: `${item.symbol}:${capturedAt}`,
        symbol: item.symbol,
        name: item.name ?? null,
        capturedAt,
        asOfDate: tRef?.asOfDate ?? null,
        close: typeof tRef?.values?.close === 'number' ? tRef.values.close : null,
        trendOk: tRef?.trendOk ?? null,
        score: tRef?.score ?? null,
        stopLossPrice: tRef?.stopLossPrice ?? null,
        buyMode: tRef?.buyMode ?? null,
        buyAction: tRef?.buyAction ?? null,
        buyZoneLow: tRef?.buyZoneLow ?? null,
        buyZoneHigh: tRef?.buyZoneHigh ?? null,
        buyWhy: tRef?.buyWhy ?? null,
        intradayChgPct: tRef?.intradayChgPct ?? null,
        gapUp: tRef?.gapUp ?? null,
        riskAlerts: tRef?.riskAlerts ?? [],
      });
    },
    [addReference],
  );

  const renderedItems = React.useMemo(() => {
    if (showHidden) {
      const hiddenSet = new Set(
        sortedItems
          .filter(
            (it) =>
              !shouldShowInWatchlistTable(
                it,
                trend[it.symbol] ?? null,
                actionBySymbol.get(it.symbol) ?? null,
              ),
          )
          .map((it) => it.symbol),
      );
      return { items: sortedItems, hiddenSet };
    }
    return {
      items: visibleSortedItems,
      hiddenSet: new Set<string>(),
    };
  }, [showHidden, sortedItems, visibleSortedItems, trend, actionBySymbol]);

  return (
    <>
      <section className="box-border grid min-w-0 w-full grid-cols-1 overflow-hidden rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="mb-2 flex items-center justify-between gap-2">
          <div className="text-sm font-medium">List</div>
          <div className="flex items-center gap-3 text-xs text-[var(--k-muted)]">
            <span>
              {visibleSortedItems.length} / {items.length} items
              {hiddenCount > 0 ? ` · ${hiddenCount} hidden (silent dead rows)` : ''}
            </span>
            {hiddenCount > 0 ? (
              <button
                type="button"
                className="inline-flex items-center gap-1 rounded border border-[var(--k-border)] bg-[var(--k-surface-2)] px-2 py-0.5 text-[11px] hover:text-[var(--k-text)]"
                onClick={() => setShowHidden((v) => !v)}
                title={
                  showHidden
                    ? 'Click to hide silent dead rows (low score, no position, TrendOK off).'
                    : 'Click to show all rows including silent dead ones (dimmed).'
                }
                aria-label={showHidden ? 'Hide silent rows' : 'Show all rows'}
              >
                {showHidden ? (
                  <>
                    <ChevronUp className="h-3 w-3" aria-hidden />
                    Hide silent
                  </>
                ) : (
                  <>
                    <ChevronDown className="h-3 w-3" aria-hidden />
                    Show all {items.length}
                  </>
                )}
              </button>
            ) : null}
          </div>
        </div>

        {items.length ? (
          <div className="min-w-0 w-full overflow-hidden rounded border border-[var(--k-border)]">
            <div className="overflow-x-auto overscroll-x-contain">
              <table className="w-max min-w-full border-separate border-spacing-0 text-sm">
                <thead className="bg-[var(--k-surface)] text-[var(--k-muted)]">
                  <tr className="text-left">
                    <th className="px-3 py-2 min-w-[44px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="color"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={300}
                      />
                    </th>
                    <th className="px-3 py-2 min-w-[110px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="symbol"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={320}
                      />
                    </th>
                    <th className="px-3 py-2 min-w-[120px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="name"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={300}
                      />
                    </th>
                    <th className="px-3 py-2 min-w-[140px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="industry"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 min-w-[96px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="positionPct"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 min-w-[96px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="costPrice"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 min-w-[88px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="currentPrice"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 min-w-[96px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="stopLoss"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={360}
                      />
                    </th>
                    <th className="px-2 py-2 min-w-[104px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="trigger"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={360}
                      />
                    </th>
                    <th className="px-2 py-2 min-w-[104px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="trail"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 min-w-[120px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="buy"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={360}
                      />
                    </th>
                    <th className="px-2 py-2 min-w-[96px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="hotTop3"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 min-w-[80px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="rs"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={360}
                      />
                    </th>
                    <th className="px-2 py-2 min-w-[88px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="vwap"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 min-w-[88px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="intradayPct"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 min-w-[88px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="volumeRatio"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 min-w-[120px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="instFlow"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 min-w-[80px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="gap"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 min-w-[120px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="alerts"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={360}
                      />
                    </th>
                    <th className="px-2 py-2 min-w-[80px] whitespace-nowrap">
                      <ColumnHeader
                        columnId="pnl"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
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
                        <ColumnHeader
                          columnId="score"
                          showTooltip={showTooltip}
                          hideTooltip={hideTooltip}
                          width={340}
                        />
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
                      className={watchlistStickyCellClass('exec', { header: true })}
                      style={watchlistStickyCellStyle('exec', { header: true })}
                    >
                      <ColumnHeader
                        columnId="execAction"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={360}
                      />
                    </th>

                    <th
                      className={watchlistStickyCellClass('trendOk', { header: true })}
                      style={watchlistStickyCellStyle('trendOk', { header: true })}
                    >
                      <ColumnHeader
                        columnId="trendOk"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={380}
                      />
                    </th>
                    <th
                      className={watchlistStickyCellClass('action', {
                        header: true,
                        extra: 'text-right',
                      })}
                      style={watchlistStickyCellStyle('action', { header: true })}
                    >
                      <ColumnHeader
                        columnId="action"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {renderedItems.items.map((it) => {
                    const rowMetrics = rowMetricsBySymbol.get(it.symbol);
                    if (!rowMetrics) return null;
                    const isHiddenRow = renderedItems.hiddenSet.has(it.symbol);
                    return (
                      <WatchlistRow
                        key={it.symbol}
                        item={it}
                        trend={trend[it.symbol]}
                        quote={quotes[it.symbol]}
                        rowMetrics={rowMetrics}
                        tradingTime={tradingTime}
                        todaySh={todaySh}
                        costPriceDraft={costPriceDrafts[it.symbol]}
                        positionPctDraft={positionPctDrafts[it.symbol]}
                        executionGate={executionGate ?? null}
                        mainlineAllow={mainlineAllow ?? null}
                        sectorOutflowBlock={sectorOutflowBlock}
                        catalyst={catalystBySymbol?.get(it.symbol) ?? null}
                        sectorExposureByIndustry={sectorExposureByIndustry}
                        sleeveExposurePct={sleeveExposurePct}
                        defensiveSleeveExposurePct={defensiveSleeveExposurePct}
                        clusterExposurePct={clusterExposureForSymbol(correlationStatus, it.symbol)}
                        rsRank={rsRanksQuery.data?.ranks[it.symbol] ?? null}
                        isS3Candidate={s3Symbols.has(it.symbol)}
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        showColorPicker={showColorPicker}
                        setItemPositionPct={setItemPositionPct}
                        setItemPositionPctDraft={setItemPositionPctDraft}
                        commitItemPositionPctDraft={commitItemPositionPctDraft}
                        setItemCostPriceDraft={setItemCostPriceDraft}
                        setItemCostPriceValue={setItemCostPriceValue}
                        commitItemCostPriceDraft={commitItemCostPriceDraft}
                        onRemove={onRemove}
                        onOpenStock={onOpenStock}
                        onAddReference={onAddReference}
                        onOpenTradeDialog={openTradeDialog}
                        rowClassName={isHiddenRow ? 'opacity-50' : undefined}
                        rowTitle={
                          isHiddenRow
                            ? 'Silent dead row (Pos%≤0 · Score<70 · TrendOK≠ok/recovering · WATCH_SILENT)'
                            : undefined
                        }
                      />
                    );
                  })}

                </tbody>
              </table>
            </div>
          </div>
        ) : (
          <div className="text-sm text-[var(--k-muted)]">No items yet. Add a ticker above.</div>
        )}
      </section>

      {tradeDialog ? (
        <TradeActionDialog
          state={tradeDialog}
          suggestPct={5}
          onClose={() => setTradeDialog(null)}
          onConfirm={(values) => void handleTradeConfirm(values)}
        />
      ) : null}

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
    </>
  );
}
