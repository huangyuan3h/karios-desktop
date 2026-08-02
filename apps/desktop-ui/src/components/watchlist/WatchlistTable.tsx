'use client';

import * as React from 'react';
import { createPortal } from 'react-dom';
import {
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  CircleX,
} from 'lucide-react';

import { Button } from '@/components/ui/button';
import { ColumnHeader } from '@/components/watchlist/ColumnHeader';
import { WatchlistRow } from '@/components/watchlist/WatchlistRow';
import type { TrendOkResult, WatchlistQuote } from '@/lib/api/types';
import type { ExecutionGate } from '@karios/shared';
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

export type WatchlistTableProps = {
  sortedItems: WatchlistItem[];
  items: WatchlistItem[];
  trend: Record<string, TrendOkResult>;
  quotes: Record<string, WatchlistQuote>;
  costPriceDrafts: Record<string, string>;
  scoreSortDir: 'desc' | 'asc';
  scoreSortEnabled: boolean;
  setScoreSortDir: React.Dispatch<React.SetStateAction<'desc' | 'asc'>>;
  setScoreSortEnabled: React.Dispatch<React.SetStateAction<boolean>>;
  setItemColor: (symbol: string, color: string) => void;
  setItemPositionPct: (symbol: string, value: string) => void;
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
): WatchlistItem[] {
  if (!scoreSortEnabled) return items;
  const arr = [...items];
  arr.sort((a, b) => {
    const sa = trend[a.symbol]?.score;
    const sb = trend[b.symbol]?.score;
    const va = typeof sa === 'number' && Number.isFinite(sa) ? sa : null;
    const vb = typeof sb === 'number' && Number.isFinite(sb) ? sb : null;
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    const d = va - vb;
    return scoreSortDir === 'asc' ? d : -d;
  });
  return arr;
}

export function WatchlistTable({
  sortedItems,
  items,
  trend,
  quotes,
  costPriceDrafts,
  scoreSortDir,
  scoreSortEnabled,
  setScoreSortDir,
  setScoreSortEnabled,
  setItemColor,
  setItemPositionPct,
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

  const defensiveSleeveExposurePct = React.useMemo(
    () => buildDefensiveSleeveExposurePct(sortedItems, trend),
    [sortedItems, trend],
  );

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

  return (
    <>
      <section className="box-border grid min-w-0 w-full grid-cols-1 overflow-hidden rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="mb-2 flex items-center justify-between">
          <div className="text-sm font-medium">List</div>
          <div className="text-xs text-[var(--k-muted)]">
            {visibleSortedItems.length} / {items.length} items
            {hiddenCount > 0 ? ` · ${hiddenCount} hidden (silent dead rows)` : ''}
          </div>
        </div>

        {items.length ? (
          <div className="min-w-0 w-full overflow-hidden rounded border border-[var(--k-border)]">
            <div className="overflow-x-auto overscroll-x-contain">
              <table className="w-max min-w-full border-separate border-spacing-0 text-sm">
                <thead className="bg-[var(--k-surface)] text-[var(--k-muted)]">
                  <tr className="text-left">
                    <th className="px-3 py-2 w-[40px]">
                      <ColumnHeader
                        columnId="color"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={300}
                      />
                    </th>
                    <th className="px-3 py-2 w-[110px]">
                      <ColumnHeader
                        columnId="symbol"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={320}
                      />
                    </th>
                    <th className="px-3 py-2 w-[120px] max-w-[120px]">
                      <ColumnHeader
                        columnId="name"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={300}
                      />
                    </th>
                    <th className="px-3 py-2 w-[120px] max-w-[140px]">
                      <ColumnHeader
                        columnId="industry"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 w-[58px]">
                      <ColumnHeader
                        columnId="positionPct"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 w-[80px]">
                      <ColumnHeader
                        columnId="costPrice"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 w-[72px]">
                      <ColumnHeader
                        columnId="currentPrice"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 w-[80px]">
                      <ColumnHeader
                        columnId="stopLoss"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={360}
                      />
                    </th>
                    <th className="px-2 py-2 w-[56px]">
                      <ColumnHeader
                        columnId="execAction"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={360}
                      />
                    </th>
                    <th className="px-2 py-2 w-[72px]">
                      <ColumnHeader
                        columnId="trigger"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={360}
                      />
                    </th>
                    <th className="px-2 py-2 w-[64px]">
                      <ColumnHeader
                        columnId="trail"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="max-w-[130px] px-2 py-2 w-[120px]">
                      <ColumnHeader
                        columnId="buy"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={360}
                      />
                    </th>
                    <th className="px-2 py-2 w-[64px]">
                      <ColumnHeader
                        columnId="hotTop3"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 w-[64px]">
                      <ColumnHeader
                        columnId="rs"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={360}
                      />
                    </th>
                    <th className="px-2 py-2 w-[68px]">
                      <ColumnHeader
                        columnId="vwap"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 w-[72px]">
                      <ColumnHeader
                        columnId="intradayPct"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 w-[72px]">
                      <ColumnHeader
                        columnId="volumeRatio"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 w-[120px]">
                      <ColumnHeader
                        columnId="instFlow"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 w-[48px]">
                      <ColumnHeader
                        columnId="gap"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={340}
                      />
                    </th>
                    <th className="px-2 py-2 w-[140px]">
                      <ColumnHeader
                        columnId="alerts"
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        width={360}
                      />
                    </th>
                    <th className="px-2 py-2 w-[64px]">
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
                  {visibleSortedItems.map((it) => {
                    const rowMetrics = rowMetricsBySymbol.get(it.symbol);
                    if (!rowMetrics) return null;
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
                        executionGate={executionGate ?? null}
                        mainlineAllow={mainlineAllow ?? null}
                        sectorOutflowBlock={sectorOutflowBlock}
                        catalyst={catalystBySymbol?.get(it.symbol) ?? null}
                        sectorExposureByIndustry={sectorExposureByIndustry}
                        sleeveExposurePct={sleeveExposurePct}
                        defensiveSleeveExposurePct={defensiveSleeveExposurePct}
                        showTooltip={showTooltip}
                        hideTooltip={hideTooltip}
                        showColorPicker={showColorPicker}
                        setItemPositionPct={setItemPositionPct}
                        setItemCostPriceDraft={setItemCostPriceDraft}
                        setItemCostPriceValue={setItemCostPriceValue}
                        commitItemCostPriceDraft={commitItemCostPriceDraft}
                        onRemove={onRemove}
                        onOpenStock={onOpenStock}
                        onAddReference={onAddReference}
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
