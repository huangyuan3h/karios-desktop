'use client';

import * as React from 'react';
import { createPortal } from 'react-dom';
import {
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  CircleX,
  ExternalLink,
  Trash2,
} from 'lucide-react';

import { Button } from '@/components/ui/button';
import type { TrendOkResult, WatchlistQuote } from '@/lib/api/types';
import { useChatStore } from '@/lib/chat/store';
import { getShanghaiTodayIso, isShanghaiTradingTime } from '@/lib/market-hours';
import {
  buildWatchlistRowMetrics,
  computePnLPct,
  formatGapUp,
  formatHotTop3,
  formatInstFlow,
  formatInstFlowTooltip,
  formatIntradayChgPct,
  formatPnLPct,
  formatRiskAlerts,
  formatVolumeRatio,
  formatVwap,
  industryDisplayName,
  isIntradaySurge,
  isInstFlowRisk,
  resolveWatchlistCurrentPrice,
  tushareIndustryTooltip,
  volumeRatioClassName,
} from '@/lib/watchlist-metrics';
import type { WatchlistItem } from '@/lib/watchlist-storage';
import {
  fmtBuyCell,
  fmtNum,
  fmtPrice,
  fmtScore,
  rowTone,
} from '@/lib/watchlist-table-cells';

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
}: WatchlistTableProps) {
  const { addReference } = useChatStore();
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

  function showTooltip(el: HTMLElement, content: React.ReactNode, width = 360) {
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
  }

  function hideColorPicker() {
    setColorPicker((prev) => (prev.open ? { ...prev, open: false, symbol: null } : prev));
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
    const volumeRatio =
      typeof t?.values?.volumeRatio === 'number' && Number.isFinite(t.values.volumeRatio)
        ? t.values.volumeRatio
        : null;
    const lowVolumeRatio = t?.checks?.lowVolumeRatio as boolean | null | undefined;
    const lowVolumeRatioOk = lowVolumeRatio == null ? null : !lowVolumeRatio;
    const lines = [
      checkLine(
        'EMA trend',
        (t?.checks?.emaOrder as boolean | null | undefined) ?? null,
        'Close > EMA(20) AND EMA(20) > EMA(60)',
      ),
      checkLine(
        'MACD > 0',
        (t?.checks?.macdPositive as boolean | null | undefined) ?? null,
        'macdLine > 0',
      ),
      checkLine(
        'MACD hist',
        (t?.checks?.macdHistExpanding as boolean | null | undefined) ?? null,
        `histogram > 0 (red bar above zero axis). Expansion is scored separately; ${macdHistDetail}`,
      ),
      checkLine(
        'Near 20D high',
        (t?.checks?.closeNear20dHigh as boolean | null | undefined) ?? null,
        'Close >= 0.90 * High(20)',
      ),
      checkLine(
        'RSI(14)',
        (t?.checks?.rsiInRange as boolean | null | undefined) ?? null,
        `50 <= RSI <= 90${rsiNow == null ? '' : ` (now: ${rsiNow.toFixed(1)})`}`,
      ),
      checkLine(
        'Volume',
        (t?.checks?.volumeSurge as boolean | null | undefined) ?? null,
        'AvgVol(5) > 0.9 * AvgVol(30)',
      ),
      checkLine(
        'VR hard cap',
        lowVolumeRatioOk,
        `VR >= 1.2${volumeRatio == null ? '' : ` (now: ${formatVolumeRatio(volumeRatio)})`}`,
      ),
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
    <>
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
                    <th className="px-2 py-2 w-[64px]" title="RS (Relative Strength) vs CSI300 20-day return">
                      RS
                    </th>
                    <th className="px-2 py-2 w-[68px]">VWAP</th>
                    <th className="px-2 py-2 w-[72px]">Intraday%</th>
                    <th className="px-2 py-2 w-[72px]">VR(量比)</th>
                    <th className="px-2 py-2 w-[120px]">Inst_Flow</th>
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
                      className={watchlistStickyCellClass('action', {
                        header: true,
                        extra: 'text-right',
                      })}
                      style={watchlistStickyCellStyle('action', { header: true })}
                    >
                      Action
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {sortedItems.map((it) => {
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
                            const tRow = trend[it.symbol];
                            const qRow = quotes[it.symbol];
                            const close0 = tRow?.values?.close;
                            const trendClose =
                              typeof close0 === 'number' && Number.isFinite(close0)
                                ? (close0 as number)
                                : null;
                            const current = resolveWatchlistCurrentPrice({
                              tradingTime: isShanghaiTradingTime(),
                              todaySh: getShanghaiTodayIso(),
                              symbol: it.symbol,
                              trendAsOfDate: tRow?.asOfDate ?? null,
                              quotePrice: qRow?.price ?? null,
                              quoteTradeTime: qRow?.tradeTime ?? null,
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
                            <span
                              className="text-emerald-600 font-medium"
                              title="Industry in today fund-flow Top3"
                            >
                              ✓
                            </span>
                          ) : (
                            '—'
                          )}
                        </td>
                        <td className="px-2 py-2 font-mono text-xs">
                          {(() => {
                            const rs = t?.rs ?? (t?.values?.rsValue as number | undefined);
                            if (typeof rs !== 'number' || !Number.isFinite(rs)) return '—';
                            const isLeader = t?.checks?.rs_leader === true;
                            return (
                              <span
                                className={
                                  isLeader
                                    ? 'font-bold text-emerald-600'
                                    : rs > 0
                                      ? 'text-emerald-600'
                                      : 'text-red-600'
                                }
                                title={
                                  isLeader
                                    ? '💪 RS_Leader (逆势抗跌) — outperforming CSI300 by >10% in weak market'
                                    : `RS vs CSI300 20D: ${rs > 0 ? '+' : ''}${rs.toFixed(1)}%`
                                }
                              >
                                {rs > 0 ? '+' : ''}
                                {rs.toFixed(1)}%
                              </span>
                            );
                          })()}
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
                          className={`px-3 py-2 font-mono ${volumeRatioClassName(rowMetrics.volumeRatio)}`}
                        >
                          {formatVolumeRatio(rowMetrics.volumeRatio)}
                        </td>
                        <td
                          className={`max-w-[120px] truncate px-2 py-2 text-xs font-mono ${
                            isInstFlowRisk(t?.instFlow) ? 'font-semibold text-red-600' : ''
                          }`}
                          title={formatInstFlowTooltip(t?.instFlow) ?? t?.instFlow?.label ?? undefined}
                        >
                          {formatInstFlow(t?.instFlow)}
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
                            <div className="truncate" title={formatRiskAlerts(rowMetrics.alerts)}>
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
                          className={`px-3 py-2 font-mono ${(() => {
                            const pnl = computePnLPct(it.costPrice ?? null, rowMetrics.current);
                            if (pnl == null) return '';
                            if (pnl >= 5) return 'text-emerald-600';
                            if (pnl <= 0) return 'text-red-600';
                            return '';
                          })()}`}
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
                          className={watchlistStickyCellClass('action', {
                            tone,
                            extra: 'text-right',
                          })}
                          style={watchlistStickyCellStyle('action')}
                        >
                          <div className="flex justify-end">
                            <Button
                              variant="ghost"
                              size="icon"
                              className="h-8 w-8"
                              onClick={() => {
                                const tRef = trend[it.symbol];
                                const capturedAt = new Date().toISOString();
                                addReference({
                                  kind: 'watchlistStock',
                                  refId: `${it.symbol}:${capturedAt}`,
                                  symbol: it.symbol,
                                  name: it.name ?? null,
                                  capturedAt,
                                  asOfDate: tRef?.asOfDate ?? null,
                                  close:
                                    typeof tRef?.values?.close === 'number'
                                      ? tRef.values.close
                                      : null,
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
