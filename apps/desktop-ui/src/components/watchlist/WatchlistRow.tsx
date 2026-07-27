'use client';

import * as React from 'react';
import { CircleX, ExternalLink, Trash2 } from 'lucide-react';

import { Button } from '@/components/ui/button';
import type { TrendOkResult, WatchlistQuote } from '@/lib/api/types';
import {
  deriveActionCard,
  isHeldMissingPositionPct,
  isHeldPosition,
  type CatalystPurgeHint,
} from '@/lib/execution-action';
import type { MainlineAllowSet } from '@/lib/hot-industry-picks';
import { getShanghaiTodayIso } from '@/lib/market-hours';
import type { ExecutionGate } from '@karios/shared';
import {
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
  type WatchlistRiskAlert,
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

type ShowTooltipFn = (el: HTMLElement, content: React.ReactNode, width?: number) => void;

function checkLine(label: string, ok: boolean | null | undefined, detail: string) {
  if (ok == null) return { label, state: '—', detail };
  return { label, state: ok ? '✅' : '❌', detail };
}

function TrendOkCell({
  sym,
  t,
  showTooltip,
  hideTooltip,
}: {
  sym: string;
  t: TrendOkResult | undefined;
  showTooltip: ShowTooltipFn;
  hideTooltip: () => void;
}) {
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

function StopLossCell({
  sym,
  t,
  showTooltip,
  hideTooltip,
}: {
  sym: string;
  t: TrendOkResult | undefined;
  showTooltip: ShowTooltipFn;
  hideTooltip: () => void;
}) {
  const p = t?.stopLossPrice ?? null;
  const parts = t?.stopLossParts ?? null;
  const get = (k: string) =>
    parts && typeof parts === 'object' ? (parts as Record<string, unknown>)[k] : undefined;
  const exitNow = Boolean(get('exit_now'));
  const exitDisplay = typeof get('exit_display') === 'string' ? String(get('exit_display')) : null;
  const warnHalf = Boolean(get('warn_reduce_half'));
  const warnDisplay = typeof get('warn_display') === 'string' ? String(get('warn_display')) : null;
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
      <div className="text-[var(--k-muted)]">Formula: max(final_support - atr_k×ATR14, hard_stop)</div>
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

function ScoreCell({
  sym,
  t,
  showTooltip,
  hideTooltip,
}: {
  sym: string;
  t: TrendOkResult | undefined;
  showTooltip: ShowTooltipFn;
  hideTooltip: () => void;
}) {
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
          <div className="mt-2 text-[var(--k-muted)]">No breakdown available (insufficient data).</div>
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

function BuyCell({
  sym,
  t,
  showTooltip,
  hideTooltip,
}: {
  sym: string;
  t: TrendOkResult | undefined;
  showTooltip: ShowTooltipFn;
  hideTooltip: () => void;
}) {
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

export type WatchlistRowMetrics = {
  current: number | null;
  vwap: number | null;
  intradayChgPct: number | null;
  volumeRatio: number | null;
  gapUp: boolean | null;
  alerts: WatchlistRiskAlert[];
};

export type WatchlistRowProps = {
  item: WatchlistItem;
  trend: TrendOkResult | undefined;
  quote: WatchlistQuote | undefined;
  rowMetrics: WatchlistRowMetrics;
  tradingTime: boolean;
  todaySh: string;
  costPriceDraft: string | undefined;
  executionGate: ExecutionGate | null;
  mainlineAllow: MainlineAllowSet | null;
  sectorOutflowBlock?: boolean;
  catalyst?: CatalystPurgeHint | null;
  sectorExposureByIndustry: Map<string, number> | null;
  sleeveExposurePct: number;
  defensiveSleeveExposurePct?: number;
  showTooltip: ShowTooltipFn;
  hideTooltip: () => void;
  showColorPicker: (el: HTMLElement, sym: string) => void;
  setItemPositionPct: (symbol: string, value: string) => void;
  setItemCostPriceDraft: (symbol: string, value: string) => void;
  setItemCostPriceValue: (symbol: string, value: number | null) => void;
  commitItemCostPriceDraft: (symbol: string) => void;
  onRemove: (sym: string) => void;
  onOpenStock?: (symbol: string) => void;
  onAddReference: (item: WatchlistItem, trend: TrendOkResult | undefined) => void;
};

function WatchlistRowInner({
  item: it,
  trend: t,
  quote: q,
  rowMetrics,
  tradingTime,
  todaySh,
  costPriceDraft,
  executionGate,
  mainlineAllow,
  sectorOutflowBlock = false,
  catalyst = null,
  sectorExposureByIndustry,
  sleeveExposurePct,
  defensiveSleeveExposurePct = 0,
  showTooltip,
  hideTooltip,
  showColorPicker,
  setItemPositionPct,
  setItemCostPriceDraft,
  setItemCostPriceValue,
  commitItemCostPriceDraft,
  onRemove,
  onOpenStock,
  onAddReference,
}: WatchlistRowProps) {
  const tone = rowTone(t, rowMetrics.alerts);
  const rowClass =
    tone === 'green'
      ? 'group border-t border-[var(--k-border)] bg-emerald-50/60 hover:bg-emerald-100/60'
      : tone === 'red'
        ? 'group border-t border-[var(--k-border)] bg-red-50/60 hover:bg-red-100/60'
        : 'group border-t border-[var(--k-border)] hover:bg-[var(--k-surface-2)]';

  const close0 = t?.values?.close;
  const trendClose =
    typeof close0 === 'number' && Number.isFinite(close0) ? (close0 as number) : null;
  const currentPrice = resolveWatchlistCurrentPrice({
    tradingTime,
    todaySh,
    symbol: it.symbol,
    trendAsOfDate: t?.asOfDate ?? null,
    quotePrice: q?.price ?? null,
    quoteTradeTime: q?.tradeTime ?? null,
    trendClose,
  });
  const actionCard = deriveActionCard({
    symbol: it.symbol,
    gate: executionGate,
    trendok: t,
    position: it,
    currentPrice,
    mainlineAllow,
    intradayChgPct: rowMetrics.intradayChgPct,
    gapUp: typeof t?.gapUp === 'boolean' ? t.gapUp : null,
    marketRegime: t?.marketRegime ?? null,
    sectorExposureByIndustry,
    sleeveExposurePct,
    defensiveSleeveExposurePct,
    sectorOutflowBlock,
    catalyst,
    todaySh: getShanghaiTodayIso(),
  });
  const execTone =
    actionCard.action === 'EXIT' || actionCard.action === 'PURGE'
      ? 'text-red-600 font-semibold'
      : actionCard.action === 'BUY' || actionCard.action === 'ADD'
        ? 'text-emerald-700 font-semibold'
        : actionCard.action === 'WATCH_SILENT' ||
            actionCard.action === 'TRIM' ||
            actionCard.why === 'T1_LOCK' ||
            actionCard.why === 'ENTRY_DATE_MISSING' ||
            actionCard.why === 'ENTRY_BELOW_STOP' ||
            actionCard.why === 'NOT_MAINLINE' ||
            actionCard.why === 'SECTOR_OUTFLOW_BLOCK' ||
            actionCard.why === 'DEFENSE_SECTOR_BLOCK' ||
            actionCard.why === 'GATE_DEFEND' ||
            actionCard.why === 'MAINLINE_FADE' ||
            actionCard.why === 'INTRADAY_SURGE_BLOCK' ||
            actionCard.why === 'GAP_UP_WEAK_BLOCK' ||
            actionCard.why === 'SIZE_CAP_BLOCK' ||
            actionCard.why === 'SECTOR_CONC_BLOCK' ||
            actionCard.why === 'SLEEVE_CAP_BLOCK' ||
            actionCard.why === 'TIME_LOCK_WEAK_REGIME' ||
            actionCard.why === 'MARKET_CLOSING_LOCK'
          ? 'text-amber-700 font-semibold'
          : 'text-[var(--k-muted)]';
  const heldForTrigger = isHeldPosition(it);
  const triggerPrice = heldForTrigger
    ? (actionCard.exitStop ?? actionCard.trigger ?? null)
    : (actionCard.entryTrigger ?? actionCard.trigger ?? null);
  const triggerTitle = heldForTrigger
    ? 'Exit_Stop (max hardStop, trailStop)'
    : 'Entry_Trigger (buyZoneHigh sniper)';

  return (
    <tr className={rowClass}>
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
          tushareIndustryTooltip((t?.values ?? null) as Record<string, unknown> | null) ??
          industryDisplayName((t?.values ?? {}) as Record<string, unknown>)
        }
      >
        {industryDisplayName((t?.values ?? {}) as Record<string, unknown>)}
      </td>
      <td className="px-2 py-2">
        <input
          className={
            isHeldMissingPositionPct(it)
              ? 'h-8 w-full min-w-0 max-w-[52px] rounded-md border border-amber-500/60 bg-[var(--k-surface-2)] px-1.5 font-mono text-xs outline-none'
              : 'h-8 w-full min-w-0 max-w-[52px] rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-1.5 font-mono text-xs outline-none'
          }
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
            costPriceDraft ??
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
            if (costPriceDraft != null) return;
            if (typeof it.costPrice === 'number' && Number.isFinite(it.costPrice)) {
              setItemCostPriceDraft(it.symbol, it.costPrice.toFixed(2));
            }
          }}
          onBlur={() => commitItemCostPriceDraft(it.symbol)}
        />
      </td>
      <td
        className="px-3 py-2 font-mono"
        title={
          t?.asOfDate
            ? `as of ${t.asOfDate}`
            : t
              ? 'as of latest cached daily bar'
              : '—'
        }
      >
        {fmtPrice(currentPrice)}
      </td>
      <td className="px-2 py-2">
        <StopLossCell sym={it.symbol} t={t} showTooltip={showTooltip} hideTooltip={hideTooltip} />
      </td>
      <td
        className={`px-2 py-2 font-mono text-xs ${execTone}`}
        title={[
          actionCard.why,
          actionCard.mainlineOk
            ? `mainline=${actionCard.mainlineTag || 'ok'}`
            : 'mainline=no',
          typeof actionCard.suggestAddPct === 'number'
            ? `suggest +${actionCard.suggestAddPct.toFixed(1)}% (${actionCard.suggestSizeNote || 'clip'})`
            : null,
        ]
          .filter(Boolean)
          .join(' · ')}
      >
        {actionCard.action}
        {typeof actionCard.suggestAddPct === 'number' ? (
          <span className="ml-1 font-normal text-emerald-700/90">
            +{actionCard.suggestAddPct.toFixed(0)}%
          </span>
        ) : null}
      </td>
      <td className="px-2 py-2 font-mono text-xs" title={triggerTitle}>
        {fmtPrice(triggerPrice)}
      </td>
      <td
        className="px-2 py-2 text-xs"
        title={
          actionCard.trailArmed
            ? `Chandelier armed · peak ${fmtPrice(actionCard.peak ?? null)} · trail ${fmtPrice(actionCard.trailStop ?? null)}`
            : 'Chandelier not armed (need PnL≥10%)'
        }
      >
        {actionCard.trailArmed ? (
          <span className="text-emerald-700">已激活↑</span>
        ) : (
          <span className="text-[var(--k-muted)]">未激活</span>
        )}
      </td>
      <td className="max-w-[130px] truncate px-2 py-2">
        <BuyCell sym={it.symbol} t={t} showTooltip={showTooltip} hideTooltip={hideTooltip} />
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
          isIntradaySurge(rowMetrics.intradayChgPct) ? 'font-semibold text-red-600' : ''
        }`}
      >
        {formatIntradayChgPct(rowMetrics.intradayChgPct)}
      </td>
      <td className={`px-3 py-2 font-mono ${volumeRatioClassName(rowMetrics.volumeRatio)}`}>
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
                className={alert.severity === 'block' ? 'text-red-600' : 'text-amber-700'}
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
      <td className={watchlistStickyCellClass('score', { tone })} style={watchlistStickyCellStyle('score')}>
        <ScoreCell sym={it.symbol} t={t} showTooltip={showTooltip} hideTooltip={hideTooltip} />
      </td>
      <td
        className={watchlistStickyCellClass('trendOk', { tone })}
        style={watchlistStickyCellStyle('trendOk')}
      >
        <TrendOkCell sym={it.symbol} t={t} showTooltip={showTooltip} hideTooltip={hideTooltip} />
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
            onClick={() => onAddReference(it, t)}
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
}

export const WatchlistRow = React.memo(WatchlistRowInner);
