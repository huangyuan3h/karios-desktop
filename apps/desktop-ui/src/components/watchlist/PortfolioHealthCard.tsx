'use client';

import * as React from 'react';

import { Bell, BellRing, RefreshCw, ShieldAlert } from 'lucide-react';

import { useQuery, useQueryClient } from '@tanstack/react-query';

import { Button } from '@/components/ui/button';
import { recordUserTrade, invalidateUserTradesQueries } from '@/lib/queries/userTrades';
import { tradeMarketForSymbol } from '@/lib/trade-recording';
import {
  addBuyReminder,
  BUY_REMINDERS_UPDATED_EVENT,
  loadBuyReminders,
  removeBuyReminder,
  type BuyReminder,
} from '@/lib/buy-reminders';
import {
  fetchPortfolioHealth,
  isMarketGateClosed,
  type PortfolioCandidate,
  type PortfolioHealthResponse,
  type PortfolioHolding,
} from '@/lib/queries/portfolioHealth';
import { useBacktestReconQuery, useTwinStarActionQuery, refreshTwinStarAction, type ReconItem } from '@/lib/queries/backtest';
import { useDashboardSentimentQuery } from '@/lib/queries/sentiment';
import { useStrategyMode } from '@/lib/strategy-settings';
import { getShanghaiMinutes, getShanghaiTodayIso, isShanghaiTradingTime, satNamesVisible } from '@/lib/market-hours';
import { cn } from '@/lib/utils';
import { isCnWatchlistSymbol, toTsCodeFromSymbol } from '@/lib/symbols';
import type { TrendOkResult, WatchlistQuote } from '@/lib/api/types';
import { useWatchlistMarketQuery } from '@/lib/queries/watchlist';
import { buildWatchlistRowMetrics, formatIntradayChgPct } from '@/lib/watchlist-metrics';
import { fmtPrice } from '@/lib/watchlist-table-cells';
import {
  loadWatchlist,
  saveWatchlist,
  upsertWatchlistOpenTrade,
  type WatchlistItem,
} from '@/lib/watchlist-storage';
import { BuyReminderDialog } from '@/components/watchlist/BuyReminderDialog';
import { QuickBuyDialog } from '@/components/watchlist/QuickBuyDialog';
import { MultiAssetHealthBlock } from './MultiAssetHealthBlock';
import { TwinStarTradePlanPanel } from './TwinStarTradePlanPanel';
import {
  SAT_MAX_POS,
  buildTwinStarTradePlan,
  etfSleeveKey,
  isLiveSatelliteStock,
  satConclusionLine,
  satConditionalLine,
  satNameTsFromAction,
  twinStarDayFlow,
  type TwinStarDayStep,
  type TwinStarTradePlan,
  type TwinStarTradeRow,
} from '@/lib/twin-star-trade-plan';

function fmtPct(v: number | null | undefined, digits = 2): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v >= 0 ? '+' : ''}${v.toFixed(digits)}%`;
}

function regimeBadge(regime: string | null | undefined): { label: string; cls: string } {
  switch (regime) {
    case 'Weak':
      return { label: 'Weak · 空仓观望', cls: 'border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-300' };
    case 'Strong':
      return { label: 'Strong · 进攻', cls: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300' };
    case 'Diverging':
      return { label: 'Diverging · 满仓进攻', cls: 'border-sky-500/40 bg-sky-500/10 text-sky-700 dark:text-sky-300' };
    default:
      return { label: String(regime ?? '—'), cls: 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]' };
  }
}

const PICK_META: Record<string, { label: string; hint: string }> = {
  STOCK: { label: '股票篮', hint: '100% 跟 S-3 CN+HK 持仓篮（等权）' },
  GOLD: { label: '黄金 ETF', hint: '518880' },
  OIL: { label: '原油 ETF', hint: '513350' },
  NASDAQ: { label: '纳指 ETF', hint: '513110/513100' },
  BOND10: { label: '国债 ETF', hint: '511260' },
  REPO: { label: '逆回购', hint: 'GC001 · 无人过线时兜底' },
};

type Sleeve = NonNullable<PortfolioHealthResponse['multiAssetSleeve']>;

function PickStrongOpsPanel({
  sleeve,
  stockHoldingsCount,
  satHoldingNames,
  onBuyEtf,
  twinStar,
  coreTargetPct = 100,
  coreBuyable = true,
}: {
  sleeve: Sleeve | null | undefined;
  stockHoldingsCount: number;
  satHoldingNames?: string[];
  onBuyEtf?: (symbol: string, name: string | null) => void;
  twinStar: boolean;
  /** Opportunity: 100 when sat idle, 50 when opening/holding. */
  coreTargetPct?: number;
  /** False when pick=STOCK but no executable basket names today. */
  coreBuyable?: boolean;
}) {
  const pickKey = sleeve?.pick?.key ?? 'REPO';
  const meta = PICK_META[pickKey] ?? { label: pickKey, hint: '' };
  const action = sleeve?.action ?? 'NONE';
  const mom = sleeve?.pick?.mom60;
  const etfSym = sleeve?.pick?.symbol;
  const isStock = pickKey === 'STOCK';
  const isRepo = pickKey === 'REPO';
  const isEtf = !isStock && !isRepo;
  const corePct = twinStar ? coreTargetPct : 100;
  const satNames = (satHoldingNames ?? []).filter(Boolean);
  const satCount = satNames.length > 0 ? satNames.length : stockHoldingsCount;
  const satNamesHint = satNames.length > 0 ? `：${satNames.join('、')}` : '';

  const steps: string[] = [];
  if (isStock) {
    steps.push(
      twinStar
        ? coreBuyable
          ? `核心腿 ${corePct}% → 股票篮（下方展开篮内买卖 · 见矫正清单仓位%）`
          : `核心腿 ${corePct}% 目标股票篮，但今日 0 只可执行 → 不要为 STOCK 清空 ETF`
        : '今日资金 100% → 股票篮（下方展开篮内买卖）',
    );
    if (sleeve?.holding && (coreBuyable || !twinStar)) steps.push('若仍持有 ETF：先卖出 ETF，再配股票');
  } else if (isEtf) {
    steps.push(
      twinStar
        ? `核心腿 ${corePct}% → ${meta.label}（${etfSym ?? pickKey}）`
        : `今日资金 100% → ${meta.label}（${etfSym ?? pickKey}）`,
    );
    if (stockHoldingsCount > 0 || satNames.length > 0) {
      steps.push(
        twinStar
          ? corePct >= 100
            ? `现有 ${satCount} 只 CN 卫星仓${satNamesHint}（不要按股票篮轮出）· 今日无新占用 → 核心 100% 配 ETF`
            : `现有 ${satCount} 只 CN 卫星仓${satNamesHint}（核心 ${corePct}% 配 ETF）`
          : `现有 ${stockHoldingsCount} 只股票仓：应减仓/清仓，切到 ETF（硬切）`,
      );
    }
    if (action === 'ROTATE' || action === 'BUY') steps.push(sleeve?.message || `买入/轮入 ${etfSym}`);
    if (action === 'HOLD') steps.push(sleeve?.message || `继续持有 ${etfSym}`);
    if (action === 'SELL_TO_REPO') steps.push(sleeve?.message || 'ETF 破 MA200 / 峰值−8% → 切逆回购');
  } else {
    steps.push(
      twinStar
        ? `今日无人过线 → 核心腿 ${corePct}% 转逆回购 / 观望`
        : '今日无人过线 → 100% 逆回购 / 空仓观望',
    );
    if (stockHoldingsCount > 0 || satNames.length > 0) {
      steps.push(
        twinStar
          ? `CN 股票属卫星仓${satNamesHint}，不是股票篮应轮出`
          : '股票仓也应清到空（单轨不持）',
      );
    }
    if (sleeve?.holding) steps.push('卖出 ETF 转 REPO');
  }

  return (
    <div className="rounded-lg border border-emerald-500/35 bg-emerald-500/5 px-3 py-2.5">
      <div className="flex flex-wrap items-center gap-2 text-[11px]">
        <span className="rounded bg-emerald-600/15 px-1.5 py-0.5 font-semibold text-emerald-800 dark:text-emerald-200">
          今日 pick · {pickKey}
        </span>
        <span className="text-[13px] font-semibold">{meta.label}</span>
        {mom != null && (
          <span className="font-mono tabular-nums text-[var(--k-muted)]">mom60 {mom}%</span>
        )}
        {sleeve?.stockPick?.mom60 != null && pickKey !== 'STOCK' && (
          <span className="font-mono text-[10px] tabular-nums text-[var(--k-muted)]">
            vs 股票篮 {sleeve.stockPick.mom60}%
          </span>
        )}
        {sleeve?.etfPick?.mom60 != null && pickKey === 'STOCK' && (
          <span className="font-mono text-[10px] tabular-nums text-[var(--k-muted)]">
            vs ETF顶 {sleeve.etfPick.key} {sleeve.etfPick.mom60}%
          </span>
        )}
        <span className="ml-auto rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5 text-[10px]">
          {sleeve?.label ?? action}
        </span>
      </div>
      <p className="mt-1 text-[10px] text-[var(--k-muted)]">
        {twinStar ? `机会口径 · 核心 ${corePct}%` : '100% 硬切'} · 定案 mom_compare · LB60/MA200
      </p>
      {sleeve?.message && (action !== 'HOLD' || !twinStar) ? (
        <p className="mt-1.5 text-[12px] text-[var(--k-fg)]">{sleeve.message}</p>
      ) : null}
      <ol className="mt-2 list-decimal space-y-1 pl-4 text-[11px] text-[var(--k-fg)]">
        {steps.map((s) => (
          <li key={s}>{s}</li>
        ))}
      </ol>
      {isEtf && etfSym && (action === 'BUY' || action === 'ROTATE') && onBuyEtf ? (
        <button
          type="button"
          onClick={() => onBuyEtf(etfSym, sleeve?.pick?.name ?? meta.label)}
          className="mt-2 inline-flex items-center rounded border border-emerald-500/50 bg-emerald-500/10 px-2 py-1 text-[11px] font-semibold text-emerald-800 hover:bg-emerald-500/20 dark:text-emerald-200"
        >
          记录买入 {etfSym}（模拟盘）
        </button>
      ) : null}
      {!twinStar && sleeve?.pick?.all_mom && Object.keys(sleeve.pick.all_mom).length > 0 ? (
        <div className="mt-2 flex flex-wrap gap-1.5 text-[10px] tabular-nums text-[var(--k-muted)]">
          {Object.entries(sleeve.pick.all_mom).map(([k, v]) => (
            <span
              key={k}
              className={cn(
                'rounded border px-1.5 py-0.5',
                k === pickKey
                  ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-800 dark:text-emerald-200'
                  : 'border-[var(--k-border)]',
              )}
            >
              {k} {v}%
              {sleeve.pick?.all_above?.[k] === false ? ' ✗MA' : ''}
            </span>
          ))}
        </div>
      ) : null}
    </div>
  );
}

export function HoldingRow({ h, onOpen }: { h: PortfolioHolding; onOpen?: (symbol: string) => void }) {
  const exit = h.action === 'EXIT';
  const pnlTone = (h.pnlPct ?? 0) >= 0 ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400';
  return (
    <div
      role={onOpen ? 'button' : undefined}
      onClick={onOpen ? () => onOpen(h.symbol) : undefined}
      className={cn(
        'rounded-lg border px-3 py-2',
        exit ? 'border-red-500/40 bg-red-500/5' : 'border-[var(--k-border)] bg-[var(--k-surface-2)]',
        onOpen && 'cursor-pointer transition-colors hover:border-[var(--k-accent)]/60',
      )}
    >
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
        <span className="text-[13px] font-semibold">{h.name || h.symbol}</span>
        <span className="text-[11px] tabular-nums text-[var(--k-muted)]">
          {h.symbol} · 仓位 {h.positionPct != null ? `${h.positionPct}%` : '—'}
        </span>
        <span className={cn('ml-auto font-mono text-[13px] font-semibold', pnlTone)}>
          {fmtPct(h.pnlPct)}
        </span>
        <span className="font-mono text-[11px] tabular-nums text-[var(--k-muted)]">
          回撤 {fmtPct(h.drawdownFromPeakPct)}
        </span>
        <span
          className={cn(
            'rounded px-1.5 py-0.5 text-[10px] font-semibold',
            exit
              ? 'bg-red-500/15 text-red-600 dark:text-red-400'
              : 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-300',
          )}
        >
          {exit ? '🔴 卖出' : '✅ 持有'}
        </span>
        {h.pyramidAdded && (
          <span className="rounded bg-sky-500/15 px-1.5 py-0.5 text-[10px] text-sky-700 dark:text-sky-300">
            已加仓
          </span>
        )}
        {h.realtimeWarning && (
          <span className="rounded bg-amber-500/15 px-1.5 py-0.5 text-[10px] font-semibold text-amber-700 dark:text-amber-300">
            ⚠ 盘中预警
          </span>
        )}
        {(h as unknown as { nearStop?: boolean; nearStopLabel?: string; nearStopDistancePct?: number }).nearStop && (
          <span className="rounded bg-amber-500/20 px-1.5 py-0.5 text-[10px] font-semibold text-amber-700 dark:text-amber-300">
            ⚠ 临近{(h as unknown as { nearStopLabel?: string }).nearStopLabel} { (h as unknown as { nearStopDistancePct?: number }).nearStopDistancePct}% · 需更新条件单
          </span>
        )}
      </div>
      <div className="mt-1 flex flex-wrap gap-x-4 gap-y-0.5 font-mono text-[10.5px] tabular-nums text-[var(--k-muted)]">
        <span>止损线 {h.stopLossLine ?? '—'}</span>
        <span>移动线 {h.trailingLine ?? '—'}</span>
        <span>金字塔线 {h.pyramidTriggerLine ?? '—'}</span>
        <span>已持 {h.holdingDays ?? '—'} 天</span>
        <span>到期 {h.expireDate ?? '—'}</span>
        {h.stopRuleDetail && (
          <span
            className={cn(
              'rounded px-1 py-px text-[9.5px]',
              h.stopRule === 'atr'
                ? 'bg-sky-500/15 text-sky-700 dark:text-sky-300'
                : 'bg-[var(--k-surface-3)] text-[var(--k-muted)]',
            )}
            title="OPT-105: Strong 日用 ATR×2 止损（入场锁定），其余固定 -5%/-8%"
          >
            规则：{h.stopRuleDetail}
          </span>
        )}
      </div>
      {(((h.alphaEvents?.length ?? 0) > 0) || h.industryFlow != null) && (
        <div className="mt-1 flex flex-col gap-0.5 text-[10.5px]">
          {h.alphaEvents?.map((e, i) => (
            <div
              key={`${e.trend}-${i}`}
              className={
                e.riskStatus === 'risk'
                  ? 'text-red-600 dark:text-red-400'
                  : 'text-amber-700 dark:text-amber-300'
              }
            >
              📰 {e.trend}
              {e.grade ? `（催化${e.grade}` : ''}
              {e.daysAgo != null ? ` · ${e.daysAgo}天前` : ''}
              {e.grade ? '）' : ''}
              {e.confidence != null ? ` · 映射${e.confidence}` : ''}
            </div>
          ))}
          {h.industryFlow && (
            <div
              className={
                (h.industryFlow.netInflow5d ?? 0) >= 0
                  ? 'text-emerald-700 dark:text-emerald-300'
                  : 'text-red-600 dark:text-red-400'
              }
            >
              🧭 {h.industryFlow.industry} 5日{' '}
              {(h.industryFlow.netInflow5d ?? 0) >= 0 ? '+' : ''}
              {h.industryFlow.netInflow5d ?? 0}亿（第{h.industryFlow.rank5d}/{h.industryFlow.total}）
            </div>
          )}
        </div>
      )}
      {h.reason && <div className="mt-1 text-[11px] text-red-600 dark:text-red-400">触发：{h.reason}</div>}
      {h.realtimeAlert && !h.reason && (
        <div className="mt-1 text-[11px] text-amber-700 dark:text-amber-300">⚠ {h.realtimeAlert}</div>
      )}
      {h.note && <div className="mt-1 text-[11px] text-[var(--k-muted)]">{h.note}</div>}
    </div>
  );
}

function TwinStarDayPlaybook({
  plan,
  afterSatWindow,
  snapshotFailed,
  gateOpen,
}: {
  plan: TwinStarTradePlan;
  afterSatWindow: boolean;
  snapshotFailed: boolean;
  gateOpen: boolean;
}) {
  const steps = twinStarDayFlow({ plan, afterSatWindow, snapshotFailed, gateOpen });
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2">
      <div className="mb-1.5 text-[11px] font-semibold">今日顺序 · 先核心再卫星</div>
      <ol className="flex flex-col gap-1">
        {steps.map((s) => (
          <li key={s.id} className="flex flex-wrap items-baseline gap-x-2 text-[11px]">
            <span className="w-10 shrink-0 font-mono text-[10px] text-[var(--k-muted)]">{s.clock}</span>
            <span className="font-medium">{s.title}</span>
            <DayStepBadge status={s.status} />
            <span className="min-w-0 text-[var(--k-muted)]">{s.detail}</span>
          </li>
        ))}
      </ol>
    </div>
  );
}

function DayStepBadge({ status }: { status: TwinStarDayStep['status'] }) {
  const label =
    status === 'blocked' ? '不可用' : status === 'wait' ? '等待' : status === 'idle' ? '无' : '做';
  const cls =
    status === 'blocked'
      ? 'border-red-500/40 bg-red-500/10 text-red-700 dark:text-red-300'
      : status === 'wait'
        ? 'border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-300'
        : status === 'idle'
          ? 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]'
          : 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300';
  return <span className={cn('rounded border px-1 py-px text-[9px] font-semibold', cls)}>{label}</span>;
}

function BuyList({
  candidates,
  total,
  suggestedSizePct,
  envScaleToday,
  remindedSymbols,
  boughtSymbols,
  onRemind,
  onBuy,
  twinStar,
  coreTargetPct = 100,
}: {
  candidates: PortfolioCandidate[];
  total?: number;
  suggestedSizePct?: number | null;
  envScaleToday?: number | null;
  remindedSymbols: Set<string>;
  boughtSymbols: Set<string>;
  onRemind: (c: PortfolioCandidate, sizePct: number) => void;
  onBuy: (c: PortfolioCandidate, sizePct: number) => void;
  twinStar: boolean;
  coreTargetPct?: number;
}) {
  const [expanded, setExpanded] = React.useState(false);
  const sleeveSize = suggestedSizePct ?? 5;
  const navSize = twinStar ? Math.round(sleeveSize * (coreTargetPct / 100) * 10) / 10 : sleeveSize;
  const shown = expanded ? candidates : candidates.slice(0, 5);
  const hidden = candidates.length - shown.length;
  const envScale = envScaleToday ?? 1;
  return (
    <div className="rounded-lg border border-emerald-500/30 bg-emerald-500/5 px-3 py-2">
      <div className="mb-1.5 flex flex-wrap items-center gap-x-2 gap-y-0.5 text-[11px] font-medium text-emerald-700 dark:text-emerald-300">
        <span>下午 2 点 · 股票篮买入（{twinStar ? '核心腿' : '单轨'} pick=STOCK · score 前 5）</span>
        {total != null && total > candidates.length && (
          <span className="text-[10px] font-normal text-[var(--k-muted)]">候选池 {total} 只</span>
        )}
        <span className="ml-auto text-[10px] font-normal text-[var(--k-muted)]">
          {twinStar
            ? `每票总资产 ${navSize}%（篮内 ${sleeveSize}% × 核心 ${coreTargetPct}%）`
            : `每票建议 ${navSize}%（10% × 今日环境×${envScale}${envScale !== 1 ? ' · 已含 D3 环境仓位' : ''}）`}
        </span>
      </div>
      <div className="flex flex-col gap-1">
        {shown.map((c, i) => {
          const symbol = c.symbol ?? c.ts_code ?? '';
          const reminded = remindedSymbols.has(symbol);
          const bought = boughtSymbols.has(symbol);
          return (
            <div key={symbol} className="flex flex-wrap items-center gap-x-2 text-[12px]">
              <span className="w-4 shrink-0 text-right font-mono text-[10px] text-[var(--k-muted)]">{i + 1}</span>
              <span className="font-medium">{c.name ?? symbol}</span>
              <span className="text-[10px] tabular-nums text-[var(--k-muted)]">{symbol}</span>
              <span className="ml-auto font-mono text-[10.5px] tabular-nums">score={c.score ?? '—'}</span>
              {typeof c.rs === 'number' && (
                <span className="font-mono text-[10.5px] tabular-nums text-[var(--k-muted)]">
                  RS 前{Math.round(c.rs * 100)}%
                </span>
              )}
              <span className="rounded bg-emerald-500/10 px-1.5 py-0.5 font-mono text-[10px] text-emerald-700 dark:text-emerald-300">
                买 {navSize}%
              </span>
              {c.alphaEvents && c.alphaEvents.length > 0 && (
                <span
                  className="rounded bg-amber-500/10 px-1.5 py-0.5 text-[10px] text-amber-700 dark:text-amber-300"
                  title={c.alphaEvents[0]?.focus}
                >
                  📰 {c.alphaEvents[0]?.trend}
                  {c.alphaEvents[0]?.grade ? `（催化${c.alphaEvents[0]?.grade}` : ''}
                  {c.alphaEvents[0]?.daysAgo != null ? ` · ${c.alphaEvents[0]?.daysAgo}天前` : ''}
                  {c.alphaEvents[0]?.grade ? '）' : ''}
                </span>
              )}
              {c.industryFlow && (
                <span
                  className={
                    (c.industryFlow.netInflow5d ?? 0) >= 0
                      ? 'rounded bg-emerald-500/10 px-1.5 py-0.5 text-[10px] text-emerald-700 dark:text-emerald-300'
                      : 'rounded bg-red-500/10 px-1.5 py-0.5 text-[10px] text-red-600 dark:text-red-400'
                  }
                  title="行业 5 日主力净流入（SW L1 · 展示层，不参与 S-3 门槛）"
                >
                  🧭 {c.industryFlow.industry} 5日{(c.industryFlow.netInflow5d ?? 0) >= 0 ? '+' : ''}
                  {c.industryFlow.netInflow5d ?? 0}亿（第{c.industryFlow.rank5d}/{c.industryFlow.total}）
                </span>
              )}
              {bought ? (
                <span className="inline-flex items-center gap-0.5 rounded border border-emerald-500/40 bg-emerald-500/10 px-1.5 py-0.5 text-[10px] text-emerald-700 dark:text-emerald-300">
                  ✓ 已买入
                </span>
              ) : (
                <button
                  type="button"
                  onClick={() => onBuy(c, navSize)}
                  className="inline-flex items-center gap-0.5 rounded border border-emerald-500/50 bg-emerald-500/10 px-1.5 py-0.5 text-[10px] font-semibold text-emerald-700 hover:bg-emerald-500/20 dark:text-emerald-300"
                  title="立刻买入：设仓位/价格，记入模拟盘（paper trade）"
                >
                  买入
                </button>
              )}
              {reminded ? (
                <span className="inline-flex items-center gap-0.5 rounded border border-emerald-500/40 bg-emerald-500/10 px-1.5 py-0.5 text-[10px] text-emerald-700 dark:text-emerald-300">
                  <BellRing size={9} className="inline-block" />
                  已提醒
                </span>
              ) : (
                <button
                  type="button"
                  onClick={() => onRemind(c, navSize)}
                  className="inline-flex items-center gap-0.5 rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5 text-[10px] text-[var(--k-muted)] hover:border-emerald-500/50 hover:text-emerald-700 dark:hover:text-emerald-300"
                  title="提醒买入：设目标价/备注并加入自选（不用输代码）"
                >
                  <Bell size={9} className="inline-block" />
                  提醒买入
                </button>
              )}
            </div>
          );
        })}
        {hidden > 0 && (
          <button
            onClick={() => setExpanded((v) => !v)}
            className="mt-0.5 self-start rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-2 py-0.5 text-[10px] text-[var(--k-muted)] hover:border-[var(--k-accent)]/60"
          >
            {expanded ? '收起' : `展开全部 ${candidates.length} 只`}
          </button>
        )}
      </div>
    </div>
  );
}

function ReconBlock({
  recon,
  onRemind,
  remindedSymbols,
  blockId,
}: {
  recon: ReconItem | undefined;
  onRemind: (c: PortfolioCandidate, sizePct: number) => void;
  remindedSymbols: Set<string>;
  blockId: string;
}) {
  const [expanded, setExpanded] = React.useState(false);
  if (!recon) return null;
  const hasGap = recon.missing > 0 || recon.extra > 0;
  const missingRows = (recon.detail ?? [])
    .filter((d) => d.type === 'missing')
    .slice(0, 20) as Array<{ symbol?: string; score?: unknown; entry?: unknown; positionPct?: unknown }>;
  const clean = !hasGap;
  return (
    <div id={blockId} className="rounded-lg border border-sky-500/30 bg-sky-500/5 px-3 py-2">
      <div className="flex flex-wrap items-center gap-x-2 gap-y-0.5 text-[11px]">
        <span
          className={
            clean
              ? 'text-emerald-700 dark:text-emerald-300'
              : 'text-amber-700 dark:text-amber-300'
          }
        >
          {clean ? '✓' : '⚠'}
        </span>
        <span className="font-medium">股票篮对账 · {recon.reconDate}</span>
        <span className="tabular-nums">
          回测应持 {recon.expected} · 实持 {recon.actual} · 缺 {recon.missing} · 多 {recon.extra}
        </span>
        {hasGap && (
          <button
            type="button"
            onClick={() => setExpanded((v) => !v)}
            className="ml-auto rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5 text-[10px] text-[var(--k-muted)] hover:border-[var(--k-accent)]/60"
          >
            {expanded ? '收起' : `看缺票（${recon.missing}）`}
          </button>
        )}
      </div>
      {expanded && missingRows.length > 0 && (
        <div className="mt-1.5 flex flex-col gap-1 border-t border-sky-500/20 pt-1.5">
          {missingRows.map((m) => {
            const symbol = String(m.symbol ?? '');
            const score =
              typeof m.score === 'number' && Number.isFinite(m.score)
                ? m.score.toFixed(1)
                : '—';
            const pct = (() => {
              const raw = m.positionPct;
              if (typeof raw === 'number' && Number.isFinite(raw) && raw > 0) {
                return Math.round(raw * 100);
              }
              if (typeof raw === 'number' && Number.isFinite(raw) && raw > 1) {
                return Math.round(raw);
              }
              return 10;
            })();
            const reminded = remindedSymbols.has(symbol);
            return (
              <div key={symbol} className="flex flex-wrap items-center gap-x-2 text-[11px]">
                <span className="font-mono text-[10px] text-sky-700 dark:text-sky-300">缺票</span>
                <span className="font-medium">{symbol}</span>
                <span className="text-[10px] tabular-nums text-[var(--k-muted)]">
                  入场 score {score}
                </span>
                <span className="rounded bg-sky-500/10 px-1 py-0.5 font-mono text-[10px] text-sky-700 dark:text-sky-300">
                  建议 {pct}%
                </span>
                {reminded ? (
                  <span className="rounded border border-emerald-500/40 bg-emerald-500/10 px-1.5 py-0.5 text-[10px] text-emerald-700 dark:text-emerald-300">
                    已提醒
                  </span>
                ) : (
                  <button
                    type="button"
                    onClick={() => onRemind({ symbol, name: null }, pct)}
                    className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5 text-[10px] text-[var(--k-muted)] hover:border-emerald-500/50 hover:text-emerald-700 dark:hover:text-emerald-300"
                    title="回测缺票：加入自选 + 设目标价/备注提醒"
                  >
                    <Bell size={9} className="mr-0.5 inline-block" />
                    提醒买入
                  </button>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

function satRowPretty(r: TwinStarTradeRow): { code: string; pretty: string | null } {
  const code = toTsCodeFromSymbol(r.symbol) ?? r.symbol;
  const pretty = r.name && r.name !== code && r.name !== r.symbol ? r.name : null;
  return { code, pretty };
}

function copyText(text: string): void {
  void navigator.clipboard.writeText(text);
}

function satLiveMetrics(
  symbol: string,
  quotes: Record<string, WatchlistQuote>,
  trend: Record<string, TrendOkResult>,
) {
  const key = symbol.toUpperCase();
  const q = quotes[symbol] ?? quotes[key];
  const t = trend[symbol] ?? trend[key];
  return buildWatchlistRowMetrics({
    symbol,
    trend: t,
    quote: q,
    tradingTime: isShanghaiTradingTime(),
    todaySh: getShanghaiTodayIso(),
  });
}

function SatStockRow({
  r,
  index,
  bought,
  onAct,
  quotes,
  trend,
}: {
  r: TwinStarTradeRow;
  index?: number;
  bought: boolean;
  onAct: (row: TwinStarTradeRow) => void;
  quotes: Record<string, WatchlistQuote>;
  trend: Record<string, TrendOkResult>;
}) {
  const { code, pretty } = satRowPretty(r);
  const isSell = r.side === 'SELL';
  const dueLabel = r.exitDue ?? '—';
  const heldLabel =
    r.heldDays != null ? `${r.heldDays}/3` : r.missingEntry ? '缺入场日' : '—';
  const live = satLiveMetrics(r.symbol, quotes, trend);
  const key = r.symbol.toUpperCase();
  const q = quotes[r.symbol] ?? quotes[key];
  const price = live.current ?? q?.price ?? r.lastClose ?? null;
  const chgPct =
    live.intradayChgPct ??
    (typeof q?.pctChg === 'number' && Number.isFinite(q.pctChg) ? q.pctChg : null);
  const showCostPnl = chgPct == null && live.current == null && q?.price == null;
  return (
    <div className="flex flex-col gap-0.5 border-b border-sky-500/10 py-1.5 last:border-b-0">
      <div className="flex flex-wrap items-center gap-x-2 text-[12px]">
        {index != null ? (
          <span className="w-4 shrink-0 text-right font-mono text-[10px] text-[var(--k-muted)]">{index}</span>
        ) : null}
        <span
          className={
            isSell
              ? 'rounded bg-red-500/15 px-1.5 py-0.5 text-[10px] font-semibold text-red-600 dark:text-red-400'
              : 'rounded bg-emerald-500/15 px-1.5 py-0.5 text-[10px] font-semibold text-emerald-700 dark:text-emerald-300'
          }
        >
          {isSell ? '卖出' : '持有'}
        </span>
        {pretty ? <span className="font-medium">{pretty}</span> : null}
        <span className="font-mono text-[11px] text-[var(--k-muted)]">{code}</span>
        <span className="ml-auto font-mono text-[12px] font-semibold tabular-nums">{r.navPct}%</span>
        {isSell && !bought ? (
          <button
            type="button"
            onClick={() => onAct(r)}
            className="rounded bg-red-600 px-1.5 py-0.5 text-[10px] font-semibold text-white"
          >
            记卖出
          </button>
        ) : null}
        {isSell && bought ? <span className="text-[10px] text-[var(--k-muted)]">已记</span> : null}
      </div>
      <div className="flex flex-wrap items-center gap-x-3 gap-y-0.5 font-mono text-[10.5px] tabular-nums text-[var(--k-muted)]">
        <span title="S-gap：入场日起第 3 个交易日收盘卖，中途不设止损">已持 {heldLabel}</span>
        <span className={isSell ? 'font-semibold text-red-600 dark:text-red-400' : undefined}>
          到期 {dueLabel}
          {isSell ? ' 收盘卖' : ''}
        </span>
        {price != null ? <span>现价 {fmtPrice(price)}</span> : null}
        {chgPct != null ? (
          <span className={chgPct >= 0 ? 'text-emerald-700 dark:text-emerald-300' : 'text-red-600 dark:text-red-400'}>
            {formatIntradayChgPct(chgPct)}
          </span>
        ) : showCostPnl && r.pnlPct != null ? (
          <span className={r.pnlPct >= 0 ? 'text-emerald-700 dark:text-emerald-300' : 'text-red-600 dark:text-red-400'}>
            {fmtPct(r.pnlPct)}
          </span>
        ) : null}
        <button
          type="button"
          onClick={() => copyText(satConditionalLine(r))}
          className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5 text-[10px] font-normal text-[var(--k-muted)] hover:border-[var(--k-accent)]/60"
          title="复制到期日，第 3 个交易日收盘卖"
        >
          复制到期
        </button>
      </div>
      <div className="text-[10px] text-[var(--k-muted)]">{r.reason}</div>
    </div>
  );
}

function SatSleevePanel({
  plan,
  boughtSymbols,
  onAct,
  hideBuys,
  quotes,
  trend,
}: {
  plan: TwinStarTradePlan;
  boughtSymbols: Set<string>;
  onAct: (row: TwinStarTradeRow) => void;
  hideBuys?: boolean;
  quotes: Record<string, WatchlistQuote>;
  trend: Record<string, TrendOkResult>;
}) {
  const holds = plan.holds.filter((r) => r.sleeve === 'sat' && r.kind === 'stock');
  const sells = plan.sells.filter((r) => r.sleeve === 'sat' && r.kind === 'stock');
  const buys = hideBuys ? [] : plan.buys.filter((r) => r.sleeve === 'sat' && r.kind === 'stock');
  const empty = holds.length === 0 && sells.length === 0 && buys.length === 0;
  const copyAllRows = [...sells, ...holds];
  const slotPct =
    plan.satSlotNavPct > 0 ? plan.satSlotNavPct : holds.find((r) => r.navPct > 0)?.navPct ?? null;
  return (
    <div className="rounded-lg border border-sky-500/30 bg-sky-500/5 px-3 py-2">
      <div className="mb-1 flex flex-wrap items-center gap-x-2 text-[11px] font-medium text-sky-800 dark:text-sky-200">
        <span>卫星仓</span>
        <span className="font-mono text-[10px] font-normal">
          {plan.satHeld}/{SAT_MAX_POS} 槽
        </span>
        {copyAllRows.length > 0 ? (
          <button
            type="button"
            onClick={() => copyText(copyAllRows.map(satConditionalLine).join('\n'))}
            className="rounded border border-sky-500/30 bg-[var(--k-surface)] px-1.5 py-0.5 text-[10px] font-normal text-[var(--k-muted)] hover:border-[var(--k-accent)]/60"
            title="复制全部到期日（第 3 个交易日收盘卖）"
          >
            复制全部到期
          </button>
        ) : null}
        <span className="ml-auto text-[10px] font-normal text-[var(--k-muted)]">
          {slotPct != null ? `每只总资产 ${slotPct}%` : '每只总资产 按已录入仓位'} · 第3日收盘卖
        </span>
      </div>
      <div className="mb-1.5 text-[10px] leading-snug text-[var(--k-muted)]">
        C4 占用对照（不是交易铃）· {plan.bookNote}
      </div>
      {empty ? (
        <div className="text-[11px] text-[var(--k-muted)]">空仓 · 等 R-wide 开闸后填槽</div>
      ) : null}
      {sells.length > 0 ? (
        <div className="mb-1">
          {sells.map((r, i) => (
            <SatStockRow
              key={`sell-${r.symbol}`}
              r={r}
              index={i + 1}
              bought={boughtSymbols.has(r.symbol)}
              onAct={onAct}
              quotes={quotes}
              trend={trend}
            />
          ))}
        </div>
      ) : null}
      {holds.length > 0 ? (
        <div className="mb-1">
          {holds.map((r) => (
            <SatStockRow key={`hold-${r.symbol}`} r={r} bought={false} onAct={onAct} quotes={quotes} trend={trend} />
          ))}
        </div>
      ) : null}
      {buys.length > 0 ? (
        <div>
          <div className="mb-1 text-[11px] font-medium text-sky-800 dark:text-sky-200">卫星缺口买入</div>
          <div className="flex flex-col gap-1">
            {buys.map((r, i) => {
              const done = boughtSymbols.has(r.symbol);
              const { code, pretty } = satRowPretty(r);
              return (
                <div key={`buy-${r.symbol}`} className="flex flex-wrap items-center gap-x-2 text-[12px]">
                  <span className="w-4 shrink-0 text-right font-mono text-[10px] text-[var(--k-muted)]">{i + 1}</span>
                  <span className="font-semibold text-emerald-700">买</span>
                  {pretty ? <span className="font-medium">{pretty}</span> : null}
                  <span className="font-mono text-[11px] text-[var(--k-muted)]">{code}</span>
                  <span className="ml-auto font-mono text-[12px] font-semibold tabular-nums">{r.navPct}%</span>
                  {r.swapFrom ? <span className="text-[10px] text-amber-700">涨停换</span> : null}
                  {done ? (
                    <span className="text-[10px] text-[var(--k-muted)]">已记</span>
                  ) : (
                    <button
                      type="button"
                      onClick={() => onAct(r)}
                      className="rounded bg-emerald-600 px-1.5 py-0.5 text-[10px] font-semibold text-white"
                    >
                      买入
                    </button>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function HealthPanel({
  title,
  tag,
  block,
  recon,
  onOpen,
  onRemind,
  onBuy,
  remindedSymbols,
  boughtSymbols,
  overall,
  allowStockBuys,
  rotateOutStocks,
  twinStar,
  coreTargetPct = 100,
  idleHint,
}: {
  title: string;
  tag: string;
  block: PortfolioHealthResponse | null | undefined;
  recon?: ReconItem | undefined;
  onOpen?: (symbol: string) => void;
  onRemind: (c: PortfolioCandidate, sizePct: number) => void;
  onBuy: (c: PortfolioCandidate, sizePct: number) => void;
  remindedSymbols: Set<string>;
  boughtSymbols: Set<string>;
  overall?: PortfolioHealthResponse | null;
  allowStockBuys: boolean;
  rotateOutStocks: boolean;
  twinStar: boolean;
  coreTargetPct?: number;
  /** Twin-star: S-3 basket is idle because CN names live in the satellite sleeve. */
  idleHint?: string | null;
}) {
  const holdings = block?.holdings ?? [];
  const candidates = block?.s3Candidates ?? [];
  const regime = regimeBadge(block?.regime);
  const idSuffix = tag === 'HK' ? '-hk' : '';
  const gateClosed = isMarketGateClosed(block);
  const sleevePick = overall?.multiAssetSleeve?.pick?.key ?? null;
  const showBuyList =
    allowStockBuys && candidates.length > 0 && block?.regime !== 'Weak' && !gateClosed;

  return (
    <div className="flex min-w-0 flex-col gap-2 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/60 p-2.5">
      <div className="flex items-center gap-2 text-[11px] font-semibold">
        <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5">{tag}</span>
        {title}
        {rotateOutStocks && holdings.length > 0 && (
          <span className="rounded bg-amber-500/15 px-1.5 py-0.5 text-[11px] font-bold text-amber-700 dark:text-amber-300">
            {twinStar ? '核心腿' : '单轨'}非 STOCK · 应轮出
          </span>
        )}
        {gateClosed && allowStockBuys && (
          <span className="rounded bg-red-500/15 px-1.5 py-0.5 text-[11px] font-bold text-red-600 dark:text-red-400">
            闸门关闭 · 今日不买
          </span>
        )}
        {holdings.length > 0 && (
          <button
            type="button"
            onClick={() => {
              const lines = holdings
                .map(
                  (h) =>
                    `${h.symbol} 止损${h.stopLossLine} 移动${h.trailingLine} 到期${h.expireDate} ${
                      rotateOutStocks || h.action === 'EXIT' ? '需卖' : '持有'
                    }`,
                )
                .join('\n');
              void navigator.clipboard.writeText(lines);
            }}
            className="ml-1 rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5 text-[10px] font-normal text-[var(--k-muted)] hover:border-[var(--k-accent)]/60"
            title="复制条件单清单到剪贴板（券商固定价单）"
          >
            复制条件单
          </button>
        )}
        <span className="ml-auto text-[10px] font-normal tabular-nums text-[var(--k-muted)]">
          {block?.tradeDate ?? '—'}
        </span>
      </div>
      <div className="flex flex-wrap items-center gap-2 text-[11px]">
        <span className={cn('rounded border px-1.5 py-0.5 font-medium', regime.cls)}>{regime.label}</span>
        {block?.strength != null && (
          <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5 tabular-nums">
            strength {block.strength.toFixed(1)}
          </span>
        )}
        {block?.panicCooldown?.active ? (
          <span className="rounded border border-amber-500/40 bg-amber-500/10 px-1.5 py-0.5 text-amber-700 dark:text-amber-300">
            恐慌冷却至 {block.panicCooldown.cooldownEndDate}
          </span>
        ) : null}
        {block?.circuitBlocked ? (
          <span className="rounded border border-red-500/40 bg-red-500/10 px-1.5 py-0.5 text-red-700 dark:text-red-300">
            回撤熔断·暂停开仓
          </span>
        ) : null}
        <span className="text-[var(--k-muted)]">
          篮内候选 {block ? (block.s3Candidates?.length ?? 0) : '…'}
          {sleevePick ? ` · ${twinStar ? '核心腿' : '单轨'} pick ${sleevePick}` : ''}
        </span>
      </div>
      {block?.infoSummary && (
        <div className="flex flex-wrap items-center gap-x-2 gap-y-0.5 text-[10.5px] text-[var(--k-muted)]">
          <span>信号 · {block.infoSummary.holdingsCount ?? 0} 持仓</span>
          {(block.infoSummary.eventHoldings ?? 0) > 0 ? (
            <span className="rounded bg-amber-500/10 px-1.5 py-0.5 text-amber-700 dark:text-amber-300">
              {block.infoSummary.eventHoldings ?? 0} 只有 α 事件
            </span>
          ) : (
            <span className="text-emerald-700 dark:text-emerald-300">无事件冲突</span>
          )}
          {(block.infoSummary.industryOutflow ?? 0) > 0 ? (
            <span className="rounded bg-red-500/10 px-1.5 py-0.5 text-red-600 dark:text-red-400">
              {block.infoSummary.industryOutflow ?? 0} 只行业资金流出 ⚠
            </span>
          ) : (
            (block.infoSummary.industryInflow ?? 0) > 0 && (
              <span className="text-emerald-700 dark:text-emerald-300">
                {block.infoSummary.industryInflow ?? 0} 只行业资金流入
              </span>
            )
          )}
        </div>
      )}
      {block && block.scoreFresh === false ? (
        <span className="w-fit rounded border border-amber-500/40 bg-amber-500/10 px-1.5 py-0.5 text-[10px] text-amber-700 dark:text-amber-300">
          分数截至 {block.scoreDataAsOfDate ?? '—'}
        </span>
      ) : null}
      {allowStockBuys ? (
        <ReconBlock
          recon={recon}
          onRemind={onRemind}
          remindedSymbols={remindedSymbols}
          blockId={`recon${idSuffix}`}
        />
      ) : null}
      {holdings.length === 0 ? (
        <div className="text-xs text-[var(--k-muted)]">
          {idleHint ?? '当前无持仓（未录入成本/仓位的 watchlist 票不算持仓）'}
        </div>
      ) : (
        <div id={`holdings${idSuffix}`} className="flex flex-col gap-1.5">
          {holdings.map((h) => (
            <HoldingRow
              key={h.symbol}
              h={
                rotateOutStocks && h.action !== 'EXIT'
                  ? {
                      ...h,
                      action: 'EXIT',
                      reason: h.reason ?? `${twinStar ? '核心腿' : '单轨'}今日 pick=${sleevePick}，股票篮应轮出`,
                    }
                  : h
              }
              onOpen={onOpen}
            />
          ))}
        </div>
      )}
      {showBuyList ? (
        <BuyList
          candidates={candidates}
          total={block?.s3CandidateTotal}
          suggestedSizePct={Number((block?.s3Rules as Record<string, unknown> | undefined)?.suggestedSizePct) || null}
          envScaleToday={Number((block?.s3Rules as Record<string, unknown> | undefined)?.envScaleToday) || null}
          remindedSymbols={remindedSymbols}
          boughtSymbols={boughtSymbols}
          onRemind={onRemind}
          onBuy={onBuy}
          twinStar={twinStar}
          coreTargetPct={coreTargetPct}
        />
      ) : !allowStockBuys && candidates.length > 0 ? (
        <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 px-3 py-2 text-[11px] text-amber-700 dark:text-amber-300">
          {twinStar ? '核心腿' : '单轨'} pick=<strong>{sleevePick}</strong> ≠ STOCK → 股票候选 {candidates.length} 只<strong>不执行买入</strong>
        </div>
      ) : allowStockBuys && block ? (
        <div className="text-[11px] text-[var(--k-muted)]">
          {block.regime === 'Weak'
            ? '今日无开仓候选（regime=Weak：股票篮空仓观望）'
            : block.circuitBlocked
              ? '回撤熔断中：暂停新开仓'
              : block.scoreFresh === false
                ? `分数未更新（截至 ${block.scoreDataAsOfDate ?? '—'}）· 盘中暂无候选`
                : gateClosed
                  ? '闸门关闭 · 今日不买'
                  : '今日无开仓候选（score≥65 · RS 前 50% · 无恐慌冷却）'}
        </div>
      ) : null}
    </div>
  );
}

export function PortfolioHealthCard({
  onOpenStock,
  quotes: quotesProp,
  trend: trendProp,
}: {
  onOpenStock?: (symbol: string) => void;
  quotes?: Record<string, WatchlistQuote>;
  trend?: Record<string, TrendOkResult>;
} = {}) {
  const [strategyMode] = useStrategyMode();
  const twinStar = strategyMode !== 'single_track';
  const queryClient = useQueryClient();
  const twinStarQ = useTwinStarActionQuery(twinStar);
  const [refreshingSat, setRefreshingSat] = React.useState(false);
  const afterSatWindow = satNamesVisible();
  const satSnapFailed = Boolean(
    twinStarQ.data?.sat?.snapshotMissing || twinStarQ.data?.sat?.snapshotStale,
  );
  const sentimentQ = useDashboardSentimentQuery();
  const q = useQuery({
    queryKey: ['portfolio-health'],
    queryFn: ({ signal }) => fetchPortfolioHealth(undefined, signal),
    refetchInterval: 5 * 60_000,
  });
  const reconQ = useBacktestReconQuery(2);
  const reconByMarket = React.useMemo(() => {
    const m = new Map<string, ReconItem>();
    for (const r of reconQ.data?.items ?? []) m.set(r.market, r);
    return m;
  }, [reconQ.data]);

  const [reminderTarget, setReminderTarget] = React.useState<{
    symbol: string;
    name: string | null;
    sizePct: number;
  } | null>(null);
  const [buyTarget, setBuyTarget] = React.useState<{
    symbol: string;
    name: string | null;
    score?: number | null;
    rs?: number | null;
    sizePct: number;
    side: 'BUY' | 'SELL';
  } | null>(null);
  const [boughtSymbols, setBoughtSymbols] = React.useState<Set<string>>(new Set());
  const [buyError, setBuyError] = React.useState<string | null>(null);
  const [reminders, setReminders] = React.useState<BuyReminder[]>([]);
  const [reminderError, setReminderError] = React.useState<string | null>(null);

  const remindedSymbols = React.useMemo(
    () => new Set(reminders.map((r) => r.symbol)),
    [reminders],
  );

  React.useEffect(() => {
    setReminders(loadBuyReminders());
    function onUpdate() {
      setReminders(loadBuyReminders());
    }
    window.addEventListener(BUY_REMINDERS_UPDATED_EVENT, onUpdate);
    return () => window.removeEventListener(BUY_REMINDERS_UPDATED_EVENT, onUpdate);
  }, []);

  const data: PortfolioHealthResponse | undefined = q.data;
  const sleeve = data?.multiAssetSleeve;
  const pickKey = sleeve?.pick?.key ?? null;
  const allowStockBuys = pickKey === 'STOCK';
  const rotateOutCn = !twinStar && pickKey != null && pickKey !== 'STOCK';
  const rotateOutHk = pickKey != null && pickKey !== 'STOCK';
  const stockHoldingsCount =
    (data?.holdings?.length ?? 0) + (data?.hkHealth?.holdings?.length ?? 0);
  const satQuoteSymbols = React.useMemo(
    () => (data?.holdings ?? []).filter((h) => isCnWatchlistSymbol(h.symbol)).map((h) => h.symbol),
    [data],
  );
  const ownMarketQ = useWatchlistMarketQuery(quotesProp == null ? satQuoteSymbols : []);
  const quoteMap = quotesProp ?? ownMarketQ.data?.quotes ?? {};
  const trendMap = trendProp ?? ownMarketQ.data?.trend ?? {};
  const satLoaded = twinStarQ.data?.sat != null;
  const coreTargetPct = twinStarQ.data?.sat?.coreTargetPct ?? 100;
  const tradePlan = React.useMemo(() => {
    if (!twinStar || sleeve == null) return null;
    const sat = twinStarQ.data?.sat;
    const cn = data;
    const hk = data?.hkHealth;
    const cnSize = Number((cn?.s3Rules as Record<string, unknown> | undefined)?.suggestedSizePct);
    const hkSize = Number((hk?.s3Rules as Record<string, unknown> | undefined)?.suggestedSizePct);
    return buildTwinStarTradePlan({
      coreTargetPct: sat?.coreTargetPct ?? 100,
      satTargetPct: sat?.satTargetPct ?? Math.max(0, 100 - (sat?.coreTargetPct ?? 100)),
      gateOpen: Boolean(sat?.gateOpen),
      afterSatWindow,
      satHoldings: sat?.book?.holdings ?? [],
      satExitsDue: sat?.book?.exitsDue ?? [],
      satCandidates: sat?.candidates ?? [],
      satBlocked: sat?.blocked ?? [],
      satAlternates: sat?.alternates ?? [],
      pickKey,
      pickSymbol: sleeve?.pick?.symbol ?? null,
      pickName: sleeve?.pick?.name ?? (pickKey != null ? PICK_META[pickKey]?.label : null) ?? null,
      cnCandidates: cn?.s3Candidates ?? [],
      hkCandidates: hk?.s3Candidates ?? [],
      cnAllowBuys: Boolean(cn && !isMarketGateClosed(cn)),
      hkAllowBuys: Boolean(hk && !isMarketGateClosed(hk)),
      suggestedSizePct: (Number.isFinite(cnSize) && cnSize > 0 ? cnSize : null) ?? (Number.isFinite(hkSize) && hkSize > 0 ? hkSize : 10),
      etfHoldings: (data?.multiAssetHoldings ?? []).map((h) => ({
        symbol: h.symbol,
        key: etfSleeveKey(h.symbol),
        name: h.name ?? null,
        positionPct: typeof h.positionPct === 'number' ? h.positionPct : null,
      })),
      liveStockHoldings: (data?.holdings ?? [])
        .filter((h) => isCnWatchlistSymbol(h.symbol))
        .map((h) => ({
          symbol: h.symbol,
          name: h.name ?? null,
          positionPct: typeof h.positionPct === 'number' ? h.positionPct : null,
          costPrice: typeof h.costPrice === 'number' ? h.costPrice : null,
          entryDate: h.entryDate ?? null,
          lastClose: typeof h.lastClose === 'number' ? h.lastClose : null,
          pnlPct: typeof h.pnlPct === 'number' ? h.pnlPct : null,
        })),
      asOfDate: data?.tradeDate ?? null,
      coreParkEtfKey: sleeve?.etfPick?.key ?? null,
      etfMomByKey: sleeve?.pick?.all_mom ?? undefined,
    });
  }, [twinStar, twinStarQ.data, data, afterSatWindow, pickKey, sleeve]);

  const satNameTs = React.useMemo(
    () => satNameTsFromAction(twinStarQ.data?.sat),
    [twinStarQ.data?.sat],
  );
  const satStockSymbols = React.useMemo(() => {
    if (!twinStar) return new Set<string>();
    const fromPlan = tradePlan
      ? [
          ...tradePlan.satHeldSymbols,
          ...[...tradePlan.holds, ...tradePlan.buys, ...tradePlan.sells]
            .filter((r) => r.sleeve === 'sat' && r.kind === 'stock')
            .map((r) => r.symbol),
        ]
      : [];
    const fromLive = (data?.holdings ?? [])
      .filter((h) => isLiveSatelliteStock(h.symbol, { pickKey, satNameTs }))
      .map((h) => h.symbol);
    return new Set([...fromPlan, ...fromLive]);
  }, [twinStar, tradePlan, data, pickKey, satNameTs]);
  const liveCnSatHoldings = React.useMemo(
    () =>
      (data?.holdings ?? []).filter((h) =>
        isLiveSatelliteStock(h.symbol, { pickKey, satNameTs }),
      ),
    [data, pickKey, satNameTs],
  );
  const satHoldingNames = liveCnSatHoldings.map((h) => (h.name ?? '').trim() || h.symbol);

  const cnBasketBlock = React.useMemo(() => {
    if (!twinStar || !data) return data;
    if (pickKey !== 'STOCK') {
      return {
        ...data,
        holdings: [],
        infoSummary: data.infoSummary
          ? { ...data.infoSummary, holdingsCount: 0 }
          : data.infoSummary,
      };
    }
    const holdings = (data.holdings ?? []).filter((h) => !satStockSymbols.has(h.symbol));
    if (holdings.length === (data.holdings ?? []).length) return data;
    return {
      ...data,
      holdings,
      infoSummary: data.infoSummary
        ? { ...data.infoSummary, holdingsCount: holdings.length }
        : data.infoSummary,
    };
  }, [twinStar, data, pickKey, satStockSymbols]);

  async function addToWatchlistAndRemind(values: { targetPrice: number | null; note: string }) {
    if (!reminderTarget) return;
    const { symbol, name } = reminderTarget;
    setReminderError(null);
    try {
      const existing = loadWatchlist();
      if (!existing.some((x) => x.symbol === symbol)) {
        const next: WatchlistItem[] = [
          {
            symbol,
            name: name ?? null,
            addedAt: new Date().toISOString(),
            color: '#ffffff',
            source: 'research',
          },
          ...existing,
        ];
        await saveWatchlist(next);
      }
      addBuyReminder({
        symbol,
        name,
        targetPrice: values.targetPrice,
        note: values.note,
        createdAt: new Date().toISOString(),
      });
      setReminderTarget(null);
    } catch (e) {
      setReminderError(e instanceof Error ? e.message : String(e));
    }
  }

  function handleRemind(c: PortfolioCandidate, sizePct: number) {
    setReminderTarget({
      symbol: c.symbol ?? c.ts_code ?? '',
      name: c.name ?? null,
      sizePct,
    });
  }

  async function confirmBuy(values: { price: number; positionPct: number }) {
    if (!buyTarget) return;
    setBuyError(null);
    try {
      const next = upsertWatchlistOpenTrade(loadWatchlist(), {
        symbol: buyTarget.symbol,
        name: buyTarget.name,
        side: buyTarget.side,
        price: values.price,
        positionPct: values.positionPct,
        entryDate: getShanghaiTodayIso(),
      });
      await saveWatchlist(next);
      await recordUserTrade({
        symbol: buyTarget.symbol,
        side: buyTarget.side,
        price: values.price,
        positionPct: values.positionPct,
        source: 'RESEARCH',
        market: tradeMarketForSymbol(buyTarget.symbol),
      });
      await invalidateUserTradesQueries(queryClient);
      setBoughtSymbols((prev) => new Set(prev).add(buyTarget.symbol));
      setBuyTarget(null);
    } catch (e) {
      setBuyError(e instanceof Error ? e.message : String(e));
    }
  }

  function handleBuy(c: PortfolioCandidate, sizePct: number) {
    setBuyTarget({
      symbol: c.symbol ?? c.ts_code ?? '',
      name: c.name ?? null,
      score: c.score,
      rs: c.rs,
      sizePct,
      side: 'BUY',
    });
  }

  function handleBuyEtf(symbol: string, name: string | null) {
    setBuyTarget({
      symbol,
      name,
      score: null,
      rs: null,
      sizePct: twinStar ? coreTargetPct : 100,
      side: 'BUY',
    });
  }

  function handlePlanAct(row: TwinStarTradeRow) {
    setBuyTarget({
      symbol: row.symbol,
      name: row.name ?? null,
      score: null,
      rs: null,
      sizePct: row.navPct,
      side: row.side === 'SELL' ? 'SELL' : 'BUY',
    });
  }

  async function handleRefreshSat() {
    setRefreshingSat(true);
    setBuyError(null);
    try {
      const next = await refreshTwinStarAction();
      queryClient.setQueryData(['backtest', 'twin-star', 'action'], next);
    } catch (e) {
      setBuyError(e instanceof Error ? e.message : String(e));
    } finally {
      setRefreshingSat(false);
    }
  }

  React.useEffect(() => {
    if (!twinStar) return;
    const id = window.setInterval(() => {
      const m = getShanghaiMinutes();
      if (m >= 9 * 60 + 30 && m <= 15 * 60) void twinStarQ.refetch();
    }, 60_000);
    return () => window.clearInterval(id);
  }, [twinStar, twinStarQ]);

  const [stockOpen, setStockOpen] = React.useState(allowStockBuys);

  React.useEffect(() => {
    setStockOpen(allowStockBuys);
  }, [allowStockBuys]);

  if (q.isError && !data) {
    return (
      <div className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-4 py-2.5 text-xs text-[var(--k-muted)]">
        <ShieldAlert size={13} className="mr-1 inline-block" />
        {twinStar ? '核心腿择优暂不可用' : '单轨择优暂不可用'}（data-sync-service 未响应）
      </div>
    );
  }

  return (
    <div className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-4 py-3">
      <div className="mb-2 flex items-center gap-2">
        <span className="text-[12px] font-semibold">{twinStar ? '机会双子星 · 今日决策' : '单轨择优 · 今日复刻（mom_compare）'}</span>
        <span className="text-[10px] text-[var(--k-muted)]">{twinStar ? '关闸/无仓 → 核心 100% · 开闸或持仓才切 50%' : '100% 硬切 · 与 Timeline 同源'}</span>
        {twinStar ? (
          <span
            className={cn(
              'ml-auto rounded px-1.5 py-0.5 text-[10px] font-medium',
              q.isError || twinStarQ.isError || sentimentQ.isError
                ? 'bg-red-500/15 text-red-600 dark:text-red-400'
                : 'bg-emerald-500/10 text-emerald-700 dark:text-emerald-300',
            )}
            title={
              q.isError || twinStarQ.isError || sentimentQ.isError
                ? '部分数据获取失败，react-query 自动重试中'
                : `数据更新于 ${new Date(Math.max(q.dataUpdatedAt, twinStarQ.dataUpdatedAt, sentimentQ.dataUpdatedAt)).toLocaleTimeString('zh-CN')} · 核心腿每 5 分钟 · 卫星盘中约 1 分钟，收盘后冻结至次日 09:00`
            }
          >
            {q.isError || twinStarQ.isError || sentimentQ.isError
              ? '⚠ 数据失败 · 重试中'
              : `实时 · ${new Date(Math.max(q.dataUpdatedAt, twinStarQ.dataUpdatedAt, sentimentQ.dataUpdatedAt)).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })}`}
          </span>
        ) : null}
        <Button
          variant="ghost"
          size="sm"
          className="h-6 px-1.5"
          onClick={() => void q.refetch()}
          disabled={q.isFetching}
          title="刷新"
        >
          <RefreshCw size={12} className={q.isFetching ? 'animate-spin' : ''} />
        </Button>
      </div>

      {reminders.length > 0 && (
        <div className="mb-2 rounded-lg border border-sky-500/30 bg-sky-500/5 px-3 py-2">
          <div className="mb-1 flex items-center gap-1.5 text-[11px] font-medium text-sky-700 dark:text-sky-300">
            <BellRing size={11} className="inline-block" />
            买入提醒（{reminders.length}）
          </div>
          <div className="flex flex-col gap-1">
            {reminders.map((r) => (
              <div key={r.symbol} className="flex flex-wrap items-center gap-x-2 text-[11px]">
                <span className="font-medium">{r.name ?? r.symbol}</span>
                <span className="font-mono text-[10px] tabular-nums text-[var(--k-muted)]">{r.symbol}</span>
                {r.targetPrice != null && (
                  <span className="rounded bg-sky-500/10 px-1 py-0.5 font-mono text-[10px] text-sky-700 dark:text-sky-300">
                    目标价 {r.targetPrice}
                  </span>
                )}
                <button
                  type="button"
                  onClick={() => removeBuyReminder(r.symbol)}
                  className="ml-auto rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5 text-[10px] text-[var(--k-muted)] hover:border-red-500/50 hover:text-red-500"
                >
                  移除提醒
                </button>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="flex flex-col gap-2">
        {twinStar && sleeve ? (
          <div className="rounded-md border border-emerald-500/30 bg-emerald-500/5 px-2.5 py-1.5 text-[11px]">
            <span className="font-semibold text-emerald-700">今日结论</span>
            <span className="ml-2">
              核心 {twinStarQ.data?.sat?.coreTargetPct ?? 100}%：{sleeve.label ?? sleeve.action}{' '}
              {sleeve.pick?.symbol ?? ''}
            </span>
            {liveCnSatHoldings.length > 0 ? (
              <div className="mt-1">
                卫星仓 {liveCnSatHoldings.length}/{SAT_MAX_POS}：{satHoldingNames.join('、')}
              </div>
            ) : null}
            {twinStarQ.data?.sat?.asOf != null ? (
              <span className="ml-2">
                · 卫星：
                {satSnapFailed
                  ? '今日盘中快照失败，卫星名单不可用'
                  : tradePlan
                    ? satConclusionLine(tradePlan, Boolean(twinStarQ.data.sat.gateOpen))
                    : !twinStarQ.data.sat.gateOpen
                      ? 'R-wide 关闸（不开仓）'
                      : afterSatWindow
                        ? `R-wide 开闸 → 14:30 模拟收盘价买入候选 ${(twinStarQ.data.sat.candidates ?? []).slice(0, 3).map((c) => c.ts).join(', ') || '—'}`
                        : 'R-wide 开闸 · 候选 14:30 后公布（当日近似）'}
              </span>
            ) : null}
            {twinStarQ.data?.sat?.asOf != null ? (
              <div className="mt-1 text-[10px] tabular-nums text-[var(--k-muted)]">
                {twinStarQ.data.sat.gateOpen
                  ? `卫星闸 · R-wide 开闸 breadth ${twinStarQ.data.sat.breadth} · ${twinStarQ.data.sat.gapCount ?? 0} 只缺口`
                  : `卫星闸 · R-wide 关闸 breadth ${twinStarQ.data.sat.breadth}`}
                {twinStarQ.data.sat.note ? ` · ${twinStarQ.data.sat.note}` : ''} · 信号日 {twinStarQ.data.sat.asOf}
                {twinStarQ.data.sat.approx
                  ? ` · 盘中近似${
                      twinStarQ.data.sat.snapshotAt?.includes('T')
                        ? `（${twinStarQ.data.sat.snapshotAt.slice(11, 16)} 快照）`
                        : ''
                    }`
                  : ''}
                {tradePlan
                  ? tradePlan.sells.filter((r) => r.kind === 'stock').length > 0
                    ? ` · 到期卖 ${tradePlan.sells
                        .filter((r) => r.kind === 'stock')
                        .map((r) => r.name ?? r.symbol)
                        .slice(0, 3)
                        .join(', ')}`
                    : ''
                  : ''}
              </div>
            ) : null}
          </div>
        ) : null}
        {twinStar && satSnapFailed ? (
          <div className="rounded-md border border-red-500/40 bg-red-500/10 px-2.5 py-1.5 text-[11px] text-red-800 dark:text-red-200">
            ⚠ 今日盘中快照失败 → 卫星名单不可用（东财 clist）。14:30 不要按 T-1 名单下单。
          </div>
        ) : null}
        {twinStar && q.data?.tradeDate && twinStarQ.data?.sat?.asOf != null && twinStarQ.data.sat.asOf < q.data.tradeDate ? (
          <div className="rounded-md border border-amber-500/40 bg-amber-500/10 px-2.5 py-1.5 text-[11px] text-amber-800 dark:text-amber-200">
            ⚠ 卫星信号滞后：信号日 {twinStarQ.data.sat.asOf} &lt; 体检数据 {q.data.tradeDate}——卫星判断可能不是最新，请检查数据同步。
          </div>
        ) : null}

        {twinStar && tradePlan ? (
          <>
            <SatSleevePanel
              plan={tradePlan}
              boughtSymbols={boughtSymbols}
              onAct={handlePlanAct}
              hideBuys={!satLoaded || satSnapFailed}
              quotes={quoteMap}
              trend={trendMap}
            />
            {satLoaded ? (
              <>
                <TwinStarDayPlaybook
                  plan={tradePlan}
                  afterSatWindow={afterSatWindow}
                  snapshotFailed={satSnapFailed}
                  gateOpen={Boolean(twinStarQ.data?.sat?.gateOpen)}
                />
                <TwinStarTradePlanPanel
                  plan={tradePlan}
                  snapshotAt={twinStarQ.data?.sat?.snapshotAt}
                  frozen={Boolean(twinStarQ.data?.sat?.frozen || twinStarQ.data?.sat?.heldOvernight)}
                  snapshotFailed={satSnapFailed}
                  onRefresh={() => void handleRefreshSat()}
                  refreshing={refreshingSat}
                />
              </>
            ) : null}
            <MultiAssetHealthBlock
              holdings={data?.multiAssetHoldings}
              sleeve={sleeve}
              onOpen={onOpenStock}
              coreDestinationReady={tradePlan.coreBuyable}
              etfTrims={tradePlan.sells
                .filter((r) => r.purpose === 'sat-fund')
                .map((r) => ({ symbol: r.symbol, navPct: r.navPct, reason: r.reason }))}
              onTrim={(symbol, name, navPct) =>
                handlePlanAct({
                  side: 'SELL',
                  sleeve: 'sat',
                  kind: 'etf',
                  symbol,
                  name,
                  navPct,
                  reason: '减仓腾给卫星股票',
                  purpose: 'sat-fund',
                })
              }
            />
            <HealthPanel
              title="A股线（股票篮生成器）"
              tag="CN"
              block={cnBasketBlock}
              recon={undefined}
              onOpen={onOpenStock}
              onRemind={handleRemind}
              onBuy={handleBuy}
              remindedSymbols={remindedSymbols}
              boughtSymbols={boughtSymbols}
              overall={data}
              allowStockBuys={allowStockBuys}
              rotateOutStocks={rotateOutCn}
              twinStar={twinStar}
              coreTargetPct={coreTargetPct}
              idleHint={
                !allowStockBuys
                  ? `股票篮未启用 · 核心是 ${pickKey ?? 'ETF'} · 卫星见上方`
                  : satStockSymbols.size > 0
                    ? '卫星仓见上方 · 下方只列股票篮剩余持仓'
                    : null
              }
            />
          </>
        ) : null}

        {twinStar ? (
          <details className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/40">
            <summary className="cursor-pointer px-3 py-2 text-[11px] text-[var(--k-muted)]">择强 / 篮 / 对账细节</summary>
            <div className="flex flex-col gap-2 border-t border-[var(--k-border)] p-2.5">
              {sleeve ? (
                <PickStrongOpsPanel
                  sleeve={sleeve}
                  stockHoldingsCount={stockHoldingsCount}
                  satHoldingNames={satHoldingNames}
                  onBuyEtf={handleBuyEtf}
                  twinStar={twinStar}
                  coreTargetPct={coreTargetPct}
                  coreBuyable={tradePlan?.coreBuyable ?? true}
                />
              ) : null}
              <HealthPanel
                title="港股线（股票篮生成器）"
                tag="HK"
                block={data?.hkHealth}
                recon={undefined}
                onOpen={onOpenStock}
                onRemind={handleRemind}
                onBuy={handleBuy}
                remindedSymbols={remindedSymbols}
                boughtSymbols={boughtSymbols}
                overall={data}
                allowStockBuys={allowStockBuys}
                rotateOutStocks={rotateOutHk}
                twinStar={twinStar}
                coreTargetPct={coreTargetPct}
              />
              {tradePlan && tradePlan.recipeNames.length > 0 ? (
                <details className="text-[10px] text-[var(--k-muted)]">
                  <summary className="cursor-pointer">引擎模拟名单（对照）</summary>
                  <div className="mt-1 font-mono leading-relaxed">
                    {tradePlan.recipeNames.map((h) => `${h.ts}${h.daysLeft != null ? `(剩${h.daysLeft}d)` : ''}`).join(' · ')}
                  </div>
                </details>
              ) : null}
            </div>
          </details>
        ) : (
          <>
            {sleeve ? (
              <PickStrongOpsPanel
                sleeve={sleeve}
                stockHoldingsCount={stockHoldingsCount}
                onBuyEtf={handleBuyEtf}
                twinStar={twinStar}
                coreTargetPct={coreTargetPct}
                coreBuyable={tradePlan?.coreBuyable ?? true}
              />
            ) : (
              <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 px-3 py-2 text-[11px] text-amber-800 dark:text-amber-200">
                live pick 未返回 — 请刷新；未拿到 pick 前不执行股票买入（避免偏离单轨）
              </div>
            )}
            <MultiAssetHealthBlock
              holdings={data?.multiAssetHoldings}
              sleeve={sleeve}
              onOpen={onOpenStock}
              coreDestinationReady={tradePlan?.coreBuyable ?? true}
            />
            <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/40">
              <button
                type="button"
                onClick={() => setStockOpen((v) => !v)}
                className="flex w-full items-center gap-2 px-3 py-2 text-left text-[11px] font-semibold"
              >
                <span>股票篮细节（仅 pick=STOCK 时开仓）</span>
                <span className="text-[10px] font-normal text-[var(--k-muted)]">
                  {allowStockBuys ? '今日可执行' : pickKey ? `今日 pick=${pickKey} · 只看仓/轮出` : '等待 pick'}
                </span>
                <span className="ml-auto text-[10px] text-[var(--k-muted)]">{stockOpen ? '收起' : '展开'}</span>
              </button>
              {stockOpen ? (
                <div className="flex flex-col gap-2 border-t border-[var(--k-border)] p-2.5">
                  <HealthPanel
                    title="A股线（股票篮生成器）"
                    tag="CN"
                    block={data}
                    recon={reconByMarket.get('CN')}
                    onOpen={onOpenStock}
                    onRemind={handleRemind}
                    onBuy={handleBuy}
                    remindedSymbols={remindedSymbols}
                    boughtSymbols={boughtSymbols}
                    overall={data}
                    allowStockBuys={allowStockBuys}
                    rotateOutStocks={rotateOutCn}
                    twinStar={twinStar}
                    coreTargetPct={coreTargetPct}
                  />
                  <HealthPanel
                    title="港股线（股票篮生成器）"
                    tag="HK"
                    block={data?.hkHealth}
                    recon={reconByMarket.get('HK')}
                    onOpen={onOpenStock}
                    onRemind={handleRemind}
                    onBuy={handleBuy}
                    remindedSymbols={remindedSymbols}
                    boughtSymbols={boughtSymbols}
                    overall={data}
                    allowStockBuys={allowStockBuys}
                    rotateOutStocks={rotateOutHk}
                    twinStar={twinStar}
                    coreTargetPct={coreTargetPct}
                  />
                </div>
              ) : null}
            </div>
          </>
        )}
      </div>

      {reminderTarget && (
        <BuyReminderDialog
          state={{ symbol: reminderTarget.symbol, name: reminderTarget.name }}
          suggestPct={reminderTarget.sizePct}
          suggestLabel={
            twinStar ? (allowStockBuys ? 'S-3 建议仓位' : '卫星建议仓位') : 'S-3 建议仓位'
          }
          onClose={() => setReminderTarget(null)}
          onConfirm={(values) => void addToWatchlistAndRemind(values)}
        />
      )}
      {buyTarget && (
        <QuickBuyDialog
          state={{
            symbol: buyTarget.symbol,
            name: buyTarget.name,
            score: buyTarget.score,
            rs: buyTarget.rs,
          }}
          suggestPct={buyTarget.sizePct}
          side={buyTarget.side}
          onClose={() => setBuyTarget(null)}
          onConfirm={(values) => void confirmBuy(values)}
        />
      )}
      {reminderError && (
        <div className="mt-2 text-[11px] text-red-500">加入自选失败：{reminderError}</div>
      )}
      {buyError && (
        <div className="mt-2 text-[11px] text-red-500">记录交易失败：{buyError}</div>
      )}
    </div>
  );
}
