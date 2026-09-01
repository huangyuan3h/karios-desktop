'use client';

import * as React from 'react';

import { Bell, BellRing, RefreshCw, ShieldAlert } from 'lucide-react';

import { useQuery } from '@tanstack/react-query';

import { Button } from '@/components/ui/button';
import { recordUserTrade } from '@/lib/queries/userTrades';
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
import { useBacktestReconQuery, useTwinStarActionQuery, type ReconItem } from '@/lib/queries/backtest';
import { useDashboardSentimentQuery } from '@/lib/queries/sentiment';
import { parseExecutionGate } from '@/lib/execution-action';
import { useStrategyMode } from '@/lib/strategy-settings';
import { cn } from '@/lib/utils';
import { loadWatchlist, saveWatchlist, type WatchlistItem } from '@/lib/watchlist-storage';
import { BuyReminderDialog } from '@/components/watchlist/BuyReminderDialog';
import { QuickBuyDialog } from '@/components/watchlist/QuickBuyDialog';
import { MultiAssetHealthBlock } from './MultiAssetHealthBlock';

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
  onBuyEtf,
  twinStar,
  gateBlocksNew,
}: {
  sleeve: Sleeve | null | undefined;
  stockHoldingsCount: number;
  onBuyEtf?: (symbol: string, name: string | null) => void;
  twinStar: boolean;
  gateBlocksNew: boolean;
}) {
  const pickKey = sleeve?.pick?.key ?? 'REPO';
  const meta = PICK_META[pickKey] ?? { label: pickKey, hint: '' };
  const action = sleeve?.action ?? 'NONE';
  const mom = sleeve?.pick?.mom60;
  const etfSym = sleeve?.pick?.symbol;
  const isStock = pickKey === 'STOCK';
  const isRepo = pickKey === 'REPO';
  const isEtf = !isStock && !isRepo;

  const steps: string[] = [];
  if (isStock) {
    steps.push(twinStar ? '核心腿 50% → 股票篮（下方展开篮内买卖）' : '今日资金 100% → 股票篮（下方展开篮内买卖）');
    if (sleeve?.holding) steps.push(twinStar ? '若仍持有 ETF：先卖出 ETF，再配股票' : '若仍持有 ETF：先卖出 ETF，再配股票');
  } else if (isEtf) {
    steps.push(twinStar ? `核心腿 50% → ${meta.label}（${etfSym ?? pickKey}）` : `今日资金 100% → ${meta.label}（${etfSym ?? pickKey}）`);
    if (stockHoldingsCount > 0) steps.push(twinStar ? `现有 ${stockHoldingsCount} 只股票仓（属卫星/S-3 体系 · 核心腿按 50% 资金配 ETF）` : `现有 ${stockHoldingsCount} 只股票仓：应减仓/清仓，切到 ETF（硬切）`);
    if (gateBlocksNew) {
      steps.push(`闸门关闭（${sleeve?.pick ? '' : ''}Execution Gate DEFEND）· 今日不开新仓 — 维持 ${etfSym}`);
    } else if (action === 'ROTATE' || action === 'BUY') steps.push(sleeve?.message || `买入/轮入 ${etfSym}`);
    if (action === 'HOLD') steps.push(sleeve?.message || `继续持有 ${etfSym}`);
    if (action === 'SELL_TO_REPO') steps.push(sleeve?.message || 'ETF 破 MA200 / 峰值−8% → 切逆回购');
  } else {
    steps.push(twinStar ? '今日无人过线 → 核心腿转逆回购 / 观望' : '今日无人过线 → 100% 逆回购 / 空仓观望');
    if (stockHoldingsCount > 0) steps.push(twinStar ? '股票仓属卫星/S-3 体系（核心腿不持股票）' : '股票仓也应清到空（单轨不持）');
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
        {twinStar ? '核心腿 50%' : '100% 硬切'} · 定案 mom_compare · LB60/MA200
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
}) {
  const [expanded, setExpanded] = React.useState(false);
  const size = suggestedSizePct ?? 5;
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
          每票建议 {size}%（10% × 今日环境×{envScale}{envScale !== 1 ? ' · 已含 D3 环境仓位' : ''}）
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
                买 {size}%
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
                  onClick={() => onBuy(c, size)}
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
                  onClick={() => onRemind(c, size)}
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
        <div className="text-xs text-[var(--k-muted)]">当前无持仓（未录入成本/仓位的 watchlist 票不算持仓）</div>
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

export function PortfolioHealthCard({ onOpenStock }: { onOpenStock?: (symbol: string) => void } = {}) {
  const [strategyMode] = useStrategyMode();
  const twinStar = strategyMode !== 'single_track';
  const twinStarQ = useTwinStarActionQuery(twinStar);
  const sentimentQ = useDashboardSentimentQuery();
  const executionGate = React.useMemo(
    () =>
      parseExecutionGate(
        (sentimentQ.data as { marketSentiment?: { executionGate?: unknown } } | undefined)
          ?.marketSentiment?.executionGate,
      ),
    [sentimentQ.data],
  );
  const gateBlocksNew = twinStar && executionGate != null && !executionGate.allowNewEntries;
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
      await recordUserTrade({
        symbol: buyTarget.symbol,
        side: 'BUY',
        price: values.price,
        positionPct: values.positionPct,
        source: 'RESEARCH',
        market: tradeMarketForSymbol(buyTarget.symbol),
      });
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
    });
  }

  function handleBuyEtf(symbol: string, name: string | null) {
    setBuyTarget({
      symbol,
      name,
      score: null,
      rs: null,
      sizePct: 100,
    });
  }

  const sleeve = data?.multiAssetSleeve;
  const pickKey = sleeve?.pick?.key ?? null;
  const allowStockBuys = pickKey === 'STOCK';
  const rotateOutStocks = pickKey != null && pickKey !== 'STOCK';
  const stockHoldingsCount =
    (data?.holdings?.length ?? 0) + (data?.hkHealth?.holdings?.length ?? 0);
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
        <span className="text-[10px] text-[var(--k-muted)]">{twinStar ? '卫星资金跟核心 · 开闸可买才切候选' : '100% 硬切 · 与 Timeline 同源'}</span>
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
                : `数据更新于 ${new Date(Math.max(q.dataUpdatedAt, twinStarQ.dataUpdatedAt, sentimentQ.dataUpdatedAt)).toLocaleTimeString('zh-CN')} · 核心腿/闸门每 5 分钟轮询，卫星每 30 分钟`
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
              {gateBlocksNew ? '🔒 闸门关闭 · 不开新仓 · ' : ''}核心腿：{sleeve.label ?? sleeve.action} {sleeve.pick?.symbol ?? ''}
            </span>
            {twinStarQ.data?.sat?.asOf != null ? (
              <span className="ml-2">
                · 卫星：
                {!twinStarQ.data.sat.gateOpen
                  ? 'R-wide 关闸（不开仓）'
                  : gateBlocksNew
                    ? `R-wide 开闸（闸门关闭暂不买入）· 候选 ${(twinStarQ.data.sat.candidates ?? []).slice(0, 3).map((c) => c.ts).join(', ') || '—'}`
                    : `R-wide 开闸 → 买入候选 ${(twinStarQ.data.sat.candidates ?? []).slice(0, 3).map((c) => c.ts).join(', ') || '—'}`}
              </span>
            ) : null}
            {twinStarQ.data?.sat?.asOf != null ? (
              <div className="mt-1 text-[10px] tabular-nums text-[var(--k-muted)]">
                {twinStarQ.data.sat.gateOpen
                  ? `卫星闸 · R-wide 开闸 breadth ${twinStarQ.data.sat.breadth} · ${twinStarQ.data.sat.gapCount ?? 0} 只缺口`
                  : `卫星闸 · R-wide 关闸 breadth ${twinStarQ.data.sat.breadth}`}
                {twinStarQ.data.sat.note ? ` · ${twinStarQ.data.sat.note}` : ''} · 信号日 {twinStarQ.data.sat.asOf} · 14:30 前调整
              </div>
            ) : null}
          </div>
        ) : null}
        {twinStar && q.data?.tradeDate && twinStarQ.data?.sat?.asOf != null && twinStarQ.data.sat.asOf < q.data.tradeDate ? (
          <div className="rounded-md border border-amber-500/40 bg-amber-500/10 px-2.5 py-1.5 text-[11px] text-amber-800 dark:text-amber-200">
            ⚠ 卫星信号滞后：信号日 {twinStarQ.data.sat.asOf} &lt; 体检数据 {q.data.tradeDate}——卫星判断可能不是最新，请检查数据同步。
          </div>
        ) : null}

        {sleeve ? (
          <PickStrongOpsPanel
            sleeve={sleeve}
            stockHoldingsCount={stockHoldingsCount}
            onBuyEtf={handleBuyEtf}
            twinStar={twinStar}
            gateBlocksNew={gateBlocksNew}
          />
        ) : (
          <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 px-3 py-2 text-[11px] text-amber-800 dark:text-amber-200">
            live pick 未返回 — 请刷新；未拿到 pick 前不执行股票买入（避免偏离{twinStar ? '核心腿' : '单轨'}）
          </div>
        )}

        <MultiAssetHealthBlock
          holdings={data?.multiAssetHoldings}
          sleeve={sleeve}
          onOpen={onOpenStock}
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
                rotateOutStocks={rotateOutStocks}
                twinStar={twinStar}
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
                rotateOutStocks={rotateOutStocks}
                twinStar={twinStar}
              />
            </div>
          ) : null}
        </div>
      </div>

      {reminderTarget && (
        <BuyReminderDialog
          state={{ symbol: reminderTarget.symbol, name: reminderTarget.name }}
          suggestPct={reminderTarget.sizePct}
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
