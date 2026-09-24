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
import {
  useBacktestReconQuery,
  useSleeveReconQuery,
  type ReconItem,
  type SleeveRecon,
} from '@/lib/queries/backtest';
import { useDashboardSentimentQuery } from '@/lib/queries/sentiment';
import { getShanghaiTodayIso } from '@/lib/market-hours';
import { cn } from '@/lib/utils';
import {
  loadWatchlist,
  saveWatchlist,
  upsertWatchlistOpenTrade,
  type WatchlistItem,
} from '@/lib/watchlist-storage';
import { A25TargetBlock } from '@/components/watchlist/A25TargetBlock';
import { B3LegBlock } from '@/components/watchlist/B3LegBlock';
import { SatelliteLegBlock } from '@/components/watchlist/SatelliteLegBlock';
import { BuyReminderDialog } from '@/components/watchlist/BuyReminderDialog';
import { PARKING_KEYS, PARKING_META } from '@/lib/parking-universe';
import { useTimelineQuery, useSatelliteLivePanelQuery, useSatellitePaperQuery, useUserSatelliteBookQuery, type TimelineStrategy } from '@/lib/queries/backtest';
import type { StrategyMode } from '@/lib/strategy-settings';
import { QuickBuyDialog } from '@/components/watchlist/QuickBuyDialog';
import { MultiAssetHealthBlock } from './MultiAssetHealthBlock';

function fmtPct(v: number | null | undefined, digits = 2): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v >= 0 ? '+' : ''}${v.toFixed(digits)}%`;
}

function regimeBadge(regime: string | null | undefined): { label: string; cls: string } {
  switch (regime) {
    case 'Weak':
      return {
        label: 'Weak · 空仓观望',
        cls: 'border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-300',
      };
    case 'Strong':
      return {
        label: 'Strong · 进攻',
        cls: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300',
      };
    case 'Diverging':
      return {
        label: 'Diverging · 满仓进攻',
        cls: 'border-sky-500/40 bg-sky-500/10 text-sky-700 dark:text-sky-300',
      };
    default:
      return {
        label: String(regime ?? '—'),
        cls: 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]',
      };
  }
}

const PICK_META: Record<string, { label: string; hint: string }> = {
  STOCK: { label: '股票篮', hint: 'S-3 CN+HK 持仓篮（等权，10% 上限）' },
  ...Object.fromEntries(
    PARKING_KEYS.map((key) => [
      key,
      { label: PARKING_META[key].short, hint: PARKING_META[key].hint },
    ]),
  ),
  REPO: { label: '逆回购', hint: 'GC001 · 无 ETF 过线时兜底' },
};

type Sleeve = NonNullable<PortfolioHealthResponse['multiAssetSleeve']>;

function PickStrongOpsPanel({
  sleeve,
  stockHoldingsCount,
  onBuyEtf,
  coreBuyable = true,
}: {
  sleeve: Sleeve | null | undefined;
  stockHoldingsCount: number;
  onBuyEtf?: (symbol: string, name: string | null) => void;
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

  const idlePct = sleeve?.idlePct;
  const parkPct = sleeve?.parkPct ?? idlePct;
  const fmt = (v: number | null | undefined) => (v != null ? `${v.toFixed(0)}%` : '—');
  const steps: string[] = [];
  if (isStock) {
    steps.push(
      coreBuyable
        ? '核心：按下方股票篮买入（见仓位%，停车只停剩下的闲钱）'
        : '核心目标股票篮，但今日 0 只可执行 → 不要为 STOCK 卖光 ETF 停车场',
    );
    if (sleeve?.holding && coreBuyable) steps.push('若仍持有停车 ETF：先卖出 ETF，再配股票');
  } else if (isEtf) {
    steps.push(
      action === 'HOLD'
        ? `闲置现金 ${fmt(idlePct)} · 继续持有 ${meta.label}（${etfSym ?? pickKey}）· 停车只停闲钱`
        : action === 'ROTATE' || action === 'BUY'
          ? `停车目标 ${fmt(parkPct)}（含换出的旧腿）→ ${meta.label}（${etfSym ?? pickKey}）· 股票核心不动`
          : `闲置现金 ${fmt(idlePct)} → 逆回购 · 股票核心不动`,
    );
    if (stockHoldingsCount > 0) {
      steps.push(
        `现有 ${stockHoldingsCount} 只股票仓按 S-3 自身规则处理（停车不要求为买 ETF 卖股票）`,
      );
    }
    if (action === 'ROTATE' || action === 'BUY')
      steps.push(sleeve?.message || `买入/轮入 ${etfSym}`);
    if (action === 'HOLD') steps.push(sleeve?.message || `继续持有 ${etfSym}`);
    if (action === 'SELL_TO_REPO')
      steps.push(sleeve?.message || 'ETF 破 MA200 / 峰值−8% → 切逆回购');
  } else {
    steps.push('今日无 ETF 站上 200 日线 → 闲置现金留逆回购（股票核心不动）');
    if (sleeve?.holding) steps.push('卖出停车 ETF 转 REPO');
  }

  return (
    <div className="rounded-lg border border-emerald-500/35 bg-emerald-500/5 px-3 py-2.5">
      <div className="mb-1.5 flex items-center gap-2 text-[11px] font-semibold">
        <span className="h-3 w-[3px] rounded-full bg-emerald-500" />
        操作引导
        <span className="text-[10px] font-normal text-[var(--k-muted)]">
          港湾：S-3 股票核心 + 闲置现金停车
        </span>
      </div>
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
        停车规则：mom60+MA200 选趋势最好的一只 · 回撤 8% 出场 · 只停闲置现金
      </p>
      {sleeve?.message && action !== 'HOLD' ? (
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
      {sleeve?.pick?.all_mom && Object.keys(sleeve.pick.all_mom).length > 0 ? (
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
              {k} {v}%{sleeve.pick?.all_above?.[k] === false ? ' ✗MA' : ''}
            </span>
          ))}
        </div>
      ) : null}
    </div>
  );
}

export function HoldingRow({
  h,
  onOpen,
}: {
  h: PortfolioHolding;
  onOpen?: (symbol: string) => void;
}) {
  const exit = h.action === 'EXIT';
  const pnlTone =
    (h.pnlPct ?? 0) >= 0
      ? 'text-emerald-600 dark:text-emerald-400'
      : 'text-red-600 dark:text-red-400';
  return (
    <div
      role={onOpen ? 'button' : undefined}
      onClick={onOpen ? () => onOpen(h.symbol) : undefined}
      className={cn(
        'rounded-lg border px-3 py-2',
        exit
          ? 'border-red-500/40 bg-red-500/5'
          : 'border-[var(--k-border)] bg-[var(--k-surface-2)]',
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
        {(
          h as unknown as {
            nearStop?: boolean;
            nearStopLabel?: string;
            nearStopDistancePct?: number;
          }
        ).nearStop && (
          <span className="rounded bg-amber-500/20 px-1.5 py-0.5 text-[10px] font-semibold text-amber-700 dark:text-amber-300">
            ⚠ 临近{(h as unknown as { nearStopLabel?: string }).nearStopLabel}{' '}
            {(h as unknown as { nearStopDistancePct?: number }).nearStopDistancePct}% · 需更新条件单
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
      {((h.alphaEvents?.length ?? 0) > 0 || h.industryFlow != null) && (
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
              🧭 {h.industryFlow.industry} 5日 {(h.industryFlow.netInflow5d ?? 0) >= 0 ? '+' : ''}
              {h.industryFlow.netInflow5d ?? 0}亿（第{h.industryFlow.rank5d}/{h.industryFlow.total}
              ）
            </div>
          )}
        </div>
      )}
      {h.reason && (
        <div className="mt-1 text-[11px] text-red-600 dark:text-red-400">触发：{h.reason}</div>
      )}
      {h.realtimeAlert && !h.reason && (
        <div className="mt-1 text-[11px] text-amber-700 dark:text-amber-300">
          ⚠ {h.realtimeAlert}
        </div>
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
}: {
  candidates: PortfolioCandidate[];
  total?: number;
  suggestedSizePct?: number | null;
  envScaleToday?: number | null;
  remindedSymbols: Set<string>;
  boughtSymbols: Set<string>;
  onRemind: (c: PortfolioCandidate, sizePct: number) => void;
  onBuy: (c: PortfolioCandidate, sizePct: number, rank?: number) => void;
}) {
  const [expanded, setExpanded] = React.useState(false);
  const navSize = suggestedSizePct ?? 10;
  const shown = expanded ? candidates : candidates.slice(0, 5);
  const hidden = candidates.length - shown.length;
  const envScale = envScaleToday ?? 1;
  // 时间感知标题：上午/盘中看到"14:30前决定"，尾盘看到"执行"，周末不催单。
  const buyWindowLabel = React.useMemo(() => {
    try {
      const parts = Object.fromEntries(
        new Intl.DateTimeFormat('zh-CN', {
          timeZone: 'Asia/Shanghai',
          hour: '2-digit',
          minute: '2-digit',
          weekday: 'short',
          hour12: false,
        })
          .formatToParts(new Date())
          .map((p) => [p.type, p.value]),
      ) as Record<string, string>;
      const [h, m] = String(parts.hour ?? '0').split(':');
      const mins = Number(h) * 60 + Number(m ?? 0);
      const wd = String(parts.weekday ?? '');
      if (wd === '周六' || wd === '周日') return '周末 · 股票篮候选（先看，不下单）';
      if (mins < 14 * 60 + 30) return '今日 14:30 前 · 股票篮买入';
      return '尾盘执行 · 股票篮买入';
    } catch {
      return '股票篮买入';
    }
  }, []);
  return (
    <div id="buy-list" className="rounded-lg border border-emerald-500/30 bg-emerald-500/5 px-3 py-2">
      <div className="mb-1.5 flex flex-wrap items-center gap-x-2 gap-y-0.5 text-[11px] font-medium text-emerald-700 dark:text-emerald-300">
        <span>{buyWindowLabel}（S-3 核心 · score 前 5）</span>
        {total != null && total > candidates.length && (
          <span className="text-[10px] font-normal text-[var(--k-muted)]">候选池 {total} 只</span>
        )}
        <span className="ml-auto text-[10px] font-normal text-[var(--k-muted)]">
          {`每票建议 ${navSize}%（10% × 今日环境×${envScale}${envScale !== 1 ? ' · 已含 D3 环境仓位' : ''}）`}
        </span>
      </div>
      <div className="flex flex-col gap-1">
        {shown.map((c, i) => {
          const symbol = c.symbol ?? c.ts_code ?? '';
          const reminded = remindedSymbols.has(symbol);
          const bought = boughtSymbols.has(symbol);
          return (
            <div key={symbol} className="flex flex-wrap items-center gap-x-2 text-[12px]">
              <span className="w-4 shrink-0 text-right font-mono text-[10px] text-[var(--k-muted)]">
                {i + 1}
              </span>
              <span className="font-medium">{c.name ?? symbol}</span>
              <span className="text-[10px] tabular-nums text-[var(--k-muted)]">{symbol}</span>
              <span className="ml-auto font-mono text-[10.5px] tabular-nums">
                score={c.score ?? '—'}
              </span>
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
                  🧭 {c.industryFlow.industry} 5日
                  {(c.industryFlow.netInflow5d ?? 0) >= 0 ? '+' : ''}
                  {c.industryFlow.netInflow5d ?? 0}亿（第{c.industryFlow.rank5d}/
                  {c.industryFlow.total}）
                </span>
              )}
              {bought ? (
                <span className="inline-flex items-center gap-0.5 rounded border border-emerald-500/40 bg-emerald-500/10 px-1.5 py-0.5 text-[10px] text-emerald-700 dark:text-emerald-300">
                  ✓ 已买入
                </span>
              ) : (
                <button
                  type="button"
                  onClick={() => onBuy(c, navSize, i + 1)}
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
  buyable = true,
}: {
  recon: ReconItem | undefined;
  onRemind: (c: PortfolioCandidate, sizePct: number) => void;
  remindedSymbols: Set<string>;
  blockId: string;
  /** 闸门关闭时只看缺口，不开"提醒买入"（弱市不新开）。 */
  buyable?: boolean;
}) {
  const [expanded, setExpanded] = React.useState(false);
  if (!recon) return null;
  const hasGap = recon.missing > 0 || recon.extra > 0;
  const missingRows = (recon.detail ?? [])
    .filter((d) => d.type === 'missing')
    .slice(0, 20) as Array<{
    symbol?: string;
    score?: unknown;
    entry?: unknown;
    positionPct?: unknown;
  }>;
  const clean = !hasGap;
  return (
    <div id={blockId} className="rounded-lg border border-sky-500/30 bg-sky-500/5 px-3 py-2">
      <div className="flex flex-wrap items-center gap-x-2 gap-y-0.5 text-[11px]">
        <span
          className={
            clean ? 'text-emerald-700 dark:text-emerald-300' : 'text-amber-700 dark:text-amber-300'
          }
        >
          {clean ? '✓' : '⚠'}
        </span>
        <span className="font-medium">股票篮对账 · {recon.reconDate}</span>
        <span className="tabular-nums">
          回测应持 {recon.expected} · 实持 {recon.actual} · 缺 {recon.missing} · 多 {recon.extra}
        </span>
        {typeof recon.alignedReturnDiffPct === 'number' ? (
          <span className="text-[10px] text-[var(--k-muted)]">
            对齐票中位差 {recon.alignedReturnDiffPct > 0 ? '+' : ''}
            {recon.alignedReturnDiffPct.toFixed(1)}pt（我 − 回测）
          </span>
        ) : null}
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
              typeof m.score === 'number' && Number.isFinite(m.score) ? m.score.toFixed(1) : '—';
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
                ) : buyable ? (
                  <button
                    type="button"
                    onClick={() => onRemind({ symbol, name: null }, pct)}
                    className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5 text-[10px] text-[var(--k-muted)] hover:border-emerald-500/50 hover:text-emerald-700 dark:hover:text-emerald-300"
                    title="回测缺票：加入自选 + 设目标价/备注提醒"
                  >
                    <Bell size={9} className="mr-0.5 inline-block" />
                    提醒买入
                  </button>
                ) : null}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

/** OPT-151/OPT-203: core-leg tracking — strategy vs paper mirror vs your book.
 * Refreshes every 60s and spells out the exact next action when you are behind. */
function SleeveReconBlock({
  recon,
  updatedAt,
  holdings,
}: {
  recon: SleeveRecon | undefined;
  updatedAt?: number;
  holdings?: PortfolioHealthResponse['multiAssetHoldings'];
}) {
  const [expanded, setExpanded] = React.useState(false);
  if (!recon) return null;
  if (recon.error) {
    return (
      <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 px-3 py-2 text-[11px] text-amber-700 dark:text-amber-300">
        核心腿对账失败：{recon.error}
      </div>
    );
  }
  const pretty = (s: string) => s.replace(/^ETF:/, '');
  const hasGap =
    recon.missedBuys.length > 0 || recon.missedSells.length > 0 || recon.extraOpens.length > 0;
  const expected = recon.expectedBuys.length > 0 || recon.expectedSells.length > 0;
  const execDate = recon.expectedBuys.length > 0 ? recon.userExecDate : recon.exitExecDate;
  const heldSyms = (holdings ?? []).map((h) => pretty(String(h.symbol ?? '')));
  const targetSym = recon.pickSymbol ? pretty(recon.pickSymbol) : null;
  const holdingRow = (holdings ?? []).find((h) => pretty(String(h.symbol ?? '')) === targetSym);
  const targetMismatch = targetSym != null && heldSyms.length > 0 && !holdingRow;
  const heldLabel = heldSyms.length
    ? `实持 ${heldSyms.join(', ')}${
        holdingRow?.positionPct != null ? ` ${holdingRow.positionPct}%` : ''
      }`
    : '实持 空仓';
  const you = (() => {
    if (!recon.decisionAvailable) return { tone: 'muted', text: '决策不可用，先不要手动操作' };
    if (!expected) return { tone: 'muted', text: '无操作' };
    if (recon.userAlignment === 'aligned') return { tone: 'ok', text: '✓ 已跟上' };
    if (recon.userAlignment === 'pending') {
      const buys = recon.expectedBuys.map(pretty).join(', ');
      const sells = recon.expectedSells.map(pretty).join(', ');
      const act = buys ? `买 ${buys}` : `卖 ${sells}`;
      return { tone: 'warn', text: `待执行 · ${execDate ?? '下一交易日'} 开盘${act}` };
    }
    if (recon.userAlignment === 'missing') {
      const buys = recon.expectedBuys.map(pretty).join(', ');
      const sells = recon.expectedSells.map(pretty).join(', ');
      const parts = [buys ? `应买 ${buys}` : '', sells ? `应卖 ${sells}` : ''].filter(Boolean);
      return { tone: 'bad', text: `未执行 · ${parts.join(' / ') || '—'}` };
    }
    return null;
  })();
  const toneCls: Record<string, string> = {
    ok: 'text-emerald-700 dark:text-emerald-300',
    warn: 'text-amber-700 dark:text-amber-300',
    bad: 'text-red-600 dark:text-red-400',
    muted: 'text-[var(--k-muted)]',
  };
  const time = updatedAt
    ? new Date(updatedAt).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })
    : null;
  return (
    <div
      id="sleeve-recon"
      className={
        hasGap
          ? 'rounded-lg border border-red-500/30 bg-red-500/5 px-3 py-2'
          : 'rounded-lg border border-sky-500/30 bg-sky-500/5 px-3 py-2'
      }
    >
      <div className="flex flex-wrap items-center gap-x-2 gap-y-0.5 text-[11px]">
        <span
          className={
            hasGap ? 'text-red-600 dark:text-red-400' : 'text-emerald-700 dark:text-emerald-300'
          }
        >
          {hasGap ? '🔴' : '✓'}
        </span>
        <span className="font-semibold">核心腿对账 · {recon.day}</span>
        {time ? <span className="text-[10px] text-[var(--k-muted)]">实时 {time}</span> : null}
        <span className="tabular-nums text-[var(--k-muted)]">
          {recon.decisionAvailable
            ? `策略 ${recon.label ?? recon.action ?? '—'}${
                targetSym ? ` · 目标 ${targetSym}` : ''
              } · ${heldLabel}`
            : '核心决策不可用（候选数据不足）'}
        </span>
        <button
          type="button"
          onClick={() => setExpanded((v) => !v)}
          className="ml-auto rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5 text-[10px] text-[var(--k-muted)] hover:border-[var(--k-accent)]/60"
        >
          {expanded ? '收起' : '细节'}
        </button>
      </div>
      <div className="mt-1 flex flex-wrap items-center gap-x-2 text-[11px]">
        <span className="text-[10px] text-[var(--k-muted)]">镜像盘</span>
        {hasGap ? (
          <span className="text-red-600 dark:text-red-400">
            🔴 偏离 ·{' '}
            {recon.missedBuys.length ? `缺买 ${recon.missedBuys.map(pretty).join(', ')} ` : ''}
            {recon.missedSells.length ? `缺卖 ${recon.missedSells.map(pretty).join(', ')} ` : ''}
            {recon.extraOpens.length ? `多开 ${recon.extraOpens.map(pretty).join(', ')}` : ''}
          </span>
        ) : (
          <span className="text-emerald-700 dark:text-emerald-300">✓ 已跟上策略</span>
        )}
      </div>
      {you ? (
        <div className="mt-0.5 flex flex-wrap items-center gap-x-2 text-[11px]">
          <span className="text-[10px] text-[var(--k-muted)]">你的账户</span>
          <span className={cn('font-medium', toneCls[you.tone])}>{you.text}</span>
          {targetMismatch ? (
            <span className="text-red-600 dark:text-red-400">· 实持与目标不一致，需换仓</span>
          ) : null}
        </div>
      ) : null}
      {expanded ? (
        <div className="mt-1.5 grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 border-t border-sky-500/20 pt-1.5 text-[10px] tabular-nums">
          <span className="text-[var(--k-muted)]">策略</span>
          <span>
            应买 {recon.expectedBuys.map(pretty).join(', ') || '—'} · 应卖{' '}
            {recon.expectedSells.map(pretty).join(', ') || '—'} · 闲置 {recon.idlePct ?? '—'}%
          </span>
          <span className="text-[var(--k-muted)]">镜像</span>
          <span>
            已买 {recon.paperBuysToday.map(pretty).join(', ') || '—'} · 已卖{' '}
            {recon.paperSellsToday.map(pretty).join(', ') || '—'}
            {hasGap ? ' · 有差异（见上）' : ' · 无差异'}
          </span>
          <span className="text-[var(--k-muted)]">你</span>
          <span>
            已买 {recon.userBuys.map(pretty).join(', ') || '—'} · 已卖{' '}
            {recon.userSells.map(pretty).join(', ') || '—'} · 执行日{' '}
            {recon.userExecDate ?? '—'} / {recon.exitExecDate ?? '—'}
          </span>
        </div>
      ) : null}
      <div className="mt-1 text-[10px] text-[var(--k-muted)]">
        镜像差异 = 自动镜像账本与引擎的偏差；你的账户按你记录的成交判定（信号 T 收盘 → T+1
        开盘执行，买卖同口径）。
      </div>
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
}) {
  const holdings = block?.holdings ?? [];
  const candidates = block?.s3Candidates ?? [];
  const regime = regimeBadge(block?.regime);
  const idSuffix = tag === 'HK' ? '-hk' : '';
  const gateClosed = isMarketGateClosed(block);
  // Parking pick (ETF key / null=REPO). It NEVER gates the stock core (OPT-206).
  const parkingPick = overall?.multiAssetSleeve?.pick?.key ?? null;
  const showBuyList =
    allowStockBuys && candidates.length > 0 && block?.regime !== 'Weak' && !gateClosed;

  return (
    <div className="flex min-w-0 flex-col gap-2 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/60 p-2.5">
      <div className="flex items-center gap-2 text-[11px] font-semibold">
        <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5">
          {tag}
        </span>
        {title}
        {rotateOutStocks && holdings.length > 0 && (
          <span className="rounded bg-amber-500/15 px-1.5 py-0.5 text-[11px] font-bold text-amber-700 dark:text-amber-300">
            核心非 STOCK · 应轮出
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
        <span className={cn('rounded border px-1.5 py-0.5 font-medium', regime.cls)}>
          {regime.label}
        </span>
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
          {parkingPick ? ` · 停车 pick ${parkingPick}` : ''}
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
      {/* 对账缺口闸门关闭时也可见：只看不买（弱市的缺口多为"不该买"，同样要确认）。 */}
      <ReconBlock
        recon={recon}
        onRemind={onRemind}
        remindedSymbols={remindedSymbols}
        blockId={`recon${idSuffix}`}
        buyable={allowStockBuys}
      />
      {holdings.length === 0 ? (
        <div className="text-xs text-[var(--k-muted)]">
          当前无持仓（未录入成本/仓位的 watchlist 票不算持仓）
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
                      reason: h.reason ?? `停车今日 pick=${parkingPick}，股票篮应轮出`,
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
          suggestedSizePct={
            Number((block?.s3Rules as Record<string, unknown> | undefined)?.suggestedSizePct) ||
            null
          }
          envScaleToday={
            Number((block?.s3Rules as Record<string, unknown> | undefined)?.envScaleToday) || null
          }
          remindedSymbols={remindedSymbols}
          boughtSymbols={boughtSymbols}
          onRemind={onRemind}
          onBuy={onBuy}
        />
      ) : !allowStockBuys && candidates.length > 0 ? (
        <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 px-3 py-2 text-[11px] text-amber-700 dark:text-amber-300">
          股票候选 <strong>{candidates.length}</strong> 只 · 今日不满足 S-3
          开仓条件（闸门/市况）→ <strong>不执行买入</strong>
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
  mode = 'harbor',
}: {
  onOpenStock?: (symbol: string) => void;
  mode?: StrategyMode;
} = {}) {
  const coreView = mode !== 'starship' && mode !== 'starship_robust' && mode !== 'starship_b';
  const satelliteMode =
    mode === 'starport' ||
    mode === 'starship' ||
    mode === 'starship_robust' ||
    mode === 'starship_b' ||
    mode === 'twin_star';
  const queryClient = useQueryClient();
  const sentimentQ = useDashboardSentimentQuery();
  const q = useQuery({
    queryKey: ['portfolio-health'],
    queryFn: ({ signal }) => fetchPortfolioHealth(undefined, signal),
    refetchInterval: 5 * 60_000,
  });
  const reconQ = useBacktestReconQuery(2);
  const sleeveReconQ = useSleeveReconQuery(true);
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
    initialPrice?: number | null;
    rank?: number | null;
  } | null>(null);
  const [boughtSymbols, setBoughtSymbols] = React.useState<Set<string>>(new Set());
  const [buyError, setBuyError] = React.useState<string | null>(null);
  const [buyBusy, setBuyBusy] = React.useState(false);
  const [reminders, setReminders] = React.useState<BuyReminder[]>([]);
  const [reminderError, setReminderError] = React.useState<string | null>(null);

  const remindedSymbols = React.useMemo(() => new Set(reminders.map((r) => r.symbol)), [reminders]);

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
  // Last close per held symbol — the a25 order surface needs a mark for a leg
  // the engine never booked (no timeline row, not in today's panel gap list).
  const holdingLastClose = React.useMemo(() => {
    const m: Record<string, number> = {};
    for (const h of data?.holdings ?? []) {
      if (h.symbol && typeof h.lastClose === 'number') m[h.symbol] = h.lastClose;
    }
    return m;
  }, [data]);
  const satToday = new Date().toISOString().slice(0, 10);
  const satStart = (() => {
    const d = new Date();
    d.setFullYear(d.getFullYear() - 1);
    return d.toISOString().slice(0, 10);
  })();
  const satStrategy: TimelineStrategy =
    mode === 'starship' || mode === 'starship_robust' || mode === 'starship_b' || mode === 'twin_star'
      ? mode
      : 'starport';
  const satQ = useTimelineQuery(satStart, satToday, satStrategy, satelliteMode);
  // OPT-222: today's 14:30 live snapshot (card prefers it over the replay day).
  // Only the satellite modes render the block — don't poll in harbor/homeport.
  const satLiveQ = useSatelliteLivePanelQuery(satelliteMode);
  // OPT-228: forward paper book (open legs + closed trades + 20-trade prereq).
  const satPaperQ = useSatellitePaperQuery(satelliteMode);
  // The user's own satellite book (journal-sourced) — drives the real prereq.
  const satUserBookQ = useUserSatelliteBookQuery(satStrategy, satelliteMode);
  const satLast = satQ.data?.rows?.[satQ.data.rows.length - 1];
  /** Parking pick (ETF key / null=REPO). */
  const pickKey = sleeve?.pick?.key ?? null;
  const s3CandidatesCount = data?.s3Candidates?.length ?? 0;
  const gateClosedToday = data != null && isMarketGateClosed(data);
  /**
   * Harbor (OPT-206): the S-3 core's buys are governed by the gate/candidates
   * (backend `s3BuySetup` = gate open + executable basket), NOT by the parking
   * pick. The old `pickKey === 'STOCK'` test silently disabled every core buy
   * whenever the idle cash was parked in an ETF.
   */
  const allowStockBuys =
    sleeve?.s3BuySetup ?? (pickKey === 'STOCK' || (s3CandidatesCount > 0 && !gateClosedToday));
  /**
   * Parking never sells the core: stock exits come from the backend's
   * per-holding S-3 actions. (Old code rotated the whole basket out whenever
   * the parking pick was an ETF.)
   */
  const rotateOutCn = false;
  const rotateOutHk = false;
  const stockHoldingsCount =
    (data?.holdings?.length ?? 0) + (data?.hkHealth?.holdings?.length ?? 0);
  /** Harbor setup but no executable basket today → keep the parking ETF. */
  const stockBuyable = allowStockBuys && s3CandidatesCount > 0 && !gateClosedToday;
  const coreDestinationReady = !allowStockBuys || stockBuyable;

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
    if (!buyTarget || buyBusy) return;
    setBuyError(null);
    setBuyBusy(true);
    const target = buyTarget;
    try {
      const next = upsertWatchlistOpenTrade(loadWatchlist(), {
        symbol: target.symbol,
        name: target.name,
        side: target.side,
        price: values.price,
        positionPct: values.positionPct,
        entryDate: getShanghaiTodayIso(),
      });
      await saveWatchlist(next);
      await recordUserTrade({
        symbol: target.symbol,
        side: target.side,
        price: values.price,
        positionPct: values.positionPct,
        source: 'RESEARCH',
        market: tradeMarketForSymbol(target.symbol),
      });
      // Close first so the dialog never sits frozen on network; the derived
      // surfaces refresh in the background (2026-09-04 modal-freeze fix).
      setBuyTarget(null);
      setBoughtSymbols((prev) => new Set(prev).add(target.symbol));
      void invalidateUserTradesQueries(queryClient).catch(() => {});
    } catch (e) {
      setBuyError(e instanceof Error ? e.message : String(e));
    } finally {
      setBuyBusy(false);
    }
  }

  function handleBuy(c: PortfolioCandidate, sizePct: number, rank?: number) {
    setBuyTarget({
      symbol: c.symbol ?? c.ts_code ?? '',
      name: c.name ?? null,
      score: c.score,
      rs: c.rs,
      sizePct,
      side: 'BUY',
      rank: rank ?? null,
    });
  }

  function handleBuyEtf(symbol: string, name: string | null) {
    setBuyTarget({
      symbol,
      name,
      score: null,
      rs: null,
      sizePct: 100,
      side: 'BUY',
    });
  }

  // 股票区默认展开：闸门关闭时买入虽停，但持仓 EXIT 与对账缺口仍需可见
  // （弱市下折叠会把"该卖"和"缺口"一起藏掉）。用户可手动收起。
  const [stockOpen, setStockOpen] = React.useState(true);

  if (q.isError && !data) {
    return (
      <div className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-4 py-2.5 text-xs text-[var(--k-muted)]">
        <ShieldAlert size={13} className="mr-1 inline-block" />
        港湾暂无数据（data-sync-service 未响应）
        <button
          type="button"
          onClick={() => void q.refetch()}
          disabled={q.isFetching}
          className="ml-2 rounded border border-[var(--k-border)] px-2 py-0.5 text-[11px] hover:bg-[var(--k-surface-2)]"
        >
          重试
        </button>
      </div>
    );
  }

  return (
    <div className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-4 py-3">
      <div className="mb-2 flex items-center justify-end gap-2">
        <span
          className={cn(
            'ml-auto rounded px-1.5 py-0.5 text-[10px] font-medium',
            q.isError || sentimentQ.isError
              ? 'bg-red-500/15 text-red-600 dark:text-red-400'
              : 'bg-emerald-500/10 text-emerald-700 dark:text-emerald-300',
          )}
          title={
            q.isError || sentimentQ.isError
              ? '部分数据获取失败，react-query 自动重试中'
              : `数据更新于 ${new Date(Math.max(q.dataUpdatedAt, sentimentQ.dataUpdatedAt)).toLocaleTimeString('zh-CN')} · 核心每 5 分钟`
          }
        >
          {q.isError || sentimentQ.isError
            ? '⚠ 数据失败 · 重试中'
            : `实时 · ${new Date(Math.max(q.dataUpdatedAt, sentimentQ.dataUpdatedAt)).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })}`}
        </span>
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

      {coreView && reminders.length > 0 && (
        <div id="buy-reminders" className="mb-2 rounded-lg border border-sky-500/30 bg-sky-500/5 px-3 py-2">
          <div className="mb-1 flex items-center gap-1.5 text-[11px] font-medium text-sky-700 dark:text-sky-300">
            <BellRing size={11} className="inline-block" />
            买入提醒（{reminders.length}）
          </div>
          <div className="flex flex-col gap-1">
            {reminders.map((r) => (
              <div key={r.symbol} className="flex flex-wrap items-center gap-x-2 text-[11px]">
                <span className="font-medium">{r.name ?? r.symbol}</span>
                <span className="font-mono text-[10px] tabular-nums text-[var(--k-muted)]">
                  {r.symbol}
                </span>
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

      <div className="flex flex-col gap-3">
        {coreView ? (
          <>
        {sleeve ? (
          <PickStrongOpsPanel
            sleeve={sleeve}
            stockHoldingsCount={stockHoldingsCount}
            onBuyEtf={handleBuyEtf}
            coreBuyable={stockBuyable}
          />
        ) : (
          <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 px-3 py-2 text-[11px] text-amber-800 dark:text-amber-200">
            live pick 未返回 — 请刷新；未拿到 pick 前不执行股票买入（避免偏离港湾）
          </div>
        )}
        <MultiAssetHealthBlock
          holdings={data?.multiAssetHoldings}
          sleeve={sleeve}
          onOpen={onOpenStock}
          coreDestinationReady={coreDestinationReady}
        />
        <SleeveReconBlock
          recon={sleeveReconQ.data?.recon}
          updatedAt={sleeveReconQ.dataUpdatedAt}
          holdings={data?.multiAssetHoldings}
        />
        <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/40">
          <button
            type="button"
            onClick={() => setStockOpen((v) => !v)}
            className="flex w-full items-center gap-2 px-3 py-2 text-left text-[11px] font-semibold"
          >
            <span>股票篮细节（仅 pick=STOCK 时开仓）</span>
            <span className="text-[10px] font-normal text-[var(--k-muted)]">
              {allowStockBuys
                ? stockBuyable
                  ? '今日可执行'
                  : '今日无候选'
                : pickKey
                  ? `今日 pick=${pickKey} · 只看仓/轮出`
                  : '等待 pick'}
            </span>
            <span className="ml-auto text-[10px] text-[var(--k-muted)]">
              {stockOpen ? '收起' : '展开'}
            </span>
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
              />
            </div>
          ) : null}
        </div>
          </>
        ) : null}
        {mode === 'starship_robust' || mode === 'starship_b' ? (
          <A25TargetBlock
            parkedHeld={satQ.data?.parkedHeld ?? null}
            sleevePick={sleeve?.pick ?? null}
            panel={satLiveQ.data?.panel ?? null}
            openPositions={satQ.data?.openPositions ?? []}
            satCapacity={satQ.data?.satCapacity ?? null}
            holdingLastClose={holdingLastClose}
            parkMode={mode === 'starship_b' ? 'starship_b' : 'a25'}
          />
        ) : null}
        {mode === 'homeport' || mode === 'starport' || mode === 'starship_robust' ? (
          <B3LegBlock />
        ) : null}
        {satelliteMode ? (
          <SatelliteLegBlock
            row={satLast}
            strategy={mode}
            satWeight={satQ.data?.satWeight ?? null}
            openPositions={satQ.data?.openPositions ?? []}
            parkedHeld={satQ.data?.parkedHeld ?? null}
            livePanel={satLiveQ.data?.panel ?? null}
            livePanelStale={satLiveQ.data?.stale === true}
            paper={satPaperQ.data?.paper ?? null}
            userBook={satUserBookQ.data ?? null}
            stockGateClosed={gateClosedToday}
            hideBuyRows={mode === 'starship_robust' || mode === 'starship_b'}
            timelineUnavailable={satQ.isLoading ? 'loading' : satQ.error ? 'error' : null}
            timelineError={satQ.error ? String(satQ.error instanceof Error ? satQ.error.message : satQ.error) : null}
            onRetryTimeline={() =>
              queryClient.invalidateQueries({ queryKey: ['backtest', 'timeline'] })
            }
          />
        ) : null}
      </div>

      {reminderTarget && (
        <BuyReminderDialog
          state={{ symbol: reminderTarget.symbol, name: reminderTarget.name }}
          suggestPct={reminderTarget.sizePct}
          suggestLabel="S-3 建议仓位"
          onClose={() => setReminderTarget(null)}
          onConfirm={(values) => void addToWatchlistAndRemind(values)}
        />
      )}
      {buyTarget && (
        <QuickBuyDialog
          key={`${buyTarget.side}-${buyTarget.symbol}`}
          state={{
            symbol: buyTarget.symbol,
            name: buyTarget.name,
            score: buyTarget.score,
            rs: buyTarget.rs,
          }}
          suggestPct={buyTarget.sizePct}
          rank={buyTarget.rank ?? null}
          side={buyTarget.side}
          initialPrice={buyTarget.initialPrice ?? null}
          busy={buyBusy}
          error={buyError}
          onClose={() => setBuyTarget(null)}
          onConfirm={(values) => void confirmBuy(values)}
        />
      )}
      {reminderError && (
        <div className="mt-2 text-[11px] text-red-500">加入自选失败：{reminderError}</div>
      )}
      {buyError && <div className="mt-2 text-[11px] text-red-500">记录交易失败：{buyError}</div>}
    </div>
  );
}
