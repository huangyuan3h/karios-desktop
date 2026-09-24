'use client';

import * as React from 'react';
import { useQueryClient } from '@tanstack/react-query';
import type { SatelliteLivePanel, SatellitePaper } from '@karios/shared';

import { Button } from '@/components/ui/button';
import { useAccountSettings } from '@/lib/account-settings';
import { slotLotShares } from '@/lib/lot-sizing';
import { getShanghaiMinutes, getShanghaiTodayIso, isWeekdayShanghai } from '@/lib/market-hours';
import { PARKING_META, parkingKeyForSymbol } from '@/lib/parking-universe';
import type {
  TimelineOpenPosition,
  TimelineParkedHeld,
  TimelineRow,
} from '@/lib/queries/backtest';
import { useSatelliteExitDueQuery } from '@/lib/queries/backtest';
import { invalidateUserTradesQueries, recordUserTrade } from '@/lib/queries/userTrades';
import { cn } from '@/lib/utils';
import {
  loadWatchlist,
  saveWatchlist,
  upsertWatchlistOpenTrade,
} from '@/lib/watchlist-storage';

type PanelRankedEntry = NonNullable<SatelliteLivePanel['ranked']>[number];

export type ResearchStrategy =
  | 'starport'
  | 'starship'
  | 'starship_robust'
  | 'starship_b'
  | 'twin_star';

/** Research-only strategy structure (weights mirror starport.py constants). */
export const RESEARCH_STRUCTURE: Record<
  ResearchStrategy,
  { title: string; parts: string; defaultWeight: number; follow: string }
> = {
  starport: {
    title: '星港',
    parts: '母港 0.8 + 卫星 0.2',
    defaultWeight: 0.2,
    follow: '卫星 0.2 = 母港（港湾×B3）之外挪出 0.2，14:30 分 4 笔买入；到期日卖出换下一批',
  },
  starship: {
    title: '星舰 v2',
    parts: '卫星 100% + 闲钱停车',
    defaultWeight: 1,
    follow: '全部资金按卫星 4 槽 ×25%；没出手的闲钱停趋势最好的 ETF（H2 迟滞换仓，2pt 领先才换）',
  },
  starship_robust: {
    title: '稳健星舰 H2-a25',
    parts: '卫星 100% + 闲钱 25% H2 ETF / 75% B3',
    defaultWeight: 1,
    follow:
      '卫星同星舰（4 槽 ×25%）；没出手的闲钱 25% 停趋势最好的 ETF、75% 停 B3 风险预算组合（5 资产 inverse-vol，月初再平衡）——回撤只有激进版约 1/4',
  },
  starship_b: {
    title: '星舰 B',
    parts: '卫星 100% + 闲钱 100% {国债+黄金+纳指}',
    defaultWeight: 1,
    follow:
      '卫星同星舰（4 槽 ×25%）；没出手的闲钱 100% 停 {国债, 黄金, 纳指} 三腿逆波动率组合（60d，月初再平衡）——回撤更浅、熊市更强，停放腿只有 3 只 ETF，好复制',
  },
  twin_star: {
    title: '双子星',
    parts: '港湾 1/2 + 卫星 1/2',
    defaultWeight: 0.5,
    follow: '卫星 1/2 = 港湾之外挪出 1/2，14:30 分 4 笔买入；到期日卖出换下一批',
  },
};

function pct(v: number): string {
  return `${(v * 100).toFixed(1).replace(/\.0$/, '')}%`;
}

function signedPct(v: number | null | undefined): string {
  if (v == null) return '—';
  return `${v > 0 ? '+' : ''}${v.toFixed(1)}%`;
}

/** Xueqiu page for a CN ts_code ("002173.SZ" -> https://xueqiu.com/S/SZ002173). */
function xueqiuUrl(ts: string): string {
  const [code, ex] = String(ts).split('.');
  return `https://xueqiu.com/S/${ex === 'SH' ? 'SH' : 'SZ'}${code}`;
}

function pnlClass(v: number | null | undefined): string {
  if (v == null) return 'text-[var(--k-muted)]';
  if (v > 0) return 'text-emerald-600 dark:text-emerald-400';
  if (v < 0) return 'text-red-600 dark:text-red-400';
  return 'text-[var(--k-muted)]';
}

/**
 * Satellite leg state + research operation hints (星港/星舰 views).
 *
 * Live stays 港湾 — these are display-only instructions for the paper/research
 * books. The satellite book (legs, capacity, weight) comes from the timeline
 * payload; the TODAY 14:30 gate/panel comes from the OPT-222 snapshot
 * (``livePanel``) when available, otherwise the replay state is shown with a
 * "待 14:30 判定" note.
 */
export function SatelliteLegBlock({
  row,
  strategy,
  satWeight,
  openPositions = [],
  parkedHeld = null,
  livePanel = null,
  livePanelStale = false,
  paper = null,
  userBook = null,
  stockGateClosed = false,
  hideBuyRows = false,
  timelineUnavailable = null,
  timelineError = null,
  onRetryTimeline,
}: {
  row?: TimelineRow | null;
  strategy?: ResearchStrategy;
  satWeight?: number | null;
  openPositions?: TimelineOpenPosition[];
  parkedHeld?: TimelineParkedHeld | null;
  /** OPT-222: today's 14:30 live snapshot (null = not generated yet). */
  livePanel?: SatelliteLivePanel | null;
  livePanelStale?: boolean;
  /** OPT-228: forward satellite paper book (null = not loaded / older backend). */
  paper?: SatellitePaper | null;
  /** The user's REAL satellite book from the journal (leg='satellite'). */
  userBook?: SatellitePaper | null;
  /** S-3 stock gate (from portfolio health) — drives the core-leg copy. */
  stockGateClosed?: boolean;
  /** When the ONE order list above already carries the buys (a25 view). */
  hideBuyRows?: boolean;
  /** Timeline row state: the frozen reference is loading / failed. */
  timelineUnavailable?: 'loading' | 'error' | null;
  timelineError?: string | null;
  onRetryTimeline?: () => void;
}) {
  const queryClient = useQueryClient();
  const { capital } = useAccountSettings();
  const [bought, setBought] = React.useState<Set<string>>(() => new Set());
  const [buying, setBuying] = React.useState<string | null>(null);
  const [buyErr, setBuyErr] = React.useState<string | null>(null);

  // The user's own open legs: the 今日下单 list only carries the STRATEGY's book,
  // so a name the user holds must be listed here or it is invisible all session.
  const myEntries = React.useMemo(
    () => (userBook?.openLegs ?? []).map((l) => l.entryDate).filter((d): d is string => !!d),
    [userBook],
  );
  const myDueQ = useSatelliteExitDueQuery(myEntries);
  const myNames = React.useMemo(() => {
    const m = new Map<string, string>();
    for (const w of loadWatchlist()) if (w.name) m.set(w.symbol, w.name);
    return m;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [userBook]);

  async function buyCandidate(e: PanelRankedEntry) {
    const price = typeof e.px1430 === 'number' && e.px1430 > 0 ? e.px1430 : null;
    const code = String(e.ts).split('.')[0];
    const symbol = `CN:${code}`;
    if (price == null || bought.has(symbol) || buying) return;
    setBuyErr(null);
    setBuying(symbol);
    const today = getShanghaiTodayIso();
    try {
      // Watchlist first (the position the user actually holds), then the journal.
      const next = upsertWatchlistOpenTrade(loadWatchlist(), {
        symbol,
        name: e.name ?? null,
        side: 'BUY',
        price,
        positionPct: perSlot * 100,
        entryDate: today,
      });
      await saveWatchlist(next);
      await recordUserTrade({
        symbol,
        side: 'BUY',
        price,
        positionPct: perSlot * 100,
        source: 'RESEARCH',
        market: 'CN',
        leg: 'satellite',
        strategyMode: strategy ?? 'starship_b',
        tradeDate: today,
      });
      setBought((prev) => new Set(prev).add(symbol));
      void invalidateUserTradesQueries(queryClient).catch(() => {});
    } catch (err) {
      setBuyErr(err instanceof Error ? err.message : String(err));
    } finally {
      setBuying(null);
    }
  }

  const userBookNode = userBook && userBook.decisionAvailable !== false ? (
          <div
            data-testid="satellite-user-book"
            className="mt-1.5 border-t border-sky-500/20 pt-1.5"
          >
            <div className="flex flex-wrap items-center gap-2">
              <span className="font-semibold text-sky-700 dark:text-sky-300">
                实盘账本（我的成交 · 自 {userBook.inception}）
              </span>
              <span
                className={cn(
                  'rounded px-1.5 py-0.5',
                  userBook.prereq.met
                    ? 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-300'
                    : 'bg-amber-500/15 text-amber-700 dark:text-amber-300',
                )}
              >
                前置 {userBook.prereq.closedCount}/{userBook.prereq.target}
                {userBook.prereq.met ? ' · 已达标' : ''}
              </span>
              <span className="tabular-nums text-[var(--k-muted)]">
                已平仓 {userBook.stats.closedCount} · 持仓 {userBook.stats.openCount}
              </span>
              {userBook.stats.winRate != null ? (
                <span className="tabular-nums">
                  胜率 {pct(userBook.stats.winRate)}（{userBook.stats.winCount}/
                  {userBook.stats.closedCount}）
                </span>
              ) : null}
              {userBook.stats.avgNetPnlPct != null ? (
                <span className={cn('tabular-nums', pnlClass(userBook.stats.avgNetPnlPct))}>
                  单笔均净 {signedPct(userBook.stats.avgNetPnlPct)}
                </span>
              ) : null}
            </div>
          {userBook.openLegs.length > 0 ? (
            <div data-testid="satellite-user-legs" className="mt-1 space-y-0.5">
              <div className="font-semibold">我的持仓（{userBook.openLegs.length}）：</div>
              {userBook.openLegs.map((l) => {
                const due = l.entryDate ? myDueQ.data?.exitDue?.[l.entryDate] : null;
                const code = l.ts.split(':')[1]?.split('.')[0] ?? l.ts;
                const worth =
                  capital != null && l.positionPct != null
                    ? ` ≈ ¥${Math.round((l.positionPct / 100) * capital).toLocaleString('zh-CN')}`
                    : '';
                return (
                  <div key={l.ts} className="tabular-nums">
                    <span className="font-mono font-semibold">{code}</span>
                    <span>{myNames.get(l.ts) ? ` ${myNames.get(l.ts)}` : ''}</span>
                    <span>{` · ${pct((l.positionPct ?? 0) / 100)}${worth}`}</span>
                    <span className="text-[var(--k-muted)]">
                      {` · 入 ${l.entryDate ?? '—'}${l.entryPrice != null ? ` @${l.entryPrice}` : ''}`}
                    </span>
                    {due ? (
                      <span>
                        {' → 到期 '}
                        <span className="font-semibold text-sky-700 dark:text-sky-300">
                          {due} 14:30
                        </span>
                        {' 卖出'}
                      </span>
                    ) : (
                      <span className="text-[var(--k-muted)]"> · 到期 —</span>
                    )}
                  </div>
                );
              })}
            </div>
          ) : null}
          </div>
        ) : null;

  if (!row) {
    return (
      <div
        data-testid="satellite-leg-block"
        className="rounded-md border border-[var(--k-border)] px-2.5 py-1.5 text-[10px] text-[var(--k-muted)]"
      >
        <div className="flex flex-wrap items-center gap-2">
          <span className="font-semibold">卫星腿：策略参照</span>
          <span>
            {timelineUnavailable === 'loading'
              ? '加载中…'
              : timelineUnavailable === 'error'
                ? `加载失败${timelineError ? `（${timelineError.slice(0, 80)}）` : ''}`
                : '最近交易日状态不可用'}
          </span>
          {timelineUnavailable === 'error' && onRetryTimeline ? (
            <Button
              type="button"
              size="sm"
              variant="outline"
              className="h-6 rounded-full px-3 text-[11px]"
              onClick={() => onRetryTimeline()}
            >
              重试
            </Button>
          ) : null}
        </div>
        {userBookNode}
      </div>
    );
  }
  const active = row.satActive === true;
  const pos = openPositions.length > 0 ? openPositions.length : (row.satPositions ?? 0);
  // `satSlots` counts slots in play today (open + closed), which exceeds the
  // capacity on recycle days — the label needs the capacity, not that count.
  const capacity = row.satCapacity ?? 4;
  const churn = typeof row.satSlots === 'number' ? Math.max(0, row.satSlots - pos) : 0;
  const live = livePanel && livePanel.decisionAvailable && !livePanelStale ? livePanel : null;
  const liveDay = live?.tradeDate ?? null;
  const liveGateOpen = live?.gateOpen === true;
  // Always-on gate tag. States are time-aware so "待判定" cannot linger after
  // the 14:30 decision slot, and the replay day's gate never reads as today's.
  const gateState: 'open' | 'closed' | 'pending' | 'missing' | 'holiday' = live
    ? liveGateOpen
      ? 'open'
      : 'closed'
    : !isWeekdayShanghai()
      ? 'holiday'
      : getShanghaiMinutes() < 14 * 60 + 40 // 14:40 = 14:30 job + build grace
        ? 'pending'
        : 'missing';
  const GATE_TAGS = {
    open: {
      label: '闸 开 · 今日 14:30',
      cls: 'border-emerald-500/40 bg-emerald-500/15 text-emerald-700 dark:text-emerald-300',
    },
    closed: {
      label: '闸 关 · 今日 14:30',
      cls: 'border-red-500/50 bg-red-500/15 text-red-700 dark:text-red-300',
    },
    pending: {
      label: '闸 待 14:30 判定',
      cls: 'border-amber-500/40 bg-amber-500/15 text-amber-700 dark:text-amber-300',
    },
    missing: {
      label: '闸 快照缺失',
      cls: 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]',
    },
    holiday: {
      label: '闸 今日休市',
      cls: 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]',
    },
  } as const;
  const gateTag = GATE_TAGS[gateState];
  const holdingLabel = active
    ? pos > 0
      ? `有仓 ${pos}/${capacity} 槽`
      : `今日有成交（现持 0/${capacity} 槽）`
    : '空仓';

  const structure = strategy ? RESEARCH_STRUCTURE[strategy] : null;
  const weight = Math.max(0, Math.min(1, satWeight ?? structure?.defaultWeight ?? 1));
  const perSlot = 0.25 * weight;
  // Exit anchoring: with a live snapshot, "due" is exitDue == today (the live
  // clock); otherwise fall back to the replay's daysLeft==1 convention.
  // OPT-223: the panel carries its own exits/heldLegs (computed by the 14:30
  // job's replay) — prefer them so the card never depends on the registry.
  type LegView = { ts: string; exitDue?: string | null; daysLeft?: number | null };
  // Panels from the OPT-223 build onward carry ``exits``; older files don't.
  // The frozen timeline is authoritative when present: the panel's replay
  // calendar ends at the panel day, so an in-flight leg's exitDue collapsed to
  // that day and every held leg read as an exit (2026-09-23 incident).
  const panelHasExitData =
    live != null && Array.isArray(live.exits) && openPositions.length === 0;
  const dueLegs: LegView[] = liveDay
    ? panelHasExitData
      ? (live?.exits ?? []).map((x) => ({ ts: x.ts, exitDue: x.exitDue ?? null }))
      : openPositions.filter((p) => p.exitDue === liveDay)
    : openPositions.filter((p) => p.daysLeft === 1);
  const overdueLegs: LegView[] =
    liveDay && !panelHasExitData
      ? openPositions.filter((p) => p.exitDue != null && p.exitDue < liveDay)
      : [];
  const heldLegs: LegView[] = liveDay
    ? panelHasExitData
      ? (live?.heldLegs ?? []).map((x) => ({
          ts: x.ts,
          exitDue: x.exitDue ?? null,
          daysLeft: x.daysLeft ?? null,
        }))
      : openPositions.filter((p) => p.exitDue != null && p.exitDue > liveDay)
    : openPositions.filter((p) => p.daysLeft !== 1);
  const freeSlots = Math.max(0, capacity - pos);
  const parkingKey = parkedHeld
    ? ((parkedHeld.key || parkingKeyForSymbol(parkedHeld.ts)) as keyof typeof PARKING_META | null)
    : null;
  const parkWeight = row.parkedWeight ?? parkedHeld?.weight ?? null;
  // "What would I buy today if the gate opened": the bucket rows that pass the
  // locked/C1/fillable guards, best-effort ordered by amp rank.
  const candidates = (live?.ranked ?? [])
    .filter((e) => e.inBucket && !e.skipReason && e.fillable)
    .slice(0, capacity);
  const blockedTop = (live?.ranked ?? [])
    .filter((e) => e.inBucket && Boolean(e.skipReason))
    .slice(0, 3);
  const parkedLabel = parkedHeld
    ? `${parkingKey ? PARKING_META[parkingKey].label : parkedHeld.name} ${parkedHeld.ts}`
    : '现金/逆回购';
  const coreLegLabel = strategy === 'starport' ? '母港腿（港湾×B3）' : '港湾腿';
  // 星舰 / 稳健星舰 park idle cash in the H2 sleeve themselves; the overlays
  // return the capital to their base leg, whose own idle parks in the sleeve.
  const isStarship =
    strategy === 'starship' || strategy === 'starship_robust' || strategy === 'starship_b';
  const isA25 = strategy === 'starship_robust';
  const isB = strategy === 'starship_b';
  const proceedsLine = isStarship
    ? `卖出资金停入 H2 停车腿（${parkedLabel}）`
    : `卖出资金回到${coreLegLabel}（该腿闲置现金停 H2 停车腿：${parkedLabel}）`;

  return (
    <div
      data-testid="satellite-leg-block"
      id="satellite-leg"
      className="rounded-md border border-violet-500/30 bg-violet-500/5 px-2.5 py-1.5 text-[10px]"
    >
      <div className="flex flex-wrap items-center gap-2">
        <span className="font-semibold">卫星腿</span>
        <span className="text-[10px] text-[var(--k-muted)]">策略</span>
        <span
          className={cn(
            'rounded px-1.5 py-0.5',
            active
              ? 'bg-violet-500/15 text-violet-700 dark:text-violet-300'
              : 'bg-[var(--k-surface-2)] text-[var(--k-muted)]',
          )}
        >
          {holdingLabel}
        </span>
        {userBook ? (
          <span
            data-testid="satellite-my-slots"
            className={cn(
              'rounded px-1.5 py-0.5',
              userBook.openLegs.length > 0
                ? 'bg-sky-500/15 text-sky-700 dark:text-sky-300'
                : 'bg-[var(--k-surface-2)] text-[var(--k-muted)]',
            )}
          >
            我的 {userBook.openLegs.length}/{capacity} 槽
          </span>
        ) : null}
        <span
          data-testid="satellite-gate-tag"
          className={cn(
            'rounded-full border px-1.5 py-0.5 text-[10px] font-semibold',
            gateTag.cls,
          )}
        >
          {gateTag.label}
        </span>
        <span className="text-[var(--k-muted)]">
          {live ? `今日 ${liveDay} 现场` : `回放 ${row.date}（非今日）`}
          {typeof row.filledToday === 'number' && row.filledToday > 0
            ? ` · 当日成交 ${row.filledToday}`
            : ''}
          {churn > 0 ? ` · 今日换 ${churn}` : ''}
        </span>
      </div>
      {userBookNode}
      {live ? (
        <div
          data-testid="satellite-live-panel"
          className={cn(
            'mt-1.5 rounded border px-2 py-1.5',
            liveGateOpen
              ? 'border-emerald-500/40 bg-emerald-500/10'
              : 'border-red-500/50 bg-red-500/10',
          )}
        >
          <div className="flex flex-wrap items-center gap-2">
            <span
              className={cn(
                'rounded border px-2 py-0.5 text-[11px] font-bold',
                liveGateOpen
                  ? 'border-emerald-500/40 bg-emerald-500/15 text-emerald-700 dark:text-emerald-300'
                  : 'border-red-500/50 bg-red-500/15 text-red-700 dark:text-red-300',
              )}
            >
              {liveGateOpen ? '闸 开 · 今日 14:30 可补仓' : '闸 关 · 今日只卖不买'}
            </span>
            <span className="tabular-nums">
              14:30 广度 {live.breadth1430 != null ? pct(live.breadth1430) : '—'}（阈值 50%）
            </span>
            <span className="tabular-nums text-[var(--k-muted)]">
              跳空 {live.gapCount ?? '—'} · 桶 {live.bucketSize ?? '—'} · 池 {live.poolSize ?? '—'}
            </span>
            {live.generatedAt ? (
              <span className="text-[var(--k-muted)]">快照 {live.generatedAt.slice(11, 16)}</span>
            ) : null}
          </div>
          <div className="mt-1">
            <span
              className={cn(
                'font-semibold',
                liveGateOpen ? 'text-emerald-700 dark:text-emerald-300' : 'text-red-700 dark:text-red-300',
              )}
            >
              {liveGateOpen
                ? `今日买入（${candidates.length || 0} 只 · 每槽 ≈${pct(perSlot)}）：`
                : `若开闸，今日本会买（${candidates.length || 0} 只 · 不执行）：`}
            </span>
            {hideBuyRows ? (
              <span className="ml-1 text-[var(--k-muted)]">
                → 买入见上方「今日下单」清单（同一操作区）
              </span>
            ) : candidates.length > 0 ? (
              <div className="mt-1.5 space-y-1.5" data-testid="satellite-buy-list">
                {capital == null ? (
                  <div className="rounded-xl border border-dashed border-[var(--k-border)]/70 bg-[var(--k-surface)]/40 px-3 py-1.5 text-[10px] text-[var(--k-muted)] backdrop-blur-sm">
                    未设总资金 → 在上方「账户」里填，即可按每槽 25% 算好每只买多少股
                  </div>
                ) : null}
                {candidates.map((e) => {
                  const code = String(e.ts).split('.')[0];
                  const symbol = `CN:${code}`;
                  const price = typeof e.px1430 === 'number' && e.px1430 > 0 ? e.px1430 : null;
                  const sizing =
                    price != null && capital != null
                      ? slotLotShares({ symbol, price, capital, slotPct: perSlot * 100 })
                      : null;
                  const isBought = bought.has(symbol);
                  return (
                    <div
                      key={e.ts}
                      data-testid="satellite-buy-row"
                      className={cn(
                        'flex flex-wrap items-center gap-x-3 gap-y-0.5 rounded-xl px-3 py-2 text-xs',
                        'border border-[var(--k-border)]/60 bg-[var(--k-surface)]/45 backdrop-blur-sm',
                        'transition-colors hover:border-[var(--k-border)] hover:bg-[var(--k-surface)]/70',
                        isBought && 'opacity-60',
                      )}
                    >
                      <a
                        href={xueqiuUrl(String(e.ts))}
                        target="_blank"
                        rel="noopener noreferrer"
                        title="在雪球查看"
                        className="flex min-w-0 items-baseline gap-1.5 hover:underline"
                      >
                        <span className="font-mono font-semibold">{code}</span>
                        {e.name ? (
                          <span className="max-w-24 truncate text-[var(--k-muted)]">{e.name}</span>
                        ) : null}
                      </a>
                      <span className="tabular-nums text-[var(--k-muted)]">
                        {`+${e.gapPct ?? '—'}% · amp ${e.amp1430Pct ?? '—'}% · ¥${price ?? '—'}`}
                      </span>
                      <span className="tabular-nums">
                        {sizing && sizing.shares > 0
                          ? `买 ${sizing.shares.toLocaleString('zh-CN')} 股 ≈ ¥${Math.round(
                              sizing.shares * (price ?? 0),
                            ).toLocaleString('zh-CN')}`
                          : capital == null
                            ? '待设总资金'
                            : '资金不足 1 手'}
                      </span>
                      {liveGateOpen ? (
                        <Button
                          size="sm"
                          variant={isBought ? 'ghost' : 'outline'}
                          className={cn(
                            'ml-auto h-6 rounded-full px-3 text-[11px]',
                            !isBought && 'border-emerald-600/40 text-emerald-700 hover:bg-emerald-500/10 dark:text-emerald-300',
                          )}
                          disabled={
                            isBought || buying != null || capital == null || !sizing || sizing.shares <= 0
                          }
                          onClick={() => void buyCandidate(e)}
                        >
                          {isBought ? '已记录' : buying === symbol ? '记录中…' : '买入'}
                        </Button>
                      ) : null}
                    </div>
                  );
                })}
                {buyErr ? (
                  <div className="text-red-600 dark:text-red-400">记录失败：{buyErr}</div>
                ) : null}
              </div>
            ) : (
              <span className="ml-1 text-[var(--k-muted)]">无（今日无合规 S-gap 候选）</span>
            )}
            {blockedTop.length > 0 ? (
              <div className="mt-1 text-[var(--k-muted)]">
                跳过：{blockedTop.map((e) => `${e.ts.split('.')[0]}(${e.skipReason})`).join('、')}
              </div>
            ) : null}
          </div>
        </div>
      ) : null}
      {structure ? (
        <details
          data-testid="satellite-strategy-ref"
          className="mt-1.5 border-t border-violet-500/20 pt-1.5 text-[var(--k-muted)]"
        >
          <summary className="cursor-pointer">
            <span className="font-semibold text-violet-700 dark:text-violet-300">
              策略参照（引擎 · 研究档）
            </span>
            <span className="ml-2 tabular-nums">
              卫星 {pct(weight)} · 每槽 ≈{pct(perSlot)}
              {dueLegs.length > 0 ? ` · 到期卖出 ${dueLegs.length} 只` : ' · 无到期卖出'}
              {overdueLegs.length > 0 ? ` · 逾期 ${overdueLegs.length} 只` : ''}
              {heldLegs.length > 0 ? ` · 继续持有 ${heldLegs.length} 只` : ''}
              {parkWeight != null ? ` · 闲钱 ${pct(parkWeight)}` : ''}
            </span>
          </summary>
          <div className="mt-1 flex flex-wrap items-center gap-2">
            <span className="rounded bg-[var(--k-surface-2)] px-1.5 py-0.5">
              {structure.title}：{structure.parts}
            </span>
          </div>
          <ol className="mt-1 list-decimal space-y-0.5 pl-4 text-[var(--k-fg)]">
            {dueLegs.length > 0 ? (
              <li>
                <span className="font-semibold text-red-600 dark:text-red-400">
                  {liveDay ? `今日 ${liveDay} 14:30 到期卖出：` : '14:30 到期卖出（余 1 日）：'}
                </span>
                {dueLegs.map((p) => p.ts).join('、')}
                <span className="text-[var(--k-muted)]">
                  {liveDay ? '（如未执行，尽快补卖）' : `（按 ${row.date} 数据；下一交易日 14:30）`}
                </span>
                <span className="ml-1 font-semibold">→ {proceedsLine}</span>
              </li>
            ) : (
              <li className="text-[var(--k-muted)]">
                今日无到期卖出
                {openPositions.length > 0
                  ? `（继续持有 ${openPositions.map((p) => p.ts).join('、')}）`
                  : '（空仓）'}
              </li>
            )}
            {overdueLegs.length > 0 ? (
              <li>
                <span className="font-semibold text-red-600 dark:text-red-400">
                  已过到期日未卖（尽快补）：
                </span>
                {overdueLegs.map((p) => `${p.ts}(${p.exitDue})`).join('、')}
              </li>
            ) : null}
            {heldLegs.length > 0 ? (
              <li className="text-[var(--k-muted)]">
                继续持有（未到期）：
                {heldLegs
                  .map((p) => `${p.ts}(${p.exitDue ? `到期 ${p.exitDue}` : `余${p.daysLeft ?? '—'}日`})`)
                  .join('、')}
              </li>
            ) : null}
            {live ? null : (
              <li>
                补仓：
                {gateState === 'missing' ? (
                  <>
                    <span className="font-semibold text-amber-600 dark:text-amber-400">
                      今日 14:30 快照缺失
                    </span>
                    （检查后端 14:30 任务）→ 未见快照前按「只卖不买」处理，空槽留现金
                    {isStarship ? '/停车' : ''}
                  </>
                ) : gateState === 'holiday' ? (
                  '今日休市，无补仓动作'
                ) : (
                  <>
                    <span className="font-semibold">待今日 14:30 现场面板</span>（工作日 14:30
                    自动抓取；开则按跳空 &gt;3% · 振幅升序 · 排除 T+1 涨停/接近涨停 的名单补满
                    {freeSlots > 0 ? ` ${freeSlots} 个空槽` : '空槽'}，每槽 ≈{pct(perSlot)}；关则只卖不买，
                    空槽留现金{isStarship ? '/停车' : ''}）
                  </>
                )}
              </li>
            )}
            {isStarship ? (
              <li>
                {isB ? (
                  <>
                     闲钱停车（星舰 B = 闲置现金 100% 停 国债+黄金+纳指 三腿逆波动率；月初再平衡）：
                    {parkedHeld ? (
                      <>
                        <span className="font-semibold">停放腿 {parkedLabel}</span>
                        {parkWeight != null ? ` · 闲钱占比 ${pct(parkWeight)}` : ''}
                        {parkedHeld.price != null ? ` · 现价 ${parkedHeld.price}` : ''}
                      </>
                    ) : (
                      '现金/逆回购（三腿均未就绪）'
                    )}
                    <span className="text-[var(--k-muted)]">
                      {' '}
                      · 国债/黄金/纳指按 60d 逆波动率分配（月初再平衡，5bp/边）
                    </span>
                  </>
                ) : isA25 ? (
                  <>
                     闲钱停车（现行 canonical H2-a25 = 闲置现金 25% 停 H2 ETF / 75% 停 B3 风险预算；K3 风险）：

                    {parkedHeld ? (
                      <>
                        <span className="font-semibold">ETF 腿持有 {parkedLabel}</span>
                        {parkWeight != null ? ` · 闲钱占比 ${pct(parkWeight)}` : ''}
                        {parkedHeld.price != null ? ` · 现价 ${parkedHeld.price}` : ''}
                      </>
                    ) : (
                      'ETF 腿 现金/逆回购（无 ETF 站上 200 日线）'
                    )}
                    <span className="text-[var(--k-muted)]">
                      {' '}
                      · 另 75% 停 B3（5 资产逆波动率，月初再平衡）；卫星卖出款/空槽现金按此拆分
                    </span>
                  </>
                ) : (
                  <>
                    闲钱停车（H2 迟滞换仓，2pt 领先才换）：
                    {parkedHeld ? (
                      <>
                        <span className="font-semibold">持有 {parkedLabel}</span>
                        {parkWeight != null ? ` · 停车权重 ${pct(parkWeight)}` : ''}
                        {parkedHeld.price != null ? ` · 现价 ${parkedHeld.price}` : ''}
                        <span className="text-[var(--k-muted)]">
                          {' '}
                          → 卫星卖出款/空槽现金停这里；换仓与 trail 由 H2 收盘口径决定（本卡不预测）
                        </span>
                      </>
                    ) : (
                      '现金/逆回购（无 ETF 站上 200 日线）'
                    )}
                  </>
                )}
              </li>
            ) : (
              <li>
                {coreLegLabel}：核心腿
                {stockGateClosed ? (
                  <span className="font-semibold">今日闸门关闭 → 不新开股票</span>
                ) : (
                  '按上方「操作引导」执行'
                )}
                ；停车腿 <span className="font-semibold">HOLD {parkedLabel}</span>（H2 迟滞换仓）
              </li>
            )}
          </ol>
          <div className="mt-1 text-[var(--k-muted)]">
            跟法：{structure.follow}。研究档测试通过 ≠ Live（星舰前置 = paper 3/20 +
            风险授权）。
            {live
              ? ' 14:30 现场面板为同一 satellite_signals_for_day 代码（mv 用昨收，误差极小）。'
              : gateState === 'missing'
                ? ' 今日 14:30 快照缺失（检查后端 14:30 任务）。'
                : gateState === 'holiday'
                  ? ' 今日休市。'
                  : ' 今日 14:30 现场面板未生成（工作日 14:30 自动抓取）。'}
          </div>
        </details>
      ) : (
        <div className="mt-1 text-[var(--k-muted)]">
          回测口径（研究 replay）。14:30 现场面板工作日 14:30 自动抓取；执行审计已过（90bps
          +310%/SR 2.45、容量 ≤5M）。
        </div>
      )}
      {paper && paper.decisionAvailable !== false ? (
        <details
          data-testid="satellite-paper-book"
          className="mt-1.5 border-t border-violet-500/20 pt-1.5"
        >
          <summary className="cursor-pointer flex flex-wrap items-center gap-2">
            <span className="font-semibold text-violet-700 dark:text-violet-300">
              paper 账本（引擎 forward · 自 {paper.inception}）
            </span>
            <span
              className={cn(
                'rounded px-1.5 py-0.5',
                paper.prereq.met
                  ? 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-300'
                  : 'bg-amber-500/15 text-amber-700 dark:text-amber-300',
              )}
            >
              前置 {paper.prereq.closedCount}/{paper.prereq.target}
              {paper.prereq.met ? ' · 已达标' : ''}
            </span>
            {paper.stats.paperPct != null ? (
              <span className={cn('tabular-nums', pnlClass(paper.stats.paperPct))}>
                paper 收益 {signedPct(paper.stats.paperPct)}
              </span>
            ) : null}
            {paper.stats.paperMaxDdPct != null ? (
              <span className="tabular-nums text-[var(--k-muted)]">
                回撤 −{Math.abs(paper.stats.paperMaxDdPct).toFixed(1)}%
              </span>
            ) : null}
            {paper.stats.winRate != null ? (
              <span className="tabular-nums">
                胜率 {pct(paper.stats.winRate)}（{paper.stats.winCount}/{paper.stats.closedCount}）
              </span>
            ) : null}
            {paper.stats.avgNetPnlPct != null ? (
              <span className={cn('tabular-nums', pnlClass(paper.stats.avgNetPnlPct))}>
                单笔均净 {signedPct(paper.stats.avgNetPnlPct)}
              </span>
            ) : null}
          </summary>
          {paper.openLegs.length > 0 ? (
            <div className="mt-1">
              <span className="font-semibold">
                当前持仓（paper {paper.openLegs.length}/{capacity} 槽）：
              </span>
              {paper.openLegs.map((p) => (
                <span key={p.ts} className="mr-2 inline-block">
                  <span className="font-mono">{p.ts.split('.')[0]}</span>
                  <span className="text-[var(--k-muted)]">
                    {` (入 ${p.entryDate ?? '—'} → 到期 ${p.exitDue ?? '—'} · 余 ${p.daysLeft ?? '—'}日 · `}
                  </span>
                  <span className={cn('tabular-nums', pnlClass(p.pnlPct))}>
                    {signedPct(p.pnlPct)}
                  </span>
                  <span className="text-[var(--k-muted)]">)</span>
                </span>
              ))}
            </div>
          ) : (
            <div className="mt-1 text-[var(--k-muted)]">当前无持仓（paper 空仓）</div>
          )}
          {paper.closed.length > 0 ? (
            <div className="mt-1">
              <span className="font-semibold">近期平仓：</span>
              {paper.closed.slice(0, 5).map((t, i) => (
                <span key={`${t.ts}-${t.exitDate}-${i}`} className="mr-2 inline-block">
                  <span className="font-mono">{t.ts.split('.')[0]}</span>
                  <span className="text-[var(--k-muted)]">{` ${t.entryDate}→${t.exitDate} `}</span>
                  <span className={cn('tabular-nums', pnlClass(t.netPnlPct))}>
                    {signedPct(t.netPnlPct)}
                  </span>
                  <span className="text-[var(--k-muted)]">{`(${t.closeReason})`}</span>
                </span>
              ))}
            </div>
          ) : null}
        </details>
      ) : null}
    </div>
  );
}
