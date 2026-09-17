'use client';

import type { SatelliteLivePanel } from '@karios/shared';

import { getShanghaiMinutes, isWeekdayShanghai } from '@/lib/market-hours';
import { PARKING_META, parkingKeyForSymbol } from '@/lib/parking-universe';
import type {
  TimelineOpenPosition,
  TimelineParkedHeld,
  TimelineRow,
} from '@/lib/queries/backtest';
import { cn } from '@/lib/utils';

export type ResearchStrategy = 'starport' | 'starship' | 'twin_star';

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
  stockGateClosed = false,
}: {
  row?: TimelineRow | null;
  strategy?: ResearchStrategy;
  satWeight?: number | null;
  openPositions?: TimelineOpenPosition[];
  parkedHeld?: TimelineParkedHeld | null;
  /** OPT-222: today's 14:30 live snapshot (null = not generated yet). */
  livePanel?: SatelliteLivePanel | null;
  livePanelStale?: boolean;
  /** S-3 stock gate (from portfolio health) — drives the core-leg copy. */
  stockGateClosed?: boolean;
}) {
  if (!row) {
    return (
      <div
        data-testid="satellite-leg-block"
        className="rounded-md border border-[var(--k-border)] px-2.5 py-1.5 text-[10px] text-[var(--k-muted)]"
      >
        卫星腿：最近交易日状态不可用
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
  const panelHasExitData = live != null && Array.isArray(live.exits);
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
  // Where the satellite sale proceeds go (the clarity ask): the starship's
  // idle parks straight into the H2 sleeve; the overlays return the capital
  // to their base leg, whose own idle parks in the same sleeve.
  const proceedsLine =
    strategy === 'starship'
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
            {candidates.length > 0 ? (
              <span className="ml-1">
                {candidates.map((e) => (
                  <span key={e.ts} className="mr-2 inline-block">
                    <span className="font-mono">{e.ts.split('.')[0]}</span>
                    <span className="text-[var(--k-muted)]">
                      {` (+${e.gapPct ?? '—'}% · amp ${e.amp1430Pct ?? '—'}% · ¥${e.px1430 ?? '—'})`}
                    </span>
                  </span>
                ))}
              </span>
            ) : (
              <span className="ml-1 text-[var(--k-muted)]">无（今日无合规 S-gap 候选）</span>
            )}
            {blockedTop.length > 0 ? (
              <span className="ml-1 text-[var(--k-muted)]">
                ；跳过：{blockedTop.map((e) => `${e.ts.split('.')[0]}(${e.skipReason})`).join('、')}
              </span>
            ) : null}
          </div>
        </div>
      ) : null}
      {structure ? (
        <div className="mt-1.5 border-t border-violet-500/20 pt-1.5">
          <div className="flex flex-wrap items-center gap-2">
            <span className="font-semibold text-violet-700 dark:text-violet-300">
              操作提示（研究档 · 不进 Live）
            </span>
            <span className="rounded bg-[var(--k-surface-2)] px-1.5 py-0.5">
              {structure.title}：{structure.parts}
            </span>
            <span className="tabular-nums text-[var(--k-muted)]">
              卫星 {pct(weight)} · 每槽 ≈{pct(perSlot)}
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
                    {strategy === 'starship' ? '/停车' : ''}
                  </>
                ) : gateState === 'holiday' ? (
                  '今日休市，无补仓动作'
                ) : (
                  <>
                    <span className="font-semibold">待今日 14:30 现场面板</span>（工作日 14:30
                    自动抓取；开则按跳空 &gt;3% · 振幅升序 · 排除 T+1 涨停/接近涨停 的名单补满
                    {freeSlots > 0 ? ` ${freeSlots} 个空槽` : '空槽'}，每槽 ≈{pct(perSlot)}；关则只卖不买，
                    空槽留现金{strategy === 'starship' ? '/停车' : ''}）
                  </>
                )}
              </li>
            )}
            {strategy === 'starship' ? (
              <li>
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
        </div>
      ) : (
        <div className="mt-1 text-[var(--k-muted)]">
          回测口径（研究 replay）。14:30 现场面板工作日 14:30 自动抓取；执行审计已过（90bps
          +310%/SR 2.45、容量 ≤5M）。
        </div>
      )}
    </div>
  );
}
