'use client';

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
    parts: '母港 2/3 + 卫星 1/3',
    defaultWeight: 1 / 3,
    follow: '卫星 1/3 = 母港（港湾×B3）之外挪出 1/3，14:30 分 4 笔买入；到期日卖出换下一批',
  },
  starship: {
    title: '星舰 v2',
    parts: '卫星 100% + 闲钱停车',
    defaultWeight: 1,
    follow: '全部资金按卫星 4 槽 ×25%；没出手的闲钱停趋势最好的 ETF（与 Live 停车同源）',
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
 * Satellite leg state + research operation hints (星港/星舰/双子星 views).
 *
 * Live stays 港湾 — these are display-only instructions for the paper/research
 * books. The satellite book (legs, capacity, weight) comes from the timeline
 * payload, never re-derived here.
 */
export function SatelliteLegBlock({
  row,
  strategy,
  satWeight,
  openPositions = [],
  parkedHeld = null,
}: {
  row?: TimelineRow | null;
  strategy?: ResearchStrategy;
  satWeight?: number | null;
  openPositions?: TimelineOpenPosition[];
  parkedHeld?: TimelineParkedHeld | null;
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
  const gateLabel = row.gateOpen === true ? '开' : row.gateOpen === false ? '关' : '—';
  const holdingLabel = active
    ? pos > 0
      ? `有仓 ${pos}/${capacity} 槽`
      : `今日有成交（现持 0/${capacity} 槽）`
    : '空仓';

  const structure = strategy ? RESEARCH_STRUCTURE[strategy] : null;
  const weight = Math.max(0, Math.min(1, satWeight ?? structure?.defaultWeight ?? 1));
  const perSlot = 0.25 * weight;
  const dueLegs = openPositions.filter((p) => p.daysLeft === 1);
  const heldLegs = openPositions.filter((p) => p.daysLeft !== 1);
  const freeSlots = Math.max(0, capacity - pos);
  const parkingKey = parkedHeld
    ? ((parkedHeld.key || parkingKeyForSymbol(parkedHeld.ts)) as keyof typeof PARKING_META | null)
    : null;
  const parkWeight = row.parkedWeight ?? parkedHeld?.weight ?? null;

  return (
    <div
      data-testid="satellite-leg-block"
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
        <span className="text-[var(--k-muted)]">
          最近交易日 {row.date} · 闸 {gateLabel}
          {typeof row.filledToday === 'number' && row.filledToday > 0
            ? ` · 当日成交 ${row.filledToday}`
            : ''}
          {churn > 0 ? ` · 今日换 ${churn}` : ''}
        </span>
      </div>
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
                  14:30 到期卖出（余 1 日）：
                </span>
                {dueLegs.map((p) => p.ts).join('、')}
                <span className="text-[var(--k-muted)]">
                  （按 {row.date} 数据；即下一个交易日 14:30）
                </span>
              </li>
            ) : null}
            {heldLegs.length > 0 ? (
              <li className="text-[var(--k-muted)]">
                继续持有（未到期）：{heldLegs.map((p) => `${p.ts}(余${p.daysLeft ?? '—'}日)`).join('、')}
              </li>
            ) : null}
            <li>
              补仓：闸 {gateLabel === '开' ? '开' : `以 ${row.date} 为 ${gateLabel}`} → 今日 14:30
              以当日广度现场判定；开则按名单（跳空 &gt;3% · 振幅升序 · 排除 T+1
              涨停/接近涨停）补满{freeSlots > 0 ? ` ${freeSlots} 个空槽` : '空槽'}，每槽 ≈
              {pct(perSlot)}；关则只卖不买（空槽留现金{strategy === 'starship' ? '/停车' : ''}）
            </li>
            {strategy === 'starship' ? (
              <li>
                闲钱停车（与 Live 停车同源）：
                {parkedHeld ? (
                  <>
                    持有 {parkingKey ? PARKING_META[parkingKey].label : parkedHeld.name}{' '}
                    <span className="font-mono">{parkedHeld.ts}</span>
                    {parkWeight != null ? ` · 停车权重 ${pct(parkWeight)}` : ''}
                    {parkedHeld.price != null ? ` · 现价 ${parkedHeld.price}` : ''}
                  </>
                ) : (
                  '现金/逆回购（无 ETF 站上 200 日线）'
                )}
              </li>
            ) : (
              <li className="text-[var(--k-muted)]">
                核心/停车腿：按上方「操作引导」执行，资金按 {pct(1 - weight)} 折算（港湾
                {strategy === 'starport' ? ' × B3' : ''}）
              </li>
            )}
          </ol>
          <div className="mt-1 text-[var(--k-muted)]">
            跟法：{structure.follow}。研究档测试通过 ≠ Live（星舰前置 = paper 3/20 +
            风险授权）；14:30 信号组件待 OPT-178/186 重接，当前为回测 replay。
          </div>
        </div>
      ) : (
        <div className="mt-1 text-[var(--k-muted)]">
          回测口径（研究 replay）。14:30 信号/成交记录组件待 OPT-178 重接 + OPT-186；执行审计已过（90bps
          +310%/SR 2.45、容量 ≤5M）。
        </div>
      )}
    </div>
  );
}
