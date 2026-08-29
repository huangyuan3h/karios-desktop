'use client';

/**
 * 单轨 vs 我的账本 · 归因对照（本质差异）
 * Replaces shallow "today pick vs holdings" nagging with leg-level capture analysis.
 */

import * as React from 'react';

import { useQuery } from '@tanstack/react-query';
import { GitCompareArrows } from 'lucide-react';

import { cn } from '@/lib/utils';
import { useReturnAttributionQuery } from '@/lib/queries/backtest';
import { fetchPortfolioHealth } from '@/lib/queries/portfolioHealth';
import {
  buildAttributionDiff,
  type LegDiffRow,
  type OpenHolding,
} from '@/lib/attribution-diff';

const PICK_LABEL: Record<string, string> = {
  STOCK: '股票篮',
  GOLD: '黄金',
  OIL: '原油',
  NASDAQ: '纳指',
  BOND10: '国债',
  REPO: '逆回购',
};

const KIND_LABEL: Record<LegDiffRow['kind'], string> = {
  under_capture: '欠捕获',
  over_weight: '超配',
  aligned: '结构近',
  track_drag: '共担拖累',
  idle_leg: '—',
};

function tone(v: number | null | undefined): string {
  if (v == null) return 'text-[var(--k-muted)]';
  return v >= 0 ? 'text-emerald-700 dark:text-emerald-300' : 'text-red-700 dark:text-red-400';
}

function kindCls(k: LegDiffRow['kind']): string {
  if (k === 'under_capture') return 'text-amber-800 dark:text-amber-200';
  if (k === 'over_weight') return 'text-red-700 dark:text-red-300';
  if (k === 'track_drag') return 'text-red-700/80';
  if (k === 'aligned') return 'text-emerald-700 dark:text-emerald-300';
  return 'text-[var(--k-muted)]';
}

export function ReplicaGapCard({
  start,
  end,
  onRangeChange,
}: {
  start: string;
  end: string;
  onRangeChange?: (start: string, end: string) => void;
}) {
  const healthQ = useQuery({
    queryKey: ['portfolio-health', 'attr-diff'],
    queryFn: () => fetchPortfolioHealth(),
    staleTime: 60_000,
  });
  const attrQ = useReturnAttributionQuery(start, end, true);
  const ps = attrQ.data?.pickStrong;
  const ut = attrQ.data?.userTrades;

  const holdings: OpenHolding[] = React.useMemo(() => {
    const cn = healthQ.data?.holdings ?? [];
    const hk = healthQ.data?.hkHealth?.holdings ?? [];
    const multi = healthQ.data?.multiAssetHoldings ?? [];
    return [
      ...cn.map((h) => ({ symbol: h.symbol, positionPct: h.positionPct, pnlPct: h.pnlPct })),
      ...hk.map((h) => ({ symbol: h.symbol, positionPct: h.positionPct, pnlPct: h.pnlPct })),
      ...multi.map((h) => ({
        symbol: h.symbol,
        positionPct: h.positionPct,
        pnlPct: (h as { pnlPct?: number }).pnlPct,
      })),
    ].filter((h) => (Number(h.positionPct) || 0) > 0);
  }, [healthQ.data]);

  const report = React.useMemo(
    () =>
      buildAttributionDiff({
        byPick: ps?.byPick,
        userByBucket: ut?.byBucket,
        holdings,
      }),
    [ps?.byPick, ut?.byBucket, holdings],
  );

  const livePick = healthQ.data?.multiAssetSleeve?.pick?.key ?? '—';
  const insights = report.insights.filter((i) => i.id !== 'method');
  const method = report.insights.find((i) => i.id === 'method');

  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
      <div className="mb-2 flex flex-wrap items-center gap-2 text-[12px] font-medium">
        <GitCompareArrows className="size-3.5" />
        单轨 vs 我的账本 · 归因对照
        <span className="rounded bg-emerald-500/10 px-1.5 py-0.5 text-[10px] font-normal text-emerald-800 dark:text-emerald-200">
          本质差异
        </span>
        <span className="ml-auto flex items-center gap-2 text-[10px] font-normal text-[var(--k-muted)]">
          <input
            type="date"
            className="rounded border border-[var(--k-border)] bg-transparent px-1 py-0.5"
            value={start}
            onChange={(e) => onRangeChange?.(e.target.value, end)}
          />
          <span>~</span>
          <input
            type="date"
            className="rounded border border-[var(--k-border)] bg-transparent px-1 py-0.5"
            value={end}
            onChange={(e) => onRangeChange?.(start, e.target.value)}
          />
        </span>
      </div>

      <p className="mb-2 text-[10px] text-[var(--k-muted)]">
        不问「今天 OIL 够不够 100%」，问：<strong>单轨钱从哪条腿来，你的仓位/成交接到了没有</strong>。
        退出机制可以改善单票结局，补不齐「主发动机腿长期欠配 / 次要腿超配」。
      </p>

      {attrQ.isError ? (
        <p className="text-xs text-red-700">{String(attrQ.error)}</p>
      ) : attrQ.isFetching && !ps ? (
        <p className="text-xs text-[var(--k-muted)]">归因计算中…（与涨跌归因同源）</p>
      ) : !ps ? (
        <p className="text-xs text-[var(--k-muted)]">暂无单轨归因</p>
      ) : (
        <div className="flex flex-col gap-3">
          <div className="grid gap-2 md:grid-cols-3">
            <div className="rounded border border-emerald-500/30 bg-emerald-500/5 px-2.5 py-2">
              <div className="text-[10px] text-[var(--k-muted)]">单轨区间几何</div>
              <div className={cn('text-[15px] font-semibold tabular-nums', tone(ps.totalGeoPct))}>
                {ps.totalGeoPct.toFixed(1)}%
              </div>
              <div className="text-[10px] text-[var(--k-muted)]">
                发动机腿 {report.trackEngine ? PICK_LABEL[report.trackEngine] : '—'}
              </div>
            </div>
            <div className="rounded border border-[var(--k-border)] px-2.5 py-2">
              <div className="text-[10px] text-[var(--k-muted)]">你最重仓腿（现仓）</div>
              <div className="text-[15px] font-semibold">
                {report.userTopWeight ? PICK_LABEL[report.userTopWeight] : '—'}
              </div>
              <div className="text-[10px] text-[var(--k-muted)]">
                已实现平仓 {ut?.closedCount ?? 0} 笔
                {ut?.insufficient ? ' · 样本少' : ''}
              </div>
            </div>
            <div className="rounded border border-[var(--k-border)] px-2.5 py-2">
              <div className="text-[10px] text-[var(--k-muted)]">结构错位？</div>
              <div className="text-[13px] font-semibold">
                {report.trackEngine &&
                report.userTopWeight &&
                report.trackEngine !== report.userTopWeight
                  ? `${PICK_LABEL[report.trackEngine]} ≠ ${PICK_LABEL[report.userTopWeight]}`
                  : '主腿一致或接近'}
              </div>
              <div className="text-[10px] text-[var(--k-muted)]">今日 pick 脚注：{livePick}</div>
            </div>
          </div>

          {insights.length > 0 && (
            <ul className="flex flex-col gap-1.5">
              {insights.slice(0, 4).map((ins) => (
                <li
                  key={ins.id}
                  className="rounded border border-amber-500/25 bg-amber-500/5 px-2.5 py-1.5 text-[11px]"
                >
                  <div className="font-medium text-amber-950 dark:text-amber-100">{ins.title}</div>
                  <div className="mt-0.5 text-[var(--k-muted)]">{ins.detail}</div>
                </li>
              ))}
            </ul>
          )}

          <div className="overflow-auto rounded border border-[var(--k-border)]">
            <table className="w-full text-left text-[11px] tabular-nums">
              <thead className="sticky top-0 bg-[var(--k-surface)] text-[10px] text-[var(--k-muted)]">
                <tr>
                  <th className="py-1 pl-2 pr-2">腿</th>
                  <th className="py-1 pr-2">单轨天数</th>
                  <th className="py-1 pr-2">单轨加法%</th>
                  <th className="py-1 pr-2">归因份额%</th>
                  <th className="py-1 pr-2">你现仓%</th>
                  <th className="py-1 pr-2">浮盈点</th>
                  <th className="py-1 pr-2">已实现Σ%</th>
                  <th className="py-1 pr-2">判定</th>
                </tr>
              </thead>
              <tbody>
                {report.rows
                  .filter((r) => r.pick !== 'REPO' && (r.trackDays > 0 || r.openWeightPct > 0 || r.realizedCount > 0))
                  .map((r) => (
                    <tr key={r.pick} className="border-t border-[var(--k-border)]/50">
                      <td className="py-1 pl-2 pr-2 font-medium">{PICK_LABEL[r.pick]}</td>
                      <td className="py-1 pr-2">{r.trackDays || '—'}</td>
                      <td className={cn('py-1 pr-2 font-medium', tone(r.trackAddPct))}>
                        {r.trackAddPct >= 0 ? '+' : ''}
                        {r.trackAddPct.toFixed(1)}
                      </td>
                      <td className="py-1 pr-2">{r.trackSharePct.toFixed(0)}</td>
                      <td className="py-1 pr-2">{r.openWeightPct.toFixed(0)}</td>
                      <td className={cn('py-1 pr-2', tone(r.openPnlPoints))}>
                        {r.openPnlPoints >= 0 ? '+' : ''}
                        {r.openPnlPoints.toFixed(1)}
                      </td>
                      <td className={cn('py-1 pr-2', tone(r.realizedSumPct))}>
                        {r.realizedCount
                          ? `${r.realizedSumPct >= 0 ? '+' : ''}${r.realizedSumPct.toFixed(1)}×${r.realizedCount}`
                          : '—'}
                      </td>
                      <td className={cn('py-1 pr-2', kindCls(r.kind))}>{KIND_LABEL[r.kind]}</td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>

          <p className="text-[10px] text-[var(--k-muted)]">
            {method?.detail} 下方「涨跌归因」是单轨单侧拆解；本卡把同一套 byPick 与你的仓位/成交对齐。
            今日 pick={livePick} 只作脚注——战术换仓看体检卡。
          </p>
        </div>
      )}
    </div>
  );
}
