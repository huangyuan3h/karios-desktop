'use client';

import * as React from 'react';

import { useFunnelHistoryQuery } from '@/lib/queries/funnel';
import { poolFromMeta, type AutomationRun } from '@/lib/watchlist-automation';

export type PoolRow = {
  tradeDate: string;
  s3Size: number;
  satelliteSize: number;
  addedS3: number;
  addedSatellite: number;
  removedS3: number;
  removedSatellite: number;
};

const CELL = 'px-2 py-1.5 text-right text-xs tabular-nums';

/** OPT-209: one day of the strategy-pool automation (S-3 + 星舰 add/GC). */
export function toPoolRow(run: AutomationRun): PoolRow | null {
  const p = poolFromMeta(run.meta as Record<string, unknown> | undefined);
  if (!p) return null;
  return {
    tradeDate: String(run.tradeDate ?? ''),
    s3Size: p.s3PoolSize,
    satelliteSize: p.satellitePoolSize,
    addedS3: p.addedS3,
    addedSatellite: p.addedSatellite,
    removedS3: p.removedS3,
    removedSatellite: p.removedSatellite,
  };
}

function Delta({ value, flip = false }: { value: number; flip?: boolean }) {
  if (value <= 0) return <span className="text-[var(--k-muted)]">—</span>;
  return (
    <span className={flip ? 'text-red-600 dark:text-red-400' : 'text-emerald-700 dark:text-emerald-300'}>
      {flip ? '−' : '+'}
      {value}
    </span>
  );
}

export function PoolHistoryTable({ limit = 10 }: { limit?: number }) {
  const query = useFunnelHistoryQuery(limit);
  const rows = React.useMemo(
    () => (query.data ?? []).map(toPoolRow).filter((r): r is PoolRow => r !== null),
    [query.data],
  );

  if (query.isFetching && rows.length === 0) {
    return <div className="text-xs text-[var(--k-muted)]">加载池变动历史…</div>;
  }
  if (rows.length === 0) {
    return (
      <div className="text-xs text-[var(--k-muted)]">
        暂无池变动记录（每日 17:30 自动跑：S-3 按 score≥65 & RS 入池，星舰按 14:30 名单入池，
        失效自动移除）
      </div>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[640px] text-left">
        <thead>
          <tr className="text-[10px] text-[var(--k-muted)]">
            <th className="px-2 py-1">日期</th>
            <th className="px-2 py-1 text-right">S-3 池</th>
            <th className="px-2 py-1 text-right">星舰</th>
            <th className="px-2 py-1 text-right">加入</th>
            <th className="px-2 py-1 text-right">移除</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.tradeDate} className="border-t border-[var(--k-border)]">
              <td className="px-2 py-1.5 text-xs font-medium">{r.tradeDate}</td>
              <td className={CELL}>{r.s3Size}</td>
              <td className={CELL}>{r.satelliteSize}</td>
              <td className={CELL}>
                S-3 <Delta value={r.addedS3} /> · 星舰 <Delta value={r.addedSatellite} />
              </td>
              <td className={CELL}>
                S-3 <Delta value={r.removedS3} flip /> · 星舰 <Delta value={r.removedSatellite} flip />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="mt-1 text-[10px] text-[var(--k-muted)]">
        池 = 两条回测腿：S-3（score≥65 &amp; RS，含弱市观察口径）+ 星舰（14:30 名单，持 3 个交易日）。
        失效规则：S-3 连续 2 日不达标、星舰到期出场即移除（有持仓的票不删）。
      </p>
    </div>
  );
}
