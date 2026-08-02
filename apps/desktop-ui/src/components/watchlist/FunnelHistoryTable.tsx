'use client';

import * as React from 'react';

import { useFunnelHistoryQuery } from '@/lib/queries/funnel';
import {
  funnelFromMeta,
  type AutomationRun,
} from '@/lib/watchlist-automation';

type FunnelRow = {
  tradeDate: string;
  tvHit: number;
  passPullback: number;
  passTrendOk: number;
  addedNew: number;
  fallbackUsed: boolean;
  fallbackHit: number;
  fallbackTrendOk: number;
  conversionPct: number | null;
};

export type { FunnelRow };

export function toFunnelRow(run: AutomationRun): FunnelRow | null {
  const f = funnelFromMeta(run.meta);
  if (!f) return null;
  const conversionPct =
    f.tvHit > 0
      ? Math.round((f.passTrendOk / f.tvHit) * 100)
      : f.fallbackUsed && f.fallbackHit > 0
        ? Math.round((f.fallbackTrendOk / f.fallbackHit) * 100)
        : null;
  return {
    tradeDate: String(run.tradeDate ?? ''),
    tvHit: f.tvHit,
    passPullback: f.passPullback,
    passTrendOk: f.passTrendOk,
    addedNew: f.addedNew,
    fallbackUsed: Boolean(f.fallbackUsed),
    fallbackHit: f.fallbackHit,
    fallbackTrendOk: f.fallbackTrendOk,
    conversionPct,
  };
}

const CELL = 'px-2 py-1.5 text-right text-xs tabular-nums';

export function FunnelHistoryTable({ limit = 10 }: { limit?: number }) {
  const query = useFunnelHistoryQuery(limit);
  const rows = React.useMemo(
    () => (query.data ?? []).map(toFunnelRow).filter((r): r is FunnelRow => r !== null),
    [query.data],
  );

  return (
    <section className="mb-4 min-w-0 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
      <div className="mb-2 text-sm font-medium">Funnel History（最近 {limit} 日漏斗转化率）</div>
      <div className="mb-2 text-[11px] leading-relaxed text-[var(--k-muted)]">
        TV 命中 → 过 52W 回撤 → 过 TrendOK → 新写入 Watchlist。转化率 = TrendOK / TV
        命中；空窗日使用兜底宇宙（fb）时按兜底口径计算。
      </div>
      {rows.length === 0 ? (
        <div className="text-xs text-[var(--k-muted)]">
          {query.isLoading ? 'Loading…' : '暂无漏斗数据 — 运行一次 Import 或 Automation 后生成'}
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full min-w-[480px] border-collapse text-xs">
            <thead>
              <tr className="border-b border-[var(--k-border)] text-[var(--k-muted)]">
                <th className="px-2 py-1.5 text-left font-medium">日期</th>
                <th className={`${CELL} font-medium`} title="TV Screener 命中去重标的数">
                  TV
                </th>
                <th className={`${CELL} font-medium`} title="过 52W 回撤 [-15%, -5%] 窗">
                  回撤
                </th>
                <th className={`${CELL} font-medium`} title="回撤后过 TrendOK">
                  TrendOK
                </th>
                <th className={`${CELL} font-medium`} title="新写入 Watchlist">
                  +新增
                </th>
                <th className={`${CELL} font-medium`} title="TrendOK / TV 命中（兜底日按 fb 口径）">
                  转化率
                </th>
                <th className={`${CELL} font-medium`} title="空窗日使用兜底宇宙（TIP-003）">
                  兜底
                </th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr
                  key={r.tradeDate}
                  className="border-b border-[var(--k-border)]/50 last:border-b-0"
                >
                  <td className="px-2 py-1.5 text-left">{r.tradeDate}</td>
                  <td className={CELL}>{r.tvHit}</td>
                  <td className={CELL}>{r.passPullback}</td>
                  <td className={CELL}>{r.passTrendOk}</td>
                  <td className={CELL}>{r.addedNew}</td>
                  <td className={`${CELL} font-medium`}>
                    {r.conversionPct !== null ? `${r.conversionPct}%` : '—'}
                  </td>
                  <td className={CELL}>
                    {r.fallbackUsed
                      ? `${r.fallbackHit}→OK ${r.fallbackTrendOk}`
                      : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
