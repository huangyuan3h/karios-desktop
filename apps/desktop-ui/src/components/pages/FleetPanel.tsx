'use client';

import * as React from 'react';
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip as RechartsTooltip,
  XAxis,
  YAxis,
} from 'recharts';

import { useFleetQuery } from '@/lib/queries/backtest';

function fmt(v: number | null | undefined, digits = 1): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v.toFixed(digits)}%`;
}

const WINDOWS = ['OOS2', 'valid', 'holdout', 'long'] as const;

export function FleetPanel() {
  const q = useFleetQuery();
  const fleet = q.data?.fleet;
  if (q.isLoading) return <div className="text-xs text-[var(--k-muted)]">舰队数据加载中…</div>;
  if (q.isError || !fleet)
    return (
      <div className="text-xs text-[var(--k-muted)]">
        舰队数据不可用（后端 fleet.json 缺失时 404，只读展示，不影响实盘）。
      </div>
    );
  const rows = (fleet.equity?.dates ?? []).map((d: string, i: number) => ({
    d,
    fleet: fleet.equity?.fleet?.[i] ?? null,
    base: fleet.equity?.base?.[i] ?? null,
    starship_b: fleet.equity?.starship_b?.[i] ?? null,
    hs300: fleet.equity?.hs300?.[i] ?? null,
  }));
  const w = fleet.windows?.fleet ?? {};
  const b = fleet.windows?.base ?? {};
  return (
    <div className="flex flex-col gap-3" data-testid="fleet-panel">
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2.5">
        <div className="text-[13px] font-semibold">
          舰队 <span className="ml-2 text-[11px] font-normal text-[var(--k-muted)]">研究 · 展示（母港M30为当前基线；星舰B为观察，不切自动交易）</span>
        </div>
        <div className="mt-1 text-[11px] leading-5 text-[var(--k-muted)]">
          底仓 50%港湾+25%母港M30+25%B3月调，预备舰按复活闸 0%/10%/20%切入（当前闸 0% fail-closed）；
          防线 L1/L4 从没触发过（免费保险），L2/L3 有小保险税（已标注，仍按批准保留）。
        </div>
      </div>
      <div className="overflow-x-auto rounded-lg border border-[var(--k-border)]">
        <table className="w-full min-w-[640px] text-[11px] tabular-nums">
          <thead>
            <tr className="bg-[var(--k-surface)] text-left text-[var(--k-muted)]">
              <th className="px-2 py-1.5">指标</th>
              {WINDOWS.map((k) => (
                <th key={k} className="px-2 py-1.5 text-right">
                  {k}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            <tr className="border-t border-[var(--k-border)]">
              <td className="px-2 py-1.5">舰队总回报</td>
              {WINDOWS.map((k) => (
                <td key={k} className="px-2 py-1.5 text-right">
                  {fmt(w[k]?.total)}
                </td>
              ))}
            </tr>
            <tr className="border-t border-[var(--k-border)]">
              <td className="px-2 py-1.5">舰队最大回撤</td>
              {WINDOWS.map((k) => (
                <td key={k} className="px-2 py-1.5 text-right">
                  {fmt(w[k]?.mdd)}
                </td>
              ))}
            </tr>
            <tr className="border-t border-[var(--k-border)]">
              <td className="px-2 py-1.5">舰队 Sharpe</td>
              {WINDOWS.map((k) => (
                <td key={k} className="px-2 py-1.5 text-right">
                  {w[k]?.sharpe == null ? '—' : Number(w[k]?.sharpe).toFixed(2)}
                </td>
              ))}
            </tr>
            <tr className="border-t border-[var(--k-border)]">
              <td className="px-2 py-1.5">无星舰基地总回报</td>
              {WINDOWS.map((k) => (
                <td key={k} className="px-2 py-1.5 text-right">
                  {fmt(b[k]?.total)}
                </td>
              ))}
            </tr>
          </tbody>
        </table>
      </div>
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2.5 text-[11px] leading-5 text-[var(--k-muted)]" data-testid="fleet-defense">
        防线状态：L1 60天-15%降档（样本0次，保留免费保险） · L2 单笔-8%暂停（样本69天，耗长窗约1.4pt，保留） ·
        L3 周-5%减半（样本20天，耗长窗约1.2pt、保持有0.3pt，保留） · L4 -25%转现金（样本0次，保留）。
        复活闸当前 0%（因子冷库星舰B冷库，paper未满，保持观察；复活需 rolling-40 sum转正 AND 月度大盘P&L转正）。
      </div>
      <div className="h-64 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-2">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={rows} margin={{ top: 8, right: 12, bottom: 4, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" opacity={0.4} />
            <XAxis dataKey="d" tick={{ fontSize: 9 }} minTickGap={120} />
            <YAxis tick={{ fontSize: 9 }} domain={['auto', 'auto']} />
            <RechartsTooltip />
            <Legend wrapperStyle={{ fontSize: 11 }} />
            <ReferenceLine y={1} stroke="#94a3b8" strokeDasharray="4 3" />
            <Line type="monotone" dataKey="fleet" name="舰队" stroke="#0ea5e9" dot={false} strokeWidth={2} />
            <Line type="monotone" dataKey="base" name="无星舰基地" stroke="#64748b" dot={false} strokeDasharray="5 3" />
            <Line type="monotone" dataKey="starship_b" name="星舰B（观察）" stroke="#8b5cf6" dot={false} />
            <Line type="monotone" dataKey="hs300" name="沪深300" stroke="#a1a1aa" dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
