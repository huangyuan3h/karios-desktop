'use client';

import * as React from 'react';
import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip as RechartsTooltip,
  XAxis,
  YAxis,
} from 'recharts';

import { cn } from '@/lib/utils';
import { useFactorVaultQuery, type FactorVaultRow } from '@/lib/queries/backtest';

const C = {
  grid: '#94a3b8',
  axis: '#71717a',
  pct: '#8b5cf6',
  net: '#059669',
} as const;

function fmtSigned(v: number | null | undefined, digits = 2): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v > 0 ? '+' : ''}${v.toFixed(digits)}`;
}

function fmtPct(v: number | null | undefined, digits = 1): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v.toFixed(digits)}%`;
}

function MiniTip({
  active,
  payload,
  label,
}: {
  active?: boolean;
  payload?: Array<{ name?: string; value?: number | string | null }>;
  label?: string | number;
}) {
  if (!active || !payload?.length) return null;
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2 text-[11px] shadow-sm">
      <div className="mb-1 font-medium text-[var(--k-fg)]">{label}</div>
      <div className="space-y-0.5">
        {payload.map((p, i) => (
          <div key={i} className="flex items-center justify-between gap-4 tabular-nums">
            <span className="text-[var(--k-muted)]">{p.name}</span>
            <span className="font-mono">
              {typeof p.value === 'number' ? fmtSigned(p.value) : String(p.value ?? '—')}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function badgeCls(status: FactorVaultRow['status']): string {
  if (status === 'revived')
    return 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300';
  if (status === 'watch')
    return 'border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-300';
  return 'border-slate-500/40 bg-slate-500/10 text-slate-600 dark:text-slate-300';
}

function Spark({ values }: { values: Array<number | null> }) {
  const pts = values.map((v, i) => ({ i, v: v ?? 50 }));
  const w = 96;
  const h = 26;
  const d = pts
    .map((p, k) => `${k === 0 ? 'M' : 'L'}${((p.i / Math.max(1, pts.length - 1)) * (w - 4) + 2).toFixed(1)},${(h - 3 - (Math.min(100, Math.max(0, p.v)) / 100) * (h - 6)).toFixed(1)}`)
    .join(' ');
  return (
    <svg width={w} height={h} data-testid="vault-spark" aria-label="分位 sparkline">
      <line x1={2} y1={h - 3 - 0.5 * (h - 6)} x2={w - 2} y2={h - 3 - 0.5 * (h - 6)} stroke={C.axis} strokeOpacity={0.4} strokeDasharray="2 2" />
      <line x1={2} y1={h - 3 - 0.75 * (h - 6)} x2={w - 2} y2={h - 3 - 0.75 * (h - 6)} stroke={C.pct} strokeOpacity={0.5} strokeDasharray="3 2" />
      <path d={d} fill="none" stroke={C.pct} strokeWidth={1.5} />
    </svg>
  );
}

function RowDetail({ row }: { row: FactorVaultRow }) {
  const data = React.useMemo(
    () =>
      row.history.map((p) => ({
        window: p.window,
        pct: p.percentile,
        net: p.net,
      })),
    [row],
  );
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/50 p-3" data-testid={`vault-detail-${row.id}`}>
      <div className="mb-1 text-[12px] font-medium">
        {row.name} · 时间轴（分位紫 / 净收益绿 · 50 降档 / 75 加仓线）
        <span className="ml-2 text-[10px] font-normal text-[var(--k-muted)]">
          {row.doc} · 建议仓位 {row.position}%（{row.confirmations} 次确认）
        </span>
      </div>
      <div className="h-[180px] w-full" data-testid={`vault-chart-${row.id}`}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
            <CartesianGrid stroke={C.grid} strokeOpacity={0.25} vertical={false} />
            <XAxis dataKey="window" tick={{ fontSize: 10, fill: C.axis }} tickLine={false} />
            <YAxis yAxisId="pct" domain={[0, 100]} tick={{ fontSize: 9, fill: C.axis }} width={36} tickLine={false} axisLine={false} />
            <YAxis yAxisId="net" orientation="right" tick={{ fontSize: 9, fill: C.axis }} width={52} tickLine={false} axisLine={false} />
            <RechartsTooltip content={<MiniTip />} />
            <ReferenceLine yAxisId="pct" y={50} stroke={C.axis} strokeOpacity={0.6} strokeDasharray="2 3" />
            <ReferenceLine yAxisId="pct" y={75} stroke={C.pct} strokeOpacity={0.7} strokeDasharray="4 3" />
            <ReferenceLine yAxisId="net" y={0} stroke={C.axis} strokeOpacity={0.5} strokeDasharray="2 3" />
            <Line yAxisId="pct" type="linear" dataKey="pct" name="分位%" stroke={C.pct} strokeWidth={1.75} dot isAnimationActive={false} connectNulls />
            <Line yAxisId="net" type="linear" dataKey="net" name="净" stroke={C.net} strokeWidth={1.5} dot isAnimationActive={false} connectNulls />
          </LineChart>
        </ResponsiveContainer>
      </div>
      <p className="mt-1 text-[10px] text-[var(--k-muted)]">
        独立来源：{row.source} · 窗内 {row.trades} 笔（{row.rolling_window ?? '—'}窗）· 胜率{' '}
        {row.win_rate != null ? `${(row.win_rate * 100).toFixed(1)}%` : '—'} · {row.days_since_change} 天未变档
      </p>
    </div>
  );
}

/** 因子冷库 tab panel: uniform H2k health table + click-to-drill time axis. */
export function FactorVaultPanel() {
  const q = useFactorVaultQuery(true);
  const vault = q.data?.vault;
  const factors = React.useMemo(() => vault?.factors ?? [], [vault]);
  const [openId, setOpenId] = React.useState<string | null>(null);
  const [filter, setFilter] = React.useState<'all' | 'cold' | 'watch' | 'revived'>('all');

  const shown = React.useMemo(
    () => (filter === 'all' ? factors : factors.filter((r) => r.status === filter)),
    [factors, filter],
  );

  if (q.isError) {
    return (
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
        <p className="text-xs text-red-700">
          因子冷库数据不可用（后端缺 factor_vault.json：先跑 scripts/generate_factor_vault.py）
        </p>
        <p className="mt-1 text-[10px] text-[var(--k-muted)]">{String(q.error)}</p>
      </div>
    );
  }
  if (!vault) {
    return (
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
        <p className="text-xs text-[var(--k-muted)]">因子冷库加载中…（27 个因子统一体检，一次约 1s）</p>
      </div>
    );
  }

  const s = vault.summary;
  const summaryLine =
    `共 ${s.n_factors} 个因子` +
    ` 冷库 ${s.n_cold} · 观察 ${s.n_watch} · 复活 ${s.n_revived}` +
    `（统一 H2k-K2：滚动40笔分位 vs同期随机 + 扣费后净收益）`;

  return (
    <div className="flex flex-col gap-4">
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2 text-[12px]" data-testid="vault-summary">
        {summaryLine}
        <div className="mt-0.5 text-[10px] text-[var(--k-muted)]">
          规则：滚动最近 40 笔（月频为 40 期）· 分位为同期随机对照（无真随机分位的试点永不显示复活）·
          冷库（分位&lt;50 或净≤0） 观察（50–75 或不足 20 笔） 复活（≥75 且净&gt;0，需两次确认，步进间隔≥20 笔，上限 20%；
          &lt;50 或熔断即降档到 0）· paper-20 与月频执行实盘仍 pin 在 0% · 母港M30为当前基线，星舰B为观察（复活需 rolling-40 sum转正 AND 月度大盘P&L转正），本表只做观察不改参
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-1">
        {(['all', 'cold', 'watch', 'revived'] as const).map((f) => (
          <button
            key={f}
            type="button"
            onClick={() => setFilter(f)}
            className={cn(
              'rounded border px-2 py-0.5 text-[11px]',
              filter === f
                ? 'border-sky-500/50 bg-sky-500/10 text-sky-800 dark:text-sky-200'
                : 'border-[var(--k-border)] text-[var(--k-muted)]',
            )}
          >
            {f === 'all' ? '全部' : f === 'cold' ? '冷库' : f === 'watch' ? '观察' : '复活'}
          </button>
        ))}
        <span className="ml-auto text-[10px] text-[var(--k-muted)]">
          {vault.meta.source} · 成本{vault.meta.cost}
        </span>
      </div>

      <div className="overflow-auto rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)]" data-testid="vault-table">
        <table className="w-full min-w-[1080px] text-left text-xs tabular-nums">
          <thead className="sticky top-0 bg-[var(--k-surface)]">
            <tr className="text-[10px] text-[var(--k-muted)]">
              <th className="px-2 py-1.5">因子</th>
              <th className="px-2 py-1.5">家族 / 独立来源</th>
              <th className="px-2 py-1.5">状态</th>
              <th className="px-2 py-1.5">滚动分位</th>
              <th className="px-2 py-1.5">滚动净</th>
              <th className="px-2 py-1.5">胜率</th>
              <th className="px-2 py-1.5">窗内笔数</th>
              <th className="px-2 py-1.5">未变档天数</th>
              <th className="px-2 py-1.5">分位 spark</th>
              <th className="px-2 py-1.5">建议仓位</th>
              <th className="px-2 py-1.5">更新</th>
            </tr>
          </thead>
          <tbody>
            {shown.map((r) => (
              <React.Fragment key={r.id}>
                <tr
                  className="cursor-pointer border-t border-[var(--k-border)]/60 hover:bg-[var(--k-surface-2)]/60"
                  onClick={() => setOpenId((v) => (v === r.id ? null : r.id))}
                  data-testid={`vault-row-${r.id}`}
                >
                  <td className="px-2 py-1.5 font-medium">{r.name}</td>
                  <td className="px-2 py-1.5 text-[var(--k-muted)]">{r.family} · {r.source}</td>
                  <td className="px-2 py-1.5">
                    <span className={cn('rounded border px-1.5 py-0.5 text-[10px]', badgeCls(r.status))}>
                      {r.badge}
                    </span>
                  </td>
                  <td className="px-2 py-1.5 font-mono">{fmtPct(r.percentile)}</td>
                  <td className={cn('px-2 py-1.5 font-mono', r.net >= 0 ? 'text-emerald-700 dark:text-emerald-300' : 'text-red-700 dark:text-red-400')}>
                    {fmtSigned(r.net)}{r.unit ? ` ${r.unit}` : ''}
                  </td>
                  <td className="px-2 py-1.5 font-mono">{r.win_rate != null ? `${(r.win_rate * 100).toFixed(1)}%` : '—'}</td>
                  <td className="px-2 py-1.5 font-mono">{r.trades}{r.trades_raw > r.trades ? ` / ${r.trades_raw}` : ''}</td>
                  <td className="px-2 py-1.5 font-mono">{r.days_since_change} 天</td>
                  <td className="px-2 py-1.5"><Spark values={r.spark} /></td>
                  <td className="px-2 py-1.5 font-mono">{r.position}%{r.confirmations ? ` (${r.confirmations}确认)` : ''}</td>
                  <td className="px-2 py-1.5 text-[var(--k-muted)]">{r.updated ?? '—'}</td>
                </tr>
                {openId === r.id ? (
                  <tr className="border-t border-[var(--k-border)]/60">
                    <td colSpan={11} className="px-2 py-2">
                      <RowDetail row={r} />
                    </td>
                  </tr>
                ) : null}
              </React.Fragment>
            ))}
          </tbody>
        </table>
      </div>

      <p className="text-[10px] text-[var(--k-muted)]">
        方法：滚动为笔数窗（40 笔/40 期，不足按实际笔数并标观察），分位为审计真随机对照（星舰B用 F long R1 15000 次正态近似 μ0.0178/σ1.2175；
        港湾用 H2d 500 次/窗 μ0.097/σ1.08；无真分位的试点显示 — 且永不复活），复活灯为 K2 两步回放（双确认 + 20 笔间隔），
        点击行展开 OOS2/train/valid/holdout 时间轴（复用 S-gap 趋势图组件）。
      </p>
    </div>
  );
}
