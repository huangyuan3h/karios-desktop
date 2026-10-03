'use client';

import * as React from 'react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ReferenceArea,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip as RechartsTooltip,
  XAxis,
  YAxis,
} from 'recharts';

import { cn } from '@/lib/utils';
import { useSgapDecayQuery } from '@/lib/queries/backtest';

const C = {
  grid: '#94a3b8',
  axis: '#71717a',
  r40: '#8b5cf6',
  r60: '#38bdf8',
  win: '#8b5cf6',
  count: '#0ea5e9',
  pct: '#8b5cf6',
  amt: '#0ea5e9',
  circ: '#a855f7',
  edge: '#f97316',
  cum: '#059669',
  dd: '#ef4444',
} as const;

const WINDOW_BANDS = [
  { key: 'OOS2', fill: '#0ea5e9', label: 'OOS2' },
  { key: 'train', fill: '#a1a1aa', label: 'train' },
  { key: 'valid', fill: '#f59e0b', label: 'valid' },
  { key: 'holdout', fill: '#ef4444', label: 'holdout' },
] as const;

const REVIVAL_FILL: Record<number, string> = {
  0: '#ef4444',
  10: '#f59e0b',
  20: '#10b981',
};

function fmtSigned(v: number | null | undefined, digits = 2): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v > 0 ? '+' : ''}${v.toFixed(digits)}`;
}

function tickDates(dates: string[], max = 8): string[] {
  if (!dates.length) return [];
  const step = Math.max(1, Math.ceil(dates.length / max));
  return dates.filter((_, i) => i % step === 0);
}

function MiniTip({
  active,
  payload,
  label,
}: {
  active?: boolean;
  payload?: Array<{ name?: string; value?: number | string | null; color?: string }>;
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

function Section({
  title,
  testId,
  children,
}: {
  title: string;
  testId: string;
  children: React.ReactNode;
}) {
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
      <div className="mb-1 text-[12px] font-medium">{title}</div>
      <div className="h-[200px] w-full" data-testid={testId}>
        <ResponsiveContainer width="100%" height="100%">
          {children as React.ReactElement}
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function WindowAreas({
  windows,
}: {
  windows: Record<string, { start: string; end: string }> | undefined;
}) {
  if (!windows) return null;
  return (
    <>
      {WINDOW_BANDS.map((w) => {
        const win = windows[w.key];
        if (!win) return null;
        return (
          <ReferenceArea
            key={w.key}
            x1={win.start}
            x2={win.end}
            fill={w.fill}
            fillOpacity={0.08}
            stroke="none"
            ifOverflow="visible"
          />
        );
      })}
    </>
  );
}

/** S-gap 失效趋势 tab panel: 7 time-axis charts + computed summary line. */
export function SgapDecayPanel() {
  const q = useSgapDecayQuery(true);
  const decay = q.data?.decay;
  const rolling = React.useMemo(() => decay?.rolling ?? [], [decay]);
  const monthly = React.useMemo(() => decay?.monthly ?? [], [decay]);
  const equity = React.useMemo(() => decay?.equity ?? [], [decay]);
  const summary = decay?.summary;
  const windows = decay?.meta.windows;

  const rollData = React.useMemo(
    () =>
      rolling.map((r) => ({
        date: r.date,
        r40: r.r40?.mean ?? null,
        r60: r.r60?.mean ?? null,
        win40: r.r40 ? r.r40.win_rate * 100 : null,
        pct40: r.r40?.percentile ?? null,
        amtYi: r.crowd_amt_w != null ? r.crowd_amt_w / 1e4 : null,
        circYi: r.crowd_circ != null ? r.crowd_circ / 1e4 : null,
        edge: r.large_edge,
        pos: r.revival_pos,
      })),
    [rolling],
  );
  const eqData = React.useMemo(
    () => equity.map((e) => ({ date: e.date, cum: e.cum, dd: e.drawdown })),
    [equity],
  );
  const ticks = React.useMemo(() => tickDates(rollData.map((d) => d.date)), [rollData]);
  const monthTicks = React.useMemo(
    () =>
      tickDates(
        monthly.map((m) => m.month),
        10,
      ),
    [monthly],
  );
  const revivalRuns = React.useMemo(() => {
    const runs: Array<{ start: number; end: number; pos: number }> = [];
    let cur: { start: number; end: number; pos: number } | null = null;
    rollData.forEach((d, i) => {
      if (d.pos == null) {
        if (cur) {
          runs.push(cur);
          cur = null;
        }
        return;
      }
      if (cur && cur.pos === d.pos && i === cur.end + 1) cur.end = i;
      else {
        if (cur) runs.push(cur);
        cur = { start: i, end: i, pos: d.pos };
      }
    });
    if (cur) runs.push(cur);
    return runs;
  }, [rollData]);

  if (q.isError) {
    return (
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
        <p className="text-xs text-red-700">
          S-gap 趋势数据不可用（后端缺 sgap_decay.json：先跑 scripts/generate_sgap_decay.py）
        </p>
        <p className="mt-1 text-[10px] text-[var(--k-muted)]">{String(q.error)}</p>
      </div>
    );
  }
  if (!decay) {
    return (
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
        <p className="text-xs text-[var(--k-muted)]">
          S-gap 趋势加载中…（1130 笔滚动计算，一次约 1s）
        </p>
      </div>
    );
  }

  const s = summary!;
  const summaryLine =
    `共 ${s.n_trades} 笔 ${s.date_start ?? '—'}~${s.date_end ?? '—'}` +
    ` 累计 ${fmtSigned(s.cum_total, 1)} 点` +
    ` 最大回撤 ${fmtSigned(s.max_drawdown, 1)} 点` +
    ` 最新40笔均值 ${fmtSigned(s.latest_r40_mean)} 点/笔` +
    ` 胜率 ${s.latest_r40_win_rate != null ? `${(s.latest_r40_win_rate * 100).toFixed(1)}%` : '—'}` +
    ` 分位 ${s.latest_r40_percentile != null ? `${s.latest_r40_percentile.toFixed(1)}%` : '—'}` +
    ` 复活灯 ${s.revival_pos}%` +
    (s.revival_pos === 0 ? '（保持观察）' : s.revival_pos === 10 ? '（试探加回）' : '（半仓观察）');

  return (
    <div className="flex flex-col gap-4">
      <div
        className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2 text-[12px]"
        data-testid="sgap-summary"
      >
        {summaryLine}
        <div className="mt-0.5 text-[10px] text-[var(--k-muted)]">
          卫星点口径（已扣 32.28bp/笔）· 分位为 F long R1 正态近似（live 用 500 次同日随机精确值）·
          复活灯 K2（N40/X75，低于 50 降档）· 背景 OOS2/train/valid/holdout 为 H2k 冻结切分
        </div>
      </div>

      <Section title="滚动均值（40笔紫 / 60笔浅蓝 · 零线）" testId="sgap-chart-rollret">
        <LineChart data={rollData} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid stroke={C.grid} strokeOpacity={0.25} vertical={false} />
          <XAxis
            dataKey="date"
            ticks={ticks}
            tick={{ fontSize: 9, fill: C.axis }}
            tickLine={false}
          />
          <YAxis
            tick={{ fontSize: 9, fill: C.axis }}
            width={44}
            tickLine={false}
            axisLine={false}
          />
          <RechartsTooltip content={<MiniTip />} />
          <WindowAreas windows={windows} />
          <ReferenceLine y={0} stroke={C.axis} strokeOpacity={0.5} strokeDasharray="2 3" />
          <Line
            type="linear"
            dataKey="r40"
            name="40笔均值"
            stroke={C.r40}
            strokeWidth={1.75}
            dot={false}
            isAnimationActive={false}
            connectNulls
          />
          <Line
            type="linear"
            dataKey="r60"
            name="60笔均值"
            stroke={C.r60}
            strokeWidth={1.25}
            dot={false}
            isAnimationActive={false}
            connectNulls
          />
        </LineChart>
      </Section>

      <Section title="滚动40笔胜率（50% 线）" testId="sgap-chart-winrate">
        <LineChart data={rollData} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid stroke={C.grid} strokeOpacity={0.25} vertical={false} />
          <XAxis
            dataKey="date"
            ticks={ticks}
            tick={{ fontSize: 9, fill: C.axis }}
            tickLine={false}
          />
          <YAxis
            domain={[0, 100]}
            tick={{ fontSize: 9, fill: C.axis }}
            width={36}
            tickLine={false}
            axisLine={false}
          />
          <RechartsTooltip content={<MiniTip />} />
          <WindowAreas windows={windows} />
          <ReferenceLine y={50} stroke={C.axis} strokeOpacity={0.6} strokeDasharray="2 3" />
          <Line
            type="linear"
            dataKey="win40"
            name="胜率%"
            stroke={C.win}
            strokeWidth={1.75}
            dot={false}
            isAnimationActive={false}
            connectNulls
          />
        </LineChart>
      </Section>

      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
        <div className="mb-1 text-[12px] font-medium">月信号数（柱 · 缺口即 drought）</div>
        <div className="h-[180px] w-full" data-testid="sgap-chart-monthly">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={monthly} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
              <CartesianGrid stroke={C.grid} strokeOpacity={0.25} vertical={false} />
              <XAxis
                dataKey="month"
                ticks={monthTicks}
                tick={{ fontSize: 9, fill: C.axis }}
                tickLine={false}
              />
              <YAxis
                tick={{ fontSize: 9, fill: C.axis }}
                width={32}
                tickLine={false}
                axisLine={false}
                allowDecimals={false}
              />
              <RechartsTooltip content={<MiniTip />} />
              <Bar
                dataKey="count"
                name="笔数"
                fill={C.count}
                fillOpacity={0.75}
                isAnimationActive={false}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
        <p className={cn('mt-1 text-[10px] text-[var(--k-muted)]')}>
          2026-03 与 2026-06 为 0 笔（43 天 + 78 天双 drought）；2026-07 仅 4 笔随后是 −20
          崩盘（与历史 drought 后反弹不同）
        </p>
      </div>

      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
        <div className="mb-1 flex flex-wrap items-center gap-2 text-[12px] font-medium">
          滚动40笔随机分位（50 降档 / 75 加仓线 · 底色=复活灯 0红/10 amber/20绿）
        </div>
        <div className="h-[200px] w-full" data-testid="sgap-chart-percentile">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={rollData} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
              <CartesianGrid stroke={C.grid} strokeOpacity={0.25} vertical={false} />
              <XAxis
                dataKey="date"
                ticks={ticks}
                tick={{ fontSize: 9, fill: C.axis }}
                tickLine={false}
              />
              <YAxis
                domain={[0, 100]}
                tick={{ fontSize: 9, fill: C.axis }}
                width={36}
                tickLine={false}
                axisLine={false}
              />
              <RechartsTooltip content={<MiniTip />} />
              {revivalRuns.map((r, k) => (
                <ReferenceArea
                  key={k}
                  x1={rollData[r.start]?.date}
                  x2={rollData[r.end]?.date}
                  fill={REVIVAL_FILL[r.pos] ?? '#a1a1aa'}
                  fillOpacity={r.pos === 0 ? 0.1 : 0.16}
                  stroke="none"
                  ifOverflow="visible"
                />
              ))}
              <ReferenceLine y={50} stroke={C.axis} strokeOpacity={0.6} strokeDasharray="2 3" />
              <ReferenceLine y={75} stroke={C.pct} strokeOpacity={0.7} strokeDasharray="4 3" />
              <Line
                type="linear"
                dataKey="pct40"
                name="分位%"
                stroke={C.pct}
                strokeWidth={1.75}
                dot={false}
                isAnimationActive={false}
                connectNulls
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      <Section title="拥挤（滚动40笔中位 · 成交亿元浅蓝 / 市值亿元紫）" testId="sgap-chart-crowd">
        <LineChart data={rollData} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid stroke={C.grid} strokeOpacity={0.25} vertical={false} />
          <XAxis
            dataKey="date"
            ticks={ticks}
            tick={{ fontSize: 9, fill: C.axis }}
            tickLine={false}
          />
          <YAxis
            tick={{ fontSize: 9, fill: C.axis }}
            width={44}
            tickLine={false}
            axisLine={false}
          />
          <RechartsTooltip content={<MiniTip />} />
          <WindowAreas windows={windows} />
          <Line
            type="linear"
            dataKey="amtYi"
            name="成交亿元"
            stroke={C.amt}
            strokeWidth={1.5}
            dot={false}
            isAnimationActive={false}
            connectNulls
          />
          <Line
            type="linear"
            dataKey="circYi"
            name="市值亿元"
            stroke={C.circ}
            strokeWidth={1.5}
            dot={false}
            isAnimationActive={false}
            connectNulls
          />
        </LineChart>
      </Section>

      <Section
        title="大单边缘（滚动40笔内高-低 large_pct 组均值差 · 零线）"
        testId="sgap-chart-edge"
      >
        <LineChart data={rollData} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid stroke={C.grid} strokeOpacity={0.25} vertical={false} />
          <XAxis
            dataKey="date"
            ticks={ticks}
            tick={{ fontSize: 9, fill: C.axis }}
            tickLine={false}
          />
          <YAxis
            tick={{ fontSize: 9, fill: C.axis }}
            width={44}
            tickLine={false}
            axisLine={false}
          />
          <RechartsTooltip content={<MiniTip />} />
          <WindowAreas windows={windows} />
          <ReferenceLine y={0} stroke={C.axis} strokeOpacity={0.5} strokeDasharray="2 3" />
          <Line
            type="linear"
            dataKey="edge"
            name="高-低点差"
            stroke={C.edge}
            strokeWidth={1.75}
            dot={false}
            isAnimationActive={false}
            connectNulls
          />
        </LineChart>
      </Section>

      <Section title="交易空间累计（绿 · 回撤红）" testId="sgap-chart-equity">
        <LineChart data={eqData} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid stroke={C.grid} strokeOpacity={0.25} vertical={false} />
          <XAxis
            dataKey="date"
            ticks={tickDates(eqData.map((d) => d.date))}
            tick={{ fontSize: 9, fill: C.axis }}
            tickLine={false}
          />
          <YAxis
            tick={{ fontSize: 9, fill: C.axis }}
            width={44}
            tickLine={false}
            axisLine={false}
          />
          <RechartsTooltip content={<MiniTip />} />
          <WindowAreas windows={windows} />
          <Line
            type="linear"
            dataKey="cum"
            name="累计点"
            stroke={C.cum}
            strokeWidth={1.75}
            dot={false}
            isAnimationActive={false}
          />
          <Line
            type="linear"
            dataKey="dd"
            name="回撤点"
            stroke={C.dd}
            strokeWidth={1.25}
            dot={false}
            isAnimationActive={false}
          />
        </LineChart>
      </Section>

      <p className="text-[10px] text-[var(--k-muted)]">
        方法：滚动为笔数窗（40/60），分位为 F long R1 正态近似（μ0.0178/笔、σ1.2175/笔），复活灯为
        K2 两步回放（双确认 + 20 笔间隔，paper-20 与月频执行略去，实盘仍
        0%），大单边缘为窗内中位分组（同日 ex post，仅归因），累计为笔收益求和（非复利）。
      </p>
    </div>
  );
}
