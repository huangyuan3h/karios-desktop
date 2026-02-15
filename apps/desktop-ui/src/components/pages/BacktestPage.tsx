'use client';

import * as React from 'react';
import { Area, AreaChart, CartesianGrid, Line, LineChart, TooltipProps, XAxis, YAxis } from 'recharts';

import { Button } from '@/components/ui/button';
import { ChartContainer, ChartTooltip, ChartTooltipContent } from '@/components/ui/chart';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';

type BacktestRunResponse = { ok: boolean; runId: string; summary: Record<string, number> };
type BacktestRunRecord = {
  id: string;
  strategy_name: string;
  start_date: string;
  end_date: string;
  status: string;
  created_at: string;
  params: unknown;
  summary: Record<string, number> | null;
  equity_curve: Array<{ date: string; equity: number }> | null;
  drawdown_curve: Array<{ date: string; drawdown: number }> | null;
  positions_curve: Array<{ date: string; invested_ratio: number }> | null;
  daily_log: Array<DailyLogEntry> | null;
  error_message: string | null;
};

type DailyLogEntry = {
  date: string;
  selected: Array<{ ts_code: string; score: number; avg_price: number }>;
  orders: Array<{
    ts_code: string;
    action: string;
    reason?: string | null;
    status?: string | null;
    exec_qty?: number | null;
    exec_price?: number | null;
  }>;
  positions: Array<{ ts_code: string; qty: number }>;
  strategy_stats?: {
    date?: string;
    regime?: string;
    bars?: number;
    breakout_ok?: number;
    pullback_ok?: number;
    sell_ok?: number;
    buy_signal?: number;
  } | null;
  cash_before: number;
  cash: number;
  equity: number;
};

type BacktestResultResponse = {
  run: BacktestRunRecord;
  trades: Array<Record<string, unknown>>;
};

type RunFormState = {
  strategy: string;
  start_date: string;
  end_date: string;
  initial_cash: string;
  fee_rate: string;
  slippage_rate: string;
  adj_mode: string;
  top_n: string;
  min_price: string;
  min_volume: string;
  exclude_keywords: string;
  min_list_days: string;
};

const defaultForm: RunFormState = {
  strategy: 'ma_crossover',
  start_date: '2023-02-01',
  end_date: '2026-01-01',
  initial_cash: '100',
  fee_rate: '0.0005',
  slippage_rate: '0',
  adj_mode: 'qfq',
  top_n: '1000',
  min_price: '2',
  min_volume: '100000',
  exclude_keywords: 'ST,退',
  min_list_days: '60',
};

const STRATEGY_OPTIONS = [
  { value: 'ma_crossover', label: '均线交叉' },
  { value: 'watchlist_trend', label: 'Watchlist趋势' },
  { value: 'watchlist_trend_v2', label: 'Watchlist趋势V2' },
  { value: 'sample_momentum', label: '样例动量' },
];

async function apiGetJson<T>(path: string): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, { cache: 'no-store' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return (txt ? (JSON.parse(txt) as T) : ({} as T));
}

async function apiPostJson<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, {
    method: 'POST',
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return (txt ? (JSON.parse(txt) as T) : ({} as T));
}

function fmtNum(val: number | undefined | null) {
  if (val === undefined || val === null || Number.isNaN(val)) return '-';
  return val.toLocaleString('en-US', { maximumFractionDigits: 4 });
}

function fmtWan(val: number | undefined | null) {
  if (val === undefined || val === null || Number.isNaN(val)) return '-';
  return `${(val / 10000).toFixed(2)}万`;
}

function fmtPct(val: number | undefined | null) {
  if (val === undefined || val === null || Number.isNaN(val)) return '-';
  return `${(val * 100).toFixed(2)}%`;
}

function DailyTooltip({
  active,
  payload,
  label,
}: TooltipProps<number, string> & {
  payload?: Array<{ payload?: Record<string, unknown> }>;
  label?: string;
}) {
  if (!active || !payload?.length) return null;
  const data = payload[0]?.payload as {
    equity?: number;
    cash?: number;
    cash_before?: number;
    orders?: string;
    strategyStats?: string;
  };
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2 text-xs shadow-sm">
      <div className="mb-1 font-medium">{label}</div>
      <div className="space-y-1">
        <div className="flex items-center justify-between gap-3">
          <span className="text-[var(--k-muted)]">现金(开盘)</span>
          <span className="font-mono">{fmtNum(data.cash_before)}</span>
        </div>
        <div className="flex items-center justify-between gap-3">
          <span className="text-[var(--k-muted)]">权益</span>
          <span className="font-mono">{fmtNum(data.equity)}</span>
        </div>
        <div className="flex items-center justify-between gap-3">
          <span className="text-[var(--k-muted)]">现金(收盘)</span>
          <span className="font-mono">{fmtNum(data.cash)}</span>
        </div>
        {data.orders ? (
          <div className="mt-2 text-[var(--k-muted)]">
            <div className="font-medium">指令</div>
            <div className="mt-1 whitespace-pre-wrap font-mono">{data.orders}</div>
          </div>
        ) : null}
        {data.strategyStats ? (
          <div className="mt-2 text-[var(--k-muted)]">
            <div className="font-medium">诊断</div>
            <div className="mt-1 whitespace-pre-wrap font-mono">{data.strategyStats}</div>
          </div>
        ) : null}
      </div>
    </div>
  );
}

function RunModal({
  open,
  busy,
  form,
  setForm,
  onClose,
  onRun,
}: {
  open: boolean;
  busy: boolean;
  form: RunFormState;
  setForm: (next: RunFormState) => void;
  onClose: () => void;
  onRun: () => void;
}) {
  if (!open) return null;
  const update = (key: keyof RunFormState) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) =>
    setForm({ ...form, [key]: e.target.value });
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30 p-4">
      <div className="w-full max-w-2xl rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-6 shadow-xl">
        <div className="mb-4 text-lg font-semibold">运行回测</div>
        <div className="grid gap-3 md:grid-cols-2">
          <label className="grid gap-1 text-sm">
            策略
            <select
              className="rounded-md border border-[var(--k-border)] px-3 py-2"
              value={form.strategy}
              onChange={update('strategy')}
            >
              {STRATEGY_OPTIONS.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
          </label>
          <label className="grid gap-1 text-sm">
            复权方式
            <select className="rounded-md border border-[var(--k-border)] px-3 py-2" value={form.adj_mode} onChange={update('adj_mode')}>
              <option value="qfq">前复权</option>
              <option value="hfq">后复权</option>
            </select>
          </label>
          <label className="grid gap-1 text-sm">
            开始日期
            <input className="rounded-md border border-[var(--k-border)] px-3 py-2" value={form.start_date} onChange={update('start_date')} />
          </label>
          <label className="grid gap-1 text-sm">
            结束日期
            <input className="rounded-md border border-[var(--k-border)] px-3 py-2" value={form.end_date} onChange={update('end_date')} />
          </label>
          <label className="grid gap-1 text-sm">
            初始资金(万)
            <input className="rounded-md border border-[var(--k-border)] px-3 py-2" value={form.initial_cash} onChange={update('initial_cash')} />
          </label>
          <label className="grid gap-1 text-sm">
            手续费率
            <input className="rounded-md border border-[var(--k-border)] px-3 py-2" value={form.fee_rate} onChange={update('fee_rate')} />
          </label>
          <label className="grid gap-1 text-sm">
            滑点率
            <input className="rounded-md border border-[var(--k-border)] px-3 py-2" value={form.slippage_rate} onChange={update('slippage_rate')} />
          </label>
          <label className="grid gap-1 text-sm">
            TopN
            <input className="rounded-md border border-[var(--k-border)] px-3 py-2" value={form.top_n} onChange={update('top_n')} />
          </label>
          <label className="grid gap-1 text-sm">
            最低价格
            <input className="rounded-md border border-[var(--k-border)] px-3 py-2" value={form.min_price} onChange={update('min_price')} />
          </label>
          <label className="grid gap-1 text-sm">
            最低成交量
            <input className="rounded-md border border-[var(--k-border)] px-3 py-2" value={form.min_volume} onChange={update('min_volume')} />
          </label>
          <label className="grid gap-1 text-sm md:col-span-2">
            排除关键词
            <input className="rounded-md border border-[var(--k-border)] px-3 py-2" value={form.exclude_keywords} onChange={update('exclude_keywords')} />
          </label>
          <label className="grid gap-1 text-sm">
            上市天数
            <input className="rounded-md border border-[var(--k-border)] px-3 py-2" value={form.min_list_days} onChange={update('min_list_days')} />
          </label>
        </div>
        <div className="mt-5 flex items-center justify-end gap-2">
          <Button variant="secondary" onClick={onClose} disabled={busy}>
            取消
          </Button>
          <Button onClick={onRun} disabled={busy}>
            {busy ? '运行中…' : '运行回测'}
          </Button>
        </div>
      </div>
    </div>
  );
}

export function BacktestPage() {
  const [form, setForm] = React.useState<RunFormState>(defaultForm);
  const [modalOpen, setModalOpen] = React.useState(false);
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [result, setResult] = React.useState<BacktestResultResponse | null>(null);
  const [filter, setFilter] = React.useState('');
  const [onlyActive, setOnlyActive] = React.useState(true);

  const summary = result?.run?.summary ?? null;
  const dailyLog = React.useMemo(() => result?.run?.daily_log ?? [], [result]);

  const chartData = React.useMemo(() => {
    return dailyLog.map((d) => ({
      date: d.date,
      equity: d.equity,
      cash: d.cash,
      cash_before: d.cash_before,
      strategyStats: d.strategy_stats
        ? `regime=${d.strategy_stats.regime} bars=${d.strategy_stats.bars} breakout=${d.strategy_stats.breakout_ok} pullback=${d.strategy_stats.pullback_ok} buy=${d.strategy_stats.buy_signal}`
        : '',
      orders: d.orders
        .filter((o) => o.status === 'executed')
        .map((o) => `${o.action === 'buy' ? '买入' : o.action === 'sell' ? '卖出' : o.action} ${o.ts_code}${o.reason ? ` (${o.reason})` : ''}`)
        .slice(0, 4)
        .join('\n'),
    }));
  }, [dailyLog]);

  const drawdownData = React.useMemo(() => {
    return (result?.run?.drawdown_curve ?? []).map((d) => ({
      date: d.date,
      drawdown: d.drawdown,
    }));
  }, [result]);

  async function runBacktest() {
    setError(null);
    setBusy(true);
    try {
      const body = {
        strategy: form.strategy,
        start_date: form.start_date,
        end_date: form.end_date,
        params: {
          initial_cash: Number(form.initial_cash) * 10000,
          fee_rate: Number(form.fee_rate),
          slippage_rate: Number(form.slippage_rate),
          adj_mode: form.adj_mode,
        },
        universe: {
          market: 'CN',
          exclude_keywords: form.exclude_keywords.split(',').map((x) => x.trim()).filter(Boolean),
          min_list_days: Number(form.min_list_days),
        },
        rules: {
          min_price: Number(form.min_price),
          min_volume: Number(form.min_volume),
        },
        scoring: {
          top_n: Number(form.top_n),
          momentum_weight: 1.0,
          volume_weight: 0.2,
          amount_weight: 0.1,
        },
      };
      const run = await apiPostJson<BacktestRunResponse>('/backtest/run', body);
      const full = await apiGetJson<BacktestResultResponse>(`/backtest/result/${run.runId}`);
      setResult(full);
      setModalOpen(false);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  const filteredLog = React.useMemo(() => {
    if (!filter.trim()) return dailyLog;
    const q = filter.trim().toLowerCase();
    return dailyLog.filter((d) => {
      const selected = d.selected.map((s) => s.ts_code).join(' ');
      const orders = d.orders
        .filter((o) => o.status === 'executed')
        .map((o) => `${o.action === 'buy' ? '买入' : o.action === 'sell' ? '卖出' : o.action} ${o.ts_code} ${o.reason ?? ''}`)
        .join(' ');
      const positions = d.positions.map((p) => `${p.ts_code} ${p.qty}`).join(' ');
      const stats = d.strategy_stats
        ? `regime=${d.strategy_stats.regime} bars=${d.strategy_stats.bars} breakout=${d.strategy_stats.breakout_ok} pullback=${d.strategy_stats.pullback_ok} buy=${d.strategy_stats.buy_signal}`
        : '';
      return `${d.date} ${selected} ${orders} ${positions} ${stats}`.toLowerCase().includes(q);
    });
  }, [dailyLog, filter]);

  const visibleLog = React.useMemo(() => {
    if (!onlyActive) return filteredLog;
    return filteredLog.filter((d) => d.orders.some((o) => o.status === 'executed'));
  }, [filteredLog, onlyActive]);

  return (
    <div className="mx-auto w-full max-w-6xl p-6">
      <div className="mb-6 flex items-start justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">回测</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">运行策略并查看资金曲线与日志。</div>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="secondary" size="sm" onClick={() => setModalOpen(true)}>
            运行回测
          </Button>
        </div>
      </div>

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      <div className="grid gap-4 md:grid-cols-4">
        <div className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="text-xs text-[var(--k-muted)]">当前策略</div>
          <div className="mt-2 text-xl font-semibold">
            {result?.run?.strategy_name
              ? STRATEGY_OPTIONS.find((opt) => opt.value === result.run.strategy_name)?.label ||
                result.run.strategy_name
              : '—'}
          </div>
        </div>
        <div className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="text-xs text-[var(--k-muted)]">累计收益</div>
          <div className="mt-2 text-xl font-semibold">{fmtPct(summary?.total_return)}</div>
        </div>
        <div className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="text-xs text-[var(--k-muted)]">最大回撤</div>
          <div className="mt-2 text-xl font-semibold">{fmtPct(summary?.max_drawdown)}</div>
        </div>
        <div className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="text-xs text-[var(--k-muted)]">交易次数</div>
          <div className="mt-2 text-xl font-semibold">{summary?.total_trades ?? '-'}</div>
        </div>
        <div className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="text-xs text-[var(--k-muted)]">最终资金</div>
          <div className="mt-2 text-xl font-semibold">{fmtWan(summary?.final_equity)}</div>
        </div>
      </div>

      <div className="mt-6 grid gap-4 lg:grid-cols-2">
        <div className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-2 text-sm font-medium">资金曲线（权益/现金）</div>
          <ChartContainer
            config={{
              equity: { label: '权益', color: 'hsl(215, 85%, 55%)' },
              cash: { label: '现金', color: 'hsl(145, 65%, 40%)' },
            }}
            className="h-[280px]"
          >
            <AreaChart data={chartData} margin={{ left: 12, right: 12 }}>
              <CartesianGrid stroke="var(--k-border)" strokeDasharray="3 3" />
              <XAxis dataKey="date" tickMargin={8} />
              <YAxis tickMargin={8} />
              <ChartTooltip content={<DailyTooltip />} />
              <Area type="monotone" dataKey="equity" stroke="var(--color-equity)" fill="var(--color-equity)" fillOpacity={0.2} />
              <Area type="monotone" dataKey="cash" stroke="var(--color-cash)" fill="var(--color-cash)" fillOpacity={0.15} />
            </AreaChart>
          </ChartContainer>
        </div>
        <div className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-2 text-sm font-medium">回撤曲线</div>
          <ChartContainer
            config={{
              drawdown: { label: '回撤', color: 'hsl(0, 75%, 55%)' },
            }}
            className="h-[280px]"
          >
            <LineChart data={drawdownData} margin={{ left: 12, right: 12 }}>
              <CartesianGrid stroke="var(--k-border)" strokeDasharray="3 3" />
              <XAxis dataKey="date" tickMargin={8} />
              <YAxis tickMargin={8} />
              <ChartTooltip
                content={
                  <ChartTooltipContent
                    formatter={(v) => fmtPct(Number(v))}
                    labelClassName="text-[var(--k-text)]"
                  />
                }
              />
              <Line type="monotone" dataKey="drawdown" stroke="var(--color-drawdown)" strokeWidth={2} dot={false} />
            </LineChart>
          </ChartContainer>
        </div>
      </div>

      <div className="mt-6 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
          <div className="text-sm font-medium">每日日志</div>
          <div className="flex w-full flex-wrap items-center justify-end gap-3 md:w-auto">
            <label className="flex items-center gap-2 text-xs text-[var(--k-muted)]">
              <input
                type="checkbox"
                className="h-4 w-4 accent-[var(--k-text)]"
                checked={onlyActive}
                onChange={(e) => setOnlyActive(e.target.checked)}
              />
              只看有操作
            </label>
            <input
              className="h-9 w-full max-w-xs rounded-md border border-[var(--k-border)] bg-transparent px-3 text-sm"
              placeholder="筛选股票代码/原因"
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
            />
          </div>
        </div>
        <div className="max-h-[420px] overflow-auto rounded-lg border border-[var(--k-border)]">
          <table className="w-full border-collapse text-xs">
            <thead className="sticky top-0 bg-[var(--k-surface-2)] text-[var(--k-muted)]">
              <tr className="text-left">
                <th className="px-3 py-2">日期</th>
                <th className="px-3 py-2">现金(开盘)</th>
                <th className="px-3 py-2">现金(收盘)</th>
                <th className="px-3 py-2">权益</th>
                <th className="px-3 py-2">选股</th>
                <th className="px-3 py-2">持仓</th>
                <th className="px-3 py-2">指令</th>
                <th className="px-3 py-2">诊断</th>
              </tr>
            </thead>
            <tbody>
              {visibleLog.map((d) => (
                <tr key={d.date} className="border-t border-[var(--k-border)]">
                  <td className="px-3 py-2 font-mono">{d.date}</td>
                  <td className="px-3 py-2 font-mono">{fmtNum(d.cash_before)}</td>
                  <td className="px-3 py-2 font-mono">{fmtNum(d.cash)}</td>
                  <td className="px-3 py-2 font-mono">{fmtNum(d.equity)}</td>
                  <td className="px-3 py-2">
                    <div className="flex flex-col gap-1">
                      {d.selected.slice(0, 3).map((s) => (
                        <div key={s.ts_code} className="font-mono">
                          {s.ts_code} 分数 {fmtNum(s.score)} 均价 {fmtNum(s.avg_price)}
                        </div>
                      ))}
                    </div>
                  </td>
                  <td className="px-3 py-2">
                    <div className="flex flex-col gap-1">
                      {d.positions.length === 0 ? (
                        <div className="text-[var(--k-muted)]">—</div>
                      ) : (
                        d.positions.slice(0, 5).map((p) => (
                          <div key={p.ts_code} className="font-mono">
                            {p.ts_code} qty {fmtNum(p.qty)}
                          </div>
                        ))
                      )}
                    </div>
                  </td>
                  <td className="px-3 py-2">
                    <div className="flex flex-col gap-1">
                      {d.orders
                        .filter((o) => o.status === 'executed')
                        .map((o, idx) => (
                        <div key={`${o.ts_code}-${idx}`} className="font-mono">
                          {o.status ?? '-'} {o.action === 'buy' ? '买入' : o.action === 'sell' ? '卖出' : o.action}{' '}
                          {o.ts_code} 数量 {fmtNum(o.exec_qty ?? null)} 价格 {fmtNum(o.exec_price ?? null)}{' '}
                          {o.reason ?? ''}
                        </div>
                      ))}
                    </div>
                  </td>
                  <td className="px-3 py-2">
                    {d.strategy_stats ? (
                      <div className="font-mono text-xs text-[var(--k-muted)]">
                        regime={d.strategy_stats.regime} bars={d.strategy_stats.bars} breakout=
                        {d.strategy_stats.breakout_ok} pullback={d.strategy_stats.pullback_ok} buy=
                        {d.strategy_stats.buy_signal}
                      </div>
                    ) : (
                      <div className="text-[var(--k-muted)]">—</div>
                    )}
                  </td>
                </tr>
              ))}
              {visibleLog.length === 0 ? (
                <tr>
                  <td className="px-3 py-6 text-center text-sm text-[var(--k-muted)]" colSpan={8}>
                    无日志记录
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>

      <RunModal
        open={modalOpen}
        busy={busy}
        form={form}
        setForm={setForm}
        onClose={() => setModalOpen(false)}
        onRun={runBacktest}
      />
    </div>
  );
}
