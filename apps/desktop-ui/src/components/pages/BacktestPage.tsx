'use client';

import React from 'react';

import { Activity, BarChart3, ShieldAlert, TrendingDown } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

import {
  GATE_LEVELS,
  useBacktestRunQuery,
  useCorrelationStatusQuery,
  useExitAttributionQuery,
  useSensitivityQuery,
  type BacktestParams,
} from '@/lib/queries/backtest';

const DEFAULT_PARAMS: BacktestParams = {
  start: '2026-06-18',
  end: new Date().toISOString().slice(0, 10),
  scoreThreshold: 85,
  maxHoldDays: 5,
  stopLossPct: -5,
  gates: 'full',
};

const INPUT_CLS =
  'h-7 rounded-md border border-[var(--k-border)] bg-transparent px-2 text-xs tabular-nums outline-none focus:border-[var(--k-accent)]';

function pct(v: number | null, digits = 1): string {
  return v == null ? '—' : `${v.toFixed(digits)}%`;
}

function winRate(v: number | null): string {
  return v == null ? '—' : `${(v * 100).toFixed(1)}%`;
}

function StatCard({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="rounded-md border border-[var(--k-border)] px-2.5 py-2">
      <div className="text-[10px] text-[var(--k-muted)]">{label}</div>
      <div className="mt-0.5 text-lg font-semibold tabular-nums leading-none">{value}</div>
      {sub && <div className="mt-1 text-[10px] text-[var(--k-muted)]">{sub}</div>}
    </div>
  );
}

function tone(v: number | null): string {
  if (v == null) return 'text-[var(--k-muted)]';
  return v >= 0 ? 'text-emerald-700 dark:text-emerald-300' : 'text-red-700 dark:text-red-400';
}

export function BacktestPage() {
  const [params, setParams] = React.useState<BacktestParams>(DEFAULT_PARAMS);
  const [submitted, setSubmitted] = React.useState<BacktestParams>(DEFAULT_PARAMS);
  const [attempt, setAttempt] = React.useState(0);
  const [gridOn, setGridOn] = React.useState(false);

  const runQ = useBacktestRunQuery(submitted, attempt);
  const sensQ = useSensitivityQuery(DEFAULT_PARAMS.start, DEFAULT_PARAMS.end, gridOn);
  const exitQ = useExitAttributionQuery(5);
  const corrQ = useCorrelationStatusQuery(true, true);

  const set = (k: keyof BacktestParams, v: string | number) =>
    setParams((p) => ({ ...p, [k]: typeof v === 'number' ? v : Number(v) }));

  const s = runQ.data?.summary;

  return (
    <div className="flex flex-col gap-4 p-4">
      {/* 参数区 */}
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
        <div className="mb-2 flex items-center gap-2 text-[12px] font-medium">
          <Activity className="size-3.5" />
          回测 · 参数（信号 = 历史实际 TrendOK 分 · 平仓逻辑与 live paper 同码）
        </div>
        <div className="flex flex-wrap items-end gap-3">
          <label className="flex flex-col gap-1 text-[10px] text-[var(--k-muted)]">
            开始
            <input
              type="date"
              className={INPUT_CLS}
              value={params.start}
              onChange={(e) => set('start', e.target.value)}
            />
          </label>
          <label className="flex flex-col gap-1 text-[10px] text-[var(--k-muted)]">
            结束
            <input
              type="date"
              className={INPUT_CLS}
              value={params.end}
              onChange={(e) => set('end', e.target.value)}
            />
          </label>
          <label className="flex flex-col gap-1 text-[10px] text-[var(--k-muted)]">
            Score 阈值
            <input
              type="number"
              className={cn(INPUT_CLS, 'w-20')}
              value={params.scoreThreshold}
              onChange={(e) => set('scoreThreshold', e.target.value)}
            />
          </label>
          <label className="flex flex-col gap-1 text-[10px] text-[var(--k-muted)]">
            Max hold（天）
            <input
              type="number"
              className={cn(INPUT_CLS, 'w-20')}
              value={params.maxHoldDays}
              onChange={(e) => set('maxHoldDays', e.target.value)}
            />
          </label>
          <label className="flex flex-col gap-1 text-[10px] text-[var(--k-muted)]">
            止损 %
            <input
              type="number"
              className={cn(INPUT_CLS, 'w-20')}
              value={params.stopLossPct}
              onChange={(e) => set('stopLossPct', e.target.value)}
            />
          </label>
          <label className="flex flex-col gap-1 text-[10px] text-[var(--k-muted)]">
            入池闸门
            <select
              className={INPUT_CLS}
              value={params.gates}
              onChange={(e) => set('gates', e.target.value)}
            >
              {GATE_LEVELS.map((g) => (
                <option key={g.value} value={g.value}>
                  {g.label}
                </option>
              ))}
            </select>
          </label>
          <Button
            size="sm"
            disabled={runQ.isFetching}
            onClick={() => {
              setSubmitted(params);
              setAttempt((a) => a + 1);
              setGridOn(false);
            }}
          >
            {runQ.isFetching ? '计算中…' : '运行回测'}
          </Button>
          <Button
            size="sm"
            variant="outline"
            onClick={() => {
              setGridOn((g) => !g);
              setSubmitted(DEFAULT_PARAMS);
            }}
          >
            {gridOn ? '关闭网格' : '敏感度网格 (36)'}
          </Button>
        </div>
        <div className="mt-2 text-[10px] text-[var(--k-muted)]">
          净口径：已扣除往返成本（CN 0.30%）。入池闸门与实盘同规则：regime=指数红绿灯
          全绿才开新仓；full=红绿灯 + 全行业资金流不转负 + 个股行业在 5D 净流入 Top3
          （数据缺失 fail-closed 拦截）。仅参数敏感度参考，不作发布依据——以 paper 实绩为准。
        </div>
      </div>

      {/* 单配置结果 */}
      {!gridOn && (
        <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
          <div className="mb-2 flex items-center gap-2 text-[12px] font-medium">
            <BarChart3 className="size-3.5" />
            单配置结果
            {runQ.isFetching && <span className="text-[10px] text-[var(--k-muted)]">计算中…</span>}
          </div>
          {runQ.isError ? (
            <p className="text-xs text-red-700">{String(runQ.error)}</p>
          ) : s ? (
            <div className="grid grid-cols-2 gap-2 md:grid-cols-4 lg:grid-cols-6">
              <StatCard label="已平仓交易" value={String(s.closed)} sub={`窗口 ${s.calendar_days} 个交易日`} />
              <StatCard label="胜率（净）" value={winRate(s.win_rate)} sub={`${s.wins} 胜 / ${s.losses} 负`} />
              <StatCard label="平均净盈亏" value={pct(s.avg_net_pnl_pct)} sub={`毛 ${pct(s.avg_gross_pnl_pct)} · 成本 ${pct(s.avg_costs_pct)}`} />
              <StatCard label="最大回撤" value={pct(s.max_drawdown_pct, 1)} sub="累计净盈亏曲线" />
              <StatCard label="窗口末持仓" value={String(s.open_at_end)} sub="无法定价的仓位" />
              <StatCard
                label="分档胜率"
                value={Object.keys(s.by_score_bucket).length ? '分档' : '—'}
                sub={Object.entries(s.by_score_bucket)
                  .map(([b, v]) => `${b}:${(v.winRate ?? 0).toFixed(2)}`)
                  .join(' · ')}
              />
              <StatCard
                label="闸门拦截"
                value={String(Object.values(s.gated_blocks).reduce((a, b) => a + b, 0))}
                sub={Object.entries(s.gated_blocks)
                  .map(([k, v]) => `${k}×${v}`)
                  .join(' · ')}
              />
            </div>
          ) : (
            <p className="text-xs text-[var(--k-muted)]">点「运行回测」开始。</p>
          )}
        </div>
      )}

      {/* 敏感度网格 */}
      {gridOn && (
        <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
          <div className="mb-2 flex items-center gap-2 text-[12px] font-medium">
            <BarChart3 className="size-3.5" />
            敏感度网格（score × hold × stop × 闸门 = 72 组 · 默认窗口）
            {sensQ.isFetching && <span className="text-[10px] text-[var(--k-muted)]">计算中（约 60s）…</span>}
          </div>
          {sensQ.isError ? (
            <p className="text-xs text-red-700">{String(sensQ.error)}</p>
          ) : sensQ.data ? (
            <div className="max-h-[480px] overflow-auto">
              <table className="w-full text-left text-xs tabular-nums">
                <thead className="sticky top-0 bg-[var(--k-surface)]">
                  <tr className="text-[10px] text-[var(--k-muted)]">
                    <th className="py-1 pr-2">闸门</th>
                    <th className="py-1 pr-2">score</th>
                    <th className="py-1 pr-2">hold</th>
                    <th className="py-1 pr-2">stop</th>
                    <th className="py-1 pr-2">trades</th>
                    <th className="py-1 pr-2">胜率</th>
                    <th className="py-1 pr-2">均净%</th>
                    <th className="py-1 pr-2">maxDD%</th>
                  </tr>
                </thead>
                <tbody>
                  {[...sensQ.data.results]
                    .sort((a, b) => (b.win_rate ?? -1) - (a.win_rate ?? -1))
                    .map((r, i) => (
                      <tr key={i} className="border-t border-[var(--k-border)]/60">
                        <td className="py-1 pr-2 text-[var(--k-muted)]">{r.config.gates}</td>
                        <td className="py-1 pr-2">{r.config.score_threshold.toFixed(0)}</td>
                        <td className="py-1 pr-2">{r.config.max_hold_days}</td>
                        <td className="py-1 pr-2">{r.config.stop_loss_pct.toFixed(0)}</td>
                        <td className="py-1 pr-2">{r.closed}</td>
                        <td className="py-1 pr-2 font-medium">{winRate(r.win_rate)}</td>
                        <td className={cn('py-1 pr-2', tone(r.avg_net_pnl_pct))}>{pct(r.avg_net_pnl_pct)}</td>
                        <td className="py-1 pr-2">{pct(r.max_drawdown_pct, 1)}</td>
                      </tr>
                    ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="text-xs text-[var(--k-muted)]">点「敏感度网格」运行（约 30s）。</p>
          )}
        </div>
      )}

      {/* 组合相关性（V7.0-01 · L3-P5） */}
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
        <div className="mb-2 flex items-center gap-2 text-[12px] font-medium">
          <ShieldAlert className="size-3.5" />
          组合相关性防火墙（因子簇 ≥30% 拦新开仓 · 不强制平仓）
          {corrQ.isFetching && <span className="text-[10px] text-[var(--k-muted)]">计算中…</span>}
        </div>
        {corrQ.isError ? (
          <p className="text-xs text-red-700">{String(corrQ.error)}</p>
        ) : corrQ.data ? (
          <div className="flex flex-col gap-3">
            <div className="grid grid-cols-2 gap-2 md:grid-cols-3">
              {Object.entries(corrQ.data.clusters)
                .sort((a, b) => b[1].exposurePct - a[1].exposurePct)
                .filter(([, c]) => c.exposurePct > 0)
                .map(([name, c]) => {
                  const over = corrQ.data.overLimit.includes(name);
                  return (
                    <div
                      key={name}
                      className={cn(
                        'rounded-md border px-2.5 py-2',
                        over
                          ? 'border-red-500/50 bg-red-500/5'
                          : 'border-[var(--k-border)]',
                      )}
                    >
                      <div className="flex items-center justify-between">
                        <span className="text-[11px] font-medium">{c.label}</span>
                        <span className={cn('text-xs font-semibold tabular-nums', over ? 'text-red-600' : '')}>
                          {c.exposurePct.toFixed(1)}%
                        </span>
                      </div>
                      <div className="mt-1 text-[10px] text-[var(--k-muted)]">
                        {c.symbols.join(' · ')}
                        {over && <span className="ml-1 text-red-600">（超限 · 新开仓被拦）</span>}
                      </div>
                    </div>
                  );
                })}
              {!Object.keys(corrQ.data.clusters).length && (
                <p className="text-xs text-[var(--k-muted)]">无持仓数据。</p>
              )}
            </div>
            {corrQ.data.topPairs.length > 0 && (
              <div className="text-[11px] text-[var(--k-muted)]">
                <span className="font-medium">高相关对（20 日收益率 r&gt;0.75）：</span>
                {corrQ.data.topPairs.map(([a, b, r]) => (
                  <span key={`${a}-${b}`} className="mr-3">
                    {a} × {b} = <span className="tabular-nums">{r.toFixed(2)}</span>
                  </span>
                ))}
              </div>
            )}
            {corrQ.data.empiricalNote && (
              <p className="text-[10px] text-[var(--k-muted)]">{corrQ.data.empiricalNote}</p>
            )}
          </div>
        ) : (
          <p className="text-xs text-[var(--k-muted)]">加载中…</p>
        )}
      </div>

      {/* 卖出归因 */}
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
        <div className="mb-2 flex items-center gap-2 text-[12px] font-medium">
          <TrendingDown className="size-3.5" />
          卖出归因（平仓后 {exitQ.data?.days ?? 5} 个交易日前向收益 · 样本 {exitQ.data?.withForwardCount ?? 0}）
        </div>
        {exitQ.isError ? (
          <p className="text-xs text-red-700">{String(exitQ.error)}</p>
        ) : exitQ.data && exitQ.data.insufficient ? (
          <p className="text-xs text-[var(--k-muted)]">
            {exitQ.data.closedCount === 0
              ? 'paper 暂无平仓记录——系统刚起步，继续积累后这里才会出数字。'
              : exitQ.data.hint}
          </p>
        ) : exitQ.data ? (
          <div className="flex flex-col gap-3">
            <div className="grid grid-cols-2 gap-2 md:grid-cols-4">
              <StatCard label="总体前向均值" value={pct(exitQ.data.overall.avgFwdPct)} sub="卖后 N 日收益" />
              <StatCard label="卖早率" value={winRate(exitQ.data.overall.earlyRate)} sub={`≥ +2% 仍上涨 ${exitQ.data.overall.earlyCount} 笔`} />
              <StatCard label="卖对率" value={winRate(exitQ.data.overall.wellRate)} sub={`≤ -1% 卖出后下跌 ${exitQ.data.overall.wellCount} 笔`} />
              <StatCard
                label="最多同时持仓"
                value={String(exitQ.data.exposure.maxSimultaneous)}
                sub={
                  exitQ.data.exposure.singleStockWeightFloorPct != null
                    ? `单票权重下界 ${exitQ.data.exposure.singleStockWeightFloorPct}%（红线 15%/板块 30%）`
                    : '—'
                }
              />
            </div>
            <div className="overflow-auto">
              <table className="w-full text-left text-xs tabular-nums">
                <thead>
                  <tr className="text-[10px] text-[var(--k-muted)]">
                    <th className="py-1 pr-3">平仓理由</th>
                    <th className="py-1 pr-3">已平仓</th>
                    <th className="py-1 pr-3">前向样本</th>
                    <th className="py-1 pr-3">平均前向%</th>
                    <th className="py-1 pr-3">卖早率</th>
                    <th className="py-1 pr-3">卖对率</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(exitQ.data.byReason).map(([reason, b]) => (
                    <tr key={reason} className="border-t border-[var(--k-border)]/60">
                      <td className="py-1 pr-3">{b.label || reason}</td>
                      <td className="py-1 pr-3">{b.count}</td>
                      <td className="py-1 pr-3">{b.withForward ?? '—'}</td>
                      <td className={cn('py-1 pr-3', tone(b.avgFwdPct ?? null))}>{pct(b.avgFwdPct ?? null)}</td>
                      <td className="py-1 pr-3">{b.earlyRate != null ? `${(b.earlyRate * 100).toFixed(0)}%` : '—'}</td>
                      <td className="py-1 pr-3">{b.wellRate != null ? `${(b.wellRate * 100).toFixed(0)}%` : '—'}</td>
                    </tr>
                  ))}
                  {!Object.keys(exitQ.data.byReason).length && (
                    <tr>
                      <td colSpan={6} className="py-2 text-[var(--k-muted)]">
                        暂无归因数据。
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
            <p className="text-[10px] text-[var(--k-muted)]">{exitQ.data.exposure.note}</p>
          </div>
        ) : (
          <p className="text-xs text-[var(--k-muted)]">加载中…</p>
        )}
      </div>
    </div>
  );
}
