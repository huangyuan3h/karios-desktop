'use client';

import React from 'react';

import { Activity, BarChart3, ChevronDown, ShieldAlert, TrendingDown } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { RecentDailyCompareCard } from '@/components/pages/RecentDailyCompareCard';
import { ReplicaGapCard } from '@/components/pages/ReplicaGapCard';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { cn } from '@/lib/utils';

import {
  GATE_LEVELS,
  useBacktestOverviewQuery,
  useBacktestReconQuery,
  useBacktestRunQuery,
  useCoreAuditQuery,
  useCorrelationStatusQuery,
  useExitAttributionQuery,
  usePaperVsBacktestQuery,
  useReturnAttributionQuery,
  useSensitivityQuery,
  useSleeveNavQuery,
  useTimelineQuery,
  type BacktestOverviewBaseline,
  type BacktestOverviewWindow,
  type BacktestParams,
} from '@/lib/queries/backtest';

const DEFAULT_PARAMS: BacktestParams = {
  start: '2025-08-01',
  end: new Date().toISOString().slice(0, 10),
  scoreThreshold: 65,
  maxHoldDays: 60,
  stopLossPct: -5,
  gates: 'full',
  trailingStopPct: -8,
  positionPct: 0.1,
  maxPositions: 10,
  rsRankMin: 0.5,
  divergingScale: 1,
  targetPnlPct: 100,
  scoreFloor: 0,
  panicCooldownDays: 2,
  slippagePct: 0.05,
  excludeBoards: '300',
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

const CN_PARAMS = [
  ['score', '65'], ['RS 前', '50%'], ['止损', '-5%'], ['移动', '-8%/ATR'],
  ['持有', '≤60 天'], ['仓位', '10%×10'], ['入场', '次日开盘'], ['闸门', 'full'], ['熔断', '-25%'],
  ['恐慌冷却', '2d'], ['流动性', '≥0.7亿'],
];

const HK_PARAMS = [
  ['score', '65'], ['RS 前', '40%'], ['止损', '-5%'], ['移动', '-12%'],
  ['持有', '≤60 天'], ['仓位', '10%×10'], ['入场', '次日开盘'], ['闸门', 'regime'],
];

function fmtDate(iso?: string | null): string {
  if (!iso) return '—';
  return iso.slice(0, 10);
}

function WindowRow({
  name,
  w,
}: {
  name: string;
  w: BacktestOverviewWindow | undefined;
}) {
  const winRate = w?.winRate != null ? `${(w.winRate * 100).toFixed(1)}%` : '—';
  return (
    <div className="flex items-center gap-2 border-t border-[var(--k-border)]/60 py-1 text-[11px] tabular-nums">
      <span className="w-12 shrink-0 font-mono text-[10px] text-[var(--k-muted)]">{name}</span>
      <span className={cn('font-semibold', tone(w?.totalNetPnlPct ?? null))}>
        {w?.totalNetPnlPct != null ? `${w.totalNetPnlPct.toFixed(1)}%` : '—'}
      </span>
      <span className="text-[var(--k-muted)]">胜率 {winRate}</span>
      <span className="text-[var(--k-muted)]">
        DD {w?.maxDrawdownPct != null ? `${w.maxDrawdownPct.toFixed(1)}%` : '—'}
      </span>
      <span className="text-[var(--k-muted)]">夏普 {w?.sharpe ?? '—'}</span>
      <span className="ml-auto text-[var(--k-muted)]">
        {w?.trades != null ? `${w.trades} 笔` : ''}
      </span>
    </div>
  );
}

function BaselinePanel({
  title,
  tag,
  baseline,
  params,
  extra,
}: {
  title: string;
  tag: string;
  baseline: BacktestOverviewBaseline | null | undefined;
  params: string[][];
  extra?: React.ReactNode;
}) {
  const windows = baseline?.windows ?? {};
  const order = ['OOS2', 'train', 'valid'];
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/60 p-2.5">
      <div className="mb-1 flex items-center gap-2 text-[11px] font-semibold">
        <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5">{tag}</span>
        {title}
        <span className="ml-auto text-[10px] font-normal tabular-nums text-[var(--k-muted)]">
          基线 {fmtDate(baseline?.generatedAt)}
        </span>
      </div>
      <div className="flex flex-wrap gap-1">
        {params.map(([k, v]) => (
          <span
            key={k}
            className="rounded bg-[var(--k-surface)] px-1.5 py-0.5 text-[10px] text-[var(--k-muted)]"
            title="回测定案参数（strategy-params.md §1）"
          >
            {k} {v}
          </span>
        ))}
      </div>
      <div className="mt-1.5 flex flex-col">
        {order.map((name) => (
          <WindowRow key={name} name={name} w={windows[name]} />
        ))}
      </div>
      <p className="mt-1 text-[10px] leading-tight text-[var(--k-muted)]">
        收益/夏普/回撤均基于<span className="font-medium">连续净值曲线</span>（每日盯市所有持仓），
        夏普为真实年化（非平仓日口径）；收益为复利口径。
      </p>
      {extra}
    </div>
  );
}

function ConclusionBoard({ overview }: { overview: ReturnType<typeof useBacktestOverviewQuery>['data'] }) {
  const long = overview?.longWindowCN;
  const byYear = long?.byYear ?? {};
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
      <div className="mb-2 flex items-center gap-2 text-[12px] font-medium">
        <BarChart3 className="size-3.5" />
        S-3 股票腿三窗（非产品主策略 · 择强单轨见上方 Timeline）
        <span className="ml-auto text-[10px] font-normal text-[var(--k-muted)]">
          固化 walk_forward_baseline · 仅 STOCK 腿生成器
        </span>
      </div>
      <div className="grid gap-2 md:grid-cols-2">
        <BaselinePanel
          title="A 股 S-3（CN 线）"
          tag="CN"
          baseline={overview?.cnBaseline}
          params={CN_PARAMS}
          extra={
            long ? (
              <div className="mt-1.5 rounded-md border border-[var(--k-accent)]/30 bg-[var(--k-accent)]/5 px-2 py-1.5">
                <div className="flex flex-wrap items-baseline gap-x-3 text-[11px]">
                  <span className="text-[10px] text-[var(--k-muted)]">长窗 {long.window}</span>
                  <span className={cn('font-semibold', tone(long.totalNetPnlPct ?? null))}>
                    {long.totalNetPnlPct != null ? `+${long.totalNetPnlPct}%` : '—'}
                  </span>
                  <span className="text-[var(--k-muted)]">DD {long.maxDrawdownPct}%</span>
                  <span className="text-[var(--k-muted)]">夏普 {long.sharpe}</span>
                  <span className="text-[var(--k-muted)]">{long.trades} 笔</span>
                </div>
                <div className="mt-1 flex flex-wrap gap-x-2 text-[10px] tabular-nums text-[var(--k-muted)]">
                  {Object.entries(byYear).map(([y, v]) => (
                    <span key={y}>
                      {y} <span className={cn('font-mono', tone(v ?? null))}>{v != null ? `${v >= 0 ? '+' : ''}${v}` : '—'}</span>
                    </span>
                  ))}
                </div>
              </div>
            ) : null
          }
        />
        <BaselinePanel
          title="港股 S-3（HK 线）"
          tag="HK"
          baseline={overview?.hkBaseline}
          params={HK_PARAMS}
          extra={
            <p className="mt-1.5 text-[10px] text-[var(--k-muted)]">
              长窗仅 CN 线验证（2026-08-12 定案）；HK 以三窗为准。
            </p>
          }
        />
      </div>
      <p className="mt-2 text-[10px] text-[var(--k-muted)]">
        产品真值 = 择强单轨（docs/modules/pick-strong-track.md）。本卡仅为 S-3 股票腿基线；
        paper/watchlist 股票规则同参（10%×10、恐慌冷却 2、CN 熔断 -25%）。
      </p>
    </div>
  );
}

function CoreAuditCard({ q }: { q: ReturnType<typeof useCoreAuditQuery> }) {
  const holdings = q.data?.holdings ?? [];
  const counts = q.data?.counts;
  const gate = q.data?.gate;
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
      <div className="mb-2 flex items-center gap-2 text-[12px] font-medium">
        <ShieldAlert className="size-3.5" />
        核心仓操作核对（手动交易 vs 策略规则）
        <span className="ml-auto text-[10px] font-normal text-[var(--k-muted)]">
          {q.data?.day ?? ''} · 闸门 {gate?.regime ?? '—'}
          {gate?.panicActive ? ' · 恐慌中' : ''} · {gate?.gateOpen ? '开' : '关'}
        </span>
      </div>
      {q.isError ? (
        <p className="text-xs text-red-700">{String(q.error)}</p>
      ) : holdings.length ? (
        <div className="flex flex-col gap-2.5">
          <div className="flex flex-wrap gap-2 text-[11px]">
            <span className="rounded-md border border-emerald-500/40 bg-emerald-500/5 px-2 py-0.5 text-emerald-700 dark:text-emerald-300">
              符合 {counts?.ok ?? 0}
            </span>
            <span className="rounded-md border border-amber-500/40 bg-amber-500/5 px-2 py-0.5 text-amber-700 dark:text-amber-300">
              偏离 {counts?.warn ?? 0}
            </span>
            <span className="rounded-md border border-red-500/40 bg-red-500/5 px-2 py-0.5 text-red-700 dark:text-red-300">
              违反 {counts?.violation ?? 0}
            </span>
          </div>
          {holdings.map((h) => (
            <div key={h.symbol} className="rounded-md border border-[var(--k-border)] p-2">
              <div className="flex flex-wrap items-baseline gap-x-3 text-[11px]">
                <span className="font-medium">{h.symbol}</span>
                <span className="text-[var(--k-muted)]">{h.name}</span>
                <span>仓位 {h.positionPct ?? 0}%</span>
                <span>成本 {h.costPrice ?? '—'}</span>
                {h.pyramidTriggerLine != null && (
                  <span className="text-[var(--k-muted)]">金字塔线 {h.pyramidTriggerLine} · {h.pyramidAdded ? '已加' : '未加'}</span>
                )}
              </div>
              <div className="mt-1 flex flex-col gap-1">
                {(h.ops ?? []).map((op, i) => (
                  <div key={i} className="flex items-start gap-2 text-[11px] leading-tight">
                    <span
                      className={cn(
                        'mt-0.5 shrink-0 rounded px-1 py-px text-[9px]',
                        op.verdict === 'ok'
                          ? 'bg-emerald-500/10 text-emerald-700 dark:text-emerald-300'
                          : op.verdict === 'warn'
                            ? 'bg-amber-500/10 text-amber-700 dark:text-amber-300'
                            : 'bg-red-500/10 text-red-700 dark:text-red-300',
                      )}
                    >
                      {op.verdict === 'ok' ? '符合' : op.verdict === 'warn' ? '偏离' : '违反'}
                    </span>
                    <span className="tabular-nums text-[var(--k-muted)]">
                      {op.date} {op.side} {op.price} · {op.positionPct}%
                    </span>
                    <span className="text-[var(--k-muted)]">{op.detail}</span>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      ) : (
        <p className="text-xs text-[var(--k-muted)]">
          暂无手动交易记录——核心仓操作后这里会出规则核对。
        </p>
      )}
    </div>
  );
}

function SleeveNavCard({ q }: { q: ReturnType<typeof useSleeveNavQuery> }) {
  const report = q.data?.report;
  const results = report?.results;
  const rows = results ? Object.entries(results) : [];
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
      <div className="mb-2 flex items-center gap-2 text-[12px] font-medium">
        <TrendingDown className="size-3.5" />
        历史 · 闲置现金套筒 NAV（非定案；定案见上方 Timeline 择强单轨）
        <span className="ml-auto text-[10px] font-normal tabular-nums text-[var(--k-muted)]">
          {report?.generatedAt ? `报告 ${report.generatedAt.slice(0, 10)}` : ''}
        </span>
      </div>
      {q.isError ? (
        <p className="text-xs text-red-700">{String(q.error)}</p>
      ) : rows.length ? (
        <div className="flex flex-col gap-2">
          <div className="overflow-auto">
            <table className="w-full text-left text-xs tabular-nums">
              <thead>
                <tr className="text-[10px] text-[var(--k-muted)]">
                  <th className="py-1 pr-2">窗口</th>
                  <th className="py-1 pr-2">基线收益%</th>
                  <th className="py-1 pr-2">套筒收益%</th>
                  <th className="py-1 pr-2">增量pt</th>
                  <th className="py-1 pr-2">基线DD%</th>
                  <th className="py-1 pr-2">套筒DD%</th>
                  <th className="py-1 pr-2">持有天数</th>
                  <th className="py-1 pr-2">平均闲置%</th>
                </tr>
              </thead>
              <tbody>
                {rows.map(([w, r]) => (
                  <tr key={w} className="border-t border-[var(--k-border)]">
                    <td className="py-1 pr-2 font-medium">{w}</td>
                    <td className="py-1 pr-2">{r.totalBasePct ?? '—'}</td>
                    <td className="py-1 pr-2">{r.totalSleevePct ?? '—'}</td>
                    <td className={cn('py-1 pr-2 font-semibold', tone(r.deltaPct ?? null))}>
                      {r.deltaPct != null ? `${r.deltaPct >= 0 ? '+' : ''}${r.deltaPct}` : '—'}
                    </td>
                    <td className="py-1 pr-2">{r.maxDdBasePct ?? '—'}</td>
                    <td className="py-1 pr-2">{r.maxDdSleevePct ?? '—'}</td>
                    <td className="py-1 pr-2">{r.holdDays ?? '—'}</td>
                    <td className="py-1 pr-2">{r.avgIdlePct ?? '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="text-[10px] text-[var(--k-muted)]">
            口径（2026-08-29）：基线 = S-3 引擎现金+MTM NAV（与 walk_forward 同码）；闲置吃
            513100（MA200 + trail -8%）或 GC001。验证：scripts/sleeve_nav_sim.py（三窗增量≥0）。
          </p>
        </div>
      ) : (
        <p className="text-xs text-[var(--k-muted)]">
          尚无套筒 NAV 报告——先跑 scripts/sleeve_nav_sim.py 生成三窗对比。
        </p>
      )}
    </div>
  );
}

function RollingOosCard({ overview }: { overview: ReturnType<typeof useBacktestOverviewQuery>['data'] }) {
  const ro = overview?.rollingOos;
  if (!ro) return null;
  const markets = ro.markets ?? {};
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
      <div className="mb-2 flex items-center gap-2 text-[12px] font-medium">
        <Activity className="size-3.5" />
        滚动 OOS（最近 90 天 · 每月首个周一自动跑）
        <span className="ml-auto text-[10px] font-normal tabular-nums text-[var(--k-muted)]">
          {ro.windowStart} ~ {ro.windowEnd}
        </span>
      </div>
      {ro.warning ? (
        <div className="mb-2 rounded-md border border-red-500/40 bg-red-500/5 px-2 py-1.5 text-[11px] text-red-700 dark:text-red-300">
          ⚠ {(ro.warnings ?? []).join(' · ') || '近期窗异常：亏损 / 夏普为负 / 零交易'}
        </div>
      ) : null}
      <div className="flex flex-col gap-1">
        {(['CN', 'HK'] as const).map((m) => {
          const r = markets[m];
          if (!r) return null;
          const bad = r.closed === 0 || r.sharpe != null && r.sharpe < 0 || (r.totalNetPnlPct ?? 0) < 0;
          return (
            <div key={m} className="flex flex-wrap items-center gap-x-3 gap-y-0.5 text-[11px] tabular-nums">
              <span className={cn('w-7 font-semibold', bad ? 'text-red-600 dark:text-red-400' : 'text-[var(--k-fg)]')}>
                {m === 'CN' ? 'A股' : '港股'}
              </span>
              <span className={cn('font-semibold', tone(r.totalNetPnlPct ?? null))}>
                {r.totalNetPnlPct != null ? `${r.totalNetPnlPct.toFixed(1)}%` : '—'}
              </span>
              <span className="text-[var(--k-muted)]">胜率 {r.winRate != null ? `${(r.winRate * 100).toFixed(1)}%` : '—'}</span>
              <span className="text-[var(--k-muted)]">DD {r.maxDrawdownPct != null ? `${r.maxDrawdownPct.toFixed(1)}%` : '—'}</span>
              <span className="text-[var(--k-muted)]">夏普 {r.sharpe ?? '—'}</span>
              <span className="ml-auto text-[var(--k-muted)]">{r.closed ?? 0} 笔</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function ReconStrip({ reconQ }: { reconQ: ReturnType<typeof useBacktestReconQuery> }) {
  const items = reconQ.data?.items ?? [];
  if (!items.length) return null;
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
      <div className="mb-1.5 flex items-center gap-2 text-[12px] font-medium">
        <ShieldAlert className="size-3.5" />
        回测 vs Paper 对账（每周一自动对账上周五）
      </div>
      <div className="flex flex-col gap-1">
        {items.map((r) => {
          const clean = r.missing === 0 && r.extra === 0;
          const market = r.market === 'HK' ? '港股' : 'A股';
          return (
            <div key={`${r.reconDate}-${r.market}`} className="flex flex-wrap items-center gap-x-3 text-[11px] tabular-nums">
              <span className={clean ? 'text-emerald-600' : 'text-amber-600 dark:text-amber-400'}>
                {clean ? '✓' : '⚠'}
              </span>
              <span className="font-medium">{market}</span>
              <span className="text-[var(--k-muted)]">{r.reconDate}</span>
              <span className="text-[var(--k-muted)]">
                回测应持 {r.expected} · 实持 {r.actual} · 一致 {r.aligned}
              </span>
              <span className={r.missing + r.extra > 0 ? 'text-amber-600 dark:text-amber-400' : 'text-[var(--k-muted)]'}>
                缺 {r.missing} · 多 {r.extra}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function PaperVsBacktestCard({ q }: { q: ReturnType<typeof usePaperVsBacktestQuery> }) {
  const report = q.data?.report;
  const rows = report?.rows ?? [];
  const summary = report?.summary;
  const settled = (report?.sampleCount ?? 0) >= 20;
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
      <div className="mb-2 flex items-center gap-2 text-[12px] font-medium">
        <ShieldAlert className="size-3.5" />
        C4 · paper vs 回测逐笔对照（S-3/S3HK 已平仓）
        <span className="ml-auto text-[10px] font-normal tabular-nums text-[var(--k-muted)]">
          {report?.generatedAt ?? ''} 生成
        </span>
      </div>
      {q.isError ? (
        <p className="text-xs text-red-700">{String(q.error)}</p>
      ) : q.data && !rows.length ? (
        <p className="text-xs text-[var(--k-muted)]">
          暂无已平仓 S-3 交易——paper 书继续积累后这里会出对照。
        </p>
      ) : rows.length ? (
        <div className="flex flex-col gap-2.5">
          <div
            className={cn(
              'rounded-md border px-2 py-1.5 text-[11px]',
              settled
                ? 'border-emerald-500/40 bg-emerald-500/5 text-emerald-700 dark:text-emerald-300'
                : 'border-amber-500/40 bg-amber-500/5 text-amber-700 dark:text-amber-300',
            )}
          >
            {report?.verdict ?? '—'} · 已平仓 {summary?.paper?.closed ?? 0} 笔
            {!settled && '（≥20 笔后出统计定论）'}
          </div>
          <div className="grid grid-cols-2 gap-2 md:grid-cols-4">
            <StatCard label="paper 胜率" value={winRate(summary?.paper?.winRate ?? null)} sub={`${summary?.paper?.closed ?? 0} 笔已平仓`} />
            <StatCard label="paper 均盈亏" value={pct(summary?.paper?.avgPnlPct ?? null)} />
            <StatCard label="回测匹配胜率" value={winRate(summary?.backtestMatched?.winRate ?? null)} sub={`${summary?.backtestMatched?.closed ?? 0} 笔有孪生`} />
            <StatCard label="回测均盈亏" value={pct(summary?.backtestMatched?.avgPnlPct ?? null)} sub="孪生交易口径" />
          </div>
          <div className="max-h-[320px] overflow-auto">
            <table className="w-full text-left text-xs tabular-nums">
              <thead className="sticky top-0 bg-[var(--k-surface)]">
                <tr className="text-[10px] text-[var(--k-muted)]">
                  <th className="py-1 pr-2">市场</th>
                  <th className="py-1 pr-2">symbol</th>
                  <th className="py-1 pr-2">入场</th>
                  <th className="py-1 pr-2">paper pnl</th>
                  <th className="py-1 pr-2">paper 平仓原因</th>
                  <th className="py-1 pr-2">回测 pnl</th>
                  <th className="py-1 pr-2">回测原因</th>
                  <th className="py-1 pr-2">入场价差%</th>
                  <th className="py-1 pr-2">备注</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => {
                  const bt = r.backtest;
                  const entryDiff = r.diff?.entryPriceDiffPct;
                  return (
                    <tr key={`${r.symbol}-${r.entryDate}`} className="border-t border-[var(--k-border)]/60">
                      <td className="py-1 pr-2 text-[var(--k-muted)]">{r.market === 'HK' ? '港股' : 'A股'}</td>
                      <td className="py-1 pr-2 font-mono">{r.symbol}</td>
                      <td className="py-1 pr-2 text-[var(--k-muted)]">{r.entryDate}</td>
                      <td className={cn('py-1 pr-2 font-medium', tone(r.paper?.pnlPct ?? null))}>{pct(r.paper?.pnlPct ?? null)}</td>
                      <td className="py-1 pr-2 text-[var(--k-muted)]">{r.paper?.closeReason ?? '—'}</td>
                      <td className={cn('py-1 pr-2', bt ? tone(bt.pnlPct ?? null) : 'text-[var(--k-muted)]')}>{bt ? pct(bt.pnlPct ?? null) : '未入场'}</td>
                      <td className="py-1 pr-2 text-[var(--k-muted)]">{bt?.closeReason ?? '—'}</td>
                      <td className={cn('py-1 pr-2', entryDiff != null && Math.abs(entryDiff) > 0.5 ? 'text-amber-600 dark:text-amber-400' : '')}>
                        {entryDiff != null ? pct(entryDiff, 2) : '—'}
                      </td>
                      <td className="py-1 pr-2 text-[var(--k-muted)]">{r.note ?? '—'}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <p className="text-[10px] text-[var(--k-muted)]">
            孪生交易 = 回测引擎同 symbol 同入场日（否则最近入场）· 价差归因执行 vs 规则 ·
            样本 &lt;20 笔不作统计结论（C4 未定案）。paper 入场已对齐回测
            ``next_open``（信号日收盘定价 → 下一交易日开盘成交；开盘未到库时先占位，
            ``run_update`` 回填）。
          </p>
        </div>
      ) : (
        <p className="text-xs text-[var(--k-muted)]">加载中…</p>
      )}
    </div>
  );
}

function TimelineCard() {
  const today = new Date().toISOString().slice(0, 10);
  const start = (() => {
    const d = new Date();
    d.setFullYear(d.getFullYear() - 1);
    return d.toISOString().slice(0, 10);
  })();
  const [strategy, setStrategy] = React.useState<'twin_star' | 'pick_strong' | 'state_bucket'>('twin_star');
  const isTwin = strategy === 'twin_star';
  const isSgap = strategy === 'state_bucket';
  const q = useTimelineQuery(start, today, strategy, true);
  const rows = q.data?.rows ?? [];
  const ALL_PICKS = ['STOCK', 'GOLD', 'OIL', 'NASDAQ', 'BOND10', 'REPO'] as const;
  const dist = rows.reduce<Record<string, number>>((acc, r) => {
    const k = r.pick ?? 'REPO';
    acc[k] = (acc[k] ?? 0) + 1;
    return acc;
  }, {});
  const distMarket = rows.reduce<Record<string, number>>((acc, r) => {
    const sm = (r as unknown as { stockMarket?: string }).stockMarket ?? '—';
    if ((r.pick ?? 'REPO') === 'STOCK') {
      acc[sm] = (acc[sm] ?? 0) + 1;
    }
    return acc;
  }, {});
  const [showAll, setShowAll] = React.useState(false);
  const last = rows[rows.length - 1];
  const pickColor: Record<string, string> = {
    STOCK: 'bg-red-500',
    GOLD: 'bg-amber-500',
    OIL: 'bg-slate-800 dark:bg-slate-200',
    NASDAQ: 'bg-blue-600',
    BOND10: 'bg-emerald-600',
    REPO: 'bg-zinc-300 dark:bg-zinc-700',
  };
  const stockBarColor = (r: (typeof rows)[number]): string => {
    if ((r.pick ?? 'REPO') !== 'STOCK') return pickColor[r.pick ?? 'REPO'] ?? 'bg-gray-300';
    const m = (r as unknown as { stockMarket?: string }).stockMarket ?? '';
    if (m === 'A股') return 'bg-red-500';
    if (m === 'HK') return 'bg-cyan-600';
    if (m === 'A+H') return 'bg-fuchsia-600';
    return pickColor.STOCK;
  };
  const pickLabel: Record<string, string> = {
    STOCK: '股票',
    GOLD: '黄金',
    OIL: '原油',
    NASDAQ: '纳指',
    BOND10: '国债',
    REPO: '逆回购',
  };
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
      <div className="mb-2 flex items-center gap-2 text-[12px] font-medium">
        <BarChart3 className="size-3.5" />
        过去一年 Timeline（
        {isTwin
          ? '机会双子星 · 择强核心 + S-gap 机会增强'
          : isSgap
            ? '状态分桶 S-gap · 可执行独立腿（涨停可能买不进）'
            : '择强单轨 · 全资产同权 · 100%'}
        ）
        <span className="ml-2 flex overflow-hidden rounded border border-[var(--k-border)] text-[10px]">
          <button
            type="button"
            onClick={() => setStrategy('twin_star')}
            className={cn('px-2 py-0.5', isTwin ? 'bg-[var(--k-accent)] text-white' : 'text-[var(--k-muted)]')}
          >
            机会双子星
          </button>
          <button
            type="button"
            onClick={() => setStrategy('pick_strong')}
            className={cn(
              'px-2 py-0.5',
              strategy === 'pick_strong' ? 'bg-[var(--k-accent)] text-white' : 'text-[var(--k-muted)]',
            )}
          >
            单轨择强
          </button>
          <button
            type="button"
            onClick={() => setStrategy('state_bucket')}
            className={cn('px-2 py-0.5', isSgap ? 'bg-[var(--k-accent)] text-white' : 'text-[var(--k-muted)]')}
            title="独立 S-gap 腿 · 与双子星卫星同源 · skip_t1_limit 可执行口径"
          >
            状态分桶
          </button>
        </span>
        <span className="ml-auto text-[10px] font-normal tabular-nums text-[var(--k-muted)]">
          {start} ~ {today} · {rows.length} 交易日
          {last
            ? isTwin
              ? ` · 卫星 ${last.satNavReturnPct ?? '—'}% · 机会双子星 ${last.navSingleReturnPct ?? last.navMultiReturnPct}%`
              : isSgap
                ? ` · S-gap ${last.navSingleReturnPct ?? last.satNavReturnPct ?? '—'}% · 仓 ${last.satPositions ?? 0}`
                : ` · 基线 ${last.navBaseReturnPct}% · 择强 ${last.navSingleReturnPct ?? last.navMultiReturnPct}%`
            : ''}
        </span>
      </div>
      {q.isError ? (
        <div className="text-xs">
          <p className="text-red-700">{String(q.error)}</p>
          <p className="mt-1 text-[var(--k-muted)]">
            {isSgap
              ? '后端计算 S-gap 全市场截面（可执行口径），请稍后刷新'
              : '后端计算需 ~50s（S-3 全市场回放），请稍后刷新或改短周期'}
          </p>
          <Button size="sm" variant="outline" className="mt-2 h-7 text-xs" onClick={() => q.refetch()}>
            重试
          </Button>
        </div>
      ) : q.isFetching && !rows.length ? (
        <p className="text-xs text-[var(--k-muted)]">计算中…（首次约 50s，已加缓存）</p>
      ) : !rows.length ? (
        <p className="text-xs text-[var(--k-muted)]">暂无数据</p>
      ) : (
        <div className="flex flex-col gap-2">
          <div className="flex flex-col gap-1">
            <div className="flex h-4 w-full overflow-hidden rounded border border-[var(--k-border)]/30">
              {rows.map((r) => {
                const sm = (r as unknown as { stockMarket?: string; stockSymbols?: string[] }).stockMarket ?? '';
                const syms = ((r as unknown as { stockSymbols?: string[] }).stockSymbols ?? []).join(',');
                const stkMom = (r as unknown as { stockMom?: number | null }).stockMom;
                const exits = ((r as unknown as { exits?: string[] }).exits ?? []).join(',');
                return (
                  <div
                    key={`single-${r.date}`}
                    className={cn('h-full flex-1', stockBarColor(r))}
                    title={`${r.date} 该买:${r.pick ?? 'REPO'}${stkMom != null ? ` 股票mom${stkMom}%` : ''} 持仓:${sm} ${r.deployedPct}% ${syms}${exits ? ` 卖出:${exits}` : ''} 基线${r.navBaseReturnPct}% 单轨${(r as unknown as { navSingleReturnPct?: number }).navSingleReturnPct ?? r.navMultiReturnPct}%`}
                  />
                );
              })}
            </div>
            <div className="flex justify-between text-[10px] text-[var(--k-muted)]">
              <span>单轨：A股红 / 港股橙 / A+H紫 / 黄金amber / 原油slate / 纳指blue / 国债emerald / 逆回购锌 · 每天一格=当天该买（红色细分港/A）</span>
            </div>
          </div>
          <div className="flex flex-wrap gap-2 text-[11px]">
            {ALL_PICKS.map((k) => {
              const v = dist[k] ?? 0;
              return (
                <span key={k} className="flex items-center gap-1">
                  <span className={cn('inline-block size-2 rounded-sm', pickColor[k] ?? 'bg-gray-300')} />
                  {pickLabel[k] ?? k} {v}天 ({rows.length ? ((v / rows.length) * 100).toFixed(0) : 0}%)
                </span>
              );
            })}
            {dist['STOCK'] ? (
              <>
                <span className="flex items-center gap-1">
                  <span className="inline-block size-2 rounded-sm bg-red-500" />A股 {distMarket['A股'] ?? 0}天
                </span>
                <span className="flex items-center gap-1">
                  <span className="inline-block size-2 rounded-sm bg-orange-500" />港股 {distMarket['HK'] ?? 0}天
                </span>
                <span className="flex items-center gap-1">
                  <span className="inline-block size-2 rounded-sm bg-purple-600" />A+H {distMarket['A+H'] ?? 0}天
                </span>
              </>
            ) : null}
            <span className="ml-auto text-[10px] text-[var(--k-muted)]">
              {isTwin
                ? `机会双子星 · 择强核心(mom_compare_t8) + S-gap机会 · 累计 ${last?.navSingleReturnPct ?? last?.navMultiReturnPct ?? '—'}% / 卫星 ${last?.satNavReturnPct ?? '—'}% / 核心 ${(q.data as unknown as { summary?: { corePct?: number } } | undefined)?.summary?.corePct != null ? `${(q.data as unknown as { summary?: { corePct?: number } }).summary?.corePct}%` : '—'}`
                : isSgap
                  ? `状态分桶 S-gap 独立腿 · 可执行(skip_t1_limit) · 累计 ${last?.navSingleReturnPct ?? last?.satNavReturnPct ?? '—'}% / dd ${(q.data as unknown as { summary?: { maxDdFusedPct?: number } } | undefined)?.summary?.maxDdFusedPct ?? '—'}%`
                  : `择强单轨 · mom_compare · pick_strong_track 同源 · 累计 ${last?.navSingleReturnPct ?? last?.navMultiReturnPct ?? '—'}% / 基线 ${last?.navBaseReturnPct ?? '—'}%`}
            </span>
          </div>
          <div className="max-h-[360px] overflow-auto rounded border border-[var(--k-border)]">
            <table className="w-full text-left text-xs tabular-nums">
              <thead className="sticky top-0 bg-[var(--k-surface)]">
                <tr className="text-[10px] text-[var(--k-muted)]">
                  <th className="py-1 pr-2 pl-2">日期</th>
                  <th className="py-1 pr-2">该买</th>
                  <th className="py-1 pr-2">持仓(A/H)</th>
                  <th className="py-1 pr-2">持有</th>
                  <th className="py-1 pr-2">卖出</th>
                  {!isSgap ? <th className="py-1 pr-2">基线NAV%</th> : null}
                  <th className="py-1 pr-2">
                    {isTwin ? '机会双子星NAV%' : isSgap ? 'S-gap NAV%' : '择强NAV%'}
                  </th>
                  {isTwin || isSgap ? <th className="py-1 pr-2">{isSgap ? '仓位' : '卫星NAV%(仓)'}</th> : null}
                  {!isSgap ? <th className="py-1 pr-2">超额</th> : null}
                </tr>
              </thead>
              <tbody>
                {(showAll ? rows : rows.slice(-30)).map((r) => {
                  const single = (r as unknown as { navSingleReturnPct?: number }).navSingleReturnPct ?? r.navMultiReturnPct;
                  const exits = (r as unknown as { exits?: string[] }).exits ?? [];
                  const isStock = (r.pick ?? 'REPO') === 'STOCK';
                  const isSgapPick = (r.pick ?? '') === 'S-GAP';
                  const pickSym = r.pick === 'GOLD' ? '518880' : r.pick === 'OIL' ? '513350' : r.pick === 'NASDAQ' ? '513110' : r.pick === 'BOND10' ? '511260' : r.pick === 'REPO' ? 'GC001' : r.pick ?? 'REPO';
                  const syms = isStock
                    ? (((r as unknown as { stockSymbols?: string[] }).stockSymbols ?? []).join(' ') || '—')
                    : isSgapPick
                      ? `${r.satPositions ?? 0}仓`
                      : `${pickSym} 1票`;
                  return (
                    <tr key={r.date} className="border-t border-[var(--k-border)]/60">
                      <td className="py-1 pr-2 pl-2 font-mono">{r.date}</td>
                      <td className="py-1 pr-2">
                        <span className={cn('rounded px-1 py-px text-[10px] text-white', isSgapPick ? 'bg-violet-600' : stockBarColor(r))}>
                          {r.pick ?? 'REPO'}
                        </span>
                      </td>
                      <td className="py-1 pr-2 text-[var(--k-muted)]">
                        {isStock ? (
                          <>
                            {(r as unknown as { stockMarket?: string }).stockMarket ?? ''} {r.positions}票{' '}
                            <span className="text-[10px]">
                              A{(r as unknown as { cnPositions?: number }).cnPositions ?? 0}/H{(r as unknown as { hkPositions?: number }).hkPositions ?? 0}
                            </span>
                          </>
                        ) : isSgapPick ? (
                          <>{r.satPositions ?? 0}仓</>
                        ) : (
                          <>{pickSym} 1票</>
                        )}
                      </td>
                      <td className="max-w-[140px] truncate py-1 pr-2 text-[10px] text-[var(--k-muted)]" title={syms}>
                        {syms}
                      </td>
                      <td className="max-w-[140px] truncate py-1 pr-2 text-[10px] text-amber-700 dark:text-amber-300" title={exits.join(' ')}>
                        {exits.length ? exits.join(' ') : '—'}
                      </td>
                      {!isSgap ? (
                        <td className={cn('py-1 pr-2', tone(r.navBaseReturnPct ?? 0))}>
                          {(r.navBaseReturnPct ?? 0).toFixed(2)}%
                        </td>
                      ) : null}
                      <td className={cn('py-1 pr-2 font-semibold', tone(single))}>{single.toFixed(2)}%</td>
                      {isTwin || isSgap ? (
                        <td className={cn('py-1 pr-2 text-[var(--k-muted)]')}>
                          {isSgap ? (
                            <span>{r.satPositions ?? 0}仓</span>
                          ) : r.satNavReturnPct != null ? (
                            <>
                              <span className={cn(tone(r.satNavReturnPct))}>{r.satNavReturnPct.toFixed(2)}%</span>
                              <span className="text-[10px]"> ({r.satPositions ?? 0}仓)</span>
                            </>
                          ) : (
                            '—'
                          )}
                        </td>
                      ) : null}
                      {!isSgap ? (
                        <td className={cn('py-1 pr-2', tone(single - (r.navBaseReturnPct ?? 0)))}>
                          {(single - (r.navBaseReturnPct ?? 0)).toFixed(2)}%
                        </td>
                      ) : null}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <div className="flex items-center gap-2">
            <p className="text-[10px] text-[var(--k-muted)]">
              {showAll ? `全部 ${rows.length} 日` : `近30日 / 共 ${rows.length} 日`} ·{' '}
              {isTwin
                  ? '机会双子星 v2：择强为主 + S-gap 机会增强（涨停可能买不进 → 可执行口径；退出日计入成本；关闸/无仓→核心100%）· 定案 docs/backtests/state-bucket-algo-2026-08-31.md §3/§7 · 实盘默认仍是单轨择强'
                : isSgap
                  ? '状态分桶 S-gap 独立腿（bq3/15槽/body3/R-wide · skip_t1_limit）· 与双子星卫星同源引擎 · 非择强替换件 · docs/backtests/state-bucket-algo-2026-08-31.md'
                  : '择强单轨（股票vs金/油/纳指/债 同池 mom60＞MA200）· 定案 docs/modules/pick-strong-track.md'}{' '}
              · 卖出=前日有今日无
            </p>
            <button type="button" onClick={() => setShowAll((v) => !v)} className="ml-auto rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-2 py-0.5 text-[10px] text-[var(--k-muted)] hover:border-[var(--k-accent)]/60">
              {showAll ? '收起只看30日' : `加载全部 ${rows.length} 天`}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

const PICK_LABELS: Record<string, string> = {
  STOCK: '股票',
  GOLD: '黄金',
  OIL: '原油',
  NASDAQ: '纳指',
  BOND10: '国债',
  REPO: '逆回购',
  STOCK_CN: 'A股成交',
  STOCK_HK: '港股成交',
  OTHER_ETF: '其他ETF',
  OTHER: '其他',
};

function ReturnAttributionCard({
  start,
  end,
  onRangeChange,
}: {
  start: string;
  end: string;
  onRangeChange?: (start: string, end: string) => void;
}) {
  const [showStock, setShowStock] = React.useState(false);
  const q = useReturnAttributionQuery(start, end, true);
  const ps = q.data?.pickStrong;
  const ut = q.data?.userTrades;
  const pickOrder = ['NASDAQ', 'GOLD', 'OIL', 'STOCK', 'BOND10', 'REPO'] as const;

  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
      <div className="mb-2 flex flex-wrap items-center gap-2 text-[12px] font-medium">
        <TrendingDown className="size-3.5 rotate-180" />
        涨跌归因（择强单轨 · 单侧拆腿）
        <span className="rounded bg-[var(--k-bg)] px-1.5 py-0.5 text-[10px] font-normal text-[var(--k-muted)]">
          明细 · 对照见上方
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
        只拆<strong>单轨</strong>各腿：加法贡献 / 几何袖 / 月度 / 极值日。与实盘对照请看上方「归因对照」。已实现成交附在表底仅作参考。
      </p>
      {q.isError ? (
        <div className="text-xs">
          <p className="text-red-700">{String(q.error)}</p>
          <Button size="sm" variant="outline" className="mt-2 h-7 text-xs" onClick={() => q.refetch()}>
            重试
          </Button>
        </div>
      ) : q.isFetching && !ps ? (
        <p className="text-xs text-[var(--k-muted)]">计算中…（首次约与 Timeline 同耗时，已共享缓存）</p>
      ) : !ps ? (
        <p className="text-xs text-[var(--k-muted)]">暂无数据</p>
      ) : (
        <div className="flex flex-col gap-3">
          <div className="grid grid-cols-2 gap-2 md:grid-cols-4">
            <StatCard
              label="择强几何总收益"
              value={`${ps.totalGeoPct.toFixed(1)}%`}
              sub={
                ps.timelineFusedPct != null
                  ? `Timeline ${ps.timelineFusedPct.toFixed(1)}% · ${ps.totalDays} 日`
                  : `${ps.totalDays} 日`
              }
            />
            <StatCard label="加法贡献合计" value={`${ps.totalAddPct.toFixed(1)}%`} sub="Σ日收益（非复利）" />
            <StatCard
              label="最大贡献腿"
              value={(() => {
                const best = pickOrder
                  .map((k) => [k, ps.byPick[k]?.contribAddPct ?? 0] as const)
                  .sort((a, b) => b[1] - a[1])[0];
                return best ? `${PICK_LABELS[best[0]] ?? best[0]} ${best[1] >= 0 ? '+' : ''}${best[1].toFixed(1)}%` : '—';
              })()}
              sub="按加法贡献"
            />
            <StatCard
              label="最大拖累腿"
              value={(() => {
                const worst = pickOrder
                  .map((k) => [k, ps.byPick[k]?.contribAddPct ?? 0] as const)
                  .sort((a, b) => a[1] - b[1])[0];
                return worst ? `${PICK_LABELS[worst[0]] ?? worst[0]} ${worst[1].toFixed(1)}%` : '—';
              })()}
              sub="按加法贡献"
            />
          </div>

          <div className="overflow-auto">
            <table className="w-full text-left text-xs tabular-nums">
              <thead>
                <tr className="text-[10px] text-[var(--k-muted)]">
                  <th className="py-1 pr-3">资产</th>
                  <th className="py-1 pr-3">天数</th>
                  <th className="py-1 pr-3">占日历%</th>
                  <th className="py-1 pr-3">加法贡献%</th>
                  <th className="py-1 pr-3">几何袖%</th>
                </tr>
              </thead>
              <tbody>
                {pickOrder.map((k) => {
                  const st = ps.byPick[k];
                  if (!st || st.days === 0) return null;
                  return (
                    <tr key={k} className="border-t border-[var(--k-border)]/60">
                      <td className="py-1 pr-3">{PICK_LABELS[k] ?? k}</td>
                      <td className="py-1 pr-3">{st.days}</td>
                      <td className="py-1 pr-3">{st.pctDays.toFixed(1)}</td>
                      <td className={cn('py-1 pr-3 font-medium', tone(st.contribAddPct))}>
                        {st.contribAddPct >= 0 ? '+' : ''}
                        {st.contribAddPct.toFixed(2)}
                      </td>
                      <td className={cn('py-1 pr-3', tone(st.contribGeoPct))}>
                        {st.contribGeoPct >= 0 ? '+' : ''}
                        {st.contribGeoPct.toFixed(2)}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          {ps.byMonth.length > 0 && (
            <div>
              <div className="mb-1 text-[11px] font-medium">月度加法贡献</div>
              <div className="overflow-auto">
                <table className="w-full text-left text-[10px] tabular-nums">
                  <thead>
                    <tr className="text-[var(--k-muted)]">
                      <th className="py-1 pr-2">月</th>
                      {pickOrder.map((k) => (
                        <th key={k} className="py-1 pr-2">
                          {PICK_LABELS[k] ?? k}
                        </th>
                      ))}
                      <th className="py-1 pr-2">合计</th>
                    </tr>
                  </thead>
                  <tbody>
                    {ps.byMonth.map((m) => (
                      <tr key={m.month} className="border-t border-[var(--k-border)]/40">
                        <td className="py-0.5 pr-2">{m.month}</td>
                        {pickOrder.map((k) => {
                          const v = m.byPick[k];
                          return (
                            <td key={k} className={cn('py-0.5 pr-2', v != null ? tone(v) : '')}>
                              {v != null ? v.toFixed(1) : '—'}
                            </td>
                          );
                        })}
                        <td className={cn('py-0.5 pr-2 font-medium', tone(m.totalAddPct))}>
                          {m.totalAddPct.toFixed(1)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {ps.topDays.length > 0 && (
            <div>
              <div className="mb-1 text-[11px] font-medium">极值日（|日收益| Top）</div>
              <ul className="grid gap-0.5 text-[11px] tabular-nums md:grid-cols-2">
                {ps.topDays.map((d) => (
                  <li key={d.date} className="flex gap-2 border-t border-[var(--k-border)]/30 py-0.5">
                    <span className="text-[var(--k-muted)]">{d.date}</span>
                    <span>{PICK_LABELS[d.pick] ?? d.pick}</span>
                    <span className={cn('ml-auto font-medium', tone(d.dayRetPct))}>
                      {d.dayRetPct >= 0 ? '+' : ''}
                      {d.dayRetPct.toFixed(2)}%
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          )}

          {ps.stockBreakdown && Object.keys(ps.stockBreakdown.bySymbol).length > 0 && (
            <div>
              <button
                type="button"
                className="mb-1 flex items-center gap-1 text-[11px] font-medium"
                onClick={() => setShowStock((v) => !v)}
              >
                <ChevronDown className={cn('size-3 transition-transform', showStock && 'rotate-180')} />
                STOCK 个股贡献（{ps.stockBreakdown.stockDays} 个股票日 · Top）
              </button>
              {showStock && (
                <table className="w-full text-left text-[11px] tabular-nums">
                  <thead>
                    <tr className="text-[10px] text-[var(--k-muted)]">
                      <th className="py-1 pr-2">标的</th>
                      <th className="py-1 pr-2">天数</th>
                      <th className="py-1 pr-2">加法贡献%</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(ps.stockBreakdown.bySymbol)
                      .slice(0, 15)
                      .map(([sym, st]) => (
                        <tr key={sym} className="border-t border-[var(--k-border)]/40">
                          <td className="py-0.5 pr-2">{sym}</td>
                          <td className="py-0.5 pr-2">{st.days}</td>
                          <td className={cn('py-0.5 pr-2', tone(st.contribAddPct))}>
                            {st.contribAddPct >= 0 ? '+' : ''}
                            {st.contribAddPct.toFixed(2)}
                          </td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              )}
            </div>
          )}

          <div className="rounded border border-dashed border-[var(--k-border)] p-2">
            <div className="mb-1 text-[11px] font-medium">我的成交对照（已实现毛盈亏）</div>
            {!ut || ut.error ? (
              <p className="text-[10px] text-[var(--k-muted)]">{ut?.error ?? '无成交数据'}</p>
            ) : ut.insufficient ? (
              <p className="text-[10px] text-[var(--k-muted)]">
                窗口内平仓 {ut.closedCount} 笔（不足 3 笔，仅供参考）
                {ut.closedCount > 0 &&
                  ' · ' +
                    Object.entries(ut.byBucket)
                      .map(([b, s]) => `${PICK_LABELS[b] ?? b} ${s.sumPnlPct >= 0 ? '+' : ''}${s.sumPnlPct.toFixed(1)}%`)
                      .join(' · ')}
              </p>
            ) : (
              <div className="flex flex-wrap gap-2 text-[11px] tabular-nums">
                {Object.entries(ut.byBucket).map(([b, s]) => (
                  <span key={b} className={cn('rounded bg-[var(--k-bg)] px-1.5 py-0.5', tone(s.sumPnlPct))}>
                    {PICK_LABELS[b] ?? b} ×{s.count} {s.sumPnlPct >= 0 ? '+' : ''}
                    {s.sumPnlPct.toFixed(1)}%
                  </span>
                ))}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

export function BacktestPage() {
  const [params, setParams] = React.useState<BacktestParams>(DEFAULT_PARAMS);
  const [submitted, setSubmitted] = React.useState<BacktestParams>(DEFAULT_PARAMS);
  const [attempt, setAttempt] = React.useState(0);
  const [gridOn, setGridOn] = React.useState(false);
  const [advancedOn, setAdvancedOn] = React.useState(false);
  const [tab, setTab] = React.useState<'compare' | 'baseline'>('compare');
  const attrEnd = new Date().toISOString().slice(0, 10);
  const attrStartDefault = (() => {
    const d = new Date();
    d.setFullYear(d.getFullYear() - 1);
    return d.toISOString().slice(0, 10);
  })();
  const [attrStart, setAttrStart] = React.useState(attrStartDefault);
  const [attrEndState, setAttrEnd] = React.useState(attrEnd);
  const onAttrRange = (s: string, e: string) => {
    setAttrStart(s);
    setAttrEnd(e);
  };

  const runQ = useBacktestRunQuery(submitted, attempt);
  const sensQ = useSensitivityQuery(DEFAULT_PARAMS.start, DEFAULT_PARAMS.end, gridOn);
  const exitQ = useExitAttributionQuery(5);
  const corrQ = useCorrelationStatusQuery(true, true);
  const overviewQ = useBacktestOverviewQuery();
  const reconQ = useBacktestReconQuery(2);
  const c4Q = usePaperVsBacktestQuery();
  const sleeveQ = useSleeveNavQuery();
  const coreQ = useCoreAuditQuery();

  const set = (k: keyof BacktestParams, v: string | number) =>
    setParams((p) => ({ ...p, [k]: typeof v === 'number' ? v : Number(v) }));

  const s = runQ.data?.summary;

  return (
    <div className="flex flex-col gap-4 p-4">
      <Tabs value={tab} onValueChange={(v) => setTab(v as 'compare' | 'baseline')}>
        <TabsList>
          <TabsTrigger value="compare">对比</TabsTrigger>
          <TabsTrigger value="baseline">回测基线</TabsTrigger>
        </TabsList>
        <TabsContent value="compare" className="mt-4 flex flex-col gap-4">
          <CoreAuditCard q={coreQ} />
          <ReplicaGapCard start={attrStart} end={attrEndState} onRangeChange={onAttrRange} />
          <ReturnAttributionCard start={attrStart} end={attrEndState} onRangeChange={onAttrRange} />
          <TimelineCard />
          <RecentDailyCompareCard />
          <SleeveNavCard q={sleeveQ} />
          <PaperVsBacktestCard q={c4Q} />
          <ReconStrip reconQ={reconQ} />
        </TabsContent>
        <TabsContent value="baseline" className="mt-4 flex flex-col gap-4">
          <ConclusionBoard overview={overviewQ.data} />
          <RollingOosCard overview={overviewQ.data} />

      {/* 高级参数工具（折叠 · 原参数敏感度工具） */}
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)]">
        <button
          type="button"
          onClick={() => setAdvancedOn((v) => !v)}
          className="flex w-full items-center gap-2 px-3 py-2.5 text-left text-[12px] font-medium"
        >
          <ChevronDown className={cn('size-3.5 text-[var(--k-muted)] transition-transform', advancedOn && 'rotate-180')} />
          高级：参数敏感度工具（单窗回测 / 网格 / 相关性 / 卖出归因）
          <span className="text-[10px] font-normal text-[var(--k-muted)]">
            仅研究用途 · 不作发布依据
          </span>
        </button>
        {advancedOn && (
          <div className="flex flex-col gap-4 px-3 pb-3">
      {/* 参数区 */}
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
        <div className="mb-2 flex items-center gap-2 text-[12px] font-medium">
          <Activity className="size-3.5" />
          回测 · 参数（信号 = 历史实际 TrendOK 分 · 平仓逻辑与 live paper 同码 · 默认=趋势跟随方案）
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
            移动止损 %
            <input
              type="number"
              className={cn(INPUT_CLS, 'w-20')}
              value={params.trailingStopPct}
              placeholder="0=关闭"
              onChange={(e) => set('trailingStopPct', e.target.value)}
            />
          </label>
          <label className="flex flex-col gap-1 text-[10px] text-[var(--k-muted)]">
            单笔仓位
            <input
              type="number"
              step="0.01"
              min="0.01"
              max="1"
              className={cn(INPUT_CLS, 'w-20')}
              value={params.positionPct}
              onChange={(e) => set('positionPct', e.target.value)}
            />
          </label>
          <label className="flex flex-col gap-1 text-[10px] text-[var(--k-muted)]">
            持仓上限
            <input
              type="number"
              min="1"
              className={cn(INPUT_CLS, 'w-20')}
              value={params.maxPositions}
              onChange={(e) => set('maxPositions', e.target.value)}
            />
          </label>
          <label className="flex flex-col gap-1 text-[10px] text-[var(--k-muted)]">
            RS 排名过滤
            <input
              type="number"
              step="0.05"
              min="0"
              max="1"
              className={cn(INPUT_CLS, 'w-20')}
              value={params.rsRankMin}
              placeholder="0=关闭"
              onChange={(e) => set('rsRankMin', e.target.value)}
            />
          </label>
          <label className="flex flex-col gap-1 text-[10px] text-[var(--k-muted)]">
            Diverging 仓位
            <input
              type="number"
              step="0.1"
              min="0"
              max="1"
              className={cn(INPUT_CLS, 'w-20')}
              value={params.divergingScale}
              placeholder="0=不开仓"
              onChange={(e) => set('divergingScale', e.target.value)}
            />
          </label>
          <label className="flex flex-col gap-1 text-[10px] text-[var(--k-muted)]">
            止盈 %
            <input
              type="number"
              className={cn(INPUT_CLS, 'w-20')}
              value={params.targetPnlPct}
              placeholder="100=不止盈"
              onChange={(e) => set('targetPnlPct', e.target.value)}
            />
          </label>
          <label className="flex flex-col gap-1 text-[10px] text-[var(--k-muted)]">
            Score 平仓线
            <input
              type="number"
              className={cn(INPUT_CLS, 'w-20')}
              value={params.scoreFloor}
              placeholder="0=不平仓"
              onChange={(e) => set('scoreFloor', e.target.value)}
            />
          </label>
          <label className="flex flex-col gap-1 text-[10px] text-[var(--k-muted)]">
            恐慌冷却（天）
            <input
              type="number"
              min="0"
              className={cn(INPUT_CLS, 'w-20')}
              value={params.panicCooldownDays}
              placeholder="0=关闭"
              onChange={(e) => set('panicCooldownDays', e.target.value)}
            />
          </label>
          <label className="flex flex-col gap-1 text-[10px] text-[var(--k-muted)]">
            滑点 %
            <input
              type="number"
              step="0.01"
              min="0"
              className={cn(INPUT_CLS, 'w-20')}
              value={params.slippagePct}
              placeholder="0=关闭"
              onChange={(e) => set('slippagePct', e.target.value)}
            />
          </label>
          <label className="flex flex-col gap-1 text-[10px] text-[var(--k-muted)]">
            排除板块（前缀逗号分隔，300=创业板）
            <input
              type="text"
              className={cn(INPUT_CLS, 'w-28')}
              value={params.excludeBoards}
              placeholder="空=不过滤"
              onChange={(e) => set('excludeBoards', e.target.value)}
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
          ∪ 动量突破（历史数据缺失日降级为仅 regime）。RS 排名过滤=全市场 20 日相对强度
          百分位（0.8 = 只买前 20% 强票；缺数据日 fail-closed）。单笔仓位×持仓上限=资金
          模型。移动止损=峰值回撤平仓（0 关闭）。Diverging 仓位=震荡市开仓比例
          （0=不开，0.5=半仓；Weak 始终不开仓）。仅参数敏感度参考，不作发布依据。
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
              <StatCard label="累计净收益" value={pct(s.total_net_pnl_pct, 1)} sub={`按 ${params.positionPct * 100}% 仓位折算`} />
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

      {/* 基准 */}
      {sensQ.data && sensQ.data.benchmarks.length > 0 && (
        <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
          <div className="mb-2 flex items-center gap-2 text-[12px] font-medium">
            <Activity className="size-3.5" />
            基准对比（窗口年化 · 目标 = 最强指数 +10%）
          </div>
          <div className="flex flex-wrap gap-2">
            {sensQ.data.benchmarks.map((b) => (
              <div key={b.ts_code} className="rounded-md border border-[var(--k-border)] px-2.5 py-1.5 text-[11px]">
                <span className="text-[var(--k-muted)]">{b.name}</span>{' '}
                <span className={cn('font-semibold tabular-nums', b.annual_pct >= 0 ? 'text-emerald-700 dark:text-emerald-300' : 'text-red-700 dark:text-red-400')}>
                  {pct(b.annual_pct, 1)}/年
                </span>
                <span className="text-[10px] text-[var(--k-muted)]">（窗口 {pct(b.total_return_pct, 1)}）</span>
              </div>
            ))}
            {(() => {
              const best = sensQ.data.benchmarks.reduce((a, b) => (b.annual_pct > a.annual_pct ? b : a), sensQ.data.benchmarks[0]);
              return (
                <div className="rounded-md border border-[var(--k-accent)]/50 bg-[var(--k-accent)]/5 px-2.5 py-1.5 text-[11px]">
                  <span className="text-[var(--k-muted)]">目标线：</span>
                  <span className="font-semibold tabular-nums">{best.name} +10% = {pct(best.annual_pct + 10, 1)}/年</span>
                </div>
              );
            })()}
          </div>
        </div>
      )}

      {/* 敏感度网格 */}
      {gridOn && (
        <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
          <div className="mb-2 flex items-center gap-2 text-[12px] font-medium">
            <BarChart3 className="size-3.5" />
            敏感度网格（score × hold × stop × 闸门 · 默认窗口 · 按胜率排序 · 点击行载入单配置）
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
                    <th className="py-1 pr-2">trail</th>
                    <th className="py-1 pr-2">trades</th>
                    <th className="py-1 pr-2">胜率</th>
                    <th className="py-1 pr-2">均净%</th>
                    <th className="py-1 pr-2">年化%</th>
                    <th className="py-1 pr-2">超额%</th>
                    <th className="py-1 pr-2">夏普</th>
                    <th className="py-1 pr-2">maxDD%</th>
                  </tr>
                </thead>
                <tbody>
                  {[...sensQ.data.results]
                    .sort((a, b) => (b.win_rate ?? -1) - (a.win_rate ?? -1))
                    .map((r, i) => (
                      <tr
                        key={i}
                        className="cursor-pointer border-t border-[var(--k-border)]/60 hover:bg-[var(--k-accent)]/5"
                        onClick={() => {
                          setParams((p) => ({
                            ...p,
                            scoreThreshold: r.config.score_threshold,
                            maxHoldDays: r.config.max_hold_days,
                            stopLossPct: r.config.stop_loss_pct,
                            gates: r.config.gates,
                          }));
                          setSubmitted((p) => ({
                            ...p,
                            scoreThreshold: r.config.score_threshold,
                            maxHoldDays: r.config.max_hold_days,
                            stopLossPct: r.config.stop_loss_pct,
                            gates: r.config.gates,
                          }));
                          setAttempt((a) => a + 1);
                          setGridOn(false);
                        }}
                        title="点击 = 载入该配置到上方单配置回测并运行"
                      >
                        <td className="py-1 pr-2 text-[var(--k-muted)]">{r.config.gates}</td>
                        <td className="py-1 pr-2">{r.config.score_threshold.toFixed(0)}</td>
                        <td className="py-1 pr-2">{r.config.max_hold_days}</td>
                        <td className="py-1 pr-2">{r.config.stop_loss_pct.toFixed(0)}</td>
                        <td className="py-1 pr-2">{r.config.trailing_stop_pct ? r.config.trailing_stop_pct.toFixed(0) : '—'}</td>
                        <td className="py-1 pr-2">{r.closed}</td>
                        <td className="py-1 pr-2 font-medium">{winRate(r.win_rate)}</td>
                        <td className={cn('py-1 pr-2', tone(r.avg_net_pnl_pct))}>{pct(r.avg_net_pnl_pct)}</td>
                        <td className={cn('py-1 pr-2 font-medium', tone(r.annual_net_pnl_pct))}>{pct(r.annual_net_pnl_pct, 1)}</td>
                        <td className={cn('py-1 pr-2', r.excess_vs_best_benchmark_pct >= 10 ? 'font-semibold text-emerald-700 dark:text-emerald-300' : tone(r.excess_vs_best_benchmark_pct))}>
                          {pct(r.excess_vs_best_benchmark_pct, 1)}
                          {r.excess_vs_best_benchmark_pct >= 10 && ' ✓'}
                        </td>
                        <td className="py-1 pr-2">{r.sharpe ?? '—'}</td>
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
        )}
      </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}
