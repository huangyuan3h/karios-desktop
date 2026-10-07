'use client';

/**
 * Watchlist · 母港 M30 跟仓面板 (homeport view).
 *
 * M30 = 0.7 × Harbor + 0.3 × B3 (backtest monthly reset; UI follow weekly).
 * Reuses: portfolio-health (harbor pick + holdings), b3-state (leg weights),
 * account 总资金, lot-sizing (lots), replica-gap (pick matching).
 * B3 ETFs are M30 targets here — never "非今日停车 ETF".
 */

import * as React from 'react';

import { useAccountSettings } from '@/lib/account-settings';
import { getShanghaiTodayIso } from '@/lib/market-hours';
import {
  buildM30Targets,
  cadenceStatus,
  FOLLOW_CADENCE_LABELS,
  M30_DRIFT_LEG_PT,
  M30_DRIFT_LINE_PT,
  M30_MIN_TRADE_PCT,
  rebalanceGate,
  setFollowCadence,
  useFollowCadence,
  type B3Entry,
  type FollowCadence,
  type M30Holding,
} from '@/lib/m30-follow';
import { fetchPortfolioHealth } from '@/lib/queries/portfolioHealth';
import { useB3StateQuery } from '@/lib/queries/backtest';
import { cn } from '@/lib/utils';
import { useQuery } from '@tanstack/react-query';

function cny(v: number | null): string {
  if (v == null) return '—';
  return `¥${Math.round(v).toLocaleString('zh-CN')}`;
}

function pct(v: number): string {
  return `${v.toFixed(2).replace(/\.?0+$/, '')}%`;
}

export function M30FollowBlock() {
  const healthQ = useQuery({
    queryKey: ['portfolio-health'],
    queryFn: ({ signal }) => fetchPortfolioHealth(undefined, signal),
    refetchInterval: 5 * 60_000,
  });
  const b3Q = useB3StateQuery();
  const { capital } = useAccountSettings();
  const cadence = useFollowCadence();
  const today = getShanghaiTodayIso();

  const holdings: M30Holding[] = React.useMemo(() => {
    const data = healthQ.data;
    if (!data) return [];
    const out: M30Holding[] = [];
    for (const h of data.holdings ?? []) {
      out.push({
        symbol: h.symbol,
        positionPct: h.positionPct,
        name: h.name,
        action: h.action,
        lastClose: h.lastClose,
      });
    }
    for (const h of data.hkHealth?.holdings ?? []) {
      out.push({
        symbol: h.symbol,
        positionPct: h.positionPct,
        name: h.name,
        action: h.action,
        lastClose: h.lastClose,
      });
    }
    for (const h of data.multiAssetHoldings ?? []) {
      out.push({
        symbol: h.symbol,
        positionPct: h.positionPct,
        name: h.name,
        lastClose:
          h.marketData && typeof h.marketData.close === 'number' ? h.marketData.close : null,
      });
    }
    return out;
  }, [healthQ.data]);

  const b3: B3Entry[] = React.useMemo(() => {
    const u = b3Q.data?.ok ? (b3Q.data.universe ?? []) : [];
    return u.map((x) => ({
      symbol: x.symbol,
      name: x.name,
      targetPct: Number(x.targetPct) || 0,
      px: x.px ?? null,
    }));
  }, [b3Q.data]);

  const sleeve = healthQ.data?.multiAssetSleeve;
  const pickKey = sleeve?.pick?.key ?? 'REPO';
  const pickSymbol = sleeve?.pick?.symbol ?? null;
  const pickPrice = typeof sleeve?.pick?.close === 'number' ? sleeve.pick.close : null;

  const report = React.useMemo(
    () =>
      buildM30Targets({
        capital,
        pickKey,
        pickSymbol,
        pickPrice,
        b3,
        holdings,
      }),
    [capital, pickKey, pickSymbol, pickPrice, b3, holdings],
  );

  const status = React.useMemo(() => cadenceStatus(cadence, today), [cadence, today]);
  const gate = React.useMemo(
    () =>
      rebalanceGate({
        isRebalanceDay: status.isRebalanceDay,
        legDriftPt: report.legDriftPt,
        maxLineDriftPt: report.maxLineDriftPt,
      }),
    [status.isRebalanceDay, report.legDriftPt, report.maxLineDriftPt],
  );

  const hardActions = React.useMemo(
    () => report.actions.filter((a) => a.reason === 'S-3 硬退出'),
    [report.actions],
  );
  const rebalanceActions = React.useMemo(
    () => report.actions.filter((a) => a.reason !== 'S-3 硬退出'),
    [report.actions],
  );
  const visibleRebalance = gate.needsRebalance ? rebalanceActions : [];

  if (healthQ.isPending) {
    return (
      <div
        id="m30-follow"
        data-testid="m30-follow-block"
        className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-4 py-3 text-sm"
      >
        <div className="text-[12px] font-medium">母港M30跟仓</div>
        <p className="mt-1 text-[11px] text-[var(--k-muted)]">对齐检查加载中…</p>
      </div>
    );
  }

  const gateLabel =
    gate.reason === 'rebalance-day'
      ? '今日是再平衡日'
      : gate.reason === 'leg-drift'
        ? `腿偏离 ${report.legDriftPt}pt ≥ ${M30_DRIFT_LEG_PT}pt`
        : gate.reason === 'line-drift'
          ? `单线偏离 ${report.maxLineDriftPt}pt ≥ ${M30_DRIFT_LINE_PT}pt`
          : '跟踪中';

  return (
    <div
      id="m30-follow"
      data-testid="m30-follow-block"
      className="mb-4 rounded-lg border border-sky-500/30 bg-sky-500/5 px-4 py-3 text-sm"
    >
      <div className="flex flex-wrap items-center gap-2 text-[12px] font-medium">
        <span>母港M30跟仓</span>
        <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5 font-mono text-[10px]">
          pick={report.pickKey}
        </span>
        <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5 font-mono text-[10px]">
          港湾70%+B3 30%
        </span>
        <span
          className={cn(
            'text-[11px] font-semibold',
            gate.needsRebalance
              ? 'text-amber-700 dark:text-amber-300'
              : 'text-emerald-700 dark:text-emerald-300',
          )}
        >
          {gate.needsRebalance ? `⚠ 需再平衡（${gateLabel}）` : `✓ ${gateLabel}`}
        </span>
        <span className="ml-auto font-mono text-[10px] font-normal text-[var(--k-muted)]">
          港湾 {report.harborCurrentPct}%/70% · B3 {report.b3CurrentPct}%/30% · 闲置{' '}
          {report.cashCurrentPct}%
        </span>
      </div>

      <div className="mt-2 flex flex-wrap items-center gap-2 text-[11px] text-[var(--k-muted)]">
        <span>
          跟仓 {FOLLOW_CADENCE_LABELS[cadence]} · 上次 {status.last} · 下次 {status.next}
        </span>
        <span className="flex gap-1">
          {(['weekly', 'monthly', 'daily'] as FollowCadence[]).map((c) => (
            <button
              key={c}
              type="button"
              onClick={() => setFollowCadence(c)}
              aria-pressed={c === cadence}
              className={cn(
                'rounded border px-1.5 py-0.5 text-[10px]',
                c === cadence
                  ? 'border-sky-500/50 bg-sky-500/10 text-sky-800 dark:text-sky-200'
                  : 'border-[var(--k-border)]',
              )}
            >
              {FOLLOW_CADENCE_LABELS[c]}
            </button>
          ))}
        </span>
        <span>回测月初再平衡，跟仓每周一次（可在 Settings 改）</span>
      </div>

      {capital == null ? (
        <p className="mt-2 text-[11px] text-amber-700 dark:text-amber-300">
          在上方「账户」填总资金后列出金额与份数（当前仅显示 %）。
        </p>
      ) : null}
      {b3Q.data && !b3Q.data.ok ? (
        <p className="mt-2 text-[11px] text-amber-700 dark:text-amber-300">
          B3 状态不可用（{b3Q.data.error ?? '未知'}）：B3 腿暂按空目标显示，港湾腿不受影响。
        </p>
      ) : null}

      <div className="mt-2 overflow-x-auto">
        <table className="w-full text-[11px]">
          <thead>
            <tr className="text-left text-[10px] text-[var(--k-muted)]">
              <th className="py-1 pr-2 font-medium">标的</th>
              <th className="py-1 pr-2 text-right font-medium">目标</th>
              <th className="py-1 pr-2 text-right font-medium">现持</th>
              <th className="py-1 pr-2 text-right font-medium">偏离</th>
              <th className="py-1 text-right font-medium">份数</th>
            </tr>
          </thead>
          <tbody>
            {report.targets.map((t) => (
              <tr
                key={t.id}
                data-testid="m30-target-row"
                className="border-t border-[var(--k-border)]/50"
              >
                <td className="py-1 pr-2">
                  <span className="font-medium">{t.name}</span>{' '}
                  <span className="font-mono text-[10px] text-[var(--k-muted)]">{t.symbol}</span>{' '}
                  <span className="rounded border border-[var(--k-border)] px-1 text-[10px] text-[var(--k-muted)]">
                    {t.leg === 'b3' ? 'B3' : t.leg === 'harbor-park' ? '港湾停车' : 'S-3'}
                  </span>
                </td>
                <td className="py-1 pr-2 text-right tabular-nums">
                  {pct(t.targetPct)} · {cny(t.targetAmt)}
                </td>
                <td className="py-1 pr-2 text-right tabular-nums">
                  {pct(t.currentPct)} · {cny(t.currentAmt)}
                </td>
                <td
                  className={cn(
                    'py-1 pr-2 text-right tabular-nums',
                    t.side === 'SELL' && 'text-red-600 dark:text-red-400',
                    t.side === 'BUY' && 'text-emerald-700 dark:text-emerald-300',
                  )}
                >
                  {t.deltaPct >= 0 ? '+' : ''}
                  {pct(t.deltaPct)}
                  {t.suppressed ? '（小）' : ''}
                </td>
                <td className="py-1 text-right tabular-nums text-[var(--k-muted)]">
                  {t.side !== 'HOLD' && t.deltaShares != null && t.deltaShares > 0
                    ? `${t.side === 'SELL' ? '卖' : '买'} ${t.deltaShares.toLocaleString('zh-CN')} 份`
                    : t.targetShares != null && t.targetShares > 0
                      ? `目标 ${t.targetShares.toLocaleString('zh-CN')} 份`
                      : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <p className="mt-1 font-mono text-[10px] text-[var(--k-muted)]">
        闲置 {report.cashCurrentPct}%{report.cashTargetPct > 0 ? `（目标现金 ${report.cashTargetPct}%）` : '（目标满仓）'}
        · S-3 每腿 7%（10%×70%）上限 10 腿 · B3 腿内占比×0.3 · 最小 {M30_MIN_TRADE_PCT}%/{'1 手'}
      </p>

      {report.nonTargets.length > 0 ? (
        <div className="mt-2 rounded border border-amber-500/40 bg-amber-500/5 px-2 py-1.5 text-[11px]">
          <div className="font-semibold text-amber-800 dark:text-amber-200">
            非M30目标（建议清仓）：{report.nonTargets.map((h) => h.symbol).join('、')}
          </div>
        </div>
      ) : null}

      {hardActions.length > 0 || visibleRebalance.length > 0 ? (
        <ul className="mt-2 space-y-1.5">
          {[...hardActions, ...visibleRebalance].map((a, i) => (
            <li
              key={`${a.side}-${a.symbol}-${i}`}
              data-testid="m30-action-row"
              className={cn(
                'rounded border px-2 py-1.5 text-[11px]',
                a.side === 'SELL'
                  ? 'border-red-500/40 bg-red-500/10 text-red-900 dark:text-red-100'
                  : 'border-emerald-500/40 bg-emerald-500/10 text-emerald-900 dark:text-emerald-100',
              )}
            >
              <span className="font-semibold">
                {a.side === 'SELL' ? '卖' : '买'} {a.name} {a.symbol}
              </span>{' '}
              <span className="font-mono tabular-nums">
                {pct(a.pct)} · {cny(a.amt)}
                {a.shares != null && a.shares > 0
                  ? ` · ${a.shares.toLocaleString('zh-CN')} 份`
                  : a.reason.startsWith('非M30目标')
                    ? ' · 全部卖出'
                    : ' · 不足1手按%执行'}
              </span>{' '}
              <span className="opacity-80">（{a.reason}）</span>
            </li>
          ))}
        </ul>
      ) : (
        <p className="mt-2 text-[11px] text-[var(--k-muted)]">
          {rebalanceActions.length > 0
            ? `漂移信息：${rebalanceActions.length} 项偏离未达阈值（腿 ${report.legDriftPt}pt / 单线最大 ${report.maxLineDriftPt}pt），跟踪中不下单。`
            : '✓ 与 M30 目标一致：无偏离、无卖出、无待执行。'}
        </p>
      )}
      <details className="mt-2 text-[10px] text-[var(--k-muted)]">
        <summary className="cursor-pointer">口径说明</summary>
        <ul className="mt-1 list-disc space-y-0.5 pl-4">
          <li>回测定义：港湾70%+B3 30%，每月首个交易日复位（5bp/边）；跟仓每周一次只定何时动手，不改引擎。</li>
          <li>港湾腿内：S-3 每腿 7%（引擎 10%×0.7，上限 10 腿，仅 A 股；港股不属于 M30），余量停当日 harbor pick ETF；pick=REPO 时余量留现金（计入港湾腿）。</li>
          <li>停车 ETF 若本身是 B3 成分（黄金 518880 / 纳指 513100 / 国债 511260），两份目标合并成一行，持仓不重复计。</li>
          <li>再平衡：再平衡日全量提示；非再平衡日仅腿偏离≥{M30_DRIFT_LEG_PT}pt 或单线≥{M30_DRIFT_LINE_PT}pt 才提示；S-3 硬退出不受此限。</li>
          <li>价格：B3 用 b3-state raw px（可交易价），停车用 sleeve pick close，股票用持仓 lastClose；缺价时只给 %。</li>
        </ul>
      </details>
    </div>
  );
}
