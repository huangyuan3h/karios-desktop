'use client';

/**
 * 港湾 · 今日决策 — the single Watchlist decision + execution-recording surface.
 *
 * Shows today's Harbor actions (S-3 core buys/sells/holds + idle-cash ETF
 * parking buy/rotate/exit) and records every fill from ONE form:
 *   - CN/HK stocks -> POST /trades (user_trades, leg=s3)
 *   - ETF parking  -> POST /commodities/sleeve/execution-log
 *
 * Replaces the former TodayActionCard / EtfExecutionLogCard / BehaviorAuditBanner
 * / TradingBriefCard prompts (2026-09-13 consolidation).
 */

import * as React from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Button } from '@/components/ui/button';
import { useBehaviorAuditQuery, useRefreshBehaviorAudit } from '@/lib/queries/behaviorAudit';
import { fetchPortfolioHealth } from '@/lib/queries/portfolioHealth';
import { useTimelineQuery } from '@/lib/queries/backtest';
import { STRATEGY_MODE_LABELS, type StrategyMode } from '@/lib/strategy-settings';
import { detectReplicaGaps, type HoldingSnap } from '@/lib/replica-gap';
import { invalidateUserTradesQueries, recordUserTrade } from '@/lib/queries/userTrades';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import { cn } from '@/lib/utils';

type SleeveBlock = {
  pick?: { symbol?: string; key?: string; mom60?: number; close?: number | null };
  action?: string;
  label?: string;
  message?: string;
  idlePct?: number;
};

type ActionItem = {
  symbol: string;
  name?: string;
  side: 'BUY' | 'SELL';
  reason: string;
  isEtf: boolean;
};

const STATUS_OPTS = [
  { id: 'filled', label: '成交' },
  { id: 'partial', label: '部分' },
  { id: 'failed', label: '未成交' },
  { id: 'skipped', label: '跳过' },
] as const;

async function postSleeveExec(body: Record<string, unknown>): Promise<void> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}/commodities/sleeve/execution-log`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const t = await res.text();
    throw new Error(t || `post ${res.status}`);
  }
}

const MODE_SUBTITLE: Record<StrategyMode, string> = {
  harbor: 'S-3 核心 + 闲置现金 ETF 停车场 · 100% 硬切 · 与 Timeline 同源',
  homeport: '港湾核心 × B3 50/50 · 核心腿买卖照常、B3 月频再平衡 · 与 Timeline 同源',
  starport: '母港底仓 × 卫星 1/3 曝露 · 核心腿买卖照常 · 与 Timeline 同源',
  starship: '卫星 standalone · 无港湾核心腿',
};

export function HarborDecisionCard({ mode = 'harbor' }: { mode?: StrategyMode } = {}) {
  const strategyName = STRATEGY_MODE_LABELS[mode];
  const qc = useQueryClient();
  const auditQuery = useBehaviorAuditQuery();
  const healthQuery = useQuery({
    queryKey: ['portfolio-health'],
    queryFn: ({ signal }) => fetchPortfolioHealth(undefined, signal),
    refetchInterval: 5 * 60_000,
  });
  const today = new Date().toISOString().slice(0, 10);
  const start = (() => {
    const d = new Date();
    d.setFullYear(d.getFullYear() - 1);
    return d.toISOString().slice(0, 10);
  })();
  const timelineQ = useTimelineQuery(start, today, mode, true);
  const refresh = useRefreshBehaviorAudit();
  const [refreshing, setRefreshing] = React.useState(false);

  const rows = auditQuery.data ?? [];
  const extraRows = rows.flatMap((r) =>
    (r.extraList ?? []).map((e) => ({ ...e, market: r.market })),
  );
  const missingRows = rows.flatMap((r) =>
    (r.missingList ?? []).map((m) => ({ ...m, market: r.market })),
  );

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const health = healthQuery.data as any;
  const cnHoldings: Array<{ symbol: string; name?: string; positionPct?: number }> =
    health?.holdings ?? [];
  const multiHoldings: Array<{ symbol: string; name?: string; positionPct?: number }> =
    health?.multiAssetHoldings ?? [];
  const hkHoldings: Array<{ symbol: string; name?: string; positionPct?: number }> =
    health?.hkHealth?.holdings ?? [];
  const holdings = [...cnHoldings, ...hkHoldings, ...multiHoldings];
  const multi = health?.multiAssetSleeve as SleeveBlock | undefined;

  const gapHoldings: HoldingSnap[] = holdings.map((h) => ({
    symbol: h.symbol,
    positionPct: h.positionPct,
    name: h.name,
  }));
  const gap = detectReplicaGaps({
    pick: multi?.pick?.key ?? null,
    holdings: gapHoldings,
  });
  const gapBlocks = gap.reasons.filter((r) => r.severity === 'block').slice(0, 2);

  const holdRows = holdings;
  const sleeveBuy = multi && (multi.action === 'BUY' || multi.action === 'ROTATE') ? multi : null;
  const sleeveHold = multi && multi.action === 'HOLD' ? multi : null;

  const rawBuyList: Array<{ symbol: string; name?: string; reason: string; market?: string }> = [
    ...missingRows.map((m) => ({
      symbol: m.symbol,
      name: (m as unknown as { name?: string }).name,
      reason: `回测应持有${(m as unknown as { score?: number }).score != null ? ` score ${(m as unknown as { score?: number }).score}` : ''}`,
      market: (m as unknown as { market?: string }).market,
    })),
    ...(sleeveBuy
      ? [
          {
            symbol: sleeveBuy.pick?.symbol ?? 'ETF',
            name: sleeveBuy.pick?.key ?? '',
            reason: `${sleeveBuy.label ?? sleeveBuy.action} mom60 ${sleeveBuy.pick?.mom60 ?? ''}%`,
            market: 'ETF',
          },
        ]
      : []),
  ];
  const sleevePickKey = multi?.pick?.key ?? null;
  const isStockPick = sleevePickKey === 'STOCK';
  const filteredBuyList = rawBuyList.filter((b) => {
    const isStock = b.symbol.startsWith('CN:') || b.symbol.startsWith('HK:');
    if (isStock && !isStockPick) return false; // Harbor pick is an ETF -> stock buys not executed
    return true;
  });
  const buyDedup = Array.from(new Map(filteredBuyList.map((b) => [b.symbol, b])).values());

  const onRefresh = () => {
    setRefreshing(true);
    void refresh.mutateAsync(undefined, { onSettled: () => setRefreshing(false) });
  };

  // ---- unified action list (all buys/sells through one form) ----
  const items: ActionItem[] = [
    ...buyDedup.map((b) => ({
      symbol: b.symbol,
      name: b.name,
      side: 'BUY' as const,
      reason: b.reason,
      isEtf: !(b.symbol.startsWith('CN:') || b.symbol.startsWith('HK:')),
    })),
    ...extraRows.map((e) => ({
      symbol: e.symbol,
      name: (e as unknown as { name?: string }).name,
      side: 'SELL' as const,
      reason: '回测应卖出',
      isEtf: !(e.symbol.startsWith('CN:') || e.symbol.startsWith('HK:')),
    })),
  ];
  const [itemKey, setItemKey] = React.useState<string>('');
  const [userPicked, setUserPicked] = React.useState(false);
  React.useEffect(() => {
    setItemKey((prev) => {
      if (!items.length) return '';
      if (!userPicked) return items[0].symbol;
      return items.some((i) => i.symbol === prev) ? prev : items[0].symbol;
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [items.map((i) => i.symbol).join(','), userPicked]);
  const selected = items.find((i) => i.symbol === itemKey) ?? null;

  const [status, setStatus] = React.useState<string>('filled');
  const [premium, setPremium] = React.useState('');
  const [fillPx, setFillPx] = React.useState('');
  const [pct, setPct] = React.useState('10');
  const [note, setNote] = React.useState('');
  const [err, setErr] = React.useState<string | null>(null);
  const [done, setDone] = React.useState<string | null>(null);

  const mut = useMutation({
    mutationFn: async () => {
      if (!selected) throw new Error('无可记录的操作');
      const px = Number(fillPx);
      if (!Number.isFinite(px) || px <= 0) throw new Error('请填成交价');
      if (selected.isEtf) {
        await postSleeveExec({
          pickKey: sleevePickKey,
          symbol: selected.symbol,
          status,
          premiumBps: premium.trim() === '' ? null : Number(premium),
          signalPrice: multi?.pick?.close ?? null,
          fillPrice: px,
          note: note.trim() || null,
          meta: { action: multi?.action ?? null, side: selected.side },
        });
      } else {
        const p = Number(pct);
        if (!Number.isFinite(p) || p <= 0) throw new Error('请填仓位%');
        const market = selected.symbol.startsWith('HK:') ? 'HK' : 'CN';
        await recordUserTrade({
          symbol: selected.symbol,
          side: selected.side,
          price: px,
          positionPct: p,
          source: 'MANUAL',
          market,
          note: note.trim() || undefined,
        });
        await invalidateUserTradesQueries(qc);
      }
    },
    onSuccess: async () => {
      setErr(null);
      setNote('');
      setPremium('');
      setFillPx('');
      setDone(selected ? `${selected.side === 'BUY' ? '买入' : '卖出'} ${selected.symbol} 已记录` : '已记录');
      if (selected?.isEtf) {
        await qc.invalidateQueries({ queryKey: ['sleeve-execution-log'] });
      }
    },
    onError: (e: Error) => setErr(e.message),
  });

  const last = timelineQ.data?.rows?.[timelineQ.data.rows.length - 1];
  const satActive = (last as unknown as { satActive?: boolean })?.satActive ?? null;
  const baseRet = last?.navBaseReturnPct;
  const singleRet =
    (last as unknown as { navSingleReturnPct?: number })?.navSingleReturnPct ??
    last?.navMultiReturnPct;
  const excess = baseRet != null && singleRet != null ? singleRet - baseRet : null;
  const pick = multi?.pick?.key ?? last?.pick ?? 'REPO';
  const buyChips = buyDedup.slice(0, 8);
  const sellChips = extraRows.slice(0, 8);
  const holdChips = holdRows.slice(0, 8);

  return (
    <div className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2.5">
      <div className="flex flex-wrap items-baseline gap-2">
        <span className="text-[13px] font-semibold">{strategyName} · 今日决策</span>
        <span className="text-[10px] text-[var(--k-muted)]">{MODE_SUBTITLE[mode]}</span>
        <Button
          variant="ghost"
          size="sm"
          className="ml-auto h-6 px-2 text-[11px]"
          onClick={onRefresh}
          disabled={refreshing || refresh.isPending}
        >
          {refreshing ? '回测中…' : '刷新'}
        </Button>
      </div>

      {baseRet != null && singleRet != null && (
        <div className="mt-2 rounded-md border border-emerald-500/30 bg-emerald-500/5 px-2.5 py-1.5 text-[11px]">
          <span className="font-semibold text-emerald-700">
            今日 {pick}
          </span>
          <span className="ml-2 tabular-nums text-[var(--k-muted)]">
            过去年 基线 {baseRet.toFixed(1)}% · {strategyName} {singleRet.toFixed(1)}% · 超额{' '}
            {excess != null ? `${excess >= 0 ? '+' : ''}${excess.toFixed(1)}pt` : '—'}
          </span>
          <span className="ml-2 text-emerald-700">
            → {sleeveHold ? `持有 ${sleeveHold.pick?.symbol}` : buyDedup.length ? `买 ${buyDedup[0].symbol}` : '持有不动'}
          </span>
        </div>
      )}

      {gap.verdict !== 'aligned' && gapBlocks.length > 0 && (
        <div className="mt-2 rounded-md border border-[var(--k-border)] bg-[var(--k-bg)]/40 px-2.5 py-1.5 text-[10px] text-[var(--k-muted)]">
          今日仓位脚注：pick={gap.pick} · 目标腿 {gap.targetWeightPct}% · 股 {gap.stockWeightPct}% ·
          ETF {gap.etfWeightPct}%（本质差异看回测「归因对照」）
        </div>
      )}

      {mode !== 'harbor' ? (
        <div className="mt-2 rounded-md border border-sky-500/30 bg-sky-500/5 px-2.5 py-1.5 text-[10px] text-[var(--k-muted)]">
          <span className="font-medium text-sky-700 dark:text-sky-300">组合腿</span>
          {mode === 'homeport'
            ? ' · B3 逆波动率 50/50（月初再平衡）——paper/实盘记账未接线（OPT-186）'
            : null}
          {mode === 'starport'
            ? ` · B3 月初再平衡（未接线 OPT-186） · 卫星有仓日 1/3 overlay（最近交易日 ${
                last ? (satActive ? '有仓' : '空仓') : '—'
              }；信号/成交组件待 OPT-178/186）`
            : null}
        </div>
      ) : null}

      <div className="mt-2 flex flex-col gap-1.5 text-xs">
        <div className="flex flex-wrap items-center gap-1.5">
          <span className="shrink-0 rounded bg-emerald-500/10 px-1.5 py-0.5 text-[11px] font-medium text-emerald-700">
            买
          </span>
          {buyChips.length ? (
            buyChips.map((b) => (
              <span
                key={b.symbol}
                className="rounded border border-emerald-500/20 bg-emerald-500/5 px-1.5 py-0.5 font-mono text-[11px]"
                title={b.reason}
              >
                {b.symbol}
              </span>
            ))
          ) : (
            <span className="text-[11px] text-[var(--k-muted)]">—</span>
          )}
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          <span className="shrink-0 rounded bg-red-500/10 px-1.5 py-0.5 text-[11px] font-medium text-red-700">
            卖
          </span>
          {sellChips.length ? (
            sellChips.map((e) => (
              <span
                key={e.symbol}
                className="rounded border border-red-500/20 bg-red-500/5 px-1.5 py-0.5 font-mono text-[11px]"
              >
                {e.symbol}
              </span>
            ))
          ) : (
            <span className="text-[11px] text-[var(--k-muted)]">—</span>
          )}
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          <span className="shrink-0 rounded bg-[var(--k-surface-2)] px-1.5 py-0.5 text-[11px] font-medium">
            持有
          </span>
          {holdChips.length
            ? holdChips.map((h) => (
                <span
                  key={h.symbol}
                  className="rounded border border-[var(--k-border)] bg-[var(--k-surface-2)] px-1.5 py-0.5 font-mono text-[11px]"
                >
                  {h.symbol}
                </span>
              ))
            : null}
          {sleeveHold && !holdRows.some((h) => h.symbol === sleeveHold.pick?.symbol) ? (
            <span
              className="rounded border border-sky-500/20 bg-sky-500/5 px-1.5 py-0.5 font-mono text-[11px] text-sky-700"
              title={`mom60 ${sleeveHold.pick?.mom60}%`}
            >
              {sleeveHold.pick?.symbol}持有
            </span>
          ) : null}
          {!holdChips.length && !sleeveHold ? (
            <span className="text-[11px] text-[var(--k-muted)]">—</span>
          ) : null}
        </div>
      </div>

      {/* 全部买卖记录：一个组件、一个表单 */}
      <div className="mt-3 border-t border-[var(--k-border)]/60 pt-2">
        <div className="flex flex-wrap items-center gap-2 text-[11px]">
          <span className="font-medium">记录成交</span>
          {items.length ? (
            <span className="text-[10px] text-[var(--k-muted)]">
              所有买卖都在这里记 — 股票进 /trades，ETF 停车进执行日志
            </span>
          ) : (
            <span className="text-[10px] text-[var(--k-muted)]">今日无待操作</span>
          )}
        </div>
        {items.length ? (
          <div className="mt-2 flex flex-wrap items-end gap-2">
            <label className="flex min-w-[12rem] flex-col gap-0.5 text-[10px] text-[var(--k-muted)]">
              标的
              <select
                className="h-8 rounded border border-[var(--k-border)] bg-transparent px-2 text-[12px] text-[var(--k-fg)]"
                value={itemKey}
                onChange={(e) => {
                  setUserPicked(true);
                  setItemKey(e.target.value);
                  setDone(null);
                }}
              >
                {items.map((i) => (
                  <option key={`${i.side}-${i.symbol}`} value={i.symbol}>
                    {i.side === 'BUY' ? '买' : '卖'} {i.symbol}
                    {i.name ? ` · ${i.name}` : ''}
                  </option>
                ))}
              </select>
            </label>
            {selected?.isEtf ? (
              <>
                <label className="flex flex-col gap-0.5 text-[10px] text-[var(--k-muted)]">
                  结果
                  <select
                    className="h-8 rounded border border-[var(--k-border)] bg-transparent px-2 text-[12px] text-[var(--k-fg)]"
                    value={status}
                    onChange={(e) => setStatus(e.target.value)}
                  >
                    {STATUS_OPTS.map((o) => (
                      <option key={o.id} value={o.id}>
                        {o.label}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="flex flex-col gap-0.5 text-[10px] text-[var(--k-muted)]">
                  溢价 bp
                  <input
                    className="h-8 w-20 rounded border border-[var(--k-border)] bg-transparent px-2 font-mono text-[12px]"
                    inputMode="decimal"
                    placeholder="e.g. 80"
                    value={premium}
                    onChange={(e) => setPremium(e.target.value)}
                  />
                </label>
              </>
            ) : (
              <label className="flex flex-col gap-0.5 text-[10px] text-[var(--k-muted)]">
                仓位 %
                <input
                  className="h-8 w-20 rounded border border-[var(--k-border)] bg-transparent px-2 font-mono text-[12px]"
                  inputMode="decimal"
                  value={pct}
                  onChange={(e) => setPct(e.target.value)}
                />
              </label>
            )}
            <label className="flex flex-col gap-0.5 text-[10px] text-[var(--k-muted)]">
              成交价
              <input
                className="h-8 w-24 rounded border border-[var(--k-border)] bg-transparent px-2 font-mono text-[12px]"
                inputMode="decimal"
                placeholder={multi?.pick?.close != null ? String(multi.pick.close) : '—'}
                value={fillPx}
                onChange={(e) => setFillPx(e.target.value)}
              />
            </label>
            <label className="min-w-[8rem] flex-1 flex-col gap-0.5 text-[10px] text-[var(--k-muted)]">
              备注
              <input
                className="h-8 w-full rounded border border-[var(--k-border)] bg-transparent px-2 text-[12px]"
                value={note}
                onChange={(e) => setNote(e.target.value)}
              />
            </label>
            <Button
              type="button"
              size="sm"
              className="h-8"
              disabled={mut.isPending || !selected}
              onClick={() => mut.mutate()}
            >
              {mut.isPending ? '写入…' : '记一笔'}
            </Button>
          </div>
        ) : null}
        {err ? <p className="mt-1 text-[11px] text-red-600 dark:text-red-400">{err}</p> : null}
        {done ? (
          <p className={cn('mt-1 text-[11px] text-emerald-700 dark:text-emerald-300')}>{done}</p>
        ) : null}
      </div>
    </div>
  );
}
