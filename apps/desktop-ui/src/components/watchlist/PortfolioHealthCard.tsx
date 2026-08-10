'use client';

import * as React from 'react';

import { RefreshCw, ShieldAlert } from 'lucide-react';

import { useQuery } from '@tanstack/react-query';

import { Button } from '@/components/ui/button';
import {
  fetchPortfolioHealth,
  type PortfolioHealthResponse,
  type PortfolioHolding,
} from '@/lib/queries/portfolioHealth';
import { cn } from '@/lib/utils';

function fmtPct(v: number | null | undefined, digits = 2): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v >= 0 ? '+' : ''}${v.toFixed(digits)}%`;
}

function regimeBadge(regime: string | null | undefined): { label: string; cls: string } {
  switch (regime) {
    case 'Weak':
      return { label: 'Weak · 空仓观望', cls: 'border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-300' };
    case 'Strong':
      return { label: 'Strong · 进攻', cls: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300' };
    case 'Diverging':
      return { label: 'Diverging · 满仓进攻', cls: 'border-sky-500/40 bg-sky-500/10 text-sky-700 dark:text-sky-300' };
    default:
      return { label: String(regime ?? '—'), cls: 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]' };
  }
}

function HoldingRow({ h, onOpen }: { h: PortfolioHolding; onOpen?: (symbol: string) => void }) {
  const exit = h.action === 'EXIT';
  const pnlTone = (h.pnlPct ?? 0) >= 0 ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400';
  return (
    <div
      role={onOpen ? 'button' : undefined}
      onClick={onOpen ? () => onOpen(h.symbol) : undefined}
      className={cn(
        'rounded-lg border px-3 py-2',
        exit ? 'border-red-500/40 bg-red-500/5' : 'border-[var(--k-border)] bg-[var(--k-surface-2)]',
        onOpen && 'cursor-pointer transition-colors hover:border-[var(--k-accent)]/60',
      )}
    >
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
        <span className="text-[13px] font-semibold">{h.name || h.symbol}</span>
        <span className="text-[11px] tabular-nums text-[var(--k-muted)]">
          {h.symbol} · 仓位 {h.positionPct != null ? `${h.positionPct}%` : '—'}
        </span>
        <span className={cn('ml-auto font-mono text-[13px] font-semibold', pnlTone)}>
          {fmtPct(h.pnlPct)}
        </span>
        <span className="font-mono text-[11px] tabular-nums text-[var(--k-muted)]">
          回撤 {fmtPct(h.drawdownFromPeakPct)}
        </span>
        <span
          className={cn(
            'rounded px-1.5 py-0.5 text-[10px] font-semibold',
            exit
              ? 'bg-red-500/15 text-red-600 dark:text-red-400'
              : 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-300',
          )}
        >
          {exit ? '🔴 建议退出' : '✅ 持有'}
        </span>
        {h.pyramidAdded && (
          <span className="rounded bg-sky-500/15 px-1.5 py-0.5 text-[10px] text-sky-700 dark:text-sky-300">
            已加仓
          </span>
        )}
      </div>
      <div className="mt-1 flex flex-wrap gap-x-4 gap-y-0.5 font-mono text-[10.5px] tabular-nums text-[var(--k-muted)]">
        <span>止损线 {h.stopLossLine ?? '—'}</span>
        <span>移动线 {h.trailingLine ?? '—'}</span>
        <span>金字塔线 {h.pyramidTriggerLine ?? '—'}</span>
        <span>已持 {h.holdingDays ?? '—'} 天</span>
        <span>到期 {h.expireDate ?? '—'}</span>
      </div>
      {h.reason && <div className="mt-1 text-[11px] text-red-600 dark:text-red-400">触发：{h.reason}</div>}
      {h.note && <div className="mt-1 text-[11px] text-[var(--k-muted)]">{h.note}</div>}
    </div>
  );
}

function HealthPanel({
  title,
  tag,
  block,
  onOpen,
}: {
  title: string;
  tag: string;
  block: PortfolioHealthResponse | null | undefined;
  onOpen?: (symbol: string) => void;
}) {
  const holdings = block?.holdings ?? [];
  const candidates = block?.s3Candidates ?? [];
  const regime = regimeBadge(block?.regime);
  return (
    <div className="flex min-w-0 flex-col gap-2 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/60 p-2.5">
      <div className="flex items-center gap-2 text-[11px] font-semibold">
        <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5">{tag}</span>
        {title}
        <span className="ml-auto text-[10px] font-normal tabular-nums text-[var(--k-muted)]">
          {block?.tradeDate ?? '—'}
        </span>
      </div>
      <div className="flex flex-wrap items-center gap-2 text-[11px]">
        <span className={cn('rounded border px-1.5 py-0.5 font-medium', regime.cls)}>{regime.label}</span>
        {block?.sentiment != null && (
          <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5">
            sentiment: {block.sentiment}
          </span>
        )}
        {block?.panicCooldown?.active ? (
          <span className="rounded border border-amber-500/40 bg-amber-500/10 px-1.5 py-0.5 text-amber-700 dark:text-amber-300">
            恐慌冷却至 {block.panicCooldown.cooldownEndDate}
          </span>
        ) : null}
        <span className="text-[var(--k-muted)]">
          S-3 候选：{block ? (block.s3Candidates?.length ?? 0) : '…'} 只
        </span>
      </div>
      {holdings.length === 0 ? (
        <div className="text-xs text-[var(--k-muted)]">当前无持仓（未录入成本/仓位的 watchlist 票不算持仓）</div>
      ) : (
        <div className="flex flex-col gap-1.5">
          {holdings.map((h) => (
            <HoldingRow key={h.symbol} h={h} onOpen={onOpen} />
          ))}
        </div>
      )}
      {candidates.length > 0 ? (
        <div className="rounded-lg border border-emerald-500/30 bg-emerald-500/5 px-3 py-2">
          <div className="mb-1 text-[11px] font-medium text-emerald-700 dark:text-emerald-300">
            今日开仓候选（每票 ~5% 仓位）
          </div>
          <div className="flex flex-wrap gap-2">
            {candidates.map((c) => (
              <span
                key={c.symbol}
                className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-2 py-0.5 text-[11px]"
              >
                {c.name ?? c.symbol}
                <span className="ml-1 text-[var(--k-muted)]">score={c.score ?? '—'}</span>
              </span>
            ))}
          </div>
        </div>
      ) : block ? (
        <div className="text-[11px] text-[var(--k-muted)]">
          {block.regime === 'Weak'
            ? '今日无开仓候选（regime=Weak：S-3 规定空仓观望）'
            : '今日无开仓候选（score≥65 · RS 前 50% · 无恐慌冷却）'}
        </div>
      ) : null}
    </div>
  );
}

export function PortfolioHealthCard({ onOpenStock }: { onOpenStock?: (symbol: string) => void } = {}) {
  const q = useQuery({
    queryKey: ['portfolio-health'],
    queryFn: ({ signal }) => fetchPortfolioHealth(undefined, signal),
    refetchInterval: 5 * 60_000,
  });

  const data: PortfolioHealthResponse | undefined = q.data;

  if (q.isError && !data) {
    return (
      <div className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-4 py-2.5 text-xs text-[var(--k-muted)]">
        <ShieldAlert size={13} className="mr-1 inline-block" />
        持仓体检暂不可用（data-sync-service 未响应）
      </div>
    );
  }

  return (
    <div className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-4 py-3">
      <div className="mb-2 flex items-center gap-2">
        <span className="text-[12px] font-semibold">S-3 持仓体检 · A 股 / 港股并行</span>
        <Button
          variant="ghost"
          size="sm"
          className="ml-auto h-6 px-1.5"
          onClick={() => void q.refetch()}
          disabled={q.isFetching}
          title="刷新体检"
        >
          <RefreshCw size={12} className={q.isFetching ? 'animate-spin' : ''} />
        </Button>
      </div>

      <div className="grid gap-2 md:grid-cols-2">
        <HealthPanel title="A 股 S-3（全闸门）" tag="CN" block={data} onOpen={onOpenStock} />
        <HealthPanel
          title="港股 S-3（regime 档 · trail -12%）"
          tag="HK"
          block={data?.hkHealth}
          onOpen={onOpenStock}
        />
      </div>
    </div>
  );
}
