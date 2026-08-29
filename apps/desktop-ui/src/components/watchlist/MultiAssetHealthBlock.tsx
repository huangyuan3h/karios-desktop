'use client';
import * as React from 'react';
import { cn } from '@/lib/utils';
import type { PortfolioHealthResponse } from '@/lib/queries/portfolioHealth';

type MultiHolding = NonNullable<PortfolioHealthResponse['multiAssetHoldings']>[number];
type MultiSleeve = PortfolioHealthResponse['multiAssetSleeve'];

const KEY_META: Record<string, { label: string; icon: string; color: string }> = {
  STOCK: { label: '股票篮', icon: '📈', color: 'border-violet-500/30 bg-violet-500/5' },
  GOLD: { label: '黄金', icon: '🪙', color: 'border-amber-500/30 bg-amber-500/5' },
  OIL: { label: '原油', icon: '🛢️', color: 'border-slate-500/30 bg-slate-500/5' },
  NASDAQ: { label: '纳指', icon: '🇺🇸', color: 'border-blue-500/30 bg-blue-500/5' },
  BOND10: { label: '国债', icon: '🏦', color: 'border-emerald-500/30 bg-emerald-500/5' },
};

function holdingKey(sym: string): string {
  const s = sym.toUpperCase();
  if (s.includes('518880') || s.includes('518800')) return 'GOLD';
  if (s.includes('513350') || s.includes('159518') || s.includes('561570')) return 'OIL';
  if (s.includes('513110') || s.includes('513100') || s.includes('513500')) return 'NASDAQ';
  if (s.includes('511260') || s.includes('511010')) return 'BOND10';
  return 'OTHER';
}

export function MultiAssetHealthBlock({
  holdings,
  sleeve,
  onOpen,
}: {
  holdings: MultiHolding[] | undefined | null;
  sleeve: MultiSleeve | undefined | null;
  onOpen?: (symbol: string) => void;
}) {
  const hasHoldings = holdings && holdings.length > 0;
  const pickKey = (sleeve as unknown as { pick?: { key?: string } })?.pick?.key;
  const actionable = sleeve?.action && sleeve.action !== 'NONE' && sleeve.action !== 'DONT_BUY';

  if (!hasHoldings && !actionable) return null;

  return (
    <div className="flex min-w-0 flex-col gap-2 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/60 p-2.5">
      <div className="flex items-center gap-2 text-[11px] font-semibold">
        <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5">择强单轨</span>
        STOCK · 金 · 油 · 纳 · 债
        {pickKey ? (
          <span className="rounded bg-sky-500/10 px-1.5 py-0.5 text-[10px] text-sky-700 dark:text-sky-300">今日：{pickKey}</span>
        ) : null}
        {sleeve?.idlePct != null ? (
          <span className="ml-auto text-[10px] font-normal tabular-nums text-[var(--k-muted)]">闲置 {sleeve.idlePct}%</span>
        ) : null}
      </div>

      {hasHoldings ? (
        <div className="flex flex-col gap-1.5">
          {holdings!.map((h) => {
            const key = holdingKey(h.symbol);
            const meta = KEY_META[key] ?? { label: key, icon: '📦', color: 'border-[var(--k-border)] bg-[var(--k-surface)]' };
            const md = (h as unknown as { marketData?: { close?: number; ma200?: number; above?: boolean } }).marketData;
            const pnl = (h as unknown as { pnlPct?: number }).pnlPct;
            const above = md?.above;
            return (
              <div key={h.symbol} className={cn('flex flex-wrap items-center gap-x-2 gap-y-1 rounded-lg border px-2.5 py-2 text-xs', meta.color)}>
                <span>{meta.icon}</span>
                <button type="button" onClick={() => onOpen?.(h.symbol)} className="font-medium hover:underline">
                  {h.symbol}
                </button>
                <span className="text-[11px] text-[var(--k-muted)]">{meta.label} · 仓位 {h.positionPct ?? '—'}%</span>
                {typeof pnl === 'number' ? (
                  <span className={cn('font-mono text-[11px]', pnl >= 0 ? 'text-emerald-600' : 'text-red-600')}>{pnl >= 0 ? '+' : ''}{pnl.toFixed(2)}%</span>
                ) : null}
                {md?.close != null ? (
                  <span className="font-mono text-[11px] tabular-nums text-[var(--k-muted)]">
                    {md.close} / MA200 {md.ma200?.toFixed(2)} {above ? '· 站上' : '· 跌破'}
                  </span>
                ) : null}
                <span className={cn('ml-auto rounded px-1.5 py-0.5 text-[10px]', above ? 'bg-emerald-500/10 text-emerald-700' : 'bg-red-500/10 text-red-600')}>
                  {above ? '持有' : '预警'}
                </span>
              </div>
            );
          })}
        </div>
      ) : (
        <div className="text-xs text-[var(--k-muted)]">当前无多资产持仓</div>
      )}

      {sleeve?.message ? <div className="text-[11px] text-[var(--k-muted)]">{sleeve.message}</div> : null}
      {sleeve?.action && sleeve.action !== 'NONE' ? (
        <div className="text-[11px] text-[var(--k-muted)]">
          动作：<span className="font-medium text-[var(--k-fg)]">{sleeve.label ?? sleeve.action}</span>
          {pickKey ? ` · 择强 ${pickKey} mom60 ${(sleeve as unknown as { pick?: { mom60?: number } }).pick?.mom60 ?? ''}%` : ''}
        </div>
      ) : null}
    </div>
  );
}
