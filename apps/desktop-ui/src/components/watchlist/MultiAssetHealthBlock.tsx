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
  coreDestinationReady = true,
}: {
  holdings: MultiHolding[] | undefined | null;
  sleeve: MultiSleeve | undefined | null;
  onOpen?: (symbol: string) => void;
  /** When pick=STOCK but the basket has 0 executable names, keep ETFs parked. */
  coreDestinationReady?: boolean;
}) {
  const openHoldings = (holdings ?? []).filter(
    (h) => typeof h.positionPct === 'number' && h.positionPct > 0,
  );
  const hasHoldings = openHoldings.length > 0;
  const pickKey = (sleeve as unknown as { pick?: { key?: string } })?.pick?.key;
  const actionable = sleeve?.action && sleeve.action !== 'NONE' && sleeve.action !== 'DONT_BUY';

  if (!hasHoldings && !actionable) return null;

  return (
    <div className="flex min-w-0 flex-col gap-2 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/60 p-2.5">
      <div className="flex items-center gap-2 text-[11px] font-semibold">
        <span className="h-3 w-[3px] rounded-full bg-sky-500" />
        停车体检
        <span className="text-[10px] font-normal text-[var(--k-muted)]">
          闲置现金 ETF 停车场
        </span>
      </div>
      <div className="flex items-center gap-2 text-[11px] font-semibold">
        <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-1.5 py-0.5">
          港湾
        </span>
        STOCK · 金 · 油 · 纳 · 债
        {pickKey ? (
          <span className="rounded bg-sky-500/10 px-1.5 py-0.5 text-[10px] text-sky-700 dark:text-sky-300">
            今日：{pickKey}
          </span>
        ) : null}
        {sleeve?.idlePct != null ? (
          <span className="ml-auto text-[10px] font-normal tabular-nums text-[var(--k-muted)]">
            闲置 {sleeve.idlePct}%
          </span>
        ) : null}
      </div>

      {sleeve?.idlePct != null &&
      sleeve.idlePct >= 1 &&
      pickKey != null &&
      (pickKey !== 'STOCK' || coreDestinationReady) ? (
        <div className="rounded-md border border-sky-500/40 bg-sky-500/10 px-2.5 py-1.5 text-[11px] text-sky-800 dark:text-sky-200">
          今日动作：用闲置 {sleeve.idlePct}% 买入{' '}
          {sleeve.pick?.symbol ?? sleeve.etfPick?.symbol ?? pickKey}，核心补足 100%
          （闲置现金停进核心 ETF，不留现金）
        </div>
      ) : null}

      {hasHoldings ? (
        <div className="flex flex-col gap-1.5">
          {openHoldings.map((h) => {
            const key = holdingKey(h.symbol);
            const meta = KEY_META[key] ?? {
              label: key,
              icon: '📦',
              color: 'border-[var(--k-border)] bg-[var(--k-surface)]',
            };
            const md = (
              h as unknown as { marketData?: { close?: number; ma200?: number; above?: boolean } }
            ).marketData;
            const pnl = (h as unknown as { pnlPct?: number }).pnlPct;
            const above = md?.above;
            const pos = typeof h.positionPct === 'number' ? h.positionPct : null;
            const isPick = pickKey != null && key === pickKey;
            const holdTip =
              pickKey === 'STOCK' && !coreDestinationReady
                ? '核心股票篮空 · 停车场 ETF 继续持有（不清仓）'
                : isPick
                  ? '今日 pick（mom_compare 定案）· 目标 100%'
                  : undefined;
            const adjust =
              pickKey != null && key !== pickKey && key !== 'OTHER' && pos != null && pos > 0
                ? pickKey === 'STOCK' && !coreDestinationReady
                  ? null
                  : {
                      label: '卖出',
                      cls: 'bg-red-500/10 text-red-600',
                      tip: `非今日 pick（${pickKey}），资金调向 ${pickKey}`,
                    }
                : isPick
                  ? pos != null && pos < 99
                    ? {
                        label: '加仓',
                        cls: 'bg-sky-500/10 text-sky-700',
                        tip: `今日 pick · 目标 100%（当前 ${pos.toFixed(1)}%）`,
                      }
                    : {
                        label: '持有',
                        cls: 'bg-emerald-500/10 text-emerald-700',
                        tip: holdTip ?? '今日 pick（mom_compare 定案）',
                      }
                  : holdTip
                    ? { label: '持有', cls: 'bg-emerald-500/10 text-emerald-700', tip: holdTip }
                    : null;
            return (
              <div
                key={h.symbol}
                className={cn(
                  'flex flex-wrap items-center gap-x-2 gap-y-1 rounded-lg border px-2.5 py-2 text-xs',
                  meta.color,
                )}
              >
                <span>{meta.icon}</span>
                <button
                  type="button"
                  onClick={() => onOpen?.(h.symbol)}
                  className="font-medium hover:underline"
                >
                  {h.symbol}
                </button>
                {adjust ? (
                  <span
                    className={cn('rounded px-1.5 py-0.5 text-[10px] font-semibold', adjust.cls)}
                    title={adjust.tip}
                  >
                    {adjust.label}
                  </span>
                ) : holdTip ? (
                  <span
                    className="rounded bg-emerald-500/10 px-1.5 py-0.5 text-[10px] font-semibold text-emerald-700"
                    title={holdTip}
                  >
                    持有
                  </span>
                ) : null}
                <span className="text-[11px] text-[var(--k-muted)]">
                  {meta.label} · 仓位 {pos?.toFixed(1) ?? '—'}%
                </span>
                {typeof pnl === 'number' ? (
                  <span
                    className={cn(
                      'font-mono text-[11px]',
                      pnl >= 0 ? 'text-emerald-600' : 'text-red-600',
                    )}
                  >
                    {pnl >= 0 ? '+' : ''}
                    {pnl.toFixed(2)}%
                  </span>
                ) : null}
                {md?.close != null ? (
                  <span className="font-mono text-[11px] tabular-nums text-[var(--k-muted)]">
                    {md.close} / MA200 {md.ma200?.toFixed(2)} {above ? '· 站上' : '· 跌破'}
                  </span>
                ) : null}
                <span
                  className={cn(
                    'ml-auto rounded px-1.5 py-0.5 text-[10px]',
                    above ? 'bg-emerald-500/10 text-emerald-700' : 'bg-red-500/10 text-red-600',
                  )}
                >
                  {above ? '持有' : '预警'}
                </span>
              </div>
            );
          })}
        </div>
      ) : (
        <div className="text-xs text-[var(--k-muted)]">当前无多资产持仓</div>
      )}

      {sleeve?.message && !(pickKey === 'STOCK' && !coreDestinationReady) ? (
        <div className="text-[11px] text-[var(--k-muted)]">{sleeve.message}</div>
      ) : null}
      {sleeve?.action && sleeve.action !== 'NONE' ? (
        <div className="text-[11px] text-[var(--k-muted)]">
          动作：
          <span className="font-medium text-[var(--k-fg)]">{sleeve.label ?? sleeve.action}</span>
          {pickKey
            ? ` · 核心 ${pickKey} mom60 ${(sleeve as unknown as { pick?: { mom60?: number } }).pick?.mom60 ?? ''}%`
            : ''}
        </div>
      ) : null}
    </div>
  );
}
