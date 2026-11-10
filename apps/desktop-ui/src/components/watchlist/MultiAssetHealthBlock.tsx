'use client';
import * as React from 'react';
import { cn } from '@/lib/utils';
import { intradayTrailWarnFromSleeve, isM30B3Symbol, pendingParkSellFromSleeve } from '@/lib/m30-follow';
import { WHIP_ANNOTATION } from '@/lib/follow-timing';
import { PARKING_META, parkingKeyForSymbol } from '@/lib/parking-universe';
import type { AgentWhipState } from '@/lib/queries/agent';
import type { PortfolioHealthResponse } from '@/lib/queries/portfolioHealth';

type MultiHolding = NonNullable<PortfolioHealthResponse['multiAssetHoldings']>[number];
type MultiSleeve = PortfolioHealthResponse['multiAssetSleeve'];

/** Bare code for `ETF:513350` / `513350.SH` (display only). */
function bareCodeOf(symbol: string): string {
  const s = String(symbol || '').toUpperCase();
  return (s.includes(':') ? s.split(':')[1]! : s).split('.')[0]!;
}

const KEY_META: Record<string, { label: string; icon: string; color: string }> = {
  STOCK: { label: '股票篮', icon: '📈', color: 'border-violet-500/30 bg-violet-500/5' },
  GOLD: { label: PARKING_META.GOLD.label, icon: '🪙', color: 'border-amber-500/30 bg-amber-500/5' },
  OIL: { label: PARKING_META.OIL.label, icon: '🛢️', color: 'border-slate-500/30 bg-slate-500/5' },
  NASDAQ: {
    label: PARKING_META.NASDAQ.label,
    icon: '🇺🇸',
    color: 'border-blue-500/30 bg-blue-500/5',
  },
  BOND10: {
    label: PARKING_META.BOND10.label,
    icon: '🏦',
    color: 'border-emerald-500/30 bg-emerald-500/5',
  },
};

/** Prefer the API's canonical key; fall back to the shared alias map. */
function holdingKey(h: MultiHolding): string {
  return h.key ?? parkingKeyForSymbol(h.symbol) ?? 'OTHER';
}

export function MultiAssetHealthBlock({
  holdings,
  sleeve,
  onOpen,
  coreDestinationReady = true,
  m30 = false,
  whip = null,
  overlayBusy = false,
  onOverlay,
}: {
  holdings: MultiHolding[] | undefined | null;
  sleeve: MultiSleeve | undefined | null;
  onOpen?: (symbol: string) => void;
  /** When pick=STOCK but the basket has 0 executable names, keep ETFs parked. */
  coreDestinationReady?: boolean;
  /**
   * 母港 M30 view: the 5 B3 ETFs are the 30% B3 leg, not stale parking — never
   * tag them 卖出 here; sizing lives in the 母港M30跟仓 panel.
   */
  m30?: boolean;
  /** Server-side same-key rebuy whip (P0). */
  whip?: AgentWhipState | null;
  overlayBusy?: boolean;
  /** Confirm / clear the `hold_same_key_rebuy` overlay (annotation only). */
  onOverlay?: (status: 'accepted' | 'cleared') => void;
}) {
  const openHoldings = (holdings ?? []).filter(
    (h) => typeof h.positionPct === 'number' && h.positionPct > 0,
  );
  const hasHoldings = openHoldings.length > 0;
  const pickKey = (sleeve as unknown as { pick?: { key?: string } })?.pick?.key;
  const actionable = sleeve?.action && sleeve.action !== 'NONE' && sleeve.action !== 'DONT_BUY';
  /** Pending Harbor exit (trail8 / MA200 wipe) — backend decision, render only. */
  const pendingSell = pendingParkSellFromSleeve(
    sleeve as unknown as Parameters<typeof pendingParkSellFromSleeve>[0],
  );
  const pendingSellCode = pendingSell ? bareCodeOf(pendingSell.symbol) : null;
  /** 14:30 conditional warning (close still decides; adds are blocked). */
  const intradayWarn = intradayTrailWarnFromSleeve(
    sleeve as unknown as Parameters<typeof intradayTrailWarnFromSleeve>[0],
  );
  const intradayWarnCode = intradayWarn ? bareCodeOf(intradayWarn.symbol) : null;
  const overlayActive = whip?.overlay?.effectiveStatus === 'accepted';
  const overlayProposed = whip?.overlay?.effectiveStatus === 'proposed';

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
          <span
            className={cn(
              'rounded px-1.5 py-0.5 text-[10px]',
              pendingSell
                ? 'bg-red-500/15 font-semibold text-red-700 dark:text-red-300'
                : intradayWarn
                  ? 'bg-amber-500/15 font-semibold text-amber-700 dark:text-amber-300'
                  : 'bg-sky-500/10 text-sky-700 dark:text-sky-300',
            )}
          >
            {pendingSell
              ? `待卖：${pendingSellCode}`
              : intradayWarn
                ? `${intradayWarn.status === 'breached' ? '已触线' : '临近'}：${intradayWarnCode}`
                : `今日：${pickKey}`}
          </span>
        ) : null}
        {sleeve?.idlePct != null ? (
          <span className="ml-auto text-[10px] font-normal tabular-nums text-[var(--k-muted)]">
            闲置 {sleeve.idlePct}%
          </span>
        ) : null}
      </div>

      {whip?.sameKeyRebuy ? (
        <div
          data-testid="parking-whip"
          className="rounded-md border border-amber-500/50 bg-amber-500/10 px-2.5 py-1.5 text-[11px] text-amber-800 dark:text-amber-200"
        >
          <div className="font-semibold">
            ⚠ {whip.pickKey ?? pickKey} 刚止损：{whip.copy ?? WHIP_ANNOTATION}
          </div>
          <div className="mt-0.5 flex flex-wrap items-center gap-2 font-normal">
            <span>
              {overlayActive
                ? `已暂缓同键回买至 ${whip.overlay?.validUntil ?? '有效期结束'}；引擎目标不变，回买前请再确认（k=0）`
                : '引擎目标不变；回买前请确认，不设冷却（k=0）'}
            </span>
            {onOverlay ? (
              <button
                type="button"
                data-testid="parking-whip-overlay"
                disabled={overlayBusy}
                onClick={() => onOverlay(overlayActive ? 'cleared' : 'accepted')}
                className="rounded border border-amber-500/50 bg-amber-500/10 px-1.5 py-0.5 text-[10px] font-semibold text-amber-800 hover:bg-amber-500/20 disabled:opacity-50 dark:text-amber-200"
              >
                {overlayBusy
                  ? '保存中…'
                  : overlayActive
                    ? '取消暂缓'
                    : overlayProposed
                      ? '确认暂缓'
                      : '暂不跟同品种回买'}
              </button>
            ) : null}
            {overlayProposed ? (
              <span className="text-[10px] font-normal">乐乐提议，待确认</span>
            ) : null}
          </div>
        </div>
      ) : null}

      {pendingSell ? (
        <div
          data-testid="parking-must-act"
          className="rounded-md border border-red-500/50 bg-red-500/10 px-2.5 py-1.5 text-[11px] text-red-800 dark:text-red-100"
        >
          <div className="font-semibold">
            ⛔ 断线止损待执行：卖出 {pendingSellCode} → 逆回购
          </div>
          <div className="mt-0.5">
            {sleeve?.message ?? '峰值回撤触线'}
            {pendingSell.signalDate ? ` · 信号日 ${pendingSell.signalDate} 收盘` : ''}
            {' · '}
            {pendingSell.executeHint ?? '次日开盘卖出转逆回购'}；卖出前不加仓、不买回
          </div>
        </div>
      ) : intradayWarn ? (
        <div
          data-testid="parking-intraday-warn"
          className="rounded-md border border-amber-500/50 bg-amber-500/10 px-2.5 py-1.5 text-[11px] text-amber-800 dark:text-amber-200"
        >
          <div className="font-semibold">
            ⚠ 14:30 {intradayWarn.status === 'breached' ? '现价已达卖出条件' : '现价逼近卖出线'}：
            卖出 {intradayWarnCode} → 逆回购
          </div>
          <div className="mt-0.5">
            {intradayWarn.px != null ? `现价 ${intradayWarn.px}` : null}
            {intradayWarn.thresholdPx != null
              ? ` ${intradayWarn.status === 'breached' ? '≤' : '≥'} 门槛价 ${intradayWarn.thresholdPx}`
              : null}
            {intradayWarn.peak != null ? ` · 峰值 ${intradayWarn.peak}` : null}
            {intradayWarn.drawdownPct != null ? ` · 回撤 ${intradayWarn.drawdownPct}%` : null}
            {' · '}
            {intradayWarn.executeHint ?? '若收盘仍低于门槛价 → 明日开盘卖出转逆回购（不盘中卖）'}
            ；今日不加仓、不买回
          </div>
        </div>
      ) : null}

      {!pendingSell &&
      !intradayWarn &&
      sleeve?.idlePct != null &&
      sleeve.idlePct >= 1 &&
      pickKey != null &&
      (pickKey !== 'STOCK' || coreDestinationReady) ? (
        <div className="rounded-md border border-sky-500/40 bg-sky-500/10 px-2.5 py-1.5 text-[11px] text-sky-800 dark:text-sky-200">
          {m30 ? (
            <>
              母港M30：港湾腿（70%）的停车部分停进{' '}
              {sleeve.pick?.symbol ?? sleeve.etfPick?.symbol ?? pickKey}
              ；份数/金额见上方操作清单
            </>
          ) : (
            <>
              今日动作：用闲置 {sleeve.parkPct ?? sleeve.idlePct}% 停进{' '}
              {sleeve.pick?.symbol ?? sleeve.etfPick?.symbol ?? pickKey}
              （港湾停车只停闲钱，股票核心不动）
            </>
          )}
        </div>
      ) : null}

      {hasHoldings ? (
        <div className="flex flex-col gap-1.5">
          {openHoldings.map((h) => {
            const key = holdingKey(h);
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
            const parkTarget = sleeve?.parkPct ?? sleeve?.idlePct;
            const holdTip =
              pickKey === 'STOCK' && !coreDestinationReady
                ? '核心股票篮空 · 停车场 ETF 继续持有（不清仓）'
                : isPick
                  ? '今日 pick（mom60+MA200 选趋势最好）· 目标 = 闲置现金'
                  : undefined;
            const m30B3 = m30 && isM30B3Symbol(h.symbol);
            const isPendingSellLeg =
              pendingSellCode != null && bareCodeOf(h.symbol) === pendingSellCode;
            const isWarnLeg =
              !isPendingSellLeg &&
              intradayWarnCode != null &&
              bareCodeOf(h.symbol) === intradayWarnCode;
            const adjust = isPendingSellLeg
              ? {
                  label: '待卖 · 止损',
                  cls: 'bg-red-500/15 text-red-700 dark:text-red-300',
                  tip: `港湾停车止损：${
                    pendingSell?.executeHint ?? '次日开盘卖出转逆回购'
                  }（${sleeve?.message ?? '峰值回撤触线'}）`,
                }
              : isWarnLeg
                ? {
                    label: intradayWarn?.status === 'breached' ? '已触线' : '临近触线',
                    cls: 'bg-amber-500/15 text-amber-700 dark:text-amber-300',
                    tip: `${
                      intradayWarn?.message ?? '14:30 现价接近卖出阈值价'
                    }；${
                      intradayWarn?.executeHint ?? '若收盘仍低于阈值价 → 明日开盘卖出转逆回购'
                    }`,
                  }
                : m30B3
              ? {
                  label: 'B3 腿',
                  cls: 'bg-sky-500/10 text-sky-700',
                  tip: '母港M30：B3 成分（30% 腿），按月频目标调整，份数见上方操作清单',
                }
              : pickKey != null && key !== pickKey && key !== 'OTHER' && pos != null && pos > 0
                ? pickKey === 'STOCK' && !coreDestinationReady
                  ? null
                  : {
                      label: '卖出',
                      cls: 'bg-red-500/10 text-red-600',
                      tip: `停车轮动：非今日 pick（${pickKey}），资金调向 ${pickKey}`,
                    }
                : isPick
                  ? pos != null && parkTarget != null && pos < parkTarget - 1
                    ? {
                        label: '加仓',
                        cls: 'bg-sky-500/10 text-sky-700',
                        tip:
                          parkTarget != null
                            ? `今日 pick · 停车目标 ${parkTarget.toFixed(0)}%（当前 ${pos.toFixed(1)}%）`
                            : `今日 pick · 当前 ${pos.toFixed(1)}%`,
                      }
                    : {
                        label: '持有',
                        cls: 'bg-emerald-500/10 text-emerald-700',
                        tip: holdTip ?? '今日 pick（mom60+MA200 选趋势最好）',
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
                    isPendingSellLeg
                      ? 'bg-red-500/15 font-semibold text-red-700 dark:text-red-300'
                      : isWarnLeg
                        ? 'bg-amber-500/15 font-semibold text-amber-700 dark:text-amber-300'
                        : above
                          ? 'bg-emerald-500/10 text-emerald-700'
                          : 'bg-red-500/10 text-red-600',
                  )}
                  title={
                    isPendingSellLeg
                      ? '停车止损：待开盘卖出转逆回购'
                      : isWarnLeg
                        ? '14:30 现价触线：收盘确认后次日开盘卖出'
                        : undefined
                  }
                >
                  {isPendingSellLeg ? '待卖' : isWarnLeg ? '触线' : above ? '持有' : '预警'}
                </span>
              </div>
            );
          })}
        </div>
      ) : (
        <div className="text-xs text-[var(--k-muted)]">当前无多资产持仓</div>
      )}

      {sleeve?.action && sleeve.action !== 'NONE' ? (
        <div className="text-[11px] text-[var(--k-muted)]">
          动作：
          <span
            className={cn(
              'font-medium',
              pendingSell || intradayWarn
                ? 'text-red-600 dark:text-red-400'
                : 'text-[var(--k-fg)]',
            )}
          >
            {pendingSell
              ? '卖出 → 逆回购（止损，待开盘执行）'
              : intradayWarn
                ? intradayWarn.status === 'breached'
                  ? '现价已达卖出条件（待收盘确认）'
                  : '现价逼近卖出线（观察）'
                : (sleeve.label ?? sleeve.action)}
          </span>
          {pickKey && !pendingSell && !intradayWarn
            ? ` · 停车 ${pickKey} mom60 ${(sleeve as unknown as { pick?: { mom60?: number } }).pick?.mom60 ?? ''}%`
            : ''}
        </div>
      ) : null}
    </div>
  );
}
