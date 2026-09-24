'use client';

import * as React from 'react';

import { setAccountCapital, useAccountSettings } from '@/lib/account-settings';
import { cn } from '@/lib/utils';

function fmt(n: number): string {
  return n.toLocaleString('zh-CN');
}

const FIELD =
  'h-7 rounded-lg border border-[var(--k-border)]/60 bg-[var(--k-surface-2)]/45 px-2 font-mono ' +
  'text-[var(--k-text)] outline-none backdrop-blur-sm transition-colors ' +
  'focus:border-[var(--k-accent)]/50 focus:bg-[var(--k-surface)]/70';

/**
 * Shared "账户" area (OPT-204 / 2026-09-21).
 *
 * One place to edit the account total capital + HKD/CNY rate. Shared app-wide
 * through ``account-settings`` (localStorage) and consumed by every
 * integer-lot order aid (satellite buy rows, QuickBuyDialog, …). Display only —
 * it never changes strategy sizing or Live decisions.
 */
export function AccountBar({ className }: { className?: string }) {
  const { capital } = useAccountSettings();
  const [capDraft, setCapDraft] = React.useState(capital != null ? String(capital) : '');

  React.useEffect(() => {
    setCapDraft(capital != null ? String(capital) : '');
  }, [capital]);

  const capNum = Number(capDraft);
  const capValid = capDraft !== '' && Number.isFinite(capNum) && capNum > 0;
  const capDirty = capValid && Number(capDraft) !== capital;

  return (
    <div
      data-testid="account-bar"
      className={cn(
        'mb-4 flex flex-wrap items-center gap-x-5 gap-y-2 rounded-2xl px-4 py-2.5',
        'border border-[var(--k-border)]/70 bg-[var(--k-surface)]/55 shadow-[0_1px_3px_rgba(0,0,0,0.05)]',
        'backdrop-blur-md',
        className,
      )}
    >
      <span className="flex items-center gap-1.5 text-[13px] font-semibold tracking-wide">
        <span className="inline-block h-1.5 w-1.5 rounded-full bg-[var(--k-accent)]/70" />
        账户
      </span>
      <label className="flex items-center gap-2 text-xs">
        <span className="text-[var(--k-muted)]">总资金</span>
        <input
          className={cn(FIELD, 'w-32')}
          inputMode="decimal"
          aria-label="总资金"
          placeholder="1000000"
          value={capDraft}
          onChange={(e) => {
            const raw = e.target.value;
            if (raw === '' || /^\d+(\.\d{0,2})?$/.test(raw)) setCapDraft(raw);
          }}
          onBlur={() => setAccountCapital(capValid ? capNum : null)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') (e.target as HTMLInputElement).blur();
          }}
        />
        {capDirty ? (
          <span className="text-[11px] text-[var(--k-accent)]">未保存</span>
        ) : capital != null ? (
          <span className="text-[11px] tabular-nums text-[var(--k-muted)]">¥{fmt(capital)}</span>
        ) : (
          <span className="text-[11px] text-amber-600 dark:text-amber-400">未设置</span>
        )}
      </label>
      <span className="text-[11px] text-[var(--k-muted)]/80">
        仓位 % → 整数股数（仅本机保存，不参与策略；港股按 0.92 估算）
      </span>
    </div>
  );
}
