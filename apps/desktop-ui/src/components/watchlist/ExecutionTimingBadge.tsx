'use client';

import * as React from 'react';

import { cn } from '@/lib/utils';
import {
  executionTiming,
  WHIP_ANNOTATION,
  WHIP_OVERLAY_COPY,
  type ExecutionTiming,
  type ExecutionTimingKind,
} from '@/lib/follow-timing';

const TONE: Record<ExecutionTiming['tone'], string> = {
  buy: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300',
  sell: 'border-red-500/40 bg-red-500/10 text-red-700 dark:text-red-300',
  warn: 'border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-300',
  muted: 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]',
};

/** One achievable execution clock chip (明日开盘跟 / 盘中可追 / 收盘待确认…). */
export function ExecutionTimingBadge({
  kind,
  now,
  testId = 'exec-timing',
  className,
}: {
  kind: ExecutionTimingKind;
  now?: Date;
  testId?: string;
  className?: string;
}) {
  const t = executionTiming(kind, now);
  return (
    <span
      data-testid={testId}
      data-timing-kind={kind}
      title={t.detail}
      className={cn(
        'inline-flex items-center rounded border px-1.5 py-0.5 text-[10px] font-medium',
        TONE[t.tone],
        className,
      )}
    >
      {t.label}
    </span>
  );
}

/** The whip-saw warning chip rendered next to a same-key post-trail BUY. */
export function WhipBadge({
  className,
  muted = false,
}: {
  className?: string;
  /** An accepted `hold_same_key_rebuy` overlay downgrades the copy only. */
  muted?: boolean;
}) {
  return (
    <span
      data-testid="whip-annotate"
      data-overlay={muted ? 'accepted' : undefined}
      title={
        muted
          ? '已设置「暂不跟同品种回买」：引擎目标不变，回买前请再确认（k=0）。'
          : '该标的刚触发停车/移动止损；次日按目标权重回买可能形成鞭打。UI 只提示，不改变策略（k=0）。'
      }
      className={cn(
        'inline-flex items-center rounded border px-1.5 py-0.5 text-[10px] font-semibold',
        muted
          ? 'border-sky-500/50 bg-sky-500/10 text-sky-700 dark:text-sky-300'
          : 'border-amber-500/60 bg-amber-500/15 text-amber-800 dark:text-amber-200',
        className,
      )}
    >
      {muted ? WHIP_OVERLAY_COPY : WHIP_ANNOTATION}
    </span>
  );
}
