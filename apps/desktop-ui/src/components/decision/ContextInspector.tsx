'use client';

import React from 'react';

import { Layers, RefreshCw } from 'lucide-react';

import { Progress } from '@/components/ui/progress';
import { Switch } from '@/components/ui/switch';
import { cn } from '@/lib/utils';
import {
  DECISION_BLOCK_DEFS,
  type DecisionActiveLayer,
} from '@/lib/decision-context';
import type { DataSourceFreshness } from '@/lib/freshness';

export const DECISION_TOKEN_BUDGET = 35_000;

const TIER_STYLE: Record<string, string> = {
  P0: 'border-indigo-600/40 bg-indigo-500/10 text-indigo-700 dark:text-indigo-300',
  P1: 'border-emerald-600/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300',
  P2: 'border-zinc-500/40 bg-zinc-500/10 text-zinc-600 dark:text-zinc-400',
};

function BlockRow({
  block,
  enabled,
  onToggle,
}: {
  block: (typeof DECISION_BLOCK_DEFS)[number] & { tokens?: number };
  enabled: boolean;
  onToggle: (enabled: boolean) => void;
}) {
  return (
    <div className="flex items-center gap-2 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-2 py-1.5">
      <span
        className={cn(
          'w-7 shrink-0 rounded border px-1 py-0.5 text-center text-[10px] font-semibold',
          TIER_STYLE[block.tier],
        )}
      >
        {block.tier}
      </span>
      <div className="min-w-0 flex-1">
        <div className="truncate text-xs font-medium">{block.label}</div>
        {block.tokens != null && (
          <div className="text-[10px] text-[var(--k-muted)]">{block.tokens.toLocaleString()} tok</div>
        )}
      </div>
      <Switch checked={enabled} onCheckedChange={onToggle} className="h-5 w-9 scale-90" />
    </div>
  );
}

function FreshnessRow({ source }: { source: DataSourceFreshness }) {
  const age = source.ageMinutes == null ? 'unknown' : `${source.ageMinutes}m`;
  return (
    <div className="flex items-center gap-1.5 text-[11px]">
      <span
        className={cn(
          'h-1.5 w-1.5 shrink-0 rounded-full',
          source.stale ? 'bg-red-500' : 'bg-emerald-500',
        )}
      />
      <span className="truncate">{source.label}</span>
      <span className={cn('ml-auto tabular-nums', source.stale ? 'font-semibold text-red-600 dark:text-red-400' : 'text-[var(--k-muted)]')}>
        {age} {source.stale ? '⚠' : ''}
      </span>
    </div>
  );
}

export function ContextInspector({
  layer,
  toggles,
  onToggle,
  windowCount,
  windowCap,
  snapshots,
  refreshing,
  onRefresh,
  onInsertArchive,
}: {
  layer: DecisionActiveLayer | null;
  toggles: Record<string, boolean>;
  onToggle: (blockId: string, enabled: boolean) => void;
  windowCount: number;
  windowCap: number;
  snapshots: Array<{
    snapshotDate: string;
    status: string;
    outcome?: { fired?: unknown[]; paper?: unknown[] } | null;
  }>;
  refreshing: boolean;
  onRefresh: () => void;
  onInsertArchive?: (snapshot: { snapshotDate: string; status: string }) => void;
}) {
  const pct = Math.min(100, Math.round(((layer?.totalTokens ?? 0) / DECISION_TOKEN_BUDGET) * 100));
  const over = (layer?.totalTokens ?? 0) > DECISION_TOKEN_BUDGET;

  return (
    <div className="flex h-full flex-col gap-3 overflow-y-auto border-l border-[var(--k-border)] bg-[var(--k-surface)] p-3 text-sm">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-1.5 text-xs font-semibold">
          <Layers size={13} className="text-[var(--k-accent)]" />
          Context
        </div>
        <button
          onClick={onRefresh}
          disabled={refreshing}
          className="flex items-center gap-1 rounded-md px-1.5 py-1 text-[11px] text-[var(--k-muted)] transition-colors hover:bg-[var(--k-surface-2)] hover:text-[var(--k-text)] disabled:opacity-50"
        >
          <RefreshCw size={11} className={cn(refreshing && 'animate-spin')} />
          刷新
        </button>
      </div>

      {/* Layer 1 */}
      <div>
        <div className="mb-1.5 flex items-baseline justify-between">
          <span className="text-[11px] font-medium text-[var(--k-muted)]">Layer 1 · 活跃层</span>
          <span className="text-[11px] tabular-nums text-[var(--k-muted)]">
            {(layer?.totalTokens ?? 0).toLocaleString()} tok
          </span>
        </div>
        <div className="flex flex-col gap-1.5">
          {DECISION_BLOCK_DEFS.map((def) => (
            <BlockRow
              key={def.id}
              block={def}
              enabled={toggles[def.id] !== false}
              onToggle={(enabled) => onToggle(def.id, enabled)}
            />
          ))}
        </div>
        <div className="mt-2 flex items-center gap-2">
          <Progress value={pct} className={cn('h-1.5', over && 'bg-red-500/20')} />
          <span
            className={cn(
              'w-20 shrink-0 text-right text-[10px] tabular-nums',
              over ? 'font-semibold text-red-600 dark:text-red-400' : 'text-[var(--k-muted)]',
            )}
          >
            {pct}% / 35k
          </span>
        </div>
      </div>

      {/* Layer 2 */}
      <div className="rounded-md border border-[var(--k-border)] px-2 py-1.5">
        <div className="flex items-baseline justify-between">
          <span className="text-[11px] font-medium text-[var(--k-muted)]">Layer 2 · 对话窗口</span>
          <span className="text-[11px] tabular-nums text-[var(--k-muted)]">
            {windowCount}/{windowCap} 条
          </span>
        </div>
        <p className="mt-0.5 text-[10px] leading-4 text-[var(--k-muted)]">
          超出的消息折叠为「判断/结果」摘要行，保留信号不保留原文
        </p>
      </div>

      {/* Layer 3 */}
      <div className="rounded-md border border-[var(--k-border)] px-2 py-1.5">
        <div className="flex items-baseline justify-between">
          <span className="text-[11px] font-medium text-[var(--k-muted)]">Layer 3 · 10 天归档</span>
          <span className="text-[10px] text-[var(--k-muted)]">点击插入引用</span>
        </div>
        {snapshots.length ? (
          <div className="mt-1 flex flex-col gap-0.5">
            {snapshots.slice(0, 8).map((s) => {
              const firedCount = s.outcome?.fired?.length ?? 0;
              return (
                <button
                  key={s.snapshotDate}
                  onClick={() => onInsertArchive?.(s)}
                  className="flex w-full items-center gap-1.5 rounded px-1 py-0.5 text-left text-[10px] text-[var(--k-muted)] transition-colors hover:bg-[var(--k-surface-2)] hover:text-[var(--k-text)]"
                >
                  <span className="h-1 w-1 shrink-0 rounded-full bg-[var(--k-accent)]" />
                  {s.snapshotDate}
                  <span className="ml-auto tabular-nums">
                    {firedCount > 0 ? `开火 ${firedCount} · ` : ''}
                    {s.status === 'open' ? '未反馈' : '已反馈'}
                  </span>
                </button>
              );
            })}
          </div>
        ) : (
          <p className="mt-0.5 text-[10px] text-[var(--k-muted)]">暂无归档快照（18:00 作业自动生成）</p>
        )}
      </div>

      {/* Freshness */}
      <div className="rounded-md border border-[var(--k-border)] px-2 py-1.5">
        <div className="mb-1 text-[11px] font-medium text-[var(--k-muted)]">数据新鲜度</div>
        <div className="flex flex-col gap-1">
          {(layer?.freshness ?? []).map((s) => (
            <FreshnessRow key={s.source} source={s} />
          ))}
          {!layer?.freshness?.length && (
            <p className="text-[10px] text-[var(--k-muted)]">不可用</p>
          )}
        </div>
      </div>
    </div>
  );
}
