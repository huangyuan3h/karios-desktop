'use client';

import * as React from 'react';

import {
  WATCHLIST_COLUMN_HELP,
  buildWatchlistColumnTooltipBody,
  type WatchlistColumnHelp,
} from '@/lib/watchlist-column-help';

/**
 * Bilingual column header with native browser tooltip + optional rich portal tooltip.
 *
 * Renders label (bold) + sub (CN, smaller, muted) on two lines.
 * Always wires `title=...` for instant native tooltip (free, no JS).
 * If showTooltip is passed, also opens rich portal on hover/focus.
 *
 * Always renders a `<span>` (not a `<button>`) so it can be safely nested
 * inside another `<button>` (e.g. the score sort handler in WatchlistTable).
 */
export type ColumnHeaderProps = {
  columnId: string;
  help?: WatchlistColumnHelp;
  className?: string;
  showTooltip?: (el: HTMLElement, content: React.ReactNode, width?: number) => void;
  hideTooltip?: () => void;
  width?: number;
};

export function ColumnHeader({
  columnId,
  help,
  className,
  showTooltip,
  hideTooltip,
  width = 360,
}: ColumnHeaderProps) {
  const h = help ?? WATCHLIST_COLUMN_HELP[columnId];
  if (!h) {
    return <span className={className}>{columnId}</span>;
  }
  const interactive = Boolean(showTooltip && hideTooltip);
  const body = (
    <div className="flex flex-col items-start gap-0.5 leading-tight">
      <span className="whitespace-nowrap">{h.label}</span>
      {h.sub ? (
        <span className="whitespace-nowrap text-[10px] font-normal text-[var(--k-muted)]">
          {h.sub}
        </span>
      ) : null}
    </div>
  );
  if (!interactive) {
    return (
      <span className={className} title={h.short}>
        {body}
      </span>
    );
  }
  return (
    <span
      role="button"
      tabIndex={0}
      className={`inline-flex cursor-help items-center rounded px-1 py-0.5 hover:text-[var(--k-text)] ${className ?? ''}`}
      title={h.short}
      onMouseEnter={(e) => {
        if (showTooltip) showTooltip(e.currentTarget, buildWatchlistColumnTooltipBody(h), width);
      }}
      onMouseLeave={() => hideTooltip?.()}
      onFocus={(e) => {
        if (showTooltip) showTooltip(e.currentTarget, buildWatchlistColumnTooltipBody(h), width);
      }}
      onBlur={() => hideTooltip?.()}
      aria-label={`${h.label} — ${h.sub ?? ''}`.trim()}
    >
      {body}
    </span>
  );
}
