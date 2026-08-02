'use client';

import * as React from 'react';

import {
  DASHBOARD_HELP,
  buildDashboardHelpTooltipBody,
  type DashboardHelp,
} from '@/lib/dashboard-card-help';

/**
 * Dashboard-side bilingual header with hover tooltip.
 *
 * The dashboard doesn't have a shared portal tooltip infrastructure like
 * WatchlistTable, so this component uses:
 *   1. `title=...` (native browser tooltip — instant, free, no JS).
 *   2. A lightweight inline <span> with hover/focus events to toggle a custom
 *      rich tooltip div (so we can show multi-line formulas).
 *
 * It is intentionally lightweight: a small absolute-positioned div, no portal.
 * For very long tooltips this can be enhanced to use createPortal later.
 */
export type DashboardHeaderProps = {
  helpId: keyof typeof DASHBOARD_HELP;
  help?: DashboardHelp;
  className?: string;
  align?: 'left' | 'right';
  width?: number;
};

export function DashboardHeader({
  helpId,
  help,
  className,
  align = 'left',
  width = 360,
}: DashboardHeaderProps) {
  const h = help ?? DASHBOARD_HELP[helpId];
  const [open, setOpen] = React.useState(false);
  const [pos, setPos] = React.useState<{ x: number; y: number }>({ x: 0, y: 0 });
  const ref = React.useRef<HTMLSpanElement | null>(null);

  const show = React.useCallback(() => {
    const el = ref.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    const pad = 8;
    const x = align === 'right' ? Math.max(pad, r.right - width) : Math.max(pad, r.left);
    const y = r.bottom + 6;
    setPos({ x, y });
    setOpen(true);
  }, [align, width]);

  const hide = React.useCallback(() => setOpen(false), []);

  if (!h) {
    return <span className={className}>{helpId}</span>;
  }

  return (
    <span
      ref={ref}
      className={`relative inline-flex flex-col items-${align} gap-0.5 leading-tight ${className ?? ''}`}
      title={h.short}
      tabIndex={0}
      onMouseEnter={show}
      onMouseLeave={hide}
      onFocus={show}
      onBlur={hide}
      aria-label={`${h.label} — ${h.sub ?? ''}`.trim()}
    >
      <span>{h.label}</span>
      {h.sub ? (
        <span className="text-[10px] font-normal text-[var(--k-muted)]">{h.sub}</span>
      ) : null}
      {open ? (
        <span
          role="tooltip"
          className="fixed z-[9999] rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3 text-[11px] text-[var(--k-text)] shadow-lg"
          style={{
            left: pos.x,
            top: pos.y,
            width,
            maxWidth: '90vw',
            maxHeight: '60vh',
            overflow: 'auto',
          }}
          onMouseEnter={() => setOpen(true)}
          onMouseLeave={hide}
        >
          {buildDashboardHelpTooltipBody(h)}
        </span>
      ) : null}
    </span>
  );
}
