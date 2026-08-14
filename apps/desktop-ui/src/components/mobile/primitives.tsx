import * as React from 'react';
import { cn } from '@/lib/utils';

/* ============================================================
   Mobile primitives (Mobile Redesign 2027)
   Every phone page MUST compose these — never hand-roll styles.
   Colors reuse --k-* (themed). Size/spacing use --m-* scale.
   ============================================================ */

type Tone = 'open' | 'closed' | 'up' | 'down' | 'warn' | 'danger' | 'neutral';

const TONE_STYLE: Record<Tone, React.CSSProperties> = {
  open: { color: 'var(--k-accent)', background: 'color-mix(in srgb, var(--k-accent) 14%, transparent)' },
  closed: { color: 'var(--k-muted)', background: 'var(--k-surface-2)' },
  up: { color: 'var(--k-up)', background: 'color-mix(in srgb, var(--k-up) 14%, transparent)' },
  down: { color: 'var(--k-down)', background: 'color-mix(in srgb, var(--k-down) 14%, transparent)' },
  warn: { color: 'var(--k-warn)', background: 'color-mix(in srgb, var(--k-warn) 14%, transparent)' },
  danger: { color: 'var(--k-danger)', background: 'color-mix(in srgb, var(--k-danger) 14%, transparent)' },
  neutral: { color: 'var(--k-muted)', background: 'var(--k-surface-2)' },
};

export function MobileCard({
  className,
  onClick,
  children,
}: {
  className?: string;
  onClick?: () => void;
  children: React.ReactNode;
}) {
  return (
    <div
      onClick={onClick}
      className={cn(
        'rounded-[var(--m-radius-md)] border border-[var(--k-border)] bg-[var(--k-surface)]',
        'shadow-[var(--m-shadow-sm)]',
        onClick && 'active:scale-[0.99] transition-transform cursor-pointer',
        className,
      )}
    >
      {children}
    </div>
  );
}

export function MobileSection({
  title,
  action,
  children,
}: {
  title: string;
  action?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <section className="space-y-3">
      <div className="flex items-center justify-between px-0.5">
        <h2 className="text-[var(--m-text-lg)] font-semibold text-[var(--k-text)]">{title}</h2>
        {action ? <div className="text-[var(--m-text-sm)] text-[var(--k-accent)]">{action}</div> : null}
      </div>
      {children}
    </section>
  );
}

export function StatusPill({ tone, children }: { tone: Tone; children: React.ReactNode }) {
  return (
    <span
      style={TONE_STYLE[tone]}
      className="inline-flex items-center rounded-[var(--m-radius-pill)] px-2 py-0.5 text-[var(--m-text-xs)] font-medium"
    >
      {children}
    </span>
  );
}

export function GateBadge({ market, open }: { market: string; open: boolean }) {
  return <StatusPill tone={open ? 'open' : 'closed'}>{market}</StatusPill>;
}

export function PriceText({ value, prefix }: { value: number; prefix?: string }) {
  const tone: Tone = value > 0 ? 'up' : value < 0 ? 'down' : 'neutral';
  const arrow = value > 0 ? '▲' : value < 0 ? '▼' : '';
  return (
    <span style={TONE_STYLE[tone]} className="text-[var(--m-text-base)] font-semibold">
      {prefix}
      {value.toFixed(2)} {arrow}
    </span>
  );
}

export function PctText({ value }: { value: number }) {
  const tone: Tone = value > 0 ? 'up' : value < 0 ? 'down' : 'neutral';
  const arrow = value > 0 ? '▲' : value < 0 ? '▼' : '';
  return (
    <span style={TONE_STYLE[tone]} className="text-[var(--m-text-sm)] font-medium">
      {arrow}
      {Math.abs(value).toFixed(2)}%
    </span>
  );
}

export function MobileRow({
  leading,
  trailing,
  onClick,
  className,
}: {
  leading: React.ReactNode;
  trailing?: React.ReactNode;
  onClick?: () => void;
  className?: string;
}) {
  return (
    <div
      onClick={onClick}
      className={cn(
        'flex min-h-[var(--m-tap)] items-center justify-between gap-3 border-b border-[var(--k-border)] px-1 py-2',
        onClick && 'active:bg-[var(--k-surface-2)] cursor-pointer',
        className,
      )}
    >
      <div className="min-w-0 flex-1">{leading}</div>
      {trailing ? <div className="shrink-0">{trailing}</div> : null}
    </div>
  );
}

export function MobileList({ children, className }: { children: React.ReactNode; className?: string }) {
  return <div className={cn('divide-y divide-[var(--k-border)]', className)}>{children}</div>;
}

type ButtonVariant = 'primary' | 'ghost' | 'danger';
type ButtonSize = 'sm' | 'base';

export function MobileButton({
  variant = 'primary',
  size = 'base',
  block,
  disabled,
  className,
  children,
  onClick,
}: {
  variant?: ButtonVariant;
  size?: ButtonSize;
  block?: boolean;
  disabled?: boolean;
  className?: string;
  children: React.ReactNode;
  onClick?: () => void;
}) {
  const base =
    'inline-flex items-center justify-center rounded-[var(--m-radius-md)] font-medium transition-colors disabled:opacity-50';
  const sizes = size === 'sm' ? 'h-9 px-3 text-[var(--m-text-sm)]' : 'h-[var(--m-tap)] px-4 text-[var(--m-text-base)]';
  const variants: Record<ButtonVariant, string> = {
    primary: 'bg-[var(--k-accent)] text-white',
    ghost: 'border border-[var(--k-accent)] text-[var(--k-accent)] bg-transparent',
    danger: 'bg-[var(--k-danger)] text-white',
  };
  return (
    <button
      type="button"
      disabled={disabled}
      onClick={onClick}
      className={cn(base, sizes, variants[variant], block && 'w-full', className)}
    >
      {children}
    </button>
  );
}

export function MobileSheet({
  open,
  onClose,
  title,
  children,
}: {
  open: boolean;
  onClose: () => void;
  title?: string;
  children: React.ReactNode;
}) {
  React.useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && onClose();
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open, onClose]);
  if (!open) return null;
  return (
    <div className="fixed inset-0 z-50 flex items-end justify-center">
      <div className="absolute inset-0 bg-black/40" onClick={onClose} aria-hidden />
      <div
        role="dialog"
        aria-modal="true"
        className="relative max-h-[82vh] w-full overflow-y-auto rounded-t-[var(--m-radius-xl)] border border-[var(--k-border)] bg-[var(--k-surface)] p-[var(--m-content-pad)] pb-[calc(var(--m-content-pad)+env(safe-area-inset-bottom))]"
      >
        {title ? (
          <div className="mb-3 flex items-center justify-between">
            <h3 className="text-[var(--m-text-lg)] font-semibold">{title}</h3>
            <button type="button" onClick={onClose} className="text-[var(--m-text-sm)] text-[var(--k-muted)]">
              关闭
            </button>
          </div>
        ) : null}
        {children}
      </div>
    </div>
  );
}

export function MobileField({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="block space-y-1.5">
      <span className="text-[var(--m-text-sm)] text-[var(--k-muted)]">{label}</span>
      {children}
    </label>
  );
}

export function SkeletonBlock({ h = 14, w = '100%', className }: { h?: number; w?: string | number; className?: string }) {
  return <div className={cn('m-shimmer', className)} style={{ height: h, width: w }} />;
}

export function EmptyState({
  icon,
  title,
  hint,
}: {
  icon?: React.ReactNode;
  title: string;
  hint?: string;
}) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 px-6 py-10 text-center">
      {icon ? <div className="text-[28px] opacity-70">{icon}</div> : null}
      <div className="text-[var(--m-text-base)] font-medium text-[var(--k-text)]">{title}</div>
      {hint ? <div className="text-[var(--m-text-sm)] text-[var(--k-muted)]">{hint}</div> : null}
    </div>
  );
}
