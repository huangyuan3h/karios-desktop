import * as React from 'react';

import { cn } from '@/lib/utils';

type TabsCtxValue = {
  value: string;
  setValue: (v: string) => void;
};

const TabsCtx = React.createContext<TabsCtxValue | null>(null);

export function Tabs({
  value,
  defaultValue,
  onValueChange,
  className,
  children,
}: {
  value?: string;
  defaultValue?: string;
  onValueChange?: (v: string) => void;
  className?: string;
  children: React.ReactNode;
}) {
  const [uncontrolled, setUncontrolled] = React.useState<string>(defaultValue ?? '');
  const v = value ?? uncontrolled;

  const setValue = React.useCallback(
    (next: string) => {
      if (value === undefined) setUncontrolled(next);
      onValueChange?.(next);
    },
    [onValueChange, value],
  );

  return (
    <TabsCtx.Provider value={{ value: v, setValue }}>
      <div className={cn('w-full', className)}>{children}</div>
    </TabsCtx.Provider>
  );
}

export function TabsList({
  className,
  children,
}: {
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <div
      className={cn(
        'inline-flex h-9 items-center justify-center rounded-lg bg-[var(--k-surface-2)] p-1',
        'border border-[var(--k-border)]',
        className,
      )}
    >
      {children}
    </div>
  );
}

export function TabsTrigger({
  value,
  className,
  children,
}: {
  value: string;
  className?: string;
  children: React.ReactNode;
}) {
  const ctx = React.useContext(TabsCtx);
  if (!ctx) throw new Error('TabsTrigger must be used within <Tabs>');

  const active = ctx.value === value;

  return (
    <button
      type="button"
      onClick={() => ctx.setValue(value)}
      className={cn(
        'inline-flex items-center justify-center whitespace-nowrap rounded-md px-3 py-1 text-sm font-medium',
        'transition-colors',
        active
          ? 'bg-[var(--k-surface)] text-[var(--k-text)] shadow-sm'
          : 'text-[var(--k-muted)] hover:text-[var(--k-text)]',
        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--k-ring)]',
        'disabled:pointer-events-none disabled:opacity-50',
        className,
      )}
      aria-pressed={active}
    >
      {children}
    </button>
  );
}

export function TabsContent({
  value,
  className,
  children,
}: {
  value: string;
  className?: string;
  children: React.ReactNode;
}) {
  const ctx = React.useContext(TabsCtx);
  if (!ctx) throw new Error('TabsContent must be used within <Tabs>');
  if (ctx.value !== value) return null;
  return <div className={cn('mt-4', className)}>{children}</div>;
}

