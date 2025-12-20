import * as React from 'react';

import { cn } from '@/lib/utils';

export type ButtonProps = React.ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: 'primary' | 'secondary' | 'ghost';
  size?: 'sm' | 'md';
};

export function Button({
  className,
  variant = 'primary',
  size = 'md',
  ...props
}: ButtonProps) {
  return (
    <button
      className={cn(
        'inline-flex items-center justify-center rounded-md text-sm font-medium transition-colors',
        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-zinc-950/20 dark:focus-visible:ring-zinc-50/20',
        'disabled:pointer-events-none disabled:opacity-50',
        size === 'sm' ? 'h-8 px-3' : 'h-10 px-4',
        variant === 'primary' &&
          'bg-zinc-950 text-zinc-50 hover:bg-zinc-950/90 dark:bg-zinc-50 dark:text-zinc-950 dark:hover:bg-zinc-50/90',
        variant === 'secondary' &&
          'bg-zinc-100 text-zinc-950 hover:bg-zinc-100/80 dark:bg-zinc-900 dark:text-zinc-50 dark:hover:bg-zinc-900/80',
        variant === 'ghost' &&
          'bg-transparent text-zinc-950 hover:bg-zinc-100 dark:text-zinc-50 dark:hover:bg-zinc-900',
        className,
      )}
      {...props}
    />
  );
}


