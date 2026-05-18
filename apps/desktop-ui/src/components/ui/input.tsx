import * as React from 'react';
import { cn } from '@/lib/utils';

export interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {}

export const Input = React.forwardRef<HTMLInputElement, InputProps>(
  ({ className, type, ...props }, ref) => {
    return (
      <input
        type={type}
        className={cn(
          'h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-3 text-sm outline-none',
          'focus:ring-2 focus:ring-[var(--k-ring)]',
          'placeholder:text-[var(--k-muted)]',
          'disabled:cursor-not-allowed disabled:opacity-50',
          className,
        )}
        ref={ref}
        {...props}
      />
    );
  },
);
Input.displayName = 'Input';
