'use client';

import * as React from 'react';
import { cn } from '@/lib/utils';

interface ProgressProps extends React.HTMLAttributes<HTMLDivElement> {
  value?: number;
}

function Progress({ value = 0, className, ...props }: ProgressProps) {
  return (
    <div
      className={cn(
        'relative h-2 w-full overflow-hidden rounded-full bg-[var(--k-surface-2)]',
        className,
      )}
      {...props}
    >
      <div
        className="h-full bg-[var(--k-primary)] transition-all duration-300 ease-in-out"
        style={{ width: `${Math.min(100, Math.max(0, value))}%` }}
      />
    </div>
  );
}

export { Progress };