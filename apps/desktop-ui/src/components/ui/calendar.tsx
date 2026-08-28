'use client';

import * as React from 'react';
import { DayPicker, getDefaultClassNames } from 'react-day-picker';
import { ChevronLeft, ChevronRight } from 'lucide-react';

import { cn } from '@/lib/utils';

export type CalendarProps = React.ComponentProps<typeof DayPicker>;

export function Calendar({ className, ...props }: CalendarProps) {
  const defaultClassNames = getDefaultClassNames();
  return (
    <DayPicker
      className={cn(
        'group/calendar bg-[var(--k-surface)] p-2 text-[var(--k-text)] [--cell-radius:0.5rem] [--cell-size:2rem]',
        className
      )}
      classNames={{
        root: cn('w-fit', defaultClassNames.root),
        months: cn('relative flex flex-col gap-4 sm:flex-row', defaultClassNames.months),
        month: cn('relative flex w-full flex-col gap-4', defaultClassNames.month),
        month_caption: cn(
          'relative flex h-9 w-full items-center justify-center',
          defaultClassNames.month_caption
        ),
        caption_label: cn('font-medium', defaultClassNames.caption_label),
        dropdowns: cn('absolute inset-0 flex items-center justify-center gap-1', defaultClassNames.dropdowns),
        dropdown: cn('absolute inset-0 appearance-none bg-transparent opacity-0', defaultClassNames.dropdown),
        dropdown_root: cn('relative rounded-md border border-[var(--k-border)]', defaultClassNames.dropdown_root),
        nav: cn('absolute top-0 inset-x-0 flex h-9 w-full items-center justify-between', defaultClassNames.nav),
        button_previous: cn(
          'absolute left-1 top-0 inline-flex size-8 items-center justify-center rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] text-[var(--k-text)] transition-colors hover:bg-[var(--k-surface-2)] focus-visible:z-10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--k-ring)] disabled:pointer-events-none disabled:opacity-40',
          defaultClassNames.button_previous
        ),
        button_next: cn(
          'absolute right-1 top-0 inline-flex size-8 items-center justify-center rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] text-[var(--k-text)] transition-colors hover:bg-[var(--k-surface-2)] focus-visible:z-10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--k-ring)] disabled:pointer-events-none disabled:opacity-40',
          defaultClassNames.button_next
        ),
        month_grid: cn('w-full border-collapse align-top', defaultClassNames.month_grid),
        weekdays: cn('flex', defaultClassNames.weekdays),
        weekday: cn(
          'w-(--cell-size) select-none text-[0.8rem] font-normal text-[var(--k-muted)]',
          defaultClassNames.weekday
        ),
        weeks: cn('flex w-full flex-col gap-1', defaultClassNames.weeks),
        week: cn('mt-0 flex w-full', defaultClassNames.week),
        day: cn(
          'group/day relative w-(--cell-size) p-0 text-center text-sm',
          defaultClassNames.day
        ),
        day_button: cn(
          'inline-flex size-(--cell-size) items-center justify-center rounded-(--cell-radius) p-0 font-normal transition-colors hover:bg-[var(--k-surface-2)] focus-visible:z-10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--k-ring)] group-data-[today]/day:bg-[var(--k-surface-2)] group-data-[today]/day:text-[var(--k-text)] group-data-[selected]/day:bg-[var(--k-accent)] group-data-[selected]/day:font-medium group-data-[selected]/day:text-white group-data-[disabled]/day:text-[var(--k-muted)] group-data-[disabled]/day:opacity-40 group-data-[disabled]/day:pointer-events-none group-data-[outside]/day:text-[var(--k-muted)] group-data-[outside]/day:opacity-50',
          defaultClassNames.day_button
        ),
        today: cn('bg-[var(--k-surface-2)] text-[var(--k-text)]', defaultClassNames.today),
        outside: cn('text-[var(--k-muted)] opacity-50', defaultClassNames.outside),
        disabled: cn('text-[var(--k-muted)] opacity-40', defaultClassNames.disabled),
        hidden: cn('invisible', defaultClassNames.hidden),
        range_start: cn('bg-[var(--k-accent)] text-white', defaultClassNames.range_start),
        range_end: cn('bg-[var(--k-accent)] text-white', defaultClassNames.range_end),
        range_middle: cn('bg-[var(--k-surface-2)] text-[var(--k-text)]', defaultClassNames.range_middle),
        chevron: cn('size-4', defaultClassNames.chevron),
      }}
      components={{
        Chevron: ({ orientation, className: c, ...rest }) =>
          orientation === 'left' ? (
            <ChevronLeft className={cn('size-4', c)} {...rest} />
          ) : (
            <ChevronRight className={cn('size-4', c)} {...rest} />
          ),
      }}
      {...props}
    />
  );
}
