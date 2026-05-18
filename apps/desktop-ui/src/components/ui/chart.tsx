import * as React from 'react';
import {
  ResponsiveContainer,
  Tooltip,
  type TooltipProps,
  type ValueType,
  type NameType,
} from 'recharts';

import { cn } from '@/lib/utils';

type ChartConfig = Record<
  string,
  {
    label?: string;
    color?: string;
  }
>;

const ChartContainer = React.forwardRef<HTMLDivElement, React.HTMLAttributes<HTMLDivElement> & { config: ChartConfig }>(
  ({ className, config, children, ...props }, ref) => {
    const style = Object.entries(config).reduce<Record<string, string>>((acc, [key, val]) => {
      if (val?.color) acc[`--color-${key}`] = val.color;
      return acc;
    }, {});
    return (
      <div ref={ref} className={cn('h-[240px] w-full', className)} style={style} {...props}>
        <ResponsiveContainer>{children as React.ReactElement}</ResponsiveContainer>
      </div>
    );
  },
);
ChartContainer.displayName = 'ChartContainer';

function ChartTooltip(props: TooltipProps<ValueType, NameType>) {
  return <Tooltip cursor={false} {...props} />;
}

function ChartTooltipContent({
  active,
  payload,
  label,
  formatter,
  nameKey = 'name',
  labelClassName,
}: TooltipProps<ValueType, NameType> & {
  nameKey?: string;
  labelClassName?: string;
}) {
  if (!active || !payload?.length) return null;
  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2 text-xs shadow-sm">
      <div className={cn('mb-1 font-medium', labelClassName)}>{label}</div>
      <div className="space-y-1">
        {payload.map((item, idx) => {
          const name = String(item.name ?? item.payload?.[nameKey] ?? '');
          const value = formatter ? formatter(item.value as ValueType, name, item) : item.value;
          return (
            <div key={`${name}-${idx}`} className="flex items-center justify-between gap-3">
              <div className="text-[var(--k-muted)]">{name}</div>
              <div className="font-mono text-[var(--k-text)]">{value as React.ReactNode}</div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

export { ChartContainer, ChartTooltip, ChartTooltipContent };
