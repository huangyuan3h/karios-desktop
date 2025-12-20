import { Button } from '@/components/ui/button';

export function DashboardPage() {
  return (
    <div className="p-6">
      <div className="mb-6">
        <h1 className="text-2xl font-semibold tracking-tight">Dashboard</h1>
        <p className="mt-1 text-sm text-[var(--k-muted)]">
          Your fast market desk—watch, analyze, act.
        </p>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <div className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-3 flex items-center justify-between">
            <div className="text-sm font-medium">Recent Reports</div>
            <Button variant="link" size="sm" className="h-auto px-0 py-0 text-xs">
              View all
            </Button>
          </div>
          <div className="space-y-2">
            {[
              ['Macro & Tech', '2025-12-20'],
              ['AI & Rates', '2025-12-19'],
              ['Risk Review', '2025-12-18'],
              ['FX & Commodities', '2025-12-16'],
            ].map(([title, date]) => (
              <div
                key={title}
                className="flex items-center justify-between rounded-lg border border-[var(--k-border)] px-3 py-2 text-sm"
              >
                <div className="truncate">{title}</div>
                <div className="text-xs text-[var(--k-muted)]">{date}</div>
              </div>
            ))}
          </div>
        </div>

        <div className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-3 text-sm font-medium">Watchlist</div>
          <div className="overflow-hidden rounded-lg border border-[var(--k-border)]">
            <div className="grid grid-cols-[1fr_80px_80px] gap-2 bg-[var(--k-surface-2)] px-3 py-2 text-xs font-medium text-[var(--k-muted)]">
              <div>Symbol</div>
              <div className="text-right">Last</div>
              <div className="text-right">Chg%</div>
            </div>
            {[
              ['CSI300', '4,568.18', '+0.34%'],
              ['SHCOMP', '3,890.45', '+0.36%'],
              ['SPX', '-', '-'],
              ['NDX', '-', '-'],
            ].map(([sym, last, chg]) => (
              <div
                key={sym}
                className="grid grid-cols-[1fr_80px_80px] gap-2 border-t border-[var(--k-border)] px-3 py-2 text-sm"
              >
                <div className="font-medium">{sym}</div>
                <div className="text-right tabular-nums">{last}</div>
                <div className="text-right text-emerald-600 tabular-nums">
                  {chg}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}


