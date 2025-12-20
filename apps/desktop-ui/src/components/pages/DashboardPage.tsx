export function DashboardPage() {
  return (
    <div className="p-6">
      <div className="mb-6">
        <h1 className="text-2xl font-semibold tracking-tight">Dashboard</h1>
        <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
          Your fast market desk—watch, analyze, act.
        </p>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <div className="rounded-xl border border-zinc-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-950">
          <div className="mb-3 flex items-center justify-between">
            <div className="text-sm font-medium">Recent Reports</div>
            <button className="text-xs text-zinc-500 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-zinc-50">
              View all
            </button>
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
                className="flex items-center justify-between rounded-lg border border-zinc-200 px-3 py-2 text-sm dark:border-zinc-800"
              >
                <div className="truncate">{title}</div>
                <div className="text-xs text-zinc-500 dark:text-zinc-400">{date}</div>
              </div>
            ))}
          </div>
        </div>

        <div className="rounded-xl border border-zinc-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-950">
          <div className="mb-3 text-sm font-medium">Watchlist</div>
          <div className="overflow-hidden rounded-lg border border-zinc-200 dark:border-zinc-800">
            <div className="grid grid-cols-[1fr_80px_80px] gap-2 bg-zinc-50 px-3 py-2 text-xs font-medium text-zinc-600 dark:bg-zinc-900 dark:text-zinc-300">
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
                className="grid grid-cols-[1fr_80px_80px] gap-2 border-t border-zinc-200 px-3 py-2 text-sm dark:border-zinc-800"
              >
                <div className="font-medium">{sym}</div>
                <div className="text-right tabular-nums">{last}</div>
                <div className="text-right text-emerald-600 tabular-nums dark:text-emerald-400">
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


