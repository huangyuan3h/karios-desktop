'use client';

import type { RegimeTarget } from '@karios/shared';

import { cn } from '@/lib/utils';

/**
 * Read-only auto regime label for the Watchlist page.
 *
 * The page no longer renders the manual strategy switcher; the target comes
 * from GET /watchlist/regime-target (month-end 000300 vs MA200, 2-state).
 */
export function RegimeAutoCard({
  data,
  isLoading,
  error,
}: {
  data?: RegimeTarget | null;
  isLoading?: boolean;
  error?: unknown;
}) {
  const ok = Boolean(data?.ok);
  const bull = data?.state === 'bull';
  const target = data?.targetLabel ?? '—';
  const state = data?.stateLabel ?? '—';

  return (
    <div
      data-testid="regime-auto-card"
      data-target={data?.target ?? ''}
      className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2"
    >
      <div className="flex flex-wrap items-baseline gap-2">
        <span className="text-[12px] font-medium">自动跟随（只读）</span>
        <span className="rounded border border-sky-500/40 bg-sky-500/10 px-1.5 py-0.5 text-[10px] text-sky-800 dark:text-sky-200">
          手动策略选择已关闭
        </span>
        {isLoading ? <span className="text-[10px] text-[var(--k-muted)]">读取 regime…</span> : null}
      </div>

      {!ok ? (
        <div className="mt-2 text-[11px] text-amber-800 dark:text-amber-200">
          {error || data?.error
            ? `regime 读取失败：${String(error ?? data?.error)}`
            : 'regime 数据不可用'}
          <div className="mt-0.5 text-[10px] text-[var(--k-muted)]">
            规则：月末确认 000300 vs MA200；数据恢复后自动切回目标策略。
          </div>
        </div>
      ) : (
        <>
          <div className="mt-2 flex flex-wrap items-center gap-2">
            <span
              className={cn(
                'rounded border px-1.5 py-0.5 text-[10px]',
                bull
                  ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-800 dark:text-emerald-200'
                  : 'border-amber-500/40 bg-amber-500/10 text-amber-800 dark:text-amber-200',
              )}
            >
              {state}
            </span>
            <span className="text-[15px] font-semibold">{target}</span>
            {data?.sgapExposure ? (
              <span
                className={cn(
                  'rounded border px-1.5 py-0.5 text-[10px]',
                  data.sgapExposure === 'high'
                    ? 'border-red-500/40 bg-red-500/10 text-red-800 dark:text-red-200'
                    : 'border-[var(--k-border)] text-[var(--k-muted)]',
                )}
              >
                S-gap 暴露：{data.sgapExposure === 'high' ? '高（卫星腿）' : '低（20%）'}
              </span>
            ) : null}
          </div>

          <div className="mt-1 text-[11px] text-[var(--k-muted)]">
            确认于 {data?.confirmedAt ?? '—'}：000300 {data?.close ?? '—'} vs MA200{' '}
            {data?.ma200 ?? '—'}（{data?.distancePct != null ? `${data.distancePct > 0 ? '+' : ''}${data.distancePct}%` : '—'}）
            · 下次复核 {data?.nextReview ?? '—'}
          </div>
          <div className="mt-1 text-[10px] text-[var(--k-muted)]">
            {data?.rule ?? '月末确认：000300 > MA200 → 稳健星舰 H2-a25；否则 → 星港 0.2'}
            {data?.note ? ` · ${data.note}` : ''}
          </div>
        </>
      )}
    </div>
  );
}
