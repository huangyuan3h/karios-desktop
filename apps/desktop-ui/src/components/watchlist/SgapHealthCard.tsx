'use client';

import { useSgapDecayQuery } from '@/lib/queries/backtest';
import { cn } from '@/lib/utils';

function fmtNum(v: number | null | undefined, digits = 2, suffix = ''): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v.toFixed(digits)}${suffix}`;
}

/**
 * S-gap crowding / decay card for the Watchlist page.
 *
 * This is the auxiliary judgment the user asked to surface next to the auto
 * regime target: the satellite leg (H2-a25 / 星舰B / 双子星 / 星港) depends on
 * S-gap; if its per-trade edge and crowding degrade, the auto target's expected
 * return should be discounted.
 */
export function SgapHealthCard() {
  const q = useSgapDecayQuery(true);
  const decay = q.data?.decay;
  const summary = decay?.summary;
  const rolling = decay?.rolling ?? [];
  const latest = [...rolling].reverse().find((r) => r.r40);

  const amtYi = latest?.crowd_amt_w != null ? latest.crowd_amt_w / 1e4 : null;
  const circYi = latest?.crowd_circ != null ? latest.crowd_circ / 1e4 : null;
  const edge = latest?.large_edge ?? null;
  const revival = summary?.revival_pos ?? 0;
  const degraded = (summary?.latest_r40_mean ?? 0) <= 0 || (summary?.latest_r40_percentile ?? 0) < 50;

  return (
    <div
      data-testid="sgap-health-card"
      className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2"
    >
      <div className="flex flex-wrap items-baseline gap-2">
        <span className="text-[12px] font-medium">S-gap 拥挤 / 失效（卫星腿引擎）</span>
        <span
          className={cn(
            'rounded border px-1.5 py-0.5 text-[10px]',
            degraded
              ? 'border-red-500/40 bg-red-500/10 text-red-800 dark:text-red-200'
              : 'border-emerald-500/40 bg-emerald-500/10 text-emerald-800 dark:text-emerald-200',
          )}
        >
          {degraded ? '衰减 / 观察' : '仍有效'}
        </span>
        <span className="text-[10px] text-[var(--k-muted)]">
          复活灯 {revival}%{revival === 0 ? '（保持观察）' : revival === 10 ? '（试探加回）' : '（半仓观察）'}
        </span>
      </div>

      {q.isError || !summary ? (
        <div className="mt-2 text-[11px] text-amber-800 dark:text-amber-200">
          S-gap 数据不可用（后端缺 sgap_decay.json：先跑 scripts/generate_sgap_decay.py）
        </div>
      ) : (
        <>
          <div className="mt-2 grid grid-cols-2 gap-x-4 gap-y-1 text-[11px] md:grid-cols-4">
            <div>
              <div className="text-[10px] text-[var(--k-muted)]">最新 40 笔</div>
              <div className="font-medium">{fmtNum(summary.latest_r40_mean, 2, ' 点/笔')}</div>
            </div>
            <div>
              <div className="text-[10px] text-[var(--k-muted)]">分位 / 胜率</div>
              <div className="font-medium">
                {fmtNum(summary.latest_r40_percentile, 1, '%')} /{' '}
                {summary.latest_r40_win_rate != null
                  ? `${(summary.latest_r40_win_rate * 100).toFixed(1)}%`
                  : '—'}
              </div>
            </div>
            <div>
              <div className="text-[10px] text-[var(--k-muted)]">成交 / 市值（最新）</div>
              <div className="font-medium">
                {fmtNum(amtYi, 2, ' 亿')} / {fmtNum(circYi, 1, ' 亿')}
              </div>
            </div>
            <div>
              <div className="text-[10px] text-[var(--k-muted)]">大单边缘</div>
              <div className="font-medium">
                {edge == null ? '—' : `${edge > 0 ? '+' : ''}${edge.toFixed(2)} 点`}
              </div>
            </div>
          </div>
          <div className="mt-1.5 text-[10px] text-[var(--k-muted)]">
            研究结论（2026-10-03）：2026H2 −1.28%/笔（t −2.77）；成交中位 0.39→2.47 亿（6.3×）；
            大单边缘 t 4.5→1.95。卫星腿收益要打折；星港 20% 暴露，M30/M50 不依赖 S-gap。
          </div>
        </>
      )}
    </div>
  );
}
