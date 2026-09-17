'use client';

import * as React from 'react';
import { FlaskConical } from 'lucide-react';

import { cn } from '@/lib/utils';
import {
  useHarborH2ShadowQuery,
  type HarborH2ShadowStatus,
} from '@/lib/queries/backtest';

function tone(v: number | null | undefined): string {
  if (v == null) return 'text-[var(--k-muted)]';
  return v >= 0 ? 'text-emerald-700 dark:text-emerald-300' : 'text-red-700 dark:text-red-400';
}

const STATUS_META: Record<HarborH2ShadowStatus, { label: string; cls: string }> = {
  tracking: {
    label: '跟踪中',
    cls: 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900 dark:text-emerald-200',
  },
  watch: {
    label: '观察',
    cls: 'bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-200',
  },
  rollback: {
    label: '已回滚',
    cls: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200',
  },
};

const ACTION_LABEL: Record<string, string> = {
  hold: '持有不动',
  enter: '建仓',
  rotate: '换仓',
  trail_exit: '回撤清仓',
};

/**
 * 港湾H2影子账本 · 每日跟单卡（OPT-216，只看不动手）。
 *
 * 数据源：GET /api/backtest/harbor-h2-shadow/latest（每日 18:35 调度追加）。
 * 与 Live 决策/下单/对账链零交集：本卡不读 portfolio-health、不调任何写接口。
 */
export function H2ShadowFollowCard() {
  const q = useHarborH2ShadowQuery(true);
  // Asia/Shanghai wall clock (UTC day breaks the stale badge on weekends).
  const nowSh = new Date(
    new Date().toLocaleString('en-US', { timeZone: 'Asia/Shanghai' }),
  );
  const today = `${nowSh.getFullYear()}-${String(nowSh.getMonth() + 1).padStart(2, '0')}-${String(nowSh.getDate()).padStart(2, '0')}`;
  const weekday = nowSh.getDay() === 0 ? 7 : nowSh.getDay();

  if (q.isPending) {
    return (
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
        <div className="text-[12px] font-medium">港湾H2影子账本（paper · 只看不动手）</div>
        <div className="mt-1 text-[11px] text-[var(--k-muted)]">加载中…</div>
      </div>
    );
  }

  if (q.isError || !q.data?.ok || !q.data?.latest) {
    return (
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
        <div className="flex items-center gap-2 text-[12px] font-medium">
          <FlaskConical className="size-3.5" />
          港湾H2影子账本（paper · 只看不动手）
        </div>
        <div className="mt-1 text-[11px] text-[var(--k-muted)]">
          影子账本尚未生成——等今日 18:35 harbor_h2_shadow 调度首次运行（需重启后端生效）。
          {q.error ? `（${String(q.error instanceof Error ? q.error.message : q.error).slice(0, 120)}）` : ''}
        </div>
      </div>
    );
  }

  const report = q.data;
  const latest = report.latest;
  const meta = STATUS_META[latest.status] ?? STATUS_META.tracking;
  // 周末无调度属正常；工作日账本日期落后今天即判过期。
  const stale =
    latest.date < today && weekday >= 1 && weekday <= 5;
  const spreadCls = tone(latest.spreadPt);

  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
      <div className="flex flex-wrap items-center gap-2">
        <FlaskConical className="size-3.5" />
        <span className="text-[12px] font-medium">港湾H2影子账本（paper · 只看不动手）</span>
        <span className={cn('rounded-full px-2 py-0.5 text-[10px] font-medium', meta.cls)}>
          {meta.label}
        </span>
        {stale && (
          <span className="rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-medium text-amber-800 dark:bg-amber-900 dark:text-amber-200">
            数据过期
          </span>
        )}
        <span className="ml-auto text-[10px] text-[var(--k-muted)]">
          账本 {latest.date} · 更新 {String(report.generatedAt).slice(0, 16).replace('T', ' ')} · 自 {report.inception} 起计
        </span>
      </div>

      <div className="mt-2 grid grid-cols-2 gap-2 text-[11px] sm:grid-cols-4">
        <div>
          <div className="text-[var(--k-muted)]">H2 paper NAV</div>
          <div className="font-medium">{latest.navH2.toFixed(4)}</div>
        </div>
        <div>
          <div className="text-[var(--k-muted)]">Live paper NAV</div>
          <div className="font-medium">{latest.navLive.toFixed(4)}</div>
        </div>
        <div>
          <div className="text-[var(--k-muted)]">领先（H2−Live）</div>
          <div className={cn('font-medium', spreadCls)}>
            {latest.spreadPt >= 0 ? '+' : ''}{latest.spreadPt.toFixed(2)}pt
          </div>
        </div>
        <div>
          <div className="text-[var(--k-muted)]">当日动作（H2停车腿）</div>
          <div className="font-medium">
            {ACTION_LABEL[latest.actionH2] ?? latest.actionH2} · {latest.pickH2}
          </div>
        </div>
        <div>
          <div className="text-[var(--k-muted)]">当日 H2 / Live</div>
          <div className="font-medium">
            <span className={tone(latest.dayH2Pct)}>{latest.dayH2Pct >= 0 ? '+' : ''}{latest.dayH2Pct.toFixed(2)}%</span>
            {' / '}
            <span className={tone(latest.dayLivePct)}>{latest.dayLivePct >= 0 ? '+' : ''}{latest.dayLivePct.toFixed(2)}%</span>
          </div>
        </div>
        <div>
          <div className="text-[var(--k-muted)]">回撤 H2 / Live</div>
          <div className="font-medium">
            {latest.ddH2Pct.toFixed(1)}% / {latest.ddLivePct.toFixed(1)}%
            <span className="text-[var(--k-muted)]">（深 {latest.mddGapPt >= 0 ? '+' : ''}{latest.mddGapPt.toFixed(1)}pt）</span>
          </div>
        </div>
        <div>
          <div className="text-[var(--k-muted)]">Live 持有 / 闲置</div>
          <div className="font-medium">{latest.pickLive} · {latest.idlePct.toFixed(0)}%</div>
        </div>
        <div>
          <div className="text-[var(--k-muted)]">回滚线</div>
          <div className="font-medium">
            落后 {report.thresholds.spreadRollbackPt}pt 或深过 {report.thresholds.mddGapRollbackPt}pt
          </div>
        </div>
      </div>

      <div className="mt-2 border-t border-[var(--k-border)] pt-2 text-[11px] text-[var(--k-muted)]">
        每日三步：① 看状态（跟踪中=正常；观察=只看不念；已回滚=H2 出局候选）② 跟 Live 下单——H2 永不下单 ③ 数据过期→调度页查 harbor_h2_shadow /
        系统健康。失败只进收件箱，不打电话。
      </div>
    </div>
  );
}
