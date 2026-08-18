'use client';

import * as React from 'react';
import { useQueryClient } from '@tanstack/react-query';

import {
  useAlphaRadarStatusQuery,
  useAlphaRadarCatalystQuery,
  runAlphaRadarPipeline,
  invalidateAlphaRadarQueries,
} from '@/lib/queries/alphaRadar';
import { MobileButton, MobileCard, MobileSection, StatusPill } from '../primitives';

/** Alpha 雷达 (mobile) — pipeline status + catalyst stocks. §5.2 高频. */
export function MobileAlphaPage() {
  const qc = useQueryClient();
  const status = useAlphaRadarStatusQuery();
  const catalyst = useAlphaRadarCatalystQuery();
  const [running, setRunning] = React.useState(false);
  const [runMsg, setRunMsg] = React.useState<string | null>(null);

  const run = async () => {
    setRunning(true);
    setRunMsg(null);
    try {
      const res = await runAlphaRadarPipeline(false);
      setRunMsg(res.skipped ? `跳过（冷却中 ${status.data?.cooldownHours ?? ''}h）` : `完成 · 新增 ${res.trendCount ?? 0} 个趋势`);
      await invalidateAlphaRadarQueries(qc);
    } catch (e) {
      setRunMsg(e instanceof Error ? e.message : String(e));
    } finally {
      setRunning(false);
    }
  };

  const st = status.data;

  return (
    <div className="space-y-4">
      <MobileSection title="管线状态">
        <MobileCard className="p-3">
          <div className="flex flex-wrap gap-1.5">
            <StatusPill tone="neutral">趋势 {st?.currentTrendCount ?? '—'}</StatusPill>
            <StatusPill tone="neutral">累计 {st?.accumulatedTrendCount ?? '—'}</StatusPill>
            <StatusPill tone={st?.withinCooldown ? 'warn' : 'open'}>
              {st?.withinCooldown ? `冷却 ${st.cooldownHours ?? ''}h` : '就绪'}
            </StatusPill>
          </div>
          {st?.lastRunAt ? (
            <div className="mt-2 text-[var(--m-text-xs)] text-[var(--k-muted)]">
              上次运行 {new Date(st.lastRunAt).toLocaleString('zh-CN')}
            </div>
          ) : null}
          {runMsg ? <div className="mt-2 text-[var(--m-text-sm)] text-[var(--k-accent)]">{runMsg}</div> : null}
          <div className="mt-3">
            <MobileButton block onClick={() => void run()} disabled={running}>
              {running ? '生成中…' : '生成趋势'}
            </MobileButton>
          </div>
        </MobileCard>
      </MobileSection>

      <MobileSection title={`催化股票（${catalyst.data?.items?.length ?? 0}）`}>
        {catalyst.data?.items?.length ? (
          <div className="space-y-2">
            {catalyst.data.items.map((c) => (
              <MobileCard key={c.symbol} className="p-3">
                <div className="flex items-center justify-between gap-2">
                  <div className="min-w-0">
                    <div className="truncate text-[var(--m-text-base)] font-semibold">{c.name}</div>
                    <div className="mt-0.5 truncate font-mono text-[var(--m-text-xs)] text-[var(--k-muted)]">
                      {c.symbol}
                    </div>
                  </div>
                  <div className="shrink-0 text-right">
                    <div className="font-mono text-[var(--m-text-sm)] tabular-nums">
                      score {c.catalystScore ?? '—'}
                    </div>
                    <div className="mt-0.5 text-[var(--m-text-xs)] text-[var(--k-muted)]">
                      {c.articleCount ?? 0} 篇
                    </div>
                  </div>
                </div>
                {c.articles?.[0]?.summary ? (
                  <div className="mt-1.5 line-clamp-2 text-[var(--m-text-sm)] text-[var(--k-muted)]">
                    {c.articles[0].summary}
                  </div>
                ) : null}
                <div className="mt-2 flex flex-wrap gap-1.5">
                  {c.autoQaSignals?.length ? <StatusPill tone="warn">QA 信号</StatusPill> : null}
                  {c.adjustedCatalystScore != null && c.adjustedCatalystScore !== c.catalystScore ? (
                    <StatusPill tone="down">调整 {c.adjustedCatalystScore}</StatusPill>
                  ) : null}
                </div>
              </MobileCard>
            ))}
          </div>
        ) : (
          <MobileCard className="px-3 py-8 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
            {catalyst.isLoading ? '加载中…' : '暂无催化股票，点「生成趋势」'}
          </MobileCard>
        )}
      </MobileSection>
    </div>
  );
}
