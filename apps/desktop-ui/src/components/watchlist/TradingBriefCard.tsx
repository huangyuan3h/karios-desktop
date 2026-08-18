'use client';

import * as React from 'react';
import { Clock3, Copy, RefreshCw } from 'lucide-react';
import ReactMarkdown from 'react-markdown';

import { Button } from '@/components/ui/button';
import { useTradingBriefQuery, TRADING_BRIEF_TYPES } from '@/lib/queries/news';

const BRIEF_META: Record<string, { label: string; time: string; hint: string }> = {
  open: { label: '开盘简报', time: '10:00', hint: 'Regime + 候选 + 隔夜新闻' },
  midday: { label: '午间简报', time: '12:00', hint: '候选漂移 + 接近止损线' },
  action: { label: '操作卡', time: '14:30', hint: '买入卡 + 条件单清单' },
};

/**
 * 三时段操作简报（2026-08-11）——服务用户实际交易节奏 10:00/12:00/14:30：
 * 每个时间点 30 秒读完继续工作。数据由后端组装现有块（portfolio-health /
 * S-3 候选 / 条件单线 / 新闻），markdown 渲染 + 复制喂券商/决策 Agent。
 */
export function TradingBriefCard() {
  const [active, setActive] = React.useState<(typeof TRADING_BRIEF_TYPES)[number]>('action');
  const { data, isLoading, refetch } = useTradingBriefQuery(active);
  const brief = data?.brief ?? null;

  return (
    <div className="flex flex-col gap-2 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/60 p-2.5 text-xs">
      <div className="flex items-center gap-2 text-[11px] font-semibold">
        <Clock3 size={12} className="text-[var(--k-muted)]" />
        今日操作简报
        <span className="ml-auto flex gap-1">
          {TRADING_BRIEF_TYPES.map((t) => (
            <button
              key={t}
              onClick={() => setActive(t)}
              className={
                active === t
                  ? 'rounded border border-[var(--k-border)] bg-[var(--k-surface-3)] px-1.5 py-0.5 font-medium'
                  : 'rounded px-1.5 py-0.5 font-normal text-[var(--k-muted)] hover:text-[var(--k-fg)]'
              }
              title={BRIEF_META[t].hint}
            >
              {BRIEF_META[t].label}
            </button>
          ))}
        </span>
      </div>

      {brief?.briefDate && (
        <div className="flex items-center gap-2 text-[10px] text-[var(--k-muted)]">
          <span>
            {brief.briefDate} {BRIEF_META[active].time}
          </span>
          <span className="ml-auto flex items-center gap-1">
            {brief.markdown && (
              <button
                className="inline-flex items-center gap-1 hover:text-[var(--k-fg)]"
                onClick={() => navigator.clipboard.writeText(brief.markdown ?? '')}
              >
                <Copy size={10} /> 复制
              </button>
            )}
            <button
              className="inline-flex items-center gap-1 hover:text-[var(--k-fg)]"
              onClick={() => refetch()}
            >
              <RefreshCw size={10} /> 刷新
            </button>
          </span>
        </div>
      )}

      {isLoading && !brief ? (
        <div className="text-[var(--k-muted)]">加载中…</div>
      ) : !brief?.markdown ? (
        <div className="text-[var(--k-muted)]">
          暂无{BRIEF_META[active].label}（工作日 {BRIEF_META[active].time} 自动生成，可手动刷新）
        </div>
      ) : (
        <div className="max-h-96 overflow-y-auto rounded bg-[var(--k-surface-1)]/60 p-2 leading-relaxed">
          <ReactMarkdown
            components={{
              p: ({ children }) => <p className="my-0.5">{children}</p>,
              strong: ({ children }) => (
                <strong className="text-[var(--k-fg)]">{children}</strong>
              ),
              li: ({ children }) => <li className="my-0.5">{children}</li>,
            }}
          >
            {brief.markdown}
          </ReactMarkdown>
        </div>
      )}
    </div>
  );
}
