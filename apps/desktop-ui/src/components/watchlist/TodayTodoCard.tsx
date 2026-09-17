'use client';

import * as React from 'react';
import { useQuery } from '@tanstack/react-query';
import { ListChecks } from 'lucide-react';

import { cn } from '@/lib/utils';
import { useBacktestReconQuery, useSleeveReconQuery } from '@/lib/queries/backtest';
import {
  fetchPortfolioHealth,
  type PortfolioHealthResponse,
} from '@/lib/queries/portfolioHealth';
import {
  BUY_REMINDERS_UPDATED_EVENT,
  loadBuyReminders,
} from '@/lib/buy-reminders';

/** Shanghai clock (Asia/Shanghai — never UTC for trading-day copy). */
function shanghaiParts(d = new Date()): { mins: number; weekday: number } {
  const fmt = new Intl.DateTimeFormat('zh-CN', {
    timeZone: 'Asia/Shanghai',
    hour: '2-digit',
    minute: '2-digit',
    weekday: 'short',
    hour12: false,
  });
  const parts = Object.fromEntries(
    fmt.formatToParts(d).map((p) => [p.type, p.value]),
  ) as Record<string, string>;
  const [h, m] = String(parts.hour ?? '0').split(':');
  // "周一".."周日" → 1..7
  const wdMap: Record<string, number> = {
    '周一': 1, '周二': 2, '周三': 3, '周四': 4, '周五': 5, '周六': 6, '周日': 7,
  };
  return {
    mins: Number(h) * 60 + Number(m ?? 0),
    weekday: wdMap[String(parts.weekday)] ?? 0,
  };
}

function phaseCopy(mins: number, weekend: boolean): string {
  if (weekend) return '周末复盘：核对持仓与提醒，不下单';
  if (mins < 9 * 60 + 15) return '盘前准备：先看待执行，再看今日候选';
  if (mins < 14 * 60 + 30) return `盘中盯盘：14:30 前做买入决定（还剩约 ${14 * 60 + 30 - mins} 分钟）`;
  if (mins < 15 * 60) return '尾盘执行窗：按清单买入/卖出';
  return '盘后核对：确认成交，对明天的待执行';
}

function scrollToAnchor(anchor: string) {
  window.dispatchEvent(new CustomEvent('karios-scroll-to', { detail: { anchor } }));
}

type TodoItem = {
  level: 'todo' | 'warn' | 'info' | 'done';
  text: React.ReactNode;
  anchor?: string;
};

/**
 * 今日待办（watchlist 页顶）：把散在各块的"今天要动手的事"排成一张有序清单。
 * 数据全部复用已有缓存（portfolio-health / sleeve-recon / recon），不新增请求；
 * 只读展示，所有动作跳到对应区块，不碰下单链。
 */
export function TodayTodoCard() {
  // Shared cache with PortfolioHealthCard (passive observer, no extra fetch).
  const healthQ = useQuery({
    queryKey: ['portfolio-health'],
    queryFn: ({ signal }) => fetchPortfolioHealth(undefined, signal),
    staleTime: 5 * 60_000,
  });
  const sleeveQ = useSleeveReconQuery(true);
  const reconQ = useBacktestReconQuery(2);
  const [remindersTick, setRemindersTick] = React.useState(0);

  React.useEffect(() => {
    const onUpdate = () => setRemindersTick((t) => t + 1);
    window.addEventListener(BUY_REMINDERS_UPDATED_EVENT, onUpdate);
    return () => window.removeEventListener(BUY_REMINDERS_UPDATED_EVENT, onUpdate);
  }, []);
  const reminders = React.useMemo(
    () => (remindersTick >= 0 ? loadBuyReminders() : []),
    [remindersTick],
  );

  const data: PortfolioHealthResponse | undefined = healthQ.data;
  const recon = (sleeveQ.data as { recon?: import('@/lib/queries/backtest').SleeveRecon } | undefined)?.recon;
  const { mins, weekday } = shanghaiParts();
  const weekend = weekday >= 6;

  const items: TodoItem[] = React.useMemo(() => {
    if (!data) return [];
    const out: TodoItem[] = [];
    // 1) 闸门：先说今天能不能买。
    if (data.circuitBlocked) {
      out.push({ level: 'warn', text: '回撤熔断中：新开仓暂停，先处理卖出', anchor: 'holdings' });
    } else if (data.regime === 'Weak') {
      out.push({ level: 'info', text: '弱市空仓观望：今日不新开，看持仓与提醒' });
    }
    if (data.panicCooldown?.active) {
      out.push({
        level: 'info',
        text: `恐慌冷却中${data.panicCooldown.cooldownEndDate ? `（至 ${data.panicCooldown.cooldownEndDate}）` : ''}：轻仓`,
      });
    }
    // 2) 停车腿缺买/缺卖/多开（最紧急：镜像偏离）。
    const missed = [...(recon?.missedBuys ?? []), ...(recon?.missedSells ?? [])];
    if (missed.length > 0 || (recon?.extraOpens ?? []).length > 0) {
      const bits: string[] = [];
      if (recon?.missedBuys?.length) bits.push(`缺买 ${recon.missedBuys.join('、')}`);
      if (recon?.missedSells?.length) bits.push(`缺卖 ${recon.missedSells.join('、')}`);
      if (recon?.extraOpens?.length) bits.push(`多开 ${recon.extraOpens.join('、')}`);
      out.push({ level: 'todo', text: `停车腿偏离：${bits.join('；')}`, anchor: 'sleeve-recon' });
    }
    // 3) 待执行（下一交易日开盘）。
    if (recon?.userAlignment === 'pending') {
      out.push({
        level: 'todo',
        text: `待执行：${recon.userExecDate ?? '下一交易日'}开盘${(recon.userBuys ?? []).length ? `买 ${(recon.userBuys ?? []).join('、')}` : ''}${(recon.userSells ?? []).length ? `卖 ${(recon.userSells ?? []).join('、')}` : ''}`,
        anchor: 'sleeve-recon',
      });
    }
    // 4) 持仓 EXIT。
    const exits = (data.holdings ?? []).filter((h) => h.action === 'EXIT');
    if (exits.length > 0) {
      out.push({
        level: 'todo',
        text: `持仓卖出 ${exits.length} 只：${exits.map((h) => h.name ?? h.symbol).join('、')}`,
        anchor: 'holdings',
      });
    }
    // 5) 回测对账缺/多（锚点精确到市场：单市场缺口直达该块）。
    const reconItems = (reconQ.data as { items?: Array<{ missing?: number; extra?: number; market?: string }> } | undefined)?.items ?? [];
    const gapMarkets = reconItems.filter((r) => (r.missing ?? 0) > 0 || (r.extra ?? 0) > 0);
    if (gapMarkets.length > 0) {
      const anchor =
        gapMarkets.length === 1 && String(gapMarkets[0].market ?? '').toUpperCase().includes('HK')
          ? 'recon-hk'
          : 'recon';
      out.push({
        level: 'todo',
        text: `回测对账有缺口：${gapMarkets.map((r) => `${r.market} 缺${r.missing ?? 0}/多${r.extra ?? 0}`).join('；')}`,
        anchor,
      });
    }
    // 6) 股票篮候选。
    const candCount = data.s3Candidates?.length ?? 0;
    if (candCount > 0 && !data.circuitBlocked && data.regime !== 'Weak') {
      out.push({
        level: weekend ? 'info' : 'todo',
        text: weekend
          ? `股票篮 ${candCount} 只候选（周末先看，不下单）`
          : `股票篮 ${candCount} 只候选，14:30 前决定买哪几只`,
        anchor: 'buy-list',
      });
    }
    // 7) 买入提醒。
    if (reminders.length > 0) {
      out.push({
        level: 'info',
        text: `买入提醒 ${reminders.length} 条：${reminders.slice(0, 3).map((r) => r.symbol).join('、')}${reminders.length > 3 ? '…' : ''}`,
        anchor: 'buy-reminders',
      });
    }
    return out;
  }, [data, recon, reconQ.data, reminders, weekend]);

  if (healthQ.isPending) {
    return (
      <div className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-4 py-3">
        <div className="text-[12px] font-medium">今日待办</div>
        <div className="mt-1 text-[11px] text-[var(--k-muted)]">待办加载中…</div>
      </div>
    );
  }

  if (healthQ.isError || !data) {
    return (
      <div className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-4 py-3">
        <div className="flex items-center gap-2 text-[12px] font-medium">
          <ListChecks className="size-3.5" />
          今日待办
        </div>
        <div className="mt-1 flex flex-wrap items-center gap-2 text-[11px] text-[var(--k-muted)]">
          <span>健康数据未加载，待办暂不可用（下拉看体检卡）</span>
          <button
            type="button"
            onClick={() => void healthQ.refetch()}
            disabled={healthQ.isFetching}
            className="rounded border border-[var(--k-border)] px-2 py-0.5 text-[11px] hover:bg-[var(--k-surface-2)]"
          >
            重试
          </button>
        </div>
      </div>
    );
  }

  const todos = items.filter((i) => i.level === 'todo' || i.level === 'warn');

  return (
    <div className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-4 py-3">
      <div className="flex flex-wrap items-center gap-2">
        <ListChecks className="size-3.5" />
        <span className="text-[12px] font-medium">
          今日待办{todos.length > 0 ? `（${todos.length}）` : ''}
        </span>
        <span className="text-[10px] text-[var(--k-muted)]">{phaseCopy(mins, weekend)}</span>
        {data.tradeDate ? (
          <span className="ml-auto font-mono text-[10px] text-[var(--k-muted)]">
            截至 {data.tradeDate}
            {data.scoreFresh === false ? ' · 分数非今日' : ''}
          </span>
        ) : null}
      </div>
      {items.length === 0 ? (
        <div className="mt-1.5 text-[12px] text-emerald-700 dark:text-emerald-300">
          ✓ 今日无待办：无偏离、无卖出、无待执行
        </div>
      ) : (
        <ol className="mt-1.5 space-y-1">
          {items.map((item, i) => (
            <li key={i} className="flex flex-wrap items-center gap-2 text-[12px]">
              <span
                className={cn(
                  'inline-flex size-4 shrink-0 items-center justify-center rounded-full text-[10px] font-semibold',
                  item.level === 'todo' && 'bg-red-500/15 text-red-700 dark:text-red-300',
                  item.level === 'warn' && 'bg-amber-500/15 text-amber-700 dark:text-amber-300',
                  item.level === 'info' && 'bg-sky-500/15 text-sky-700 dark:text-sky-300',
                  item.level === 'done' && 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-300',
                )}
              >
                {i + 1}
              </span>
              <span>{item.text}</span>
              {item.anchor ? (
                <button
                  type="button"
                  onClick={() => scrollToAnchor(item.anchor as string)}
                  className="text-[11px] text-sky-700 underline-offset-2 hover:underline dark:text-sky-300"
                >
                  去处理 →
                </button>
              ) : null}
            </li>
          ))}
        </ol>
      )}
    </div>
  );
}
