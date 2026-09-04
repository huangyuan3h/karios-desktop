'use client';

import * as React from 'react';

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import { Button } from '@/components/ui/button';
import { apiGetJson, apiPostJson } from '@/lib/api/client';

const EVENT_TYPES = [
  'twin_star_reminder',
  'job_failed',
  'paper_chain_issue',
  'execution_card',
  'audit_issues',
  'near_stop',
  'oos_warning',
  'recon_missing',
  'candidate_added',
  'intraday_drawdown',
  'test',
];

const EVENT_LABELS: Record<string, string> = {
  twin_star_reminder: '双子星14:30提醒',
  job_failed: 'cron 失败',
  paper_chain_issue: 'paper 链断链',
  execution_card: '执行卡（单轨）',
  audit_issues: '行为对账（单轨）',
  near_stop: '接近止损',
  oos_warning: 'OOS 预警',
  recon_missing: '对账缺票',
  candidate_added: '新候选入池',
  intraday_drawdown: '盘中 -8%',
  test: '测试事件',
};

export type WebhookSubscription = {
  id: number;
  url: string;
  secret: string;
  eventTypes: string[];
  enabled: boolean;
  createdAt: string;
};

const INPUT_CLS =
  'h-8 rounded-md border border-[var(--k-border)] bg-transparent px-2 text-xs outline-none focus:border-[var(--k-accent)]';

export function WebhookPage() {
  const qc = useQueryClient();
  const [url, setUrl] = React.useState('');
  const [selected, setSelected] = React.useState<string[]>([
    'twin_star_reminder',
    'job_failed',
  ]);
  const [newSecret, setNewSecret] = React.useState<string | null>(null);

  const listQ = useQuery({
    queryKey: ['webhook', 'subscriptions'],
    queryFn: () =>
      apiGetJson<{ ok: boolean; items: WebhookSubscription[] }>(
        '/api/webhook/subscriptions',
      ),
    staleTime: 30_000,
  });

  const createMut = useMutation({
    mutationFn: () =>
      apiPostJson<{ ok: boolean; subscription: WebhookSubscription }>(
        '/api/webhook/subscriptions',
        { url, event_types: selected },
      ),
    onSuccess: (data) => {
      setNewSecret(data.subscription.secret);
      setUrl('');
      void qc.invalidateQueries({ queryKey: ['webhook'] });
    },
  });

  const testMut = useMutation({
    mutationFn: () => apiPostJson('/api/webhook/test'),
  });

  const deleteMut = useMutation({
    mutationFn: (id: number) =>
      fetch(`/api/webhook/subscriptions/${id}`, { method: 'DELETE' }),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['webhook'] }),
  });

  const toggleType = (t: string) =>
    setSelected((prev) =>
      prev.includes(t) ? prev.filter((x) => x !== t) : [...prev, t],
    );

  return (
    <div className="mx-auto flex w-full max-w-4xl flex-col gap-4 p-6">
      <div className="text-[12px] font-medium">
        Webhook 事件订阅
        <span className="ml-2 text-[10px] font-normal text-[var(--k-muted)]">
          HMAC 签名推送 · 失败退避重试 · 供外部 AI 助手 / 决策 Agent 订阅
        </span>
      </div>

      {/* 新增订阅 */}
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
        <div className="mb-2 text-[11px] font-medium">新增订阅</div>
        <div className="flex flex-wrap items-end gap-2">
          <label className="flex flex-col gap-1 text-[10px] text-[var(--k-muted)]">
            Consumer URL（POST 接收端）
            <input
              className={INPUT_CLS}
              style={{ width: 280 }}
              value={url}
              placeholder="http://127.0.0.1:8001/hook"
              onChange={(e) => setUrl(e.target.value)}
            />
          </label>
          <Button
            size="sm"
            disabled={url.trim().length < 8 || createMut.isPending}
            onClick={() => createMut.mutate()}
          >
            {createMut.isPending ? '创建中…' : '创建订阅'}
          </Button>
        </div>
        <div className="mt-2 flex flex-wrap gap-1.5">
          {EVENT_TYPES.map((t) => (
            <button
              key={t}
              type="button"
              onClick={() => toggleType(t)}
              className={
                selected.includes(t)
                  ? 'rounded border border-[var(--k-accent)]/60 bg-[var(--k-accent)]/10 px-1.5 py-0.5 text-[10px] text-[var(--k-fg)]'
                  : 'rounded border border-[var(--k-border)] px-1.5 py-0.5 text-[10px] text-[var(--k-muted)]'
              }
            >
              {EVENT_LABELS[t] ?? t}
            </button>
          ))}
        </div>
        {newSecret && (
          <div className="mt-2 rounded-md border border-amber-500/40 bg-amber-500/5 px-2 py-1.5 text-[10px] text-amber-700 dark:text-amber-300">
            已创建 · HMAC Secret（只显示一次，请立即保存）：
            <span className="ml-1 font-mono">{newSecret}</span>
          </div>
        )}
      </div>

      {/* 测试 */}
      <div className="flex items-center gap-2">
        <Button size="sm" variant="outline" onClick={() => testMut.mutate()}>
          {testMut.isPending ? '发送中…' : '发送测试事件'}
        </Button>
        <span className="text-[10px] text-[var(--k-muted)]">
          向所有订阅 url 推一条 test 事件（下一分钟投递）
        </span>
      </div>

      {/* 订阅列表 */}
      <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)]">
        {listQ.isLoading ? (
          <p className="p-3 text-xs text-[var(--k-muted)]">加载中…</p>
        ) : !listQ.data?.items.length ? (
          <p className="p-3 text-xs text-[var(--k-muted)]">暂无订阅。</p>
        ) : (
          <div className="flex flex-col">
            {listQ.data.items.map((s) => (
              <div
                key={s.id}
                className="flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-[var(--k-border)]/60 px-3 py-2 last:border-b-0"
              >
                <span className="text-[11px] font-medium">{s.url}</span>
                <span className="text-[10px] text-[var(--k-muted)]">
                  {s.eventTypes.map((t) => EVENT_LABELS[t] ?? t).join(' · ')}
                </span>
                <span
                  className={
                    s.enabled
                      ? 'text-[10px] text-emerald-600'
                      : 'text-[10px] text-[var(--k-muted)]'
                  }
                >
                  {s.enabled ? '启用' : '停用'}
                </span>
                <Button
                  size="sm"
                  variant="outline"
                  className="ml-auto"
                  disabled={deleteMut.isPending}
                  onClick={() => deleteMut.mutate(s.id)}
                >
                  删除
                </Button>
              </div>
            ))}
          </div>
        )}
      </div>

      <p className="text-[10px] leading-relaxed text-[var(--k-muted)]">
        事件体：{'{'}event_id, event_type, payload, sent_at{'}'} · 签名头
        X-Karios-Signature: sha256=HMAC-SHA256(body, secret) · 投递：5s 超时 ·
        失败退避 5/15/60 分钟 ×3 · 单订阅 30 条/分钟限频。接收端示例见
        docs/integrations/ai-agent-cookbook.md §9。
      </p>
    </div>
  );
}
