'use client';

import * as React from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';

import { apiGetJson, apiPostJson, apiDeleteJson } from '@/lib/api/client';
import { MobileButton, MobileCard, MobileField, MobileSection, MobileSheet, StatusPill } from '../primitives';

/** Webhook (mobile) — subscriptions + create + test + delete. §5.2 低频. */

const EVENT_TYPES = [
  'job_failed',
  'paper_chain_issue',
  'near_stop',
  'oos_warning',
  'recon_missing',
  'candidate_added',
  'intraday_drawdown',
  'test',
] as const;

type WebhookSubscription = {
  id: number;
  url: string;
  secret: string;
  eventTypes: string[];
  enabled: boolean;
  createdAt: string;
};

export function MobileWebhookPage() {
  const qc = useQueryClient();
  const subs = useQuery({
    queryKey: ['webhook', 'subscriptions'],
    queryFn: () => apiGetJson<{ subscriptions: WebhookSubscription[] }>('/api/webhook/subscriptions'),
  });

  const [creating, setCreating] = React.useState(false);
  const [url, setUrl] = React.useState('');
  const [selected, setSelected] = React.useState<string[]>(['candidate_added']);
  const [secretMsg, setSecretMsg] = React.useState<string | null>(null);
  const [testMsg, setTestMsg] = React.useState<string | null>(null);

  const toggleEvent = (ev: string) => {
    setSelected((prev) => (prev.includes(ev) ? prev.filter((x) => x !== ev) : [...prev, ev]));
  };

  const create = async () => {
    if (!url.trim() || !selected.length) return;
    try {
      const res = await apiPostJson<{ subscription: WebhookSubscription }>('/api/webhook/subscriptions', {
        url: url.trim(),
        event_types: selected,
      });
      setSecretMsg(`订阅 ${res.subscription.id} 已创建 · Secret: ${res.subscription.secret}`);
      setCreating(false);
      setUrl('');
      await qc.invalidateQueries({ queryKey: ['webhook'] });
    } catch (e) {
      setSecretMsg(e instanceof Error ? e.message : String(e));
    }
  };

  const test = async () => {
    setTestMsg(null);
    try {
      const res = await apiPostJson<{ ok?: boolean; error?: string }>('/api/webhook/test', {});
      setTestMsg(res.ok ? '测试推送已发出' : res.error ?? '测试推送完成');
    } catch (e) {
      setTestMsg(e instanceof Error ? e.message : String(e));
    }
  };

  const remove = async (id: number) => {
    await apiDeleteJson(`/api/webhook/subscriptions/${id}`);
    await qc.invalidateQueries({ queryKey: ['webhook'] });
  };

  const list = subs.data?.subscriptions ?? [];

  return (
    <div className="space-y-4">
      <MobileSection
        title={`Webhook 订阅（${list.length}）`}
        action={
          <div className="flex items-center gap-2">
            <button type="button" onClick={() => void test()} className="text-[var(--m-text-sm)] text-[var(--k-accent)]">
              测试
            </button>
            <button type="button" onClick={() => setCreating(true)} className="text-[var(--m-text-sm)] text-[var(--k-accent)]">
              + 新建
            </button>
          </div>
        }
      >
        {testMsg ? (
          <MobileCard className="px-3 py-2 text-[var(--m-text-sm)] text-[var(--k-accent)]">{testMsg}</MobileCard>
        ) : null}
        {secretMsg ? (
          <MobileCard className="border-[var(--k-warn)]/40 bg-[var(--k-warn)]/5 px-3 py-2 font-mono text-[var(--m-text-xs)] text-[var(--k-warn)]">
            {secretMsg}
          </MobileCard>
        ) : null}

        {list.length ? (
          <div className="space-y-2">
            {list.map((s) => (
              <MobileCard key={s.id} className="p-3">
                <div className="flex items-center justify-between gap-2">
                  <div className="min-w-0">
                    <div className="truncate font-mono text-[var(--m-text-sm)]">{s.url}</div>
                    <div className="mt-0.5 text-[var(--m-text-xs)] text-[var(--k-muted)]">
                      创建于 {new Date(s.createdAt).toLocaleDateString('zh-CN')}
                    </div>
                  </div>
                  <StatusPill tone={s.enabled ? 'open' : 'closed'}>{s.enabled ? '启用' : '停用'}</StatusPill>
                </div>
                <div className="mt-2 flex flex-wrap gap-1.5">
                  {s.eventTypes.map((ev) => (
                    <StatusPill key={ev} tone="neutral">
                      {ev}
                    </StatusPill>
                  ))}
                </div>
                <div className="mt-2 flex justify-end">
                  <MobileButton size="sm" variant="danger" onClick={() => void remove(s.id)}>
                    删除
                  </MobileButton>
                </div>
              </MobileCard>
            ))}
          </div>
        ) : (
          <MobileCard className="px-3 py-8 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
            暂无订阅，点「+ 新建」
          </MobileCard>
        )}
      </MobileSection>

      <MobileSheet open={creating} onClose={() => setCreating(false)} title="新建订阅">
        <div className="space-y-2.5">
          <MobileField label="接收 URL">
            <input
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              placeholder="https://…"
              className="h-[var(--m-tap)] w-full rounded-[var(--m-radius-md)] border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 font-mono text-[var(--m-text-sm)] outline-none focus:border-[var(--k-accent)]"
            />
          </MobileField>
          <MobileField label="事件类型">
            <div className="flex flex-wrap gap-1.5">
              {EVENT_TYPES.map((ev) => (
                <button
                  key={ev}
                  type="button"
                  onClick={() => toggleEvent(ev)}
                  className={`rounded-[var(--m-radius-pill)] px-2.5 py-1 text-[var(--m-text-xs)] font-medium ${
                    selected.includes(ev)
                      ? 'bg-[var(--k-accent)] text-white'
                      : 'border border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]'
                  }`}
                >
                  {ev}
                </button>
              ))}
            </div>
          </MobileField>
          <MobileButton block onClick={() => void create()} disabled={!url.trim() || !selected.length}>
            创建
          </MobileButton>
        </div>
      </MobileSheet>
    </div>
  );
}
