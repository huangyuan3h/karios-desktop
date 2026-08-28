'use client';

import * as React from 'react';

import { AI_BASE_URL } from '@/lib/endpoints';
import { fetchSystemEvents, resolveSystemEvent, type SystemEvent } from '@/lib/queries/systemEvents';
import { MobileButton, MobileCard, MobileSection, StatusPill } from '../primitives';

/** 设置 (mobile) — theme switch + AI model profiles. §5.2 中频. */

type Profile = {
  id: string;
  name: string;
  provider: 'openai' | 'ollama';
  modelId: string;
  openai?: { hasKey: boolean; keyLast4?: string; baseUrl?: string };
  ollama?: { baseUrl?: string; hasKey: boolean; keyLast4?: string };
};

type Config = {
  source: string;
  activeProfileId: string | null;
  profiles: Profile[];
};

const THEME_KEY = 'karios.theme';

export function MobileSettingsPage() {
  const [theme, setTheme] = React.useState<'light' | 'dark'>('light');
  const [config, setConfig] = React.useState<Config | null>(null);
  const [testing, setTesting] = React.useState<string | null>(null);
  const [testMsg, setTestMsg] = React.useState<string | null>(null);

  React.useEffect(() => {
    const stored = localStorage.getItem(THEME_KEY);
    if (stored === 'dark' || stored === 'light') setTheme(stored);
  }, []);

  const loadConfig = React.useCallback(async () => {
    try {
      const res = await fetch(`${AI_BASE_URL}/config`, { cache: 'no-store' });
      if (res.ok) setConfig((await res.json()) as Config);
    } catch {
      /* AI service unreachable — keep last state */
    }
  }, []);

  React.useEffect(() => {
    void loadConfig();
  }, [loadConfig]);

  const toggleTheme = () => {
    const next = theme === 'light' ? 'dark' : 'light';
    setTheme(next);
    localStorage.setItem(THEME_KEY, next);
    document.documentElement.dataset.theme = next;
  };

  const setActive = async (profileId: string) => {
    try {
      await fetch(`${AI_BASE_URL}/config/active`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ profileId }),
      });
      await loadConfig();
    } catch {
      /* ignore */
    }
  };

  const testProfile = async (profileId: string) => {
    setTesting(profileId);
    setTestMsg(null);
    try {
      const res = await fetch(`${AI_BASE_URL}/config/test`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ profileId }),
      });
      const data = (await res.json()) as { ok?: boolean; error?: string };
      setTestMsg(data.ok ? '连接正常' : data.error ?? '连接失败');
    } catch (e) {
      setTestMsg(e instanceof Error ? e.message : String(e));
    } finally {
      setTesting(null);
    }
  };

  return (
    <div className="space-y-4">
      <MobileSection title="外观">
        <MobileCard className="p-3">
          <div className="flex items-center justify-between">
            <div className="text-[var(--m-text-base)] font-medium">深色模式</div>
            <button
              type="button"
              onClick={toggleTheme}
              className={`h-7 w-12 rounded-[var(--m-radius-pill)] transition-colors ${
                theme === 'dark' ? 'bg-[var(--k-accent)]' : 'bg-[var(--k-surface-2)] border border-[var(--k-border)]'
              }`}
              aria-label="切换深色模式"
            >
              <span
                className={`block h-5 w-5 rounded-full bg-white transition-transform ${
                  theme === 'dark' ? 'translate-x-6' : 'translate-x-1'
                }`}
              />
            </button>
          </div>
        </MobileCard>
      </MobileSection>

      <MobileSection
        title="AI 模型"
        action={
          <button type="button" onClick={() => void loadConfig()} className="text-[var(--m-text-sm)] text-[var(--k-accent)]">
            刷新
          </button>
        }
      >
        {testMsg ? (
          <MobileCard className="px-3 py-2 text-[var(--m-text-sm)] text-[var(--k-accent)]">{testMsg}</MobileCard>
        ) : null}
        {config?.profiles.length ? (
          <div className="space-y-2">
            {config.profiles.map((p) => (
              <MobileCard key={p.id} className="p-3">
                <div className="flex items-center justify-between gap-2">
                  <div className="min-w-0">
                    <div className="truncate text-[var(--m-text-base)] font-medium">
                      {p.name}
                      {config.activeProfileId === p.id ? (
                        <span className="ml-1.5"><StatusPill tone="open">当前</StatusPill></span>
                      ) : null}
                    </div>
                    <div className="mt-0.5 truncate font-mono text-[var(--m-text-xs)] text-[var(--k-muted)]">
                      {p.provider} · {p.modelId}
                      {p.provider === 'openai' && p.openai?.keyLast4 ? ` · key ****${p.openai.keyLast4}` : ''}
                    </div>
                  </div>
                  <div className="flex shrink-0 gap-1.5">
                    {config.activeProfileId !== p.id ? (
                      <MobileButton size="sm" variant="ghost" onClick={() => void setActive(p.id)}>
                        启用
                      </MobileButton>
                    ) : null}
                    <MobileButton size="sm" variant="ghost" onClick={() => void testProfile(p.id)} disabled={testing === p.id}>
                      {testing === p.id ? '测试中…' : '测试'}
                    </MobileButton>
                  </div>
                </div>
              </MobileCard>
            ))}
          </div>
        ) : (
          <MobileCard className="px-3 py-8 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
            {config ? '暂无模型配置' : 'AI 服务不可达'}
          </MobileCard>
        )}
      </MobileSection>

      <MobileSection title="系统日志">
        <SystemLogsMobile />
      </MobileSection>

      <MobileSection title="关于">
        <MobileCard className="p-3 text-[var(--m-text-sm)] text-[var(--k-muted)]">
          Karios 手机版 · 与桌面端共享数据层 · 网关密钥由系统自动携带
        </MobileCard>
      </MobileSection>
    </div>
  );
}

function SystemLogsMobile() {
  const [events, setEvents] = React.useState<SystemEvent[]>([]);
  const [filter, setFilter] = React.useState<'all' | 'high' | 'low'>('all');
  const load = React.useCallback(async () => {
    try {
      const data = await fetchSystemEvents(50);
      setEvents(data);
    } catch {
      /* ignore */
    }
  }, []);
  React.useEffect(() => {
    void load();
  }, [load]);
  const list = events.filter((e) => (filter === 'all' ? true : e.severity === filter));
  return (
    <div className="space-y-2">
      <div className="flex gap-1">
        {(['all', 'high', 'low'] as const).map((v) => (
          <MobileButton key={v} size="sm" variant={filter === v ? 'primary' : 'ghost'} onClick={() => setFilter(v)}>
            {v === 'all' ? '全部' : v === 'high' ? '高' : '低'}
          </MobileButton>
        ))}
        <MobileButton size="sm" variant="ghost" onClick={() => void load()}>
          刷新
        </MobileButton>
      </div>
      <div className="text-[var(--m-text-xs)] text-[var(--k-muted)]">高推 Bark，低仅落表 · 每周集中修复</div>
      {list.length === 0 ? (
        <MobileCard className="px-3 py-6 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">暂无未处理事件</MobileCard>
      ) : (
        <div className="space-y-2">
          {list.slice(0, 20).map((ev) => (
            <MobileCard key={ev.id} className="p-3">
              <div className="flex items-start justify-between gap-2">
                <div className="min-w-0">
                  <div className="truncate text-[var(--m-text-sm)] font-medium">
                    {ev.severity === 'high' ? '●' : '○'} {ev.title}
                  </div>
                  <div className="mt-0.5 truncate text-[var(--m-text-xs)] text-[var(--k-muted)]">{ev.detail || JSON.stringify(ev.payload).slice(0, 60)}</div>
                  <div className="mt-1 text-[var(--m-text-xs)] text-[var(--k-muted)]">{new Date(ev.createdAt).toLocaleString('zh-CN')}</div>
                </div>
                <MobileButton
                  size="sm"
                  variant="ghost"
                  onClick={async () => {
                    await resolveSystemEvent(ev.id);
                    await load();
                  }}
                >
                  已修复
                </MobileButton>
              </div>
            </MobileCard>
          ))}
        </div>
      )}
    </div>
  );
}
