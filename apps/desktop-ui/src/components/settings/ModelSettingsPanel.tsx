'use client';

import * as React from 'react';
import { Check, Pencil, Plus, RefreshCw, Trash2, X } from 'lucide-react';

import { AI_BASE_URL } from '@/lib/endpoints';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { cn } from '@/lib/utils';

type Provider = 'openai' | 'ollama';

type Profile = {
  id: string;
  name: string;
  provider: Provider;
  modelId: string;
  openai?: { hasKey: boolean; keyLast4: string | null; baseUrl: string | null };
  ollama?: { baseUrl: string | null; hasKey: boolean; keyLast4: string | null };
};

type Config = {
  source: 'file' | 'env' | 'default';
  activeProfileId: string | null;
  profiles: Profile[];
};

type FormData = {
  name: string;
  provider: Provider;
  modelId: string;
  openaiKey: string;
  openaiBaseUrl: string;
  ollamaBaseUrl: string;
  ollamaKey: string;
  setActive: boolean;
};

const defaultFormData: FormData = {
  name: '',
  provider: 'openai',
  modelId: 'gpt-4o',
  openaiKey: '',
  openaiBaseUrl: '',
  ollamaBaseUrl: 'http://127.0.0.1:11434/v1',
  ollamaKey: '',
  setActive: true,
};

async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(`${AI_BASE_URL}${path}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

async function apiSend<T>(
  path: string,
  method: 'POST' | 'PUT' | 'DELETE',
  body?: unknown,
): Promise<T> {
  const res = await fetch(`${AI_BASE_URL}${path}`, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  }
  return res.json();
}

export function ModelSettingsPanel() {
  const [config, setConfig] = React.useState<Config | null>(null);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [success, setSuccess] = React.useState<string | null>(null);

  const [showAdd, setShowAdd] = React.useState(false);
  const [formData, setFormData] = React.useState<FormData>(defaultFormData);

  const [editingId, setEditingId] = React.useState<string | null>(null);
  const [editForm, setEditForm] = React.useState<FormData>(defaultFormData);

  const loadConfig = React.useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const cfg = await apiGet<Config>('/config');
      setConfig(cfg);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    loadConfig();
  }, [loadConfig]);

  const showSuccess = (msg: string) => {
    setSuccess(msg);
    setTimeout(() => setSuccess(null), 3000);
  };

  const setActiveProfile = async (profileId: string) => {
    setLoading(true);
    setError(null);
    try {
      const cfg = await apiSend<Config>('/config/active', 'POST', { profileId });
      setConfig(cfg);
      showSuccess('已切换');
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  const createProfile = async () => {
    if (!formData.name.trim()) {
      setError('请输入名称');
      return;
    }
    if (!formData.modelId.trim()) {
      setError('请输入模型 ID');
      return;
    }
    if (formData.provider === 'openai' && !formData.openaiKey.trim()) {
      setError('请输入 OpenAI API Key');
      return;
    }

    setLoading(true);
    setError(null);
    try {
      const payload =
        formData.provider === 'openai'
          ? {
              name: formData.name.trim(),
              provider: 'openai' as const,
              modelId: formData.modelId.trim(),
              setActive: formData.setActive,
              openai: {
                apiKey: formData.openaiKey.trim(),
                baseUrl: formData.openaiBaseUrl.trim() || undefined,
              },
            }
          : {
              name: formData.name.trim(),
              provider: 'ollama' as const,
              modelId: formData.modelId.trim(),
              setActive: formData.setActive,
              ollama: {
                baseUrl: formData.ollamaBaseUrl.trim() || 'http://127.0.0.1:11434/v1',
                apiKey: formData.ollamaKey.trim() || undefined,
              },
            };

      const cfg = await apiSend<Config>('/config/profiles', 'POST', payload);
      setConfig(cfg);
      setShowAdd(false);
      setFormData(defaultFormData);
      showSuccess('已添加');
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  const updateProfile = async () => {
    if (!editingId) return;
    if (!editForm.name.trim()) {
      setError('请输入名称');
      return;
    }
    if (!editForm.modelId.trim()) {
      setError('请输入模型 ID');
      return;
    }

    setLoading(true);
    setError(null);
    try {
      const payload =
        editForm.provider === 'openai'
          ? {
              name: editForm.name.trim(),
              modelId: editForm.modelId.trim(),
              openai: {
                apiKey: editForm.openaiKey.trim() || undefined,
                baseUrl: editForm.openaiBaseUrl.trim() || undefined,
              },
            }
          : {
              name: editForm.name.trim(),
              modelId: editForm.modelId.trim(),
              ollama: {
                baseUrl: editForm.ollamaBaseUrl.trim() || 'http://127.0.0.1:11434/v1',
                apiKey: editForm.ollamaKey.trim() || undefined,
              },
            };

      const cfg = await apiSend<Config>(
        `/config/profiles/${encodeURIComponent(editingId)}`,
        'PUT',
        payload,
      );
      setConfig(cfg);
      setEditingId(null);
      showSuccess('已更新');
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  const deleteProfile = async (id: string) => {
    if (!confirm('确定删除此配置？')) return;
    setLoading(true);
    setError(null);
    try {
      const cfg = await apiSend<Config>(`/config/profiles/${encodeURIComponent(id)}`, 'DELETE');
      setConfig(cfg);
      showSuccess('已删除');
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  const testConfig = async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await apiSend<{
        ok: boolean;
        error?: string;
        provider?: string;
        modelId?: string;
      }>('/config/test', 'POST', { profileId: config?.activeProfileId ?? undefined });
      if (result.ok) {
        showSuccess(`测试通过 (${result.provider} / ${result.modelId})`);
      } else {
        setError(result.error || '测试失败');
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  const openEdit = (p: Profile) => {
    setEditingId(p.id);
    setEditForm({
      name: p.name,
      provider: p.provider,
      modelId: p.modelId,
      openaiKey: '',
      openaiBaseUrl: p.openai?.baseUrl ?? '',
      ollamaBaseUrl: p.ollama?.baseUrl ?? 'http://127.0.0.1:11434/v1',
      ollamaKey: '',
      setActive: config?.activeProfileId === p.id,
    });
    setError(null);
  };

  const activeProfile = config?.profiles.find((p) => p.id === config.activeProfileId);

  return (
    <div className="space-y-4">
      {error && (
        <div className="rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      )}
      {success && (
        <div className="rounded-lg border border-emerald-500/30 bg-emerald-500/10 px-3 py-2 text-sm text-emerald-700">
          {success}
        </div>
      )}

      <div className="flex items-center justify-between">
        <div>
          <div className="font-medium">模型配置</div>
          <div className="text-sm text-[var(--k-muted)]">
            管理 OpenAI 和 Ollama 配置，选择当前使用的模型
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="secondary" size="sm" onClick={loadConfig} disabled={loading}>
            <RefreshCw className="mr-1 h-3 w-3" />
            刷新
          </Button>
          <Button variant="secondary" size="sm" onClick={testConfig} disabled={loading}>
            测试连接
          </Button>
          <Button size="sm" onClick={() => setShowAdd(true)} disabled={loading}>
            <Plus className="mr-1 h-3 w-3" />
            添加
          </Button>
        </div>
      </div>

      {activeProfile && (
        <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2 text-sm">
          <span className="text-[var(--k-muted)]">当前使用：</span>
          <span className="font-medium">{activeProfile.name}</span>
          <span className="text-[var(--k-muted)]">
            {' '}
            ({activeProfile.provider} / {activeProfile.modelId})
          </span>
        </div>
      )}

      <div className="overflow-hidden rounded-lg border border-[var(--k-border)]">
        <div className="grid grid-cols-12 gap-2 bg-[var(--k-surface-2)] px-3 py-2 text-xs text-[var(--k-muted)]">
          <div className="col-span-1" />
          <div className="col-span-3">名称</div>
          <div className="col-span-2">类型</div>
          <div className="col-span-3">模型</div>
          <div className="col-span-2">URL</div>
          <div className="col-span-1 text-right">操作</div>
        </div>

        <div className="divide-y divide-[var(--k-border)]">
          {config?.profiles.map((p) => {
            const isActive = config.activeProfileId === p.id;
            const isEditing = editingId === p.id;

            return (
              <div
                key={p.id}
                className={cn(
                  'grid grid-cols-12 gap-2 px-3 py-2',
                  isActive && 'bg-[var(--k-surface-2)]',
                )}
              >
                <div className="col-span-1 grid place-items-center">
                  <button
                    type="button"
                    className={cn(
                      'h-5 w-5 rounded-full border grid place-items-center transition-colors',
                      isActive
                        ? 'bg-[var(--k-text)] text-[var(--k-surface)] border-[var(--k-text)]'
                        : 'bg-[var(--k-surface)] border-[var(--k-border)] hover:border-[var(--k-text)]',
                    )}
                    onClick={() => setActiveProfile(p.id)}
                    disabled={loading}
                    title={isActive ? '当前使用' : '选择'}
                  >
                    {isActive && <Check className="h-3 w-3" />}
                  </button>
                </div>

                {isEditing ? (
                  <>
                    <div className="col-span-3">
                      <Input
                        value={editForm.name}
                        onChange={(e) => setEditForm({ ...editForm, name: e.target.value })}
                        className="h-8"
                      />
                    </div>
                    <div className="col-span-2 flex items-center text-sm text-[var(--k-muted)]">
                      {editForm.provider}
                    </div>
                    <div className="col-span-3">
                      <Input
                        value={editForm.modelId}
                        onChange={(e) => setEditForm({ ...editForm, modelId: e.target.value })}
                        className="h-8"
                      />
                    </div>
                    <div className="col-span-2">
                      <Input
                        value={
                          editForm.provider === 'openai'
                            ? editForm.openaiBaseUrl
                            : editForm.ollamaBaseUrl
                        }
                        onChange={(e) =>
                          editForm.provider === 'openai'
                            ? setEditForm({ ...editForm, openaiBaseUrl: e.target.value })
                            : setEditForm({ ...editForm, ollamaBaseUrl: e.target.value })
                        }
                        placeholder="默认"
                        className="h-8"
                      />
                    </div>
                    <div className="col-span-1 flex items-center justify-end gap-1">
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-7 w-7 p-0"
                        onClick={updateProfile}
                        disabled={loading}
                      >
                        <Check className="h-3 w-3" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-7 w-7 p-0"
                        onClick={() => setEditingId(null)}
                        disabled={loading}
                      >
                        <X className="h-3 w-3" />
                      </Button>
                    </div>
                  </>
                ) : (
                  <>
                    <div className="col-span-3 flex items-center truncate text-sm">{p.name}</div>
                    <div className="col-span-2 flex items-center text-sm text-[var(--k-muted)]">
                      {p.provider}
                    </div>
                    <div className="col-span-3 flex items-center truncate font-mono text-xs text-[var(--k-muted)]">
                      {p.modelId}
                    </div>
                    <div className="col-span-2 flex items-center truncate text-xs text-[var(--k-muted)]">
                      {(p.provider === 'openai' ? p.openai?.baseUrl : p.ollama?.baseUrl) || '默认'}
                    </div>
                    <div className="col-span-1 flex items-center justify-end gap-1">
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-7 w-7 p-0"
                        onClick={() => openEdit(p)}
                        disabled={loading}
                      >
                        <Pencil className="h-3 w-3" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-7 w-7 p-0 text-red-600 hover:text-red-600"
                        onClick={() => deleteProfile(p.id)}
                        disabled={loading}
                      >
                        <Trash2 className="h-3 w-3" />
                      </Button>
                    </div>
                  </>
                )}
              </div>
            );
          })}

          {(!config?.profiles || config.profiles.length === 0) && (
            <div className="px-3 py-6 text-center text-sm text-[var(--k-muted)]">
              暂无配置，点击"添加"创建
            </div>
          )}
        </div>
      </div>

      {editingId && (
        <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-3 text-sm font-medium">
            编辑 {editForm.provider === 'openai' ? 'OpenAI' : 'Ollama'} 配置
          </div>
          {editForm.provider === 'openai' ? (
            <div className="grid gap-2">
              <div className="text-xs text-[var(--k-muted)]">API Key（留空保持不变）</div>
              <Input
                type="password"
                value={editForm.openaiKey}
                onChange={(e) => setEditForm({ ...editForm, openaiKey: e.target.value })}
                placeholder="sk-..."
              />
            </div>
          ) : (
            <div className="grid gap-2">
              <div className="text-xs text-[var(--k-muted)]">API Key（可选，留空保持不变）</div>
              <Input
                type="password"
                value={editForm.ollamaKey}
                onChange={(e) => setEditForm({ ...editForm, ollamaKey: e.target.value })}
              />
            </div>
          )}
        </div>
      )}

      {showAdd && (
        <div className="fixed inset-0 z-[100]">
          <div className="absolute inset-0 bg-black/40" onClick={() => setShowAdd(false)} />
          <div className="absolute left-1/2 top-1/2 w-[480px] max-w-[92vw] -translate-x-1/2 -translate-y-1/2 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4 shadow-xl">
            <div className="mb-4 text-sm font-semibold">添加模型配置</div>

            <div className="grid gap-3">
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <div className="mb-1 text-xs text-[var(--k-muted)]">名称</div>
                  <Input
                    value={formData.name}
                    onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                    placeholder="如：OpenAI 生产"
                  />
                </div>
                <div>
                  <div className="mb-1 text-xs text-[var(--k-muted)]">类型</div>
                  <Select
                    value={formData.provider}
                    onValueChange={(value: Provider) => {
                      if (value === 'openai') {
                        setFormData({ ...formData, provider: 'openai', modelId: 'gpt-4o' });
                      } else {
                        setFormData({
                          ...formData,
                          provider: 'ollama',
                          modelId: '',
                          ollamaBaseUrl: 'http://127.0.0.1:11434/v1',
                        });
                      }
                    }}
                  >
                    <SelectTrigger className="h-9">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="openai">OpenAI</SelectItem>
                      <SelectItem value="ollama">Ollama</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              <div>
                <div className="mb-1 text-xs text-[var(--k-muted)]">模型 ID</div>
                <Input
                  value={formData.modelId}
                  onChange={(e) => setFormData({ ...formData, modelId: e.target.value })}
                  placeholder={
                    formData.provider === 'openai'
                      ? 'gpt-4o / gpt-4o-mini'
                      : 'qwen2.5:14b / llama3.1:8b'
                  }
                />
              </div>

              {formData.provider === 'openai' ? (
                <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                  <div className="mb-2 text-xs font-medium">OpenAI 配置</div>
                  <div className="grid gap-2">
                    <div>
                      <div className="mb-1 text-xs text-[var(--k-muted)]">API Key *</div>
                      <Input
                        type="password"
                        value={formData.openaiKey}
                        onChange={(e) => setFormData({ ...formData, openaiKey: e.target.value })}
                        placeholder="sk-..."
                      />
                    </div>
                    <div>
                      <div className="mb-1 text-xs text-[var(--k-muted)]">Base URL（可选）</div>
                      <Input
                        value={formData.openaiBaseUrl}
                        onChange={(e) =>
                          setFormData({ ...formData, openaiBaseUrl: e.target.value })
                        }
                        placeholder="默认使用 api.openai.com"
                      />
                    </div>
                  </div>
                </div>
              ) : (
                <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                  <div className="mb-2 text-xs font-medium">Ollama 配置</div>
                  <div className="grid gap-2">
                    <div>
                      <div className="mb-1 text-xs text-[var(--k-muted)]">Base URL</div>
                      <Input
                        value={formData.ollamaBaseUrl}
                        onChange={(e) =>
                          setFormData({ ...formData, ollamaBaseUrl: e.target.value })
                        }
                        placeholder="http://127.0.0.1:11434/v1"
                      />
                    </div>
                    <div>
                      <div className="mb-1 text-xs text-[var(--k-muted)]">API Key（可选）</div>
                      <Input
                        type="password"
                        value={formData.ollamaKey}
                        onChange={(e) => setFormData({ ...formData, ollamaKey: e.target.value })}
                        placeholder="某些代理服务需要"
                      />
                    </div>
                  </div>
                </div>
              )}

              <label className="flex items-center gap-2 text-xs">
                <input
                  type="checkbox"
                  checked={formData.setActive}
                  onChange={(e) => setFormData({ ...formData, setActive: e.target.checked })}
                />
                设为当前使用
              </label>
            </div>

            <div className="mt-4 flex justify-end gap-2">
              <Button variant="secondary" size="sm" onClick={() => setShowAdd(false)}>
                取消
              </Button>
              <Button size="sm" onClick={createProfile} disabled={loading}>
                保存
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
