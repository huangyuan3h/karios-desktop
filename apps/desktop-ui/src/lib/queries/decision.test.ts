import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/lib/api/client', () => ({
  apiGetJson: vi.fn(),
}));

import { apiGetJson } from '@/lib/api/client';

import {
  appendDecisionMessage,
  createDecisionSession,
  decisionMessagesQueryKey,
  decisionSessionsQueryKey,
  decisionSnapshotToMarkdown,
  deleteDecisionMessage,
  fetchDecisionMessages,
  fetchDecisionSessions,
  fetchDecisionSnapshot,
  renameDecisionSession,
  updateDecisionSession,
  type DecisionSnapshot,
} from './decision';

const mockedApiGetJson = vi.mocked(apiGetJson);
const fetchMock = vi.fn();

function okFetch(body: unknown, init?: ResponseInit): Response {
  return {
    ok: true,
    status: 200,
    json: () => Promise.resolve(body),
  } as Response & { status: number };
}

describe('query keys', () => {
  it('distinguishes sessions vs per-session messages', () => {
    expect(decisionSessionsQueryKey()).toEqual(['decision', 'sessions']);
    expect(decisionMessagesQueryKey(7)).toEqual(['decision', 'messages', 7]);
  });
});

describe('fetchDecisionSessions', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
  });

  it('returns sessions list from payload', async () => {
    mockedApiGetJson.mockResolvedValue({ ok: true, sessions: [{ id: 1 }] });
    const out = await fetchDecisionSessions();
    expect(out).toEqual([{ id: 1 }]);
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe('/api/decision/sessions');
  });

  it('returns [] when sessions missing', async () => {
    mockedApiGetJson.mockResolvedValue({ ok: true });
    expect(await fetchDecisionSessions()).toEqual([]);
  });
});

describe('createDecisionSession', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', fetchMock);
    fetchMock.mockReset();
  });

  it('posts options and returns session', async () => {
    fetchMock.mockResolvedValue(okFetch({ ok: true, session: { id: 2 } }));
    const out = await createDecisionSession({ title: 'T' });
    expect(out.id).toBe(2);
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining('/api/decision/sessions'),
      expect.objectContaining({ method: 'POST' }),
    );
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({ title: 'T' });
  });

  it('throws on non-ok response', async () => {
    fetchMock.mockResolvedValue({ ok: false, status: 500 });
    await expect(createDecisionSession()).rejects.toThrow('create session failed: 500');
  });

  it('posts empty body when no options', async () => {
    fetchMock.mockResolvedValue(okFetch({ ok: true, session: { id: 3 } }));
    await createDecisionSession();
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({});
  });
});

describe('updateDecisionSession', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', fetchMock);
    fetchMock.mockReset();
  });

  it('patches title and system prompt', async () => {
    fetchMock.mockResolvedValue(okFetch({ ok: true, session: { id: 1, title: 'New' } }));
    const out = await updateDecisionSession(1, { title: 'New', systemPrompt: 'p' });
    expect(out.title).toBe('New');
    expect(fetchMock.mock.calls[0][1].method).toBe('PATCH');
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({
      title: 'New',
      system_prompt: 'p',
    });
  });

  it('nulls unspecified fields', async () => {
    fetchMock.mockResolvedValue(okFetch({ ok: true, session: { id: 1 } }));
    await updateDecisionSession(1, {});
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({
      title: null,
      system_prompt: null,
    });
  });

  it('throws on failure', async () => {
    fetchMock.mockResolvedValue({ ok: false, status: 409 });
    await expect(updateDecisionSession(1, { title: 'T' })).rejects.toThrow(
      'update session failed: 409',
    );
  });
});

describe('renameDecisionSession', () => {
  it('delegates to update with title', async () => {
    vi.stubGlobal('fetch', fetchMock);
    fetchMock.mockReset();
    fetchMock.mockResolvedValue(okFetch({ ok: true, session: { id: 1, title: 'R' } }));
    const out = await renameDecisionSession(1, 'R');
    expect(out.title).toBe('R');
  });
});

describe('fetchDecisionMessages', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
  });

  it('returns messages and encodes session id', async () => {
    mockedApiGetJson.mockResolvedValue({ ok: true, messages: [{ id: 9 }] });
    const out = await fetchDecisionMessages(7);
    expect(out).toEqual([{ id: 9 }]);
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe(
      '/api/decision/sessions/7/messages',
    );
  });
});

describe('fetchDecisionSnapshot', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
  });

  it('returns snapshot when present', async () => {
    mockedApiGetJson.mockResolvedValue({ ok: true, snapshot: { snapshotDate: 'd' } });
    expect(await fetchDecisionSnapshot('2026-08-07')).toEqual({ snapshotDate: 'd' });
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe(
      '/api/decision/snapshots/2026-08-07',
    );
  });

  it('returns null when missing', async () => {
    mockedApiGetJson.mockResolvedValue({ ok: true });
    expect(await fetchDecisionSnapshot('2026-08-07')).toBeNull();
  });
});

describe('decisionSnapshotToMarkdown', () => {
  it('marks reviewed vs unreviewed', () => {
    const reviewed: DecisionSnapshot = {
      snapshotDate: '2026-08-07',
      status: 'reviewed',
      activeLayerRef: null,
      agentExchanges: [],
      outcome: null,
    };
    expect(decisionSnapshotToMarkdown(reviewed)).toContain('已反馈');
    const open = { ...reviewed, status: 'open' };
    expect(decisionSnapshotToMarkdown(open)).toContain('未反馈');
  });

  it('renders fired and paper sections with truncation', () => {
    const fired: Array<{
      symbol: string | null;
      field: string | null;
      newValue: string | null;
      source: string | null;
    }> = Array.from({ length: 15 }, (_, i) => ({
      symbol: `CN:60000${i}`,
      field: 'positionPct',
      newValue: `1${i}`,
      source: 'auto',
    }));
    fired[1] = { symbol: null, field: 'positionPct', newValue: null, source: null };
    const paper = [
      { symbol: 'CN:600519', side: 'buy', status: 'open', pnlPct: -2.5 },
      { symbol: null, side: null, status: null, pnlPct: null },
    ];
    const snap: DecisionSnapshot = {
      snapshotDate: '2026-08-07',
      status: 'open',
      activeLayerRef: null,
      agentExchanges: [],
      outcome: { fired, paper },
    };
    const md = decisionSnapshotToMarkdown(snap);
    expect(md).toContain('## 当日开火（15）');
    expect(md).toContain('## 当日模拟盘（2）');
    expect(md).toContain('pnl=-2.5%');
    expect(md).toContain('- — positionPct (?)');
    expect(md).toContain('—   pnl=—%');
    expect(md).toContain('CN:600009 19 (auto)');
    expect(md).not.toContain('CN:600010');
  });

  it('renders agent exchanges limited to last 6', () => {
    const exchanges = Array.from({ length: 8 }, (_, i) => ({
      role: i % 2 ? 'assistant' : 'user',
      content: `msg ${i}`,
      createdAt: `2026-08-07T10:0${i}:00`,
    }));
    const snap: DecisionSnapshot = {
      snapshotDate: '2026-08-07',
      status: 'open',
      activeLayerRef: null,
      agentExchanges: exchanges,
      outcome: null,
    };
    const md = decisionSnapshotToMarkdown(snap);
    expect(md).toContain('## 当日决策对话（8 条，列最近 6 条）');
    expect(md).toContain('msg 2');
    expect(md).not.toContain('msg 0');
    expect(md).not.toContain('msg 1');
  });

  it('renders minimal snapshot without outcome', () => {
    const snap: DecisionSnapshot = {
      snapshotDate: '2026-08-07',
      status: 'open',
      activeLayerRef: null,
      agentExchanges: [],
      outcome: null,
    };
    const md = decisionSnapshotToMarkdown(snap);
    expect(md).toContain('以上为历史归档数据');
    expect(md).not.toContain('## 当日开火');
  });
});

describe('deleteDecisionMessage', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', fetchMock);
    fetchMock.mockReset();
  });

  it('returns true when ok', async () => {
    fetchMock.mockResolvedValue(okFetch({ ok: true }));
    expect(await deleteDecisionMessage(1, 2)).toBe(true);
    expect(fetchMock.mock.calls[0][1].method).toBe('DELETE');
  });

  it('returns false when response not ok', async () => {
    fetchMock.mockResolvedValue({ ok: false, status: 404 });
    expect(await deleteDecisionMessage(1, 2)).toBe(false);
  });

  it('returns false when ok flag false', async () => {
    fetchMock.mockResolvedValue(okFetch({ ok: false }));
    expect(await deleteDecisionMessage(1, 2)).toBe(false);
  });
});

describe('appendDecisionMessage', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', fetchMock);
    fetchMock.mockReset();
  });

  it('posts message and returns it', async () => {
    fetchMock.mockResolvedValue(okFetch({ ok: true, message: { id: 5 } }));
    const out = await appendDecisionMessage(1, { role: 'user', content: 'hi' });
    expect(out.id).toBe(5);
    expect(fetchMock.mock.calls[0][1].method).toBe('POST');
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({
      role: 'user',
      content: 'hi',
    });
  });

  it('throws on failure', async () => {
    fetchMock.mockResolvedValue({ ok: false, status: 500 });
    await expect(appendDecisionMessage(1, { role: 'user', content: 'hi' })).rejects.toThrow(
      'append message failed: 500',
    );
  });
});
