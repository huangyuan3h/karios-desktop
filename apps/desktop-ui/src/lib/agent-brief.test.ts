import { afterEach, describe, expect, it, vi } from 'vitest';

import {
  buildJudgmentBriefUrl,
  fetchJudgmentBriefMarkdown,
  JudgmentBriefUnavailableError,
} from './agent-brief';

describe('agent-brief client', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  function mockFetch(text: string, status = 200) {
    const fn = vi.fn(async () => ({
      ok: status >= 200 && status < 300,
      status,
      statusText: status === 200 ? 'OK' : 'Not Found',
      text: async () => text,
    }));
    vi.stubGlobal('fetch', fn);
    return fn;
  }

  it('builds the judgement-brief markdown url with mode (and tradeDate)', () => {
    const url = buildJudgmentBriefUrl({ mode: 'compact', tradeDate: '2026-11-10' });
    expect(url).toContain('/v1/agent/judgment-brief.md?');
    expect(url).toContain('mode=compact');
    expect(url).toContain('tradeDate=2026-11-10');
    expect(buildJudgmentBriefUrl()).toContain('mode=full');
  });

  it('returns the markdown body verbatim', async () => {
    mockFetch('# 决策简报 judgment brief\n');
    await expect(fetchJudgmentBriefMarkdown({ mode: 'full' })).resolves.toBe(
      '# 决策简报 judgment brief\n',
    );
  });

  it('maps a 404 to the restart hint instead of falling back', async () => {
    mockFetch('{"detail":"Not Found"}', 404);
    await expect(fetchJudgmentBriefMarkdown()).rejects.toBeInstanceOf(
      JudgmentBriefUnavailableError,
    );
    await expect(fetchJudgmentBriefMarkdown()).rejects.toThrow(/data-sync 服务需要重启/);
  });

  it('surfaces other HTTP failures', async () => {
    mockFetch('boom', 500);
    await expect(fetchJudgmentBriefMarkdown()).rejects.toThrow(/HTTP 500/);
  });

  it('rejects an empty body', async () => {
    mockFetch('   ');
    await expect(fetchJudgmentBriefMarkdown()).rejects.toThrow(/返回为空/);
  });
});
