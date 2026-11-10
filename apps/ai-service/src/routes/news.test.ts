import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const { generateNewsTextMock } = vi.hoisted(() => ({
  generateNewsTextMock: vi.fn(),
}));

vi.mock('../news_llm.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../news_llm.js')>();
  return { ...actual, generateNewsText: generateNewsTextMock };
});

import { app } from '../app.js';
import { OPENCODE_DEFAULT_MODEL } from '../opencode.js';

function postSummary(body: unknown) {
  return app.request('/news/summary', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
}

beforeEach(() => {
  generateNewsTextMock.mockReset();
});

afterEach(() => {
  vi.unstubAllEnvs();
});

describe('POST /news/summary', () => {
  it('returns an empty summary for an empty item list and records the model', async () => {
    const res = await postSummary({ items: [] });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({
      summary: '',
      itemsCount: 0,
      model: OPENCODE_DEFAULT_MODEL,
    });
    expect(generateNewsTextMock).not.toHaveBeenCalled();
  });

  it('uses the OpenCode driver, strips thinking tags and returns the model id', async () => {
    generateNewsTextMock.mockResolvedValue({
      text: '<think>internal reasoning</think>\n1. 央行降准\n2. 半导体走强',
      modelId: OPENCODE_DEFAULT_MODEL,
    });

    const res = await postSummary({
      items: [{ title: '央行宣布降准' }, { title: '半导体板块大涨' }],
      hours: 24,
    });

    expect(res.status).toBe(200);
    const json = await res.json();
    expect(json).toEqual({
      summary: '1. 央行降准\n2. 半导体走强',
      itemsCount: 2,
      model: OPENCODE_DEFAULT_MODEL,
    });
    expect(generateNewsTextMock).toHaveBeenCalledOnce();
    const prompt = generateNewsTextMock.mock.calls[0][0].prompt as string;
    expect(prompt).toContain('央行宣布降准');
    expect(prompt).toContain('半导体板块大涨');
  });

  it('returns a JSON error and 500 when the driver fails', async () => {
    generateNewsTextMock.mockRejectedValue(new Error('OpenCode timed out after 180000ms'));

    const res = await postSummary({ items: [{ title: '某条新闻' }] });

    expect(res.status).toBe(500);
    const json = (await res.json()) as { error?: string; model?: string };
    expect(json.error).toContain('OpenCode timed out');
    expect(json.model).toBe(OPENCODE_DEFAULT_MODEL);
  });

  it('returns 502 when the model produced only thinking content', async () => {
    generateNewsTextMock.mockResolvedValue({
      text: '<think>only reasoning</think>',
      modelId: OPENCODE_DEFAULT_MODEL,
    });

    const res = await postSummary({ items: [{ title: '某条新闻' }] });

    expect(res.status).toBe(502);
    const json = (await res.json()) as { error?: string; model?: string };
    expect(json.error).toContain('only thinking content');
    expect(json.model).toBe(OPENCODE_DEFAULT_MODEL);
  });

  it('rejects an invalid body without calling the driver', async () => {
    const res = await postSummary({ items: [{ sourceId: 'x' }] });
    expect(res.status).toBe(400);
    expect(generateNewsTextMock).not.toHaveBeenCalled();
  });
});
