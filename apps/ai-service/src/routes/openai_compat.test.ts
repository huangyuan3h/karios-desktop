import { beforeEach, describe, expect, it, vi } from 'vitest';

const { generateNewsTextMock } = vi.hoisted(() => ({
  generateNewsTextMock: vi.fn(),
}));

vi.mock('../news_llm.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../news_llm.js')>();
  return { ...actual, generateNewsText: generateNewsTextMock };
});

import { app } from '../app.js';
import { OPENCODE_DEFAULT_MODEL } from '../opencode.js';

function postCompletion(body: unknown) {
  return app.request('/v1/chat/completions', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
}

beforeEach(() => {
  generateNewsTextMock.mockReset();
});

describe('POST /v1/chat/completions with the OpenCode news model', () => {
  it('routes the request through the OpenCode driver and reports its model id', async () => {
    generateNewsTextMock.mockResolvedValue({
      text: '```json\n[{"id":"i0","importance":3}]\n```',
      modelId: OPENCODE_DEFAULT_MODEL,
    });

    const res = await postCompletion({
      model: OPENCODE_DEFAULT_MODEL,
      messages: [{ role: 'user', content: 'extract fields' }],
      response_format: { type: 'json_object' },
      max_tokens: 4096,
    });

    expect(res.status).toBe(200);
    const json = (await res.json()) as {
      model?: string;
      choices: Array<{ message: { content: string } }>;
    };
    expect(json.model).toBe(OPENCODE_DEFAULT_MODEL);
    expect(json.choices[0].message.content).toBe('[{"id":"i0","importance":3}]');
    expect(generateNewsTextMock).toHaveBeenCalledWith(
      expect.objectContaining({
        prompt: 'extract fields',
        requestedModel: OPENCODE_DEFAULT_MODEL,
        maxOutputTokens: 4096,
      }),
    );
  });

  it('returns a JSON error and 500 when OpenCode fails', async () => {
    generateNewsTextMock.mockRejectedValue(new Error('OpenCode returned no text'));

    const res = await postCompletion({
      model: OPENCODE_DEFAULT_MODEL,
      messages: [{ role: 'user', content: 'extract fields' }],
    });

    expect(res.status).toBe(500);
    const json = (await res.json()) as { error?: string; model?: string };
    expect(json.error).toContain('OpenCode returned no text');
    expect(json.model).toBe(OPENCODE_DEFAULT_MODEL);
  });
});
