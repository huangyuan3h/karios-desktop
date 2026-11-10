import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const { runOpenCodeMock, getResolvedModelMock, generateTextMock } = vi.hoisted(() => ({
  runOpenCodeMock: vi.fn(),
  getResolvedModelMock: vi.fn(),
  generateTextMock: vi.fn(),
}));

vi.mock('./opencode.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('./opencode.js')>();
  return { ...actual, runOpenCode: runOpenCodeMock };
});

vi.mock('./model.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('./model.js')>();
  return { ...actual, getResolvedModel: getResolvedModelMock };
});

vi.mock('ai', async (importOriginal) => {
  const actual = await importOriginal<typeof import('ai')>();
  return { ...actual, generateText: generateTextMock };
});

import {
  generateNewsText,
  getConfiguredNewsModelId,
  getNewsLlmSettings,
  OPENCODE_DEFAULT_MODEL,
  OPENCODE_DEFAULT_TIMEOUT_MS,
  shouldUseOpenCode,
} from './news_llm.js';

const ENV_KEYS = ['NEWS_LLM_DRIVER', 'NEWS_LLM_MODEL', 'NEWS_LLM_TIMEOUT_MS'] as const;

let savedEnv: Record<string, string | undefined> = {};

beforeEach(() => {
  savedEnv = {};
  for (const key of ENV_KEYS) {
    savedEnv[key] = process.env[key];
    delete process.env[key];
  }
  runOpenCodeMock.mockReset();
  getResolvedModelMock.mockReset();
  generateTextMock.mockReset();
});

afterEach(() => {
  for (const key of ENV_KEYS) {
    const value = savedEnv[key];
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
});

describe('getNewsLlmSettings', () => {
  it('defaults to the local OpenCode DeepSeek v4.1 flash model', () => {
    expect(getNewsLlmSettings()).toEqual({
      driver: 'opencode',
      model: 'opencode-go/deepseek-v4.1-flash',
      timeoutMs: OPENCODE_DEFAULT_TIMEOUT_MS,
    });
  });

  it('honours model, timeout and profile fallback overrides', () => {
    process.env.NEWS_LLM_MODEL = 'opencode-go/other-model';
    process.env.NEWS_LLM_TIMEOUT_MS = '45000';
    expect(getNewsLlmSettings()).toMatchObject({
      driver: 'opencode',
      model: 'opencode-go/other-model',
      timeoutMs: 45_000,
    });

    process.env.NEWS_LLM_DRIVER = 'profile';
    expect(getNewsLlmSettings().driver).toBe('profile');
    expect(getConfiguredNewsModelId()).toBeNull();
  });

  it('falls back to the default timeout for invalid values', () => {
    process.env.NEWS_LLM_TIMEOUT_MS = 'nope';
    expect(getNewsLlmSettings().timeoutMs).toBe(OPENCODE_DEFAULT_TIMEOUT_MS);
  });
});

describe('shouldUseOpenCode', () => {
  it('accepts the configured model and rejects other models', () => {
    expect(shouldUseOpenCode(OPENCODE_DEFAULT_MODEL)).toBe(true);
    expect(shouldUseOpenCode(undefined)).toBe(true);
    expect(shouldUseOpenCode('deepseek-v4-flash')).toBe(false);
  });

  it('is disabled when the profile fallback is selected', () => {
    process.env.NEWS_LLM_DRIVER = 'profile';
    expect(shouldUseOpenCode(OPENCODE_DEFAULT_MODEL)).toBe(false);
  });
});

describe('generateNewsText', () => {
  it('uses the OpenCode driver by default and records its model id', async () => {
    runOpenCodeMock.mockResolvedValue('1. 摘要');
    const result = await generateNewsText({ prompt: 'prompt text' });
    expect(runOpenCodeMock).toHaveBeenCalledWith({
      prompt: 'prompt text',
      model: OPENCODE_DEFAULT_MODEL,
      timeoutMs: OPENCODE_DEFAULT_TIMEOUT_MS,
    });
    expect(result).toEqual({ text: '1. 摘要', modelId: OPENCODE_DEFAULT_MODEL });
  });

  it('uses the profile path for a non-OpenCode requested model', async () => {
    const model = { modelId: 'profile-model' };
    getResolvedModelMock.mockResolvedValue({
      model,
      modelId: 'profile-model',
      provider: 'openai',
      looseStructuredOutputs: false,
    });
    generateTextMock.mockResolvedValue({ text: 'cloud answer' });

    const result = await generateNewsText({
      prompt: 'prompt text',
      requestedModel: 'some-cloud-model',
    });

    expect(runOpenCodeMock).not.toHaveBeenCalled();
    expect(generateTextMock).toHaveBeenCalled();
    expect(result).toEqual({ text: 'cloud answer', modelId: 'profile-model' });
  });

  it('propagates driver failures so callers can return an error', async () => {
    runOpenCodeMock.mockRejectedValue(new Error('opencode down'));
    await expect(generateNewsText({ prompt: 'x' })).rejects.toThrow('opencode down');
  });
});
