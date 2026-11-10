import { generateText } from 'ai';

import { getResolvedModel } from './model.js';
import {
  getOpenCodeModel,
  OPENCODE_DEFAULT_MODEL,
  OPENCODE_DEFAULT_PROXY,
  OPENCODE_DEFAULT_TIMEOUT_MS,
  runOpenCode,
} from './opencode.js';

/**
 * News text generation driver.
 *
 * Default: local OpenCode CLI model (`opencode-go/deepseek-v4.1-flash`).
 * Fallback: the existing AI SDK profile/env resolution (`NEWS_LLM_DRIVER=profile`),
 * so the old cloud path can be restored without a code edit.
 */
export type NewsLlmDriver = 'opencode' | 'profile';

export type NewsLlmSettings = {
  driver: NewsLlmDriver;
  /** Model id recorded on responses and trusted by the compat route. */
  model: string;
  timeoutMs: number;
};

export { OPENCODE_DEFAULT_MODEL, OPENCODE_DEFAULT_PROXY, OPENCODE_DEFAULT_TIMEOUT_MS };

function positiveInt(raw: string | undefined, fallback: number): number {
  const parsed = Number((raw ?? '').trim());
  return Number.isFinite(parsed) && parsed > 0 ? Math.trunc(parsed) : fallback;
}

export function getNewsLlmSettings(): NewsLlmSettings {
  const rawDriver = (process.env.NEWS_LLM_DRIVER ?? 'opencode').trim().toLowerCase();
  const driver: NewsLlmDriver =
    rawDriver === 'profile' || rawDriver === 'cloud' || rawDriver === 'ai-sdk'
      ? 'profile'
      : 'opencode';
  return {
    driver,
    model: getOpenCodeModel(),
    timeoutMs: positiveInt(process.env.NEWS_LLM_TIMEOUT_MS, OPENCODE_DEFAULT_TIMEOUT_MS),
  };
}

/** Model id to record before a call, or null when the profile path decides it. */
export function getConfiguredNewsModelId(): string | null {
  const settings = getNewsLlmSettings();
  return settings.driver === 'opencode' ? settings.model : null;
}

/**
 * True when a generation should use the OpenCode driver.
 *
 * `/v1/chat/completions` is shared with other callers, so a request only
 * takes the OpenCode path when its `model` matches the configured news model
 * (the Python enrichment worker sends exactly that id). Other models keep the
 * old profile-backed behaviour.
 */
export function shouldUseOpenCode(requestedModel?: string | null): boolean {
  const settings = getNewsLlmSettings();
  if (settings.driver !== 'opencode') return false;
  if (!requestedModel) return true;
  return requestedModel.trim() === settings.model;
}

export type GenerateNewsTextOptions = {
  prompt: string;
  /** Model requested by the caller (OpenAI-compat route). */
  requestedModel?: string | null;
  temperature?: number;
  maxOutputTokens?: number;
  timeoutMs?: number;
};

export type GenerateNewsTextResult = {
  text: string;
  modelId: string;
};

/**
 * Generate news text with the configured driver. Thinking tags are not
 * stripped here — callers keep their existing `stripModelThinking` step.
 */
export async function generateNewsText(
  options: GenerateNewsTextOptions,
): Promise<GenerateNewsTextResult> {
  const settings = getNewsLlmSettings();

  if (shouldUseOpenCode(options.requestedModel)) {
    const text = await runOpenCode({
      prompt: options.prompt,
      model: settings.model,
      timeoutMs: options.timeoutMs ?? settings.timeoutMs,
    });
    return { text, modelId: settings.model };
  }

  const resolved = await getResolvedModel();
  const { text } = await generateText({
    model: resolved.model,
    prompt: options.prompt,
    temperature: options.temperature ?? 0,
    maxOutputTokens: options.maxOutputTokens ?? 2048,
  });
  return { text, modelId: resolved.modelId };
}
