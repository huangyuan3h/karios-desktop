import { generateText } from 'ai';
import { createOpenAI, openai } from '@ai-sdk/openai';
import { createGoogleGenerativeAI, google } from '@ai-sdk/google';
import { fetch as undiciFetch, EnvHttpProxyAgent } from 'undici';
import { z } from 'zod';

import { AiConfigStoreSchema, AiProfileSchema, loadConfigStore } from './config';
import { asTrimmedString } from './utils';

/**
 * Hard cap for any single upstream AI request. ai v5 has no `timeout` call
 * option, so we guard at the fetch layer: a stalled provider connection
 * (no headers, no chunk, no close) must not hold the request forever.
 * Long-but-healthy streams are unaffected while chunks keep flowing.
 */
export const AI_FETCH_TIMEOUT_MS = 600_000;

function withFetchTimeout(
  inner: typeof fetch,
  timeoutMs: number = AI_FETCH_TIMEOUT_MS,
): typeof fetch {
  return (input, init) => {
    const timer = AbortSignal.timeout(timeoutMs);
    const signal = init?.signal ? AbortSignal.any([init.signal, timer]) : timer;
    return inner(input, { ...init, signal });
  };
}

const fetchWithTimeout = withFetchTimeout(globalThis.fetch);

type AiModel = Parameters<typeof generateText>[0]['model'];

export type { AiModel };

/** Resolved model plus flags for OpenAI-compatible backends that are not api.openai.com. */
export type ResolvedModelBundle = {
  model: AiModel;
  provider: string;
  modelId: string;
  /**
   * When true, pass `providerOptions.openai.structuredOutputs: false` to `generateObject` so the
   * SDK uses `response_format: { type: "json_object" }` instead of `json_schema`. Ollama and many
   * local gateways return: "This response_format type is unavailable now" for json_schema.
   */
  looseStructuredOutputs: boolean;
};

/**
 * Extra options for `generateObject` on OpenAI-compatible servers that do not support json_schema.
 */
export function generateObjectCompatOptions(looseStructuredOutputs: boolean): {
  providerOptions: { openai: { structuredOutputs: false } };
} | Record<string, never> {
  if (!looseStructuredOutputs) return {};
  return { providerOptions: { openai: { structuredOutputs: false as const } } };
}

/**
 * Hint OpenAI-compatible APIs to return JSON for `generateText` fallbacks when `json_schema` is unavailable.
 */
export function generateTextJsonObjectModeOptions(looseStructuredOutputs: boolean): {
  providerOptions: { openai: { responseFormat: { type: 'json_object' } } };
} | Record<string, never> {
  if (!looseStructuredOutputs) return {};
  return {
    providerOptions: { openai: { responseFormat: { type: 'json_object' as const } } },
  };
}

/**
 * OpenAI-compatible servers (Ollama /v1, LM Studio, etc.) often reject the `developer`
 * role that @ai-sdk/openai emits for "reasoning-style" model IDs — which includes any
 * model id not matching gpt-3*, gpt-4*, chatgpt-4o, or gpt-5-chat (e.g. llama3, qwen).
 * Rewrite to `system` before the request leaves the process.
 *
 * MiniMax-M3 embeds chain-of-thought in `content` as <think>…</think> unless
 * `reasoning_split: true` is set (then thinking goes to reasoning_content).
 */
export function rewriteOpenAiCompatibleRequestBody(
  body: string,
  opts?: { baseURL?: string },
): string {
  try {
    const parsed = JSON.parse(body) as Record<string, unknown>;
    let changed = false;

    const messages = parsed.messages;
    if (Array.isArray(messages)) {
      const next = messages.map((m: unknown) => {
        if (
          m !== null &&
          typeof m === 'object' &&
          'role' in m &&
          (m as { role: string }).role === 'developer'
        ) {
          changed = true;
          return { ...(m as Record<string, unknown>), role: 'system' };
        }
        return m;
      });
      if (changed) parsed.messages = next;
    }

    if (shouldEnableMiniMaxReasoningSplit(parsed, opts?.baseURL) && parsed.reasoning_split !== true) {
      parsed.reasoning_split = true;
      changed = true;
    }

    return changed ? JSON.stringify(parsed) : body;
  } catch {
    return body;
  }
}

/** @deprecated Use rewriteOpenAiCompatibleRequestBody */
export function rewriteDeveloperMessageRolesInJsonString(body: string): string {
  return rewriteOpenAiCompatibleRequestBody(body);
}

function shouldEnableMiniMaxReasoningSplit(
  parsed: Record<string, unknown>,
  baseURL?: string,
): boolean {
  const model = typeof parsed.model === 'string' ? parsed.model : '';
  if (/minimax/i.test(model)) return true;
  if (baseURL && /minimaxi?\.com/i.test(baseURL)) return true;
  return false;
}

function openAiCompatibleFetch(
  baseURL: string | undefined,
  innerFetch: typeof fetch = globalThis.fetch,
): typeof fetch {
  return async (input, init) => {
    if (!init?.body || typeof init.body !== 'string') {
      return innerFetch(input, init);
    }
    const body = rewriteOpenAiCompatibleRequestBody(init.body, { baseURL });
    if (body === init.body) {
      return innerFetch(input, init);
    }
    return innerFetch(input, { ...init, body });
  };
}

export function pickActiveProfile(
  store: z.infer<typeof AiConfigStoreSchema>,
): z.infer<typeof AiProfileSchema> | null {
  const id = store.activeProfileId;
  if (!id) return null;
  return store.profiles.find((p) => p.id === id) ?? null;
}

export function modelFromProfile(p: z.infer<typeof AiProfileSchema>): ResolvedModelBundle {
  if (p.provider === 'google') {
    const key = p.google?.apiKey?.trim();
    return {
      model: createGoogleGenerativeAI({
        apiKey: key,
        fetch: withFetchTimeout(getGeminiFetch()),
      }).languageModel(p.modelId),
      provider: 'google',
      modelId: p.modelId,
      looseStructuredOutputs: false,
    };
  }

  if (p.provider === 'ollama') {
    const baseURL = p.ollama?.baseUrl?.trim() || 'http://127.0.0.1:11434/v1';
    const apiKey = p.ollama?.apiKey?.trim() || 'ollama';
    const ollamaClient = createOpenAI({
      apiKey,
      baseURL,
      fetch: withFetchTimeout(openAiCompatibleFetch(baseURL)),
    });
    return {
      model: ollamaClient.chat(p.modelId),
      provider: 'ollama',
      modelId: p.modelId,
      looseStructuredOutputs: true,
    };
  }

  const apiKey = p.openai?.apiKey?.trim() || '';
  const baseURL = p.openai?.baseUrl?.trim() || undefined;
  const openaiClient =
    apiKey || baseURL
      ? createOpenAI({
          apiKey,
          baseURL,
          ...(baseURL ? { fetch: withFetchTimeout(openAiCompatibleFetch(baseURL)) } : {}),
        })
      : createOpenAI({ fetch: fetchWithTimeout });
  return {
    model: openaiClient.chat(p.modelId),
    provider: 'openai',
    modelId: p.modelId,
    looseStructuredOutputs: Boolean(baseURL),
  };
}

export async function getResolvedModel(): Promise<ResolvedModelBundle> {
  const provider = asTrimmedString(process.env.AI_PROVIDER).toLowerCase() || 'openai';
  const envModelId = asTrimmedString(process.env.AI_MODEL);

  const store = await loadConfigStore();
  const active = store ? pickActiveProfile(store) : null;

  if (!active) {
    if (!envModelId) throw new Error('Missing AI_MODEL');
    if (provider === 'google') {
      return {
        model: google(envModelId),
        modelId: envModelId,
        provider,
        looseStructuredOutputs: false,
      };
    }
    const envOpenAiBase = asTrimmedString(process.env.OPENAI_BASE_URL);
    return {
      model: openai.chat(envModelId),
      modelId: envModelId,
      provider,
      looseStructuredOutputs: Boolean(envOpenAiBase),
    };
  }

  return modelFromProfile(active);
}

export function getStrategyFallbackModelId(): string | null {
  const id = (process.env.AI_STRATEGY_FALLBACK_MODEL ?? '').trim();
  return id || null;
}

export const DECISION_DEFAULT_MODEL_ID = 'gemini-3.6-flash';

let geminiFetch: typeof fetch | null = null;

/**
 * Gemini calls go through the HTTP(S) proxy from env (https_proxy/http_proxy),
 * since the API is not directly reachable from some networks. Falls back to the
 * global fetch when no proxy env var is configured; no_proxy is respected.
 */
function getGeminiFetch(): typeof fetch {
  if (geminiFetch) return geminiFetch;
  const hasProxy =
    (process.env.https_proxy ??
      process.env.HTTPS_PROXY ??
      process.env.http_proxy ??
      process.env.HTTP_PROXY ??
      '').trim().length > 0;
  if (!hasProxy) {
    geminiFetch = globalThis.fetch;
    return geminiFetch;
  }
  const agent = new EnvHttpProxyAgent();
  geminiFetch = (input: Parameters<typeof fetch>[0], init?: RequestInit) =>
    undiciFetch(input as never, { ...init, dispatcher: agent } as never) as Promise<Response>;
  return geminiFetch;
}

/**
 * Decision Agent model resolver: use Gemini when GEMINI_API_KEY is configured,
 * otherwise fall back to the normal profile/env resolution.
 */
export async function getDecisionModelBundle(): Promise<ResolvedModelBundle> {
  const key = asTrimmedString(process.env.GEMINI_API_KEY);
  if (!key) {
    return getResolvedModel();
  }
  const modelId = asTrimmedString(process.env.AI_DECISION_MODEL) || DECISION_DEFAULT_MODEL_ID;
  const geminiClient = createGoogleGenerativeAI({
    apiKey: key,
    fetch: withFetchTimeout(getGeminiFetch()),
  });
  return {
    model: geminiClient.languageModel(modelId),
    provider: 'google',
    modelId,
    looseStructuredOutputs: false,
  };
}

export async function getStrategyPrimaryAndFallbackModels(): Promise<{
  model: AiModel;
  modelId: string;
  fallbackModel: AiModel | null;
  fallbackModelId: string | null;
  looseStructuredOutputs: boolean;
}> {
  const store = await loadConfigStore();
  const primary = await getResolvedModel();

  if (store && store.activeProfileId) {
    return {
      model: primary.model,
      modelId: primary.modelId,
      fallbackModel: null,
      fallbackModelId: null,
      looseStructuredOutputs: primary.looseStructuredOutputs,
    };
  }

  const fbId = getStrategyFallbackModelId();
  if (!fbId) {
    return {
      model: primary.model,
      modelId: primary.modelId,
      fallbackModel: null,
      fallbackModelId: null,
      looseStructuredOutputs: primary.looseStructuredOutputs,
    };
  }

  const provider = asTrimmedString(process.env.AI_PROVIDER).toLowerCase() || 'openai';
  const fb = provider === 'google' ? google(fbId) : openai.chat(fbId);
  return {
    model: primary.model,
    modelId: primary.modelId,
    fallbackModel: fb,
    fallbackModelId: fbId,
    looseStructuredOutputs: primary.looseStructuredOutputs,
  };
}
