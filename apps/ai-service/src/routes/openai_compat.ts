import { Hono } from 'hono';
import { generateText } from 'ai';

import { getResolvedModel } from '../model.js';
import { generateTextJsonObjectModeOptions } from '../model.js';
import { generateNewsText, shouldUseOpenCode } from '../news_llm.js';
import { stripJsonCodeFence, stripModelThinking } from '../model_thinking.js';

/**
 * OpenAI-compatible /v1/chat/completions endpoint.
 * Bridges data-sync-service's urllib calls to the Vercel AI SDK.
 *
 * Requests whose `model` matches the configured news model
 * (`opencode-go/deepseek-v4.1-flash` by default) are routed through the
 * bounded local OpenCode driver. Other models keep the existing profile
 * resolution so unrelated callers are unaffected.
 */

type OpenAIMessage = {
  role: 'system' | 'user' | 'assistant';
  content: string;
};

type OpenAIRequest = {
  model: string;
  messages: OpenAIMessage[];
  temperature?: number;
  max_tokens?: number;
  response_format?: { type: string };
};

type OpenAIChoice = {
  index: number;
  message: { role: string; content: string };
  finish_reason: string;
};

type OpenAIResponse = {
  id: string;
  object: string;
  created: number;
  model: string;
  choices: OpenAIChoice[];
  usage: { prompt_tokens: number; completion_tokens: number; total_tokens: number };
};

export const openaiCompatRoutes = new Hono();

function promptFromMessages(messages: OpenAIMessage[]): string {
  const system = messages
    .filter((m) => m.role === 'system')
    .map((m) => m.content)
    .join('\n\n');
  const rest = messages
    .filter((m) => m.role !== 'system')
    .map((m) => m.content)
    .join('\n\n');
  return system ? `${system}\n\n${rest}` : rest;
}

function buildResponse(
  modelId: string,
  content: string,
  usage?: OpenAIResponse['usage'],
): OpenAIResponse {
  return {
    id: `chatcmpl-${Date.now()}`,
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model: modelId,
    choices: [
      {
        index: 0,
        message: { role: 'assistant', content },
        finish_reason: 'stop',
      },
    ],
    usage: usage ?? { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
  };
}

openaiCompatRoutes.post('/chat/completions', async (c) => {
  const body = (await c.req.json().catch(() => null)) as OpenAIRequest | null;
  if (!body || !Array.isArray(body.messages) || body.messages.length === 0) {
    return c.json({ error: 'Invalid request: messages array required' }, 400);
  }

  // Convert OpenAI messages to Vercel AI SDK format
  const aiMessages = body.messages.map((m) => ({
    role: m.role as 'system' | 'user' | 'assistant',
    content: m.content,
  }));

  // Thinking models (e.g. MiniMax-M3) can spend huge tokens on internal
  // CoT before any visible answer; cap output to keep enrichment
  // (5 items per call) bounded. Default 1024 unless caller specifies.
  const maxTokens = body.max_tokens ?? 1024;

  if (shouldUseOpenCode(body.model)) {
    try {
      const result = await generateNewsText({
        prompt: promptFromMessages(aiMessages),
        requestedModel: body.model,
        temperature: body.temperature ?? 0.1,
        maxOutputTokens: maxTokens,
      });
      return c.json(
        buildResponse(result.modelId, stripJsonCodeFence(stripModelThinking(result.text))),
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : 'LLM call failed';
      console.error('OpenCode compat error:', message);
      return c.json({ error: message, model: body.model }, 500);
    }
  }

  let model;
  let looseStructuredOutputs = false;
  try {
    const resolved = await getResolvedModel();
    model = resolved.model;
    looseStructuredOutputs = resolved.looseStructuredOutputs;
  } catch (err) {
    const message = err instanceof Error ? err.message : 'AI model not configured';
    return c.json({ error: message }, 500);
  }

  try {
    const result = await generateText({
      model,
      messages: aiMessages,
      temperature: body.temperature ?? 0.1,
      maxOutputTokens: maxTokens,
      // Honor the caller's response_format (json_object) when the backend
      // supports it. Dropped by the SDK with a warning for reasoning models.
      ...(body.response_format?.type === 'json_object'
        ? generateTextJsonObjectModeOptions(looseStructuredOutputs)
        : {}),
    });

    const content = stripJsonCodeFence(stripModelThinking(result.text));
    return c.json(
      buildResponse(body.model, content, {
        prompt_tokens: result.usage.inputTokens ?? 0,
        completion_tokens: result.usage.outputTokens ?? 0,
        total_tokens: (result.usage.inputTokens ?? 0) + (result.usage.outputTokens ?? 0),
      }),
    );
  } catch (err) {
    const message = err instanceof Error ? err.message : 'LLM call failed';
    console.error('OpenAI compat error:', message);
    return c.json({ error: message }, 500);
  }
});
