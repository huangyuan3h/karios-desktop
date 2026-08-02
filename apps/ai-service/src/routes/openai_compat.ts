import { Hono } from 'hono';
import { generateText } from 'ai';

import { getResolvedModel } from '../model';
import { stripJsonCodeFence, stripModelThinking } from '../model_thinking';

/**
 * OpenAI-compatible /v1/chat/completions endpoint.
 * Bridges data-sync-service's urllib calls to the Vercel AI SDK.
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

openaiCompatRoutes.post('/chat/completions', async (c) => {
  const body = (await c.req.json().catch(() => null)) as OpenAIRequest | null;
  if (!body || !Array.isArray(body.messages) || body.messages.length === 0) {
    return c.json({ error: 'Invalid request: messages array required' }, 400);
  }

  let model;
  try {
    const resolved = await getResolvedModel();
    model = resolved.model;
  } catch (err) {
    const message = err instanceof Error ? err.message : 'AI model not configured';
    return c.json({ error: message }, 500);
  }

  // Convert OpenAI messages to Vercel AI SDK format
  const aiMessages = body.messages.map((m) => ({
    role: m.role as 'system' | 'user' | 'assistant',
    content: m.content,
  }));

  try {
    // Thinking models (e.g. MiniMax-M3) can spend huge tokens on internal
    // CoT before any visible answer; cap output to keep enrichment
    // (5 items per call) bounded. Default 1024 unless caller specifies.
    const maxTokens = body.max_tokens ?? 1024;
    const result = await generateText({
      model,
      messages: aiMessages,
      temperature: body.temperature ?? 0.1,
      maxOutputTokens: maxTokens,
    });

    const content = stripJsonCodeFence(stripModelThinking(result.text));

    const response: OpenAIResponse = {
      id: `chatcmpl-${Date.now()}`,
      object: 'chat.completion',
      created: Math.floor(Date.now() / 1000),
      model: body.model,
      choices: [
        {
          index: 0,
          message: { role: 'assistant', content },
          finish_reason: 'stop',
        },
      ],
      usage: {
        prompt_tokens: result.usage.inputTokens ?? 0,
        completion_tokens: result.usage.outputTokens ?? 0,
        total_tokens: (result.usage.inputTokens ?? 0) + (result.usage.outputTokens ?? 0),
      },
    };

    return c.json(response);
  } catch (err) {
    const message = err instanceof Error ? err.message : 'LLM call failed';
    console.error('OpenAI compat error:', message);
    return c.json({ error: message }, 500);
  }
});
