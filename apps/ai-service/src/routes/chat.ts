import { Hono } from 'hono';
import { streamText } from 'ai';

import { ChatRequestSchema, toModelMessagesFromChatRequest } from '../chat';
import { getResolvedModel, AiModel } from '../model';
import { ThinkingStreamStripper } from '../model_thinking';

export const chatRoutes = new Hono();

chatRoutes.post('/', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = ChatRequestSchema.safeParse(body);
  if (!parsed.success) {
    return c.json({ error: 'Invalid request body', issues: parsed.error.issues }, 400);
  }

  const messages = toModelMessagesFromChatRequest(parsed.data);

  let model: AiModel;
  try {
    model = (await getResolvedModel()).model;
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Invalid AI configuration';
    return c.json({ error: message }, 500);
  }

  // Client disconnect (abort) must cancel the upstream LLM stream — otherwise
  // the model keeps generating and billing tokens after the user navigates away.
  const abortController = new AbortController();
  const result = await streamText({
    model,
    messages,
    abortSignal: abortController.signal,
  });

  // Prefer async iteration over ReadableStream.getReader — AI SDK exposes a dual-type stream.
  const stripper = new ThinkingStreamStripper();
  const cleaned = new ReadableStream<string>({
    start(controller) {
      const pump = async () => {
        try {
          for await (const chunk of result.textStream) {
            const visible = stripper.push(chunk);
            if (visible) controller.enqueue(visible);
          }
          const rest = stripper.flush();
          if (rest) controller.enqueue(rest);
          controller.close();
        } catch (err) {
          controller.error(err);
        }
      };
      void pump();
    },
    cancel() {
      // Client disconnected or timed out — stop the upstream generation.
      abortController.abort();
    },
  });

  return new Response(cleaned.pipeThrough(new TextEncoderStream()), {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' },
  });
});
