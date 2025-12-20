import { z } from 'zod';
import type { ModelMessage } from 'ai';

const AttachmentSchema = z.object({
  kind: z.literal('image'),
  dataUrl: z.string().min(1),
  mediaType: z.string().min(1),
  name: z.string().min(1).optional(),
});

export const ChatRequestSchema = z.object({
  messages: z.array(
    z.object({
      role: z.enum(['system', 'user', 'assistant']),
      content: z.string(),
      attachments: z.array(AttachmentSchema).optional(),
    }),
  ),
});

export type ChatRequest = z.infer<typeof ChatRequestSchema>;

function toModelMessage(msg: ChatRequest['messages'][number]): ModelMessage {
  if (msg.role === 'user' && msg.attachments?.length) {
    return {
      role: 'user',
      content: [
        { type: 'text', text: msg.content },
        ...msg.attachments.map((a) => ({
          type: 'file' as const,
          data: a.dataUrl,
          mediaType: a.mediaType,
          filename: a.name,
        })),
      ],
    };
  }
  return { role: msg.role, content: msg.content };
}

export function toModelMessagesFromChatRequest(req: ChatRequest): ModelMessage[] {
  return req.messages.map(toModelMessage);
}


