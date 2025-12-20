import { describe, expect, it } from 'vitest';

import { ChatRequestSchema, toModelMessagesFromChatRequest } from './chat';

describe('chat request conversion', () => {
  it('converts user attachments to file parts', () => {
    const body = {
      messages: [
        { role: 'system', content: 'You are helpful.' },
        {
          role: 'user',
          content: 'What is in this image?',
          attachments: [
            {
              kind: 'image',
              name: 'test.png',
              mediaType: 'image/png',
              dataUrl: 'data:image/png;base64,AAA',
            },
          ],
        },
      ],
    };

    const parsed = ChatRequestSchema.parse(body);
    const messages = toModelMessagesFromChatRequest(parsed);

    expect(messages).toHaveLength(2);
    expect(messages[1]?.role).toBe('user');
    expect(Array.isArray(messages[1]?.content)).toBe(true);

    const parts = messages[1]?.content as Array<{ type: string }>;
    expect(parts[0]).toMatchObject({ type: 'text' });
    expect(parts[1]).toMatchObject({ type: 'file' });
  });
});


