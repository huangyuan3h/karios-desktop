import { streamText, tool } from 'ai';
import { google } from '@ai-sdk/google';
import { z } from 'zod';
import path from 'node:path';
import dotenv from 'dotenv';

dotenv.config({ path: path.resolve(process.cwd(), '.env') });
dotenv.config({ path: path.resolve(process.cwd(), '..', '..', '.env') });
process.env.HTTPS_PROXY = 'http://127.0.0.1:7890';
process.env.HTTP_PROXY = 'http://127.0.0.1:7890';

const result = streamText({
  model: google('gemini-2.5-flash'),
  system: 'You are a probe. Use the echo tool once, then answer in one short sentence what you did.',
  prompt: 'Call echo with text "probe-ok" and then tell me what happened.',
  tools: {
    echo: tool({
      description: 'echo the text',
      parameters: z.object({ text: z.string() }),
      execute: async ({ text }) => text,
    }),
  },
});
const text = await result.text;
const finish = await result.finishReason;
console.log(JSON.stringify({ finish, textLen: text.length, text: text.slice(0, 300) }));
