import dotenv from 'dotenv';
import { existsSync } from 'node:fs';
import path from 'node:path';
dotenv.config();
const rootEnv = path.resolve(process.cwd(), '..', '..', '.env');
if (existsSync(rootEnv)) dotenv.config({ path: rootEnv });
import { getDecisionModelBundle } from './src/model';
import { streamText, tool } from 'ai';
import { z } from 'zod';

const t0 = Date.now();
(async () => {
  const bundle = await getDecisionModelBundle();
  const healthTool = tool({ description: '决策体检', inputSchema: z.object({}), execute: async () => 'regime=Weak' });
  const result = await streamText({ model: bundle.model, messages: [{ role: 'user', content: '市场现在什么状态？' }], tools: { query_s3_holdings_health: healthTool }, maxSteps: 5 });
  let n = 0;
  for await (const c of result.textStream) n += c.length;
  console.log('chars:', n, 'after', Date.now() - t0, 'ms');
  const finish = await Promise.resolve(result.finishReason);
  console.log('finishReason:', finish);
  try {
    const steps = await Promise.resolve(result.steps);
    console.log('steps type:', typeof steps, Array.isArray(steps) ? steps.length : '');
    if (Array.isArray(steps)) {
      for (let i = 0; i < steps.length; i++) {
        const s: any = steps[i];
        console.log(' step', i, 'text:', JSON.stringify(String(s.text ?? '').slice(0, 80)), 'toolCalls:', JSON.stringify((s.toolCalls ?? []).map((t: any) => t.toolName)));
      }
    }
  } catch (e) { console.log('steps err:', (e as Error).message.slice(0, 120)); }
})().catch((e: unknown) => {
  const err = e as Error;
  console.log('ERROR', Date.now() - t0, 'ms:', err.message.slice(0, 300));
});
