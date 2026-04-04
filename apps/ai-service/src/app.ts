import { cors } from 'hono/cors';
import { Hono } from 'hono';

import { configRoutes } from './routes/config';
import { strategyRoutes } from './routes/strategy';
import { leaderRoutes } from './routes/leader';
import { mainlineRoutes } from './routes/mainline';
import { quantRoutes } from './routes/quant';
import { titleRoutes } from './routes/title';
import { brokerRoutes } from './routes/broker';
import { newsRoutes } from './routes/news';
import { chatRoutes } from './routes/chat';

export const app = new Hono();
app.use('*', cors());

app.onError((err, c) => {
  console.error('AI service error:', err);
  const message = err instanceof Error ? err.message : String(err);
  const stack =
    process.env.NODE_ENV !== 'production' && err instanceof Error ? (err.stack ?? null) : null;
  return c.json({ error: 'Internal server error', message, stack }, 500);
});

app.get('/healthz', (c) => c.json({ ok: true }));

app.route('/config', configRoutes);
app.route('/strategy', strategyRoutes);
app.route('/leader', leaderRoutes);
app.route('/mainline', mainlineRoutes);
app.route('/quant', quantRoutes);
app.route('/title', titleRoutes);
app.route('/extract/broker', brokerRoutes);
app.route('/news', newsRoutes);
app.route('/chat', chatRoutes);

export default app;
