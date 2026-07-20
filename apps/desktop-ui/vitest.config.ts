import path from 'node:path';

import { defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
      // Resolve workspace package from source so tests work before `pnpm build`.
      '@karios/shared': path.resolve(__dirname, '../../packages/shared/src/index.ts'),
    },
  },
  test: {
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      include: ['src/lib/**/*.{ts,tsx}'],
      exclude: [
        'src/lib/**/*.test.{ts,tsx}',
        'src/lib/**/*.spec.{ts,tsx}',
        'src/lib/chat/types.ts',
        'src/lib/chat/store.ts',
      ],
      thresholds: {
        lines: 60,
        functions: 60,
        branches: 60,
        statements: 60,
      },
    },
  },
});
