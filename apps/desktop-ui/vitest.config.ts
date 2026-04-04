import { defineConfig } from 'vitest/config';

export default defineConfig({
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
        lines: 50,
        functions: 50,
        branches: 50,
        statements: 50,
      },
    },
  },
});
