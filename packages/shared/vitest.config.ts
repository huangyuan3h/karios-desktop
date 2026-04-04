import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      include: ['src/schemas/**/*.ts'],
      exclude: ['src/schemas/**/*.test.ts'],
      thresholds: {
        lines: 50,
        functions: 50,
        branches: 50,
        statements: 50,
        perFile: true,
      },
    },
  },
});
