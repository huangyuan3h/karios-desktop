import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { WebhookPage } from './WebhookPage';

const { apiGetJson, apiPostJson } = vi.hoisted(() => ({
  apiGetJson: vi.fn(),
  apiPostJson: vi.fn(),
}));
vi.mock('@/lib/api/client', () => ({ apiGetJson, apiPostJson }));

function renderPage() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <WebhookPage />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  apiGetJson.mockReset();
  apiPostJson.mockReset();
  apiGetJson.mockImplementation(async (path: string) => {
    if (String(path).includes('/api/webhook/subscriptions')) {
      return {
        ok: true,
        items: [
          {
            id: 1,
            url: 'http://127.0.0.1:8001/hook',
            secret: 'abc',
            eventTypes: ['job_failed'],
            enabled: true,
            createdAt: '2026-08-12',
          },
        ],
      };
    }
    throw new Error(`unexpected: ${path}`);
  });
});

describe('WebhookPage', () => {
  it('lists subscriptions with event labels and delete', async () => {
    renderPage();
    expect(await screen.findByText('http://127.0.0.1:8001/hook')).toBeDefined();
    expect(screen.getAllByText('cron 失败').length).toBeGreaterThanOrEqual(1);
    fireEvent.click(screen.getByRole('button', { name: '删除' }));
    await waitFor(() =>
      expect(
        screen.getByText('http://127.0.0.1:8001/hook'),
      ).toBeDefined(),
    );
  });

  it('creates a subscription and reveals the secret once', async () => {
    apiPostJson.mockResolvedValue({
      ok: true,
      subscription: {
        id: 2,
        url: 'http://127.0.0.1:9000/hook',
        secret: 'new-secret-hex',
        eventTypes: ['job_failed', 'intraday_drawdown'],
        enabled: true,
        createdAt: '2026-08-12',
      },
    });
    renderPage();
    await screen.findByText('http://127.0.0.1:8001/hook');
    fireEvent.change(screen.getByPlaceholderText('http://127.0.0.1:8001/hook'), {
      target: { value: 'http://127.0.0.1:9000/hook' },
    });
    fireEvent.click(screen.getByRole('button', { name: '创建订阅' }));
    expect(await screen.findByText(/new-secret-hex/)).toBeDefined();
    expect(apiPostJson).toHaveBeenCalledWith('/api/webhook/subscriptions', {
      url: 'http://127.0.0.1:9000/hook',
      event_types: ['job_failed', 'intraday_drawdown'],
    });
  });

  it('sends a test event', async () => {
    apiPostJson.mockResolvedValue({ ok: true });
    renderPage();
    await screen.findByText('http://127.0.0.1:8001/hook');
    fireEvent.click(screen.getByRole('button', { name: '发送测试事件' }));
    await waitFor(() => expect(apiPostJson).toHaveBeenCalledWith('/api/webhook/test'));
  });
});
