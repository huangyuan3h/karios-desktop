'use client';

import * as React from 'react';

import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { AccountSettingsPanel } from '@/components/settings/AccountSettingsPanel';
import { ModelSettingsPanel } from '@/components/settings/ModelSettingsPanel';
import { SystemEventsPanel } from '@/components/settings/SystemEventsPanel';
import { StrategySettingsPanel } from '@/components/settings/StrategySettingsPanel';
import { WebhookPage } from '@/components/pages/WebhookPage';

export function SettingsPage() {
  const [tab, setTab] = React.useState<'models' | 'strategy' | 'account' | 'logs' | 'webhook'>(
    'models',
  );

  return (
    <div className="mx-auto w-full max-w-4xl p-6">
      <Tabs
        value={tab}
        onValueChange={(v) => setTab(v as 'models' | 'strategy' | 'account' | 'logs' | 'webhook')}
      >
        <div className="mb-6">
          <TabsList>
            <TabsTrigger value="models">Models</TabsTrigger>
            <TabsTrigger value="strategy">策略</TabsTrigger>
            <TabsTrigger value="account">账户</TabsTrigger>
            <TabsTrigger value="logs">系统日志</TabsTrigger>
            <TabsTrigger value="webhook">Webhook</TabsTrigger>
          </TabsList>
        </div>

        <TabsContent value="models">
          <ModelSettingsPanel />
        </TabsContent>
        <TabsContent value="strategy">
          <StrategySettingsPanel />
        </TabsContent>
        <TabsContent value="account">
          <AccountSettingsPanel />
        </TabsContent>
        <TabsContent value="logs">
          <SystemEventsPanel />
        </TabsContent>
        <TabsContent value="webhook">
          <WebhookPage />
        </TabsContent>
      </Tabs>
    </div>
  );
}
