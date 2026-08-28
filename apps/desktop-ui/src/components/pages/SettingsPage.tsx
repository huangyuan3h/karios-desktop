'use client';

import * as React from 'react';

import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { ModelSettingsPanel } from '@/components/settings/ModelSettingsPanel';
import { SystemEventsPanel } from '@/components/settings/SystemEventsPanel';
import { WebhookPage } from '@/components/pages/WebhookPage';

export function SettingsPage() {
  const [tab, setTab] = React.useState<'models' | 'logs' | 'webhook'>('models');

    return (
    <div className="mx-auto w-full max-w-4xl p-6">
      <Tabs value={tab} onValueChange={(v) => setTab(v as 'models' | 'logs')}>
        <div className="mb-6">
          <TabsList>
            <TabsTrigger value="models">Models</TabsTrigger>
            <TabsTrigger value="logs">系统日志</TabsTrigger>
            <TabsTrigger value="webhook">Webhook</TabsTrigger>
          </TabsList>
        </div>

        <TabsContent value="models">
          <ModelSettingsPanel />
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
