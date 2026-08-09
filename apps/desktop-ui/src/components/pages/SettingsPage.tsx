'use client';

import * as React from 'react';

import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { ModelSettingsPanel } from '@/components/settings/ModelSettingsPanel';

export function SettingsPage() {
  const [tab, setTab] = React.useState<'models'>('models');

  return (
    <div className="mx-auto w-full max-w-4xl p-6">
      <Tabs value={tab} onValueChange={(v) => setTab(v as 'models')}>
        <div className="mb-6">
          <TabsList>
            <TabsTrigger value="models">Models</TabsTrigger>
          </TabsList>
        </div>

        <TabsContent value="models">
          <ModelSettingsPanel />
        </TabsContent>
      </Tabs>
    </div>
  );
}
