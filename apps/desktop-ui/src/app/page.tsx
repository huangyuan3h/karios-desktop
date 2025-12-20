import { AppShell } from '@/components/layout/AppShell';
import { ChatStoreProvider } from '@/lib/chat/store';

export default function Home() {
  return (
    <ChatStoreProvider>
      <AppShell />
    </ChatStoreProvider>
  );
}
