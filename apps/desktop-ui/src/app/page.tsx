import { AppShell } from '@/components/layout/AppShell';
import { AuthGate } from '@/components/auth/AuthGate';
import { ChatStoreProvider } from '@/lib/chat/store';

export default function Home() {
  return (
    <ChatStoreProvider>
      <AuthGate>
        <AppShell />
      </AuthGate>
    </ChatStoreProvider>
  );
}
