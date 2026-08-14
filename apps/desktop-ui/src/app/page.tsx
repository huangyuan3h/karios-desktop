import { AppShell } from '@/components/layout/AppShell';
import { AuthGate } from '@/components/auth/AuthGate';
import { ChatStoreProvider } from '@/lib/chat/store';
import { installFetchAuth } from '@/lib/auth';

if (typeof window !== 'undefined') {
  installFetchAuth();
}

export default function Home() {
  return (
    <ChatStoreProvider>
      <AuthGate>
        <AppShell />
      </AuthGate>
    </ChatStoreProvider>
  );
}
