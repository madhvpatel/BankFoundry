import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  askAgent,
  fetchDashboardMetrics,
  fetchMerchantOptions,
  fetchMerchantSnapshot,
  refreshProactive,
  uploadDisputeReceipt,
} from './api';
import DashboardView from './components/DashboardView';
import ChatView from './components/ChatView';
import ProactiveView from './components/ProactiveView';
import MerchantWorkspaceShell from './components/MerchantWorkspaceShell';

const VIEWS = {
  DASHBOARD: 'dashboard',
  CHAT: 'chat',
  INBOX: 'inbox',
};

const MERCHANT_NAV = [
  { id: VIEWS.DASHBOARD, icon: 'OV', label: 'Dashboard', description: 'Overview', eyebrow: 'Overview' },
  { id: VIEWS.CHAT,      icon: 'AI', label: 'Copilot',   description: 'Copilot',  eyebrow: 'Copilot'  },
  { id: VIEWS.INBOX,     icon: 'IN', label: 'Inbox',     description: 'Signals',  eyebrow: 'Signals'  },
];

function newThreadScope() {
  return `thread_${Date.now().toString(36)}`;
}

function initialView() {
  const params = new URLSearchParams(window.location.search);
  const v = params.get('view');
  if (v === VIEWS.CHAT)  return VIEWS.CHAT;
  if (v === VIEWS.INBOX) return VIEWS.INBOX;
  return VIEWS.DASHBOARD;
}

export default function MerchantApp() {
  const [view, setView]                   = useState(initialView);
  const [messages, setMessages]           = useState([]);
  const [chatMemory, setChatMemory]       = useState(null);
  const [chatThreadScope, setChatThreadScope] = useState('default');
  const [chatLoading, setChatLoading]     = useState(false);
  const [merchantOptions, setMerchantOptions] = useState([]);
  const [merchantId, setMerchantId]       = useState('');
  const [snapshot, setSnapshot]           = useState(null);
  const [snapshotLoading, setSnapshotLoading] = useState(true);
  const [dashboardData, setDashboardData] = useState(null);
  const [dashboardLoading, setDashboardLoading] = useState(true);
  const [refreshMessage, setRefreshMessage] = useState('');

  const merchantLabel = useMemo(() => {
    const selected = merchantOptions.find((m) => m.merchant_id === merchantId);
    return selected?.label || snapshot?.merchant_label || merchantId || 'Merchant';
  }, [merchantId, merchantOptions, snapshot]);

  // Sync view to URL
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    params.set('view', view);
    window.history.replaceState({}, '', `${window.location.pathname}?${params.toString()}`);
  }, [view]);

  const loadSnapshot = useCallback(async (id = merchantId) => {
    if (!id) return;
    setSnapshotLoading(true);
    try {
      const data = await fetchMerchantSnapshot({ merchantId: id });
      setSnapshot(data.snapshot || null);
    } catch (e) {
      console.error('Snapshot error:', e);
    } finally {
      setSnapshotLoading(false);
    }
  }, [merchantId]);

  const loadDashboard = useCallback(async (id = merchantId) => {
    if (!id) return;
    setDashboardLoading(true);
    try {
      const data = await fetchDashboardMetrics({ merchantId: id });
      setDashboardData(data || null);
    } catch (e) {
      console.error('Dashboard error:', e);
      setDashboardData(null);
    } finally {
      setDashboardLoading(false);
    }
  }, [merchantId]);

  useEffect(() => {
    fetchMerchantOptions()
      .then((data) => {
        const merchants = data.merchants || [];
        setMerchantOptions(merchants);
        const initial = data.default_merchant_id || merchants[0]?.merchant_id || '';
        setMerchantId(initial);
      })
      .catch((e) => console.error('Merchant options error:', e));
  }, []);

  useEffect(() => {
    if (!merchantId) return;
    loadSnapshot(merchantId);
    loadDashboard(merchantId);
  }, [merchantId, loadSnapshot, loadDashboard]);

  useEffect(() => {
    if (!merchantId) return;
    setMessages([]);
    setChatMemory(null);
    setChatThreadScope('default');
    setRefreshMessage('');
  }, [merchantId]);

  const handleSend = async (prompt) => {
    setMessages((prev) => [...prev, { role: 'user', text: prompt }]);
    setChatLoading(true);
    try {
      const history = messages.slice(-6).map((m) => ({ role: m.role, text: m.text }));
      const response = await askAgent({ merchantId, prompt, threadScope: chatThreadScope, history });
      setChatMemory(response.memory || null);
      setChatThreadScope(response.thread_scope || chatThreadScope);
      setMessages((prev) => [
        ...prev,
        {
          role: 'assistant',
          text: response.answer || "I couldn't find an answer. Try rephrasing?",
          sessionKey: response.session_key || '',
          threadScope: response.thread_scope || chatThreadScope,
          memory: response.memory || null,
          verificationStatus: response.verification_status || '',
          verificationSummary: response.verification_summary || '',
          validationStatus: response.validation_status || 'clean',
          validationIssues: response.validation_issues || [],
          displayNotice: response.display_notice || null,
          answerSections: response.answer_sections || {},
          sources: response.sources || [],
          structuredResult: response.structured_result || null,
          followUps: response.follow_ups || [],
          actionPreview: response.action_preview || null,
          clarifyingQuestion: response.clarifying_question || null,
          answerSource: response.answer_source || 'engine',
          trace: response.trace || null,
          debug: response.debug || null,
          scope: response.scope || null,
          intent: response.intent || 'business_overview',
          prompt,
        },
      ]);
    } catch (e) {
      setMessages((prev) => [...prev, { role: 'assistant', text: `Error: ${e.message}` }]);
    } finally {
      setChatLoading(false);
    }
  };

  const handleRefreshProactive = async () => {
    if (!merchantId) return;
    try {
      const result = await refreshProactive({ merchantId, force: true });
      const status = result.refresh_status || {};
      setRefreshMessage(
        status.refreshed
          ? `Refreshed ${status.generated_count || 0} inbox item(s).`
          : `Refresh skipped: ${status.reason || 'unknown'}.`,
      );
      await loadSnapshot(merchantId);
    } catch (e) {
      setRefreshMessage(`Refresh failed: ${e.message}`);
    }
  };

  const handleNewThread = useCallback(() => {
    setMessages([]);
    setChatMemory(null);
    setChatThreadScope(newThreadScope());
  }, []);

  const currentCards = useMemo(() => snapshot?.proactive_cards || [], [snapshot]);

  const renderView = () => {
    switch (view) {
      case VIEWS.DASHBOARD:
        return (
          <DashboardView
            data={dashboardData}
            loading={dashboardLoading}
            snapshot={snapshot}
            merchantLabel={merchantLabel}
          />
        );
      case VIEWS.CHAT:
        return (
          <ChatView
            messages={messages}
            loading={chatLoading}
            onSend={handleSend}
            onNewThread={handleNewThread}
            merchantLabel={merchantLabel}
            terminalId=""
            threadScope={chatThreadScope}
            activeMemory={chatMemory}
            onUploadDisputeReceipt={async (file, context) => {
              if (!merchantId) throw new Error('No merchant selected');
              return uploadDisputeReceipt({ merchantId, file, context });
            }}
          />
        );
      case VIEWS.INBOX:
        return (
          <ProactiveView
            cards={currentCards}
            loading={snapshotLoading}
            merchantId={merchantId}
            onChanged={() => loadSnapshot()}
            onRefresh={handleRefreshProactive}
            refreshStatus={snapshot?.refresh_status}
          />
        );
      default:
        return null;
    }
  };

  return (
    <MerchantWorkspaceShell
      view={view}
      navItems={MERCHANT_NAV}
      merchantLabel={merchantLabel}
      snapshot={snapshot}
      merchantId={merchantId}
      merchantOptions={merchantOptions}
      refreshMessage={refreshMessage}
      currentScopeNote="Merchant"
      onSelectView={setView}
      onMerchantChange={setMerchantId}
    >
      {renderView()}
    </MerchantWorkspaceShell>
  );
}
