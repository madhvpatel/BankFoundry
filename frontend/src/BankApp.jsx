import { useCallback, useEffect, useMemo, useState } from 'react';
import { fetchMerchantOptions, fetchMerchantSnapshot, promoteOpsCase } from './api';
import OpsConsoleView from './components/OpsConsoleView';
import BankOperationsShell from './components/BankOperationsShell';

const VIEWS = {
  OPS_CONSOLE: 'ops_console',
};

const BANK_NAV = [
  { id: VIEWS.OPS_CONSOLE, icon: 'OP', label: 'Ops Console', description: 'Queue', eyebrow: 'Operations' },
];

export default function BankApp() {
  const [view, setView]                     = useState(VIEWS.OPS_CONSOLE);
  const [merchantOptions, setMerchantOptions] = useState([]);
  const [merchantId, setMerchantId]           = useState('');
  const [snapshot, setSnapshot]               = useState(null);
  const [bankLane, setBankLane]               = useState('operations');
  const [bankRole, setBankRole]               = useState('acquiring_ops');
  const [bankRefreshSeed, setBankRefreshSeed] = useState(0);
  const [promotionBusy, setPromotionBusy]     = useState('');
  const [refreshMessage, setRefreshMessage]   = useState('');

  const merchantLabel = useMemo(() => {
    const selected = merchantOptions.find((m) => m.merchant_id === merchantId);
    return selected?.label || snapshot?.merchant_label || merchantId || 'Merchant';
  }, [merchantId, merchantOptions, snapshot]);

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
    fetchMerchantSnapshot({ merchantId })
      .then((data) => setSnapshot(data.snapshot || null))
      .catch((e) => console.error('Snapshot error:', e));
  }, [merchantId]);

  const handlePromoteCase = useCallback(async ({ sourceType, sourceRef, sourcePayload }) => {
    if (!merchantId) return;
    const key = `${sourceType}:${sourceRef || 'payload'}`;
    setPromotionBusy(key);
    try {
      await promoteOpsCase({ merchantId, lane: bankLane, role: bankRole, sourceType, sourceRef, sourcePayload });
      setRefreshMessage('Moved into Ops Console.');
      setView(VIEWS.OPS_CONSOLE);
      setBankRefreshSeed((v) => v + 1);
    } catch (e) {
      setRefreshMessage(`Case promotion failed: ${e.message}`);
    } finally {
      setPromotionBusy('');
    }
  }, [merchantId, bankLane, bankRole]);

  const renderView = () => {
    switch (view) {
      case VIEWS.OPS_CONSOLE:
        return (
          <OpsConsoleView
            merchantId={merchantId}
            merchantLabel={merchantLabel}
            terminalId=""
            lane={bankLane}
            role={bankRole}
            refreshSeed={bankRefreshSeed}
          />
        );
      default:
        return null;
    }
  };

  return (
    <BankOperationsShell
      view={view}
      navItems={BANK_NAV}
      merchantLabel={merchantLabel}
      snapshot={snapshot}
      merchantId={merchantId}
      merchantOptions={merchantOptions}
      lane={bankLane}
      role={bankRole}
      refreshMessage={refreshMessage}
      onSelectView={setView}
      onMerchantChange={setMerchantId}
      onLaneChange={setBankLane}
      onRoleChange={setBankRole}
    >
      {renderView()}
    </BankOperationsShell>
  );
}
