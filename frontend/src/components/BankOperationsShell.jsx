export default function BankOperationsShell({
  view,
  navItems,
  merchantLabel,
  snapshot,
  merchantId,
  merchantOptions,
  lane,
  role,
  refreshMessage,
  onSelectView,
  onMerchantChange,
  onLaneChange,
  onRoleChange,
  children,
}) {
  const currentItem = navItems.find((item) => item.id === view) || {};
  const currentTitle = currentItem.label || 'Ops Console';
  const dataWindow = snapshot?.window?.from && snapshot?.window?.to
    ? `${snapshot.window.from} to ${snapshot.window.to}`
    : '';

  return (
    <div className="app-shell bank-surface-shell">
      <nav className="sidebar">
        <div className="sidebar-brand-block">
          <div className="sidebar-logo">
            Bank <span>Foundry</span>
          </div>
          <div className="sidebar-surface-note">Internal Ops</div>
          <button
            type="button"
            className="sidebar-surface-switch"
            onClick={() => { window.location.href = '/merchant'; }}
          >
            ← Merchant
          </button>
        </div>
        <div className="sidebar-meta-panel">
          <strong>{merchantLabel}</strong>
          <p>{`Lane ${lane.replace(/_/g, ' ')} · role ${role.replace(/_/g, ' ')}`}</p>
        </div>
        {navItems.map((item) => (
          <button
            key={item.id}
            className={`sidebar-btn ${view === item.id ? 'active' : ''}`}
            onClick={() => onSelectView(item.id)}
          >
            <span className="icon">{item.icon}</span>
            <span className="sidebar-btn-copy">
              <strong>{item.label}</strong>
            </span>
          </button>
        ))}
      </nav>

      <div className="main-content">
        <div className="top-bar extended">
          <div className="top-bar-copy">
            <h2>{currentTitle}</h2>
            <p className="top-subtitle">{merchantLabel}</p>
          </div>

          <div className="top-controls">
            <div className="selector-block">
              <label>Merchant</label>
              <select value={merchantId} onChange={(e) => onMerchantChange(e.target.value)}>
                {merchantOptions.map((option) => (
                  <option key={option.merchant_id} value={option.merchant_id}>
                    {option.label}
                  </option>
                ))}
              </select>
            </div>
            <div className="selector-block">
              <label>Lane</label>
              <select value={lane} onChange={(e) => onLaneChange(e.target.value)}>
                <option value="operations">Settlement Ops</option>
                <option value="support">Merchant Support</option>
                <option value="risk">Risk / Fraud</option>
              </select>
            </div>
            <div className="selector-block">
              <label>Role</label>
              <select value={role} onChange={(e) => onRoleChange(e.target.value)}>
                <option value="acquiring_ops">Acquiring Ops</option>
                <option value="support">Support</option>
                <option value="risk_fraud">Risk / Fraud</option>
                <option value="admin">Admin</option>
              </select>
            </div>
          </div>
        </div>

        <div className="scope-strip">
          <div className="scope-pill-row">
            <span className="scope-pill">{lane.replace(/_/g, ' ')}</span>
            <span className="scope-pill">{role.replace(/_/g, ' ')}</span>
            {dataWindow && <span className="scope-pill">Window {dataWindow}</span>}
          </div>
          {refreshMessage && <span className="scope-status">{refreshMessage}</span>}
        </div>

        <div className="content-area">{children}</div>
      </div>
    </div>
  );
}
