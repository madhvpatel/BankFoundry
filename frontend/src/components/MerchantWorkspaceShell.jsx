export default function MerchantWorkspaceShell({
  view,
  navItems,
  merchantLabel,
  snapshot,
  merchantId,
  merchantOptions,
  refreshMessage,
  currentScopeNote,
  onSelectView,
  onMerchantChange,
  children,
}) {
  const currentItem = navItems.find((item) => item.id === view) || {};
  const currentTitle = currentItem.label || 'Demo Workspace';
  const merchant = snapshot?.merchant_profile?.merchant || {};
  const businessLine = [merchant.nature_of_business, merchant.business_city].filter(Boolean).join(' · ');
  const coverage = snapshot?.data_coverage?.coverage_label || 'Merchant-wide view';
  const dataWindow = snapshot?.window?.from && snapshot?.window?.to
    ? `${snapshot.window.from} to ${snapshot.window.to}`
    : '';

  return (
    <div className="app-shell merchant-surface-shell">
      <nav className="sidebar">
        <div className="sidebar-brand-block">
          <div className="sidebar-logo">
            Bank <span>Foundry</span>
          </div>
          <div className="sidebar-surface-note">Merchant</div>
        </div>
        <div className="sidebar-meta-panel">
          <strong>{merchantLabel}</strong>
          {businessLine && <p>{businessLine}</p>}
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
          </div>
        </div>

        <div className="scope-strip">
          <div className="scope-pill-row">
            <span className="scope-pill">{currentScopeNote || 'Merchant'}</span>
            <span className="scope-pill">{coverage}</span>
            {dataWindow && <span className="scope-pill">Window {dataWindow}</span>}
          </div>
          <button
            type="button"
            className="sidebar-surface-switch"
            onClick={() => { window.location.href = '/bank'; }}
          >
            Bank Ops →
          </button>
          {refreshMessage && <span className="scope-status">{refreshMessage}</span>}
        </div>

        <div className="content-area">{children}</div>
      </div>
    </div>
  );
}
