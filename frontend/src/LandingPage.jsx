export default function LandingPage() {
  return (
    <div className="landing-surface">
      <div className="landing-brand">
        <div className="landing-logo">
          Bank <span>Foundry</span>
        </div>
        <p className="landing-tagline">Choose your workspace</p>
      </div>

      <div className="landing-card-grid">
        <button
          type="button"
          className="landing-surface-card merchant"
          onClick={() => { window.location.href = '/merchant'; }}
        >
          <div className="landing-card-icon">M</div>
          <div className="landing-card-copy">
            <strong>Merchant Portal</strong>
            <p>Dashboard, AI copilot, proactive inbox, and payout insights for your business.</p>
          </div>
          <div className="landing-card-arrow">→</div>
        </button>

        <button
          type="button"
          className="landing-surface-card bank"
          onClick={() => { window.location.href = '/bank'; }}
        >
          <div className="landing-card-icon">B</div>
          <div className="landing-card-copy">
            <strong>Bank Ops</strong>
            <p>Internal case queue, approvals, settlement ops, and risk workflows for your team.</p>
          </div>
          <div className="landing-card-arrow">→</div>
        </button>
      </div>

      <p className="landing-footer">Bank Foundry · Acquiring Intelligence Platform</p>
    </div>
  );
}
