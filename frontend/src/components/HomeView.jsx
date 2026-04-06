import { formatCompactCurrency, formatPercent } from '../utils';
import DataTable from './DataTable';
import TaskList from './TaskList';

export default function HomeView({ snapshot, merchantId, onChanged }) {
  if (!snapshot) {
    return (
      <div className="empty-state">
        <div className="icon">🏪</div>
        <h3>No merchant snapshot loaded</h3>
      </div>
    );
  }

  const summary = snapshot.summary || {};
  const merchant = snapshot.merchant_profile?.merchant || {};
  const modeRows = snapshot.kpi_by_mode || [];
  const proactiveCards = snapshot.proactive_cards || [];

  return (
    <div className="view-stack">
      <div className="glass-card hero-card">
        <div>
          <div className="eyebrow">Merchant</div>
          <h2>{merchant.merchant_trade_name || merchantId}</h2>
          <p>
            {merchant.nature_of_business || 'Payments merchant'}{merchant.business_city ? ` · ${merchant.business_city}` : ''}
          </p>
        </div>
        <div className="hero-tags">
          <span className="badge neutral">Risk {merchant.merchant_risk_category || 'Unknown'}</span>
          <span className="badge neutral">Status {merchant.merchant_status || 'Unknown'}</span>
          <span className="badge neutral">Coverage {snapshot.data_coverage?.coverage_label || 'Payments only'}</span>
        </div>
      </div>

      <div className="kpi-grid">
        <div className="glass-card kpi-card accent">
          <span className="kpi-label">Attempts</span>
          <span className="kpi-value">{Number(summary.attempts || 0).toLocaleString('en-IN')}</span>
        </div>
        <div className="glass-card kpi-card success">
          <span className="kpi-label">Success Rate</span>
          <span className="kpi-value">{formatPercent(summary.success_rate_pct)}</span>
        </div>
        <div className="glass-card kpi-card danger">
          <span className="kpi-label">Failed Txns</span>
          <span className="kpi-value">{Number(summary.fail_txns || 0).toLocaleString('en-IN')}</span>
        </div>
        <div className="glass-card kpi-card info">
          <span className="kpi-label">Success GMV</span>
          <span className="kpi-value">{formatCompactCurrency(summary.success_gmv)}</span>
        </div>
      </div>

      <div className="two-col-grid">
        <DataTable
          title="Payment mode KPIs"
          rows={modeRows}
          emptyText="No payment-mode KPI rows are available."
        />
        <DataTable
          title="Response-code failure drivers"
          rows={snapshot.failure_drivers?.response_code?.rows || []}
          emptyText="No verified failure-driver rows are available."
        />
      </div>

      <TaskList
        merchantId={merchantId}
        title="Growth queue"
        tasks={snapshot.growth_tasks || []}
        emptyText="No growth tasks were generated for this merchant."
        onChanged={onChanged}
      />

      <DataTable
        title="Proactive inbox snapshot"
        rows={proactiveCards.map((card) => ({
          lane: card.lane,
          title: card.title,
          type: card.type,
          confidence: card.confidence,
          impact_rupees: card.impact_rupees,
          state: card.card_state,
        }))}
        emptyText="No proactive cards are available."
      />
    </div>
  );
}
