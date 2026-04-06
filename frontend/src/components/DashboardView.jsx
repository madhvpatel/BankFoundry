import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer } from 'recharts';
import { formatCompactCurrency, formatCurrency, formatPercent } from '../utils';
import DataTable from './DataTable';

const COLORS = ['#1237c9', '#039669', '#f57c40', '#cf4f4f', '#7b6cff'];

function formatRupeesShort(value) {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount)) return 'Rs 0';

  if (Math.abs(amount) >= 10000000) {
    return `Rs ${(amount / 10000000).toFixed(1)}Cr`;
  }

  if (Math.abs(amount) >= 100000) {
    return `Rs ${(amount / 100000).toFixed(1)}L`;
  }

  return `Rs ${amount.toLocaleString('en-IN', { maximumFractionDigits: 0 })}`;
}

export default function DashboardView({ data, loading, snapshot, merchantLabel }) {
  if (loading) {
    return (
      <div className="empty-state">
        <div className="thinking-dots"><span></span><span></span><span></span></div>
        <p>Loading dashboard metrics…</p>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="empty-state">
        <div className="icon">02</div>
        <h3>No data available</h3>
        <p>Dashboard metrics will appear here once the API responds.</p>
      </div>
    );
  }

  const { kpis = {}, charts = {}, window: win = {} } = data;
  const merchant = snapshot?.merchant_profile?.merchant || {};
  const summary = snapshot?.summary || {};
  const paymentModes = Array.isArray(snapshot?.kpi_by_mode) && snapshot.kpi_by_mode.length > 0
    ? snapshot.kpi_by_mode
    : charts.payment_modes || [];
  const proactiveCards = Array.isArray(snapshot?.proactive_cards) ? snapshot.proactive_cards : [];
  const activeSignals = proactiveCards.filter((card) => card?.card_state !== 'DISMISSED');
  const cashflow = snapshot?.cashflow || {};
  const recentSettlements = Array.isArray(cashflow.recent) ? cashflow.recent.slice(0, 4) : [];
  const heldSettlements = recentSettlements.filter((item) => item.status === 'HELD');
  const topSignals = activeSignals.slice(0, 3);
  const heroTags = [
    merchant.merchant_risk_category ? `Risk ${merchant.merchant_risk_category}` : '',
    merchant.merchant_status ? `Status ${merchant.merchant_status}` : '',
    snapshot?.data_coverage?.coverage_label || '',
  ].filter(Boolean);

  const modeRows = paymentModes.map((mode) => ({
    payment_mode: mode.bucket || mode.name || 'Unknown',
    attempts: mode.attempts ?? '',
    success_rate_pct: mode.success_rate_pct ?? '',
    success_gmv: mode.success_gmv ?? mode.value ?? '',
    failed_gmv: mode.failed_gmv ?? '',
  }));

  return (
    <div className="dashboard-view">
      <div className="dashboard-topline">
        <section className="glass-card dashboard-hero">
          <div className="dashboard-hero-copy">
            <h3>{merchant.merchant_trade_name || merchantLabel}</h3>
            <div className="hero-tags">
              {heroTags.map((tag) => (
                <span key={tag} className="badge neutral">{tag}</span>
              ))}
            </div>
          </div>
          <div className="dashboard-hero-rail compact">
            <div className="dashboard-mini-card">
              <span>Window</span>
              <strong>{win.from && win.to ? `${win.from} to ${win.to}` : 'Live'}</strong>
            </div>
            <div className="dashboard-mini-card">
              <span>Signals</span>
              <strong>{activeSignals.length}</strong>
            </div>
            <div className="dashboard-mini-card">
              <span>Held</span>
              <strong>{heldSettlements.length}</strong>
            </div>
            <div className="dashboard-mini-card">
              <span>Disputes</span>
              <strong>{summary.open_chargebacks || 0}</strong>
            </div>
          </div>
        </section>

        <div className="kpi-grid dashboard-kpi-grid">
          <div className="glass-card kpi-card accent">
            <span className="kpi-label">Attempts</span>
            <span className="kpi-value">{Number(kpis.attempts || 0).toLocaleString('en-IN')}</span>
          </div>
          <div className="glass-card kpi-card success">
            <span className="kpi-label">Success</span>
            <span className="kpi-value">{formatPercent(kpis.success_rate_pct)}</span>
            <span className="kpi-sub">{Number(kpis.success_txns || 0).toLocaleString('en-IN')} ok</span>
          </div>
          <div className="glass-card kpi-card danger">
            <span className="kpi-label">Failed</span>
            <span className="kpi-value">{Number(kpis.fail_txns || 0).toLocaleString('en-IN')}</span>
            <span className="kpi-sub">{formatRupeesShort(summary.failed_gmv)} GMV</span>
          </div>
          <div className="glass-card kpi-card info">
            <span className="kpi-label">GMV</span>
            <span className="kpi-value">{formatRupeesShort(kpis.success_gmv)}</span>
            <span className="kpi-sub">{summary.terminal_count || 0} terminals</span>
          </div>
        </div>
      </div>

      <div className="dashboard-grid">
        <div className="glass-card chart-container">
          <div className="section-header">
            <div>
              <h3>Payment mix</h3>
            </div>
            <span className="badge neutral">{paymentModes.length} mode(s)</span>
          </div>
          {charts?.payment_modes?.length ? (
            <div className="chart-with-legend">
              <ResponsiveContainer width="100%" height={280}>
                <PieChart>
                  <Pie
                    data={charts.payment_modes}
                    cx="50%"
                    cy="50%"
                    innerRadius={76}
                    outerRadius={112}
                    dataKey="value"
                    nameKey="name"
                    stroke="none"
                    paddingAngle={3}
                  >
                    {charts.payment_modes.map((_, index) => (
                      <Cell key={index} fill={COLORS[index % COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={{
                      background: '#ffffff',
                      border: '1px solid rgba(18, 38, 63, 0.08)',
                      borderRadius: 18,
                      boxShadow: '0 24px 60px rgba(15, 23, 42, 0.16)',
                      color: '#12263f',
                    }}
                    formatter={(value) => formatRupeesShort(value)}
                  />
                </PieChart>
              </ResponsiveContainer>

              <div className="chart-legend-list">
                {charts.payment_modes.map((item, index) => (
                  <div key={item.name} className="chart-legend-item">
                    <span className="chart-dot" style={{ backgroundColor: COLORS[index % COLORS.length] }} />
                    <div>
                      <strong>{item.name}</strong>
                      <p>{formatRupeesShort(item.value)}</p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <p className="empty-inline">Payment-mode chart data is not available for this merchant yet.</p>
          )}
        </div>

        <div className="glass-card section-card">
          <div className="section-header">
            <div>
              <h3>Settlements</h3>
            </div>
            <span className="badge neutral">{recentSettlements.length} recent rows</span>
          </div>
          <div className="hero-tags">
            <span className="badge warning">Pending {formatCurrency(cashflow?.amounts?.pending_amount || 0)}</span>
            <span className="badge success">Settled {formatCurrency(cashflow?.amounts?.settled_amount || 0)}</span>
          </div>
          {recentSettlements.length > 0 ? (
            <div className="stack-list">
              {recentSettlements.map((item) => (
                <div key={`${item.settlement_id}:${item.expected_date}`} className="sub-card settlement-row-card">
                  <div className="sub-card-header">
                    <strong>{`#${item.settlement_id}`}</strong>
                    <span className={`badge ${item.status === 'HELD' ? 'warning' : 'success'}`}>{item.status}</span>
                  </div>
                  <div className="inline-meta">
                    <span>{item.expected_date || 'n/a'}</span>
                    <span>{formatCurrency(item.amount_rupees)}</span>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <p className="empty-inline">No recent settlement rows are available.</p>
          )}
        </div>
      </div>

      <div className="two-col-grid dashboard-detail-grid">
        <DataTable
          title="Payment mode KPIs"
          rows={modeRows}
          emptyText="No payment-mode KPI rows are available."
        />

        <div className="glass-card section-card">
          <div className="section-header">
            <div>
              <h3>Signals</h3>
            </div>
            <span className="badge neutral">{activeSignals.length}</span>
          </div>
          {topSignals.length > 0 ? (
            <div className="stack-list">
              {topSignals.map((card) => (
                <div key={card.dedupe_key} className="sub-card attention-card">
                  <div className="sub-card-header">
                    <strong>{card.title}</strong>
                    <span className={`nudge-badge ${card.lane === 'growth' ? 'growth' : 'ops'}`}>
                      {card.lane === 'growth' ? 'Growth' : 'Operations'}
                    </span>
                  </div>
                  <div className="inline-meta">
                    {card.impact_rupees != null && <span>Impact {formatCompactCurrency(card.impact_rupees)}</span>}
                    {card.confidence != null && <span>Confidence {Number(card.confidence).toFixed(2)}</span>}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <p className="empty-inline">No active signals are waiting on this merchant right now.</p>
          )}
        </div>
      </div>
    </div>
  );
}
