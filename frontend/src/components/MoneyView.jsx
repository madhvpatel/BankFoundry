import { formatCurrency } from '../utils';
import DataTable from './DataTable';
import TaskList from './TaskList';

export default function MoneyView({ snapshot, merchantId, onChanged }) {
  const cashflow = snapshot?.cashflow || {};
  const amounts = cashflow.amounts || {};
  const pastExpected = cashflow.past_expected || {};
  const tasks = (snapshot?.operations_tasks || []).filter((task) =>
    String(task.action_type || '').startsWith('SETTLEMENT'),
  );

  return (
    <div className="view-stack">
      <div className="kpi-grid">
        <div className="glass-card kpi-card info">
          <span className="kpi-label">Pending amount</span>
          <span className="kpi-value">{formatCurrency(amounts.pending_amount)}</span>
        </div>
        <div className="glass-card kpi-card success">
          <span className="kpi-label">Settled amount</span>
          <span className="kpi-value">{formatCurrency(amounts.settled_amount)}</span>
        </div>
        <div className="glass-card kpi-card danger">
          <span className="kpi-label">Past expected count</span>
          <span className="kpi-value">{Number(pastExpected.past_expected_count || 0).toLocaleString('en-IN')}</span>
        </div>
        <div className="glass-card kpi-card accent">
          <span className="kpi-label">Past expected amount</span>
          <span className="kpi-value">{formatCurrency(pastExpected.past_expected_amount)}</span>
        </div>
      </div>

      <TaskList
        merchantId={merchantId}
        title="Money tasks"
        tasks={tasks}
        emptyText="No settlement or payout tasks were generated for the current window."
        onChanged={onChanged}
      />

      <DataTable
        title="Recent settlement timing view"
        rows={cashflow.recent || []}
        emptyText="No recent settlement timing data was returned."
      />
      <DataTable
        title="In-window settlements"
        rows={snapshot?.settlements?.rows || []}
        emptyText="No settlements were returned for the current window."
      />
    </div>
  );
}
