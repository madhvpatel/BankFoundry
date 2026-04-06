import DataTable from './DataTable';
import TaskList from './TaskList';

export default function DisputesView({ snapshot, merchantId, onChanged }) {
  const chargebacks = snapshot?.chargebacks?.rows || [];
  const refunds = snapshot?.refunds?.rows || [];
  const tasks = (snapshot?.operations_tasks || []).filter((task) =>
    String(task.action_type || '').includes('CHARGEBACK'),
  );
  const openChargebacks = chargebacks.filter((row) => !['CLOSED', 'RESOLVED'].includes(String(row.status || '').toUpperCase())).length;

  return (
    <div className="view-stack">
      <div className="kpi-grid three-up">
        <div className="glass-card kpi-card danger">
          <span className="kpi-label">Open chargebacks</span>
          <span className="kpi-value">{Number(openChargebacks || 0).toLocaleString('en-IN')}</span>
        </div>
        <div className="glass-card kpi-card accent">
          <span className="kpi-label">Total chargebacks</span>
          <span className="kpi-value">{Number(chargebacks.length || 0).toLocaleString('en-IN')}</span>
        </div>
        <div className="glass-card kpi-card info">
          <span className="kpi-label">Refund rows</span>
          <span className="kpi-value">{Number(refunds.length || 0).toLocaleString('en-IN')}</span>
        </div>
      </div>

      <TaskList
        merchantId={merchantId}
        title="Dispute tasks"
        tasks={tasks}
        emptyText="No dispute tasks were generated for the current window."
        onChanged={onChanged}
      />

      <div className="two-col-grid">
        <DataTable title="Chargebacks" rows={chargebacks} emptyText="No chargeback rows were returned." />
        <DataTable title="Refunds" rows={refunds} emptyText="No refund rows were returned." />
      </div>
    </div>
  );
}
