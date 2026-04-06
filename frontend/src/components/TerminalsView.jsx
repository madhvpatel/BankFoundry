import DataTable from './DataTable';

export default function TerminalsView({ snapshot }) {
  return (
    <div className="view-stack">
      <div className="kpi-grid two-up">
        <div className="glass-card kpi-card accent">
          <span className="kpi-label">Active terminal rows</span>
          <span className="kpi-value">{Number((snapshot?.terminals?.rows || []).length).toLocaleString('en-IN')}</span>
        </div>
        <div className="glass-card kpi-card info">
          <span className="kpi-label">Health signal rows</span>
          <span className="kpi-value">{Number((snapshot?.terminal_health?.rows || []).length).toLocaleString('en-IN')}</span>
        </div>
      </div>

      <DataTable
        title="Terminal performance"
        rows={snapshot?.terminals?.rows || []}
        emptyText="No terminal performance data was returned for the current window."
      />
      <DataTable
        title="Terminal health summary"
        rows={snapshot?.terminal_health?.rows || []}
        emptyText="No terminal health telemetry is connected for this merchant."
      />
    </div>
  );
}
