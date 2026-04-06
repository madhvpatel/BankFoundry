import DataTable from './DataTable';

export default function ConnectedSystemsView({ snapshot }) {
  const coverage = snapshot?.data_coverage || {};
  const classification = snapshot?.classification || {};
  const integrations = coverage.integrations || {};
  const dataDomains = coverage.data_domains || {};
  const integrationRows = Object.entries(integrations)
    .filter(([, payload]) => payload && typeof payload === 'object')
    .map(([name, payload]) => ({
      integration: name,
      connected: payload.connected ? 'Yes' : 'No',
      provider: payload.provider || '',
      status: payload.status || '',
      source_table: payload.source_table || '',
    }));
  const dataRows = Object.entries(dataDomains)
    .filter(([, payload]) => payload && typeof payload === 'object')
    .map(([name, payload]) => ({
      domain: name,
      available: payload.available ? 'Yes' : 'No',
      source_table: payload.source_table || '',
      row_count: payload.row_count ?? '',
      latest_date: payload.latest_date || '',
    }));
  const signalRows = Object.entries(snapshot?.operating_signals || {}).map(([signal, value]) => ({ signal, value }));

  return (
    <div className="view-stack">
      <div className="glass-card hero-card compact">
        <div>
          <div className="eyebrow">Connected Systems</div>
          <h2>{coverage.coverage_label || 'Payments only'}</h2>
          <p>Merchant segment: {classification.label || 'Unknown'}</p>
        </div>
      </div>

      <DataTable title="External integrations" rows={integrationRows} emptyText="No external integrations were detected for this merchant." />
      <DataTable title="Internal data coverage" rows={dataRows} emptyText="No internal acquiring data coverage was detected for this merchant." />
      <DataTable title="Operating signals" rows={signalRows} emptyText="No extra operating signals were derived from the dataset." />
    </div>
  );
}
