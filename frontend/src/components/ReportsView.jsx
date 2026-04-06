import { downloadText, rowsToCsv } from '../utils';

export default function ReportsView({ reports, merchantId }) {
  const packs = reports?.packs || [];
  const briefsById = Object.fromEntries((reports?.briefs || []).map((brief) => [brief.id, brief]));

  if (packs.length === 0) {
    return (
      <div className="empty-state">
        <div className="icon">🧾</div>
        <h3>No report packs available</h3>
      </div>
    );
  }

  return (
    <div className="view-stack">
      {packs.map((pack) => {
        const brief = briefsById[pack.id];
        return (
          <div key={pack.id} className="glass-card section-card">
            <div className="section-header">
              <div>
                <div className="eyebrow">Report pack</div>
                <h3>{pack.title}</h3>
              </div>
              {brief && (
                <div className="button-row">
                  <button onClick={() => downloadText(`${merchantId}_${pack.id}_brief.txt`, brief.email_text)}>
                    Download email brief
                  </button>
                  <button className="secondary" onClick={() => downloadText(`${merchantId}_${pack.id}_brief.html`, brief.print_html, 'text/html;charset=utf-8')}>
                    Download print brief
                  </button>
                </div>
              )}
            </div>

            {brief?.summary_lines?.length > 0 && (
              <div className="brief-block">
                <h4>Briefing summary</h4>
                <ul className="plain-list">
                  {brief.summary_lines.map((line, idx) => (
                    <li key={idx}>{line}</li>
                  ))}
                </ul>
              </div>
            )}

            <div className="stack-list">
              {(pack.datasets || []).map((dataset) => (
                <div key={dataset.key || dataset.title} className="sub-card">
                  <div className="sub-card-header">
                    <strong>{dataset.title || dataset.key}</strong>
                    <button
                      className="secondary"
                      onClick={() =>
                        downloadText(
                          `${merchantId}_${dataset.key || 'dataset'}.csv`,
                          rowsToCsv(dataset.rows || []),
                          'text/csv;charset=utf-8',
                        )
                      }
                    >
                      Download CSV
                    </button>
                  </div>
                  <p>{(dataset.rows || []).length} row(s)</p>
                </div>
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}
