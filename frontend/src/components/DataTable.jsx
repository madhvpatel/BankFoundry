import { formatCurrency, formatPercent } from '../utils';

export default function DataTable({ title, rows, emptyText = 'No rows available.', maxRows = 25 }) {
  const safeRows = Array.isArray(rows) ? rows.filter((row) => row && typeof row === 'object') : [];
  const columns = safeRows.length > 0
    ? Array.from(new Set(safeRows.flatMap((row) => Object.keys(row))))
    : [];

  return (
    <div className="glass-card section-card">
      {title && (
        <div className="section-header">
          <div>
            <div className="eyebrow">Data view</div>
            <h3>{title}</h3>
          </div>
          {safeRows.length > 0 && <span className="badge neutral">{Math.min(safeRows.length, maxRows)} row(s)</span>}
        </div>
      )}

      {safeRows.length === 0 ? (
        <p className="empty-inline">{emptyText}</p>
      ) : (
        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                {columns.map((key) => (
                  <th key={key}>{humanizeKey(key)}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {safeRows.slice(0, maxRows).map((row, idx) => (
                <tr key={idx}>
                  {columns.map((key) => (
                    <td key={key}>{formatValue(row[key], key)}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function humanizeKey(key) {
  return String(key || '')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function formatValue(value, key) {
  if (value === null || value === undefined || value === '') return '—';

  const normalizedKey = String(key || '').toLowerCase();

  if (normalizedKey.includes('pct') || normalizedKey.includes('rate')) {
    return formatPercent(Number(value));
  }

  if (
    normalizedKey.includes('gmv')
    || normalizedKey.includes('amount')
    || normalizedKey.includes('rupees')
    || normalizedKey.includes('impact')
  ) {
    return formatCurrency(value);
  }

  if (typeof value === 'number') {
    return value.toLocaleString('en-IN');
  }

  if (typeof value === 'object') {
    return JSON.stringify(value);
  }

  return String(value);
}
