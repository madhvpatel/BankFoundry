export function formatCurrency(value) {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount)) return 'Rs 0.00';
  return `Rs ${amount.toLocaleString('en-IN', { maximumFractionDigits: 2, minimumFractionDigits: 2 })}`;
}

export function formatCompactCurrency(value) {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount)) return 'Rs 0';
  return `Rs ${amount.toLocaleString('en-IN', { maximumFractionDigits: 0 })}`;
}

export function formatPercent(value) {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount)) return '0.00%';
  return `${amount.toFixed(2)}%`;
}

export function stripMarkdown(text) {
  return String(text || '')
    .replace(/\*\*/g, '')
    .replace(/^#+\s*/gm, '')
    .replace(/`/g, '')
    .trim();
}

export function parseAnswer(answer) {
  const text = String(answer || '').trim();
  if (!text) {
    return { body: '', verification: '', evidenceIds: [] };
  }

  const verificationMatch = text.match(/\n*\s*Verification status:\s*(.+?)(?:\n+Evidence IDs:|\s*$)/is);
  const evidenceMatch = text.match(/\n*\s*Evidence IDs:\s*(.+)$/is);

  let body = text;
  if (verificationMatch) {
    body = body.slice(0, verificationMatch.index).trim();
  } else if (evidenceMatch) {
    body = body.slice(0, evidenceMatch.index).trim();
  }

  const evidenceIds = evidenceMatch
    ? evidenceMatch[1]
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean)
    : [];

  return {
    body: stripMarkdown(body),
    verification: verificationMatch ? stripMarkdown(verificationMatch[1]) : '',
    evidenceIds,
  };
}

export function rowsToCsv(rows) {
  const safeRows = Array.isArray(rows) ? rows.filter((row) => row && typeof row === 'object') : [];
  if (safeRows.length === 0) return '';

  const columns = [];
  safeRows.forEach((row) => {
    Object.keys(row).forEach((key) => {
      if (!columns.includes(key)) columns.push(key);
    });
  });

  const quote = (value) => {
    if (value === null || value === undefined) return '';
    const text = String(value).replace(/"/g, '""');
    return /[",\n]/.test(text) ? `"${text}"` : text;
  };

  const lines = [columns.map(quote).join(',')];
  safeRows.forEach((row) => {
    lines.push(columns.map((column) => quote(row[column])).join(','));
  });
  return lines.join('\n');
}

export function downloadText(filename, content, mimeType = 'text/plain;charset=utf-8') {
  const blob = new Blob([content || ''], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(url);
}

export function normalizedLaneLabel(lane) {
  if (lane === 'operations') return 'Operations';
  if (lane === 'growth') return 'Growth';
  return 'General';
}
