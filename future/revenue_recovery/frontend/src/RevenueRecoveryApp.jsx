import { startTransition, useEffect, useMemo, useState } from 'react';
import { askRevenueRecoveryPreview, fetchMerchantOptions } from './revenueRecoveryApi';

const EXAMPLE_PROMPTS = [
  'Why did failures increase in the last 30 days?',
  'Where is revenue leaking this week?',
  'Which terminals are driving failed GMV?',
];

function StatusPill({ label, tone = 'neutral' }) {
  return <span className={`rr-pill rr-pill-${tone}`}>{label}</span>;
}

function JsonBlock({ title, value }) {
  if (!value) return null;
  return (
    <details className="rr-json-block">
      <summary>{title}</summary>
      <pre>{JSON.stringify(value, null, 2)}</pre>
    </details>
  );
}

export default function RevenueRecoveryApp() {
  const [merchantOptions, setMerchantOptions] = useState([]);
  const [merchantId, setMerchantId] = useState('');
  const [userRole, setUserRole] = useState('ops');
  const [requestedActionLevel, setRequestedActionLevel] = useState('read_only');
  const [prompt, setPrompt] = useState(EXAMPLE_PROMPTS[0]);
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    fetchMerchantOptions()
      .then((data) => {
        const merchants = data.merchants || [];
        setMerchantOptions(merchants);
        setMerchantId(data.default_merchant_id || merchants[0]?.merchant_id || '');
      })
      .catch((fetchError) => {
        setError(`Merchant options failed: ${fetchError.message}`);
      });
  }, []);

  const merchantLabel = useMemo(() => {
    const selected = merchantOptions.find((item) => item.merchant_id === merchantId);
    return selected?.label || merchantId || 'Merchant';
  }, [merchantId, merchantOptions]);

  async function handleSubmit(event) {
    event.preventDefault();
    setLoading(true);
    setError('');
    try {
      const payload = await askRevenueRecoveryPreview({
        merchantId,
        prompt,
        userRole,
        requestedActionLevel,
      });
      startTransition(() => {
        setResult(payload);
      });
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setLoading(false);
    }
  }

  const findings = result?.response?.findings || [];
  const caveats = result?.response?.caveats || [];
  const planSteps = result?.state?.plan?.steps || [];
  const traces = result?.traces || [];
  const compiledQueries = result?.state?.compiled_queries || [];
  const evidenceBundles = result?.state?.evidence_store?.bundles || [];

  return (
    <main className="rr-app">
      <section className="rr-hero">
        <div>
          <p className="rr-kicker">Separate Test Frontend</p>
          <h1>Merchant Revenue Recovery Runtime</h1>
          <p className="rr-subtitle">
            This page talks only to the new preview runtime. It does not use the live `/api/v1/ask` flow.
          </p>
        </div>
        <div className="rr-hero-badges">
          <StatusPill label="Preview Runtime" tone="info" />
          <StatusPill label={merchantLabel} />
        </div>
      </section>

      <section className="rr-layout">
        <div className="rr-column rr-column-primary">
          <form className="rr-panel rr-form" onSubmit={handleSubmit}>
            <div className="rr-form-grid">
              <label>
                Merchant
                <select value={merchantId} onChange={(event) => setMerchantId(event.target.value)}>
                  {merchantOptions.map((merchant) => (
                    <option key={merchant.merchant_id} value={merchant.merchant_id}>
                      {merchant.label}
                    </option>
                  ))}
                </select>
              </label>

              <label>
                Role
                <select value={userRole} onChange={(event) => setUserRole(event.target.value)}>
                  <option value="ops">ops</option>
                  <option value="growth">growth</option>
                  <option value="support">support</option>
                  <option value="admin">admin</option>
                </select>
              </label>

              <label>
                Action Level
                <select value={requestedActionLevel} onChange={(event) => setRequestedActionLevel(event.target.value)}>
                  <option value="read_only">read_only</option>
                  <option value="draft_operational">draft_operational</option>
                  <option value="approval_required">approval_required</option>
                </select>
              </label>
            </div>

            <label className="rr-prompt-field">
              Investigation prompt
              <textarea
                value={prompt}
                onChange={(event) => setPrompt(event.target.value)}
                rows={5}
                placeholder="Why did failures increase in the last 30 days?"
              />
            </label>

            <div className="rr-example-row">
              {EXAMPLE_PROMPTS.map((example) => (
                <button key={example} type="button" className="rr-example-chip" onClick={() => setPrompt(example)}>
                  {example}
                </button>
              ))}
            </div>

            <div className="rr-form-actions">
              <button type="submit" className="rr-submit" disabled={loading || !merchantId || !prompt.trim()}>
                {loading ? 'Running preview…' : 'Run preview'}
              </button>
              <p className="rr-form-note">The preview will show plan, queries, evidence, diagnosis, and trace.</p>
            </div>
          </form>

          {error ? <div className="rr-panel rr-error">{error}</div> : null}

          <section className="rr-panel">
            <div className="rr-section-head">
              <h2>Answer</h2>
              {result?.status ? (
                <StatusPill
                  label={result.status}
                  tone={result.status === 'completed' ? 'success' : result.status === 'waiting_for_approval' ? 'warning' : 'neutral'}
                />
              ) : null}
            </div>
            <p className="rr-answer">{result?.response?.executive_summary || 'Run the preview to see the structured answer.'}</p>

            {result?.clarification_request ? (
              <div className="rr-clarify">
                <h3>Clarification Needed</h3>
                <p>{result.clarification_request.reason}</p>
                <ul>
                  {(result.clarification_request.choices || []).map((choice) => (
                    <li key={choice}>{choice}</li>
                  ))}
                </ul>
              </div>
            ) : null}

            <div className="rr-summary-grid">
              <div className="rr-stat-card">
                <span>Coverage</span>
                <strong>{result ? `${Math.round((result.coverage_score || 0) * 100)}%` : '--'}</strong>
              </div>
              <div className="rr-stat-card">
                <span>Consistency</span>
                <strong>{result ? `${Math.round((result.consistency_score || 0) * 100)}%` : '--'}</strong>
              </div>
              <div className="rr-stat-card">
                <span>Run ID</span>
                <strong>{result?.run_id || '--'}</strong>
              </div>
            </div>
          </section>

          <section className="rr-panel">
            <div className="rr-section-head">
              <h2>Findings</h2>
              <span>{findings.length || 0} item(s)</span>
            </div>
            {findings.length ? (
              <div className="rr-list">
                {findings.map((finding) => (
                  <article key={finding.title} className="rr-list-item">
                    <h3>{finding.title}</h3>
                    <p>{finding.summary}</p>
                    <small>Evidence: {(finding.evidence_ids || []).join(', ') || 'none'}</small>
                  </article>
                ))}
              </div>
            ) : (
              <p className="rr-muted">No findings yet.</p>
            )}
          </section>

          <section className="rr-panel">
            <div className="rr-section-head">
              <h2>Caveats</h2>
            </div>
            {caveats.length ? (
              <ul className="rr-bullet-list">
                {caveats.map((caveat) => (
                  <li key={caveat}>{caveat}</li>
                ))}
              </ul>
            ) : (
              <p className="rr-muted">No caveats returned.</p>
            )}
          </section>
        </div>

        <div className="rr-column rr-column-secondary">
          <section className="rr-panel">
            <div className="rr-section-head">
              <h2>Trace</h2>
              <span>{traces.length} node(s)</span>
            </div>
            <div className="rr-trace-list">
              {traces.map((trace) => (
                <article key={`${trace.node_name}-${trace.created_at}`} className="rr-trace-item">
                  <div>
                    <strong>{trace.node_name}</strong>
                    <p>{trace.context_manifest_version}</p>
                  </div>
                  <small>{trace.created_at}</small>
                </article>
              ))}
            </div>
          </section>

          <section className="rr-panel">
            <div className="rr-section-head">
              <h2>Plan</h2>
              <span>{planSteps.length} step(s)</span>
            </div>
            <ol className="rr-step-list">
              {planSteps.map((step) => (
                <li key={step.step_id}>
                  <strong>{step.node_name}</strong>
                  <p>{step.purpose}</p>
                </li>
              ))}
            </ol>
          </section>

          <section className="rr-panel">
            <div className="rr-section-head">
              <h2>Compiled SQL</h2>
              <span>{compiledQueries.length} query(s)</span>
            </div>
            {compiledQueries.map((query) => (
              <details key={query.query_id} className="rr-query-card">
                <summary>{query.query_id}</summary>
                <pre>{query.sql}</pre>
              </details>
            ))}
          </section>

          <section className="rr-panel">
            <div className="rr-section-head">
              <h2>Evidence Bundles</h2>
              <span>{evidenceBundles.length} bundle(s)</span>
            </div>
            <div className="rr-list">
              {evidenceBundles.map((bundle) => (
                <article key={bundle.evidence_id} className="rr-list-item">
                  <h3>{bundle.evidence_id}</h3>
                  <p>{bundle.summary}</p>
                  <small>Tables: {(bundle.provenance?.table_names || []).join(', ')}</small>
                </article>
              ))}
            </div>
          </section>

          <JsonBlock title="Runtime State" value={result?.state} />
        </div>
      </section>
    </main>
  );
}
