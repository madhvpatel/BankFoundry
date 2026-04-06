import { useState, useRef } from 'react';

const STARTER_PROMPTS = [
  'Why did success drop?',
  'Biggest settlement risk',
  'Best growth opportunity',
  'Explain my last payout deduction',
  'I want to contest a chargeback'
];

export default function ChatView({
  messages,
  loading,
  onSend,
  onNewThread,
  merchantLabel,
  terminalId,
  threadScope,
  activeMemory,
  onPromoteFinding,
  promotionBusy,
  onUploadDisputeReceipt,
}) {
  const [uploadingDispute, setUploadingDispute] = useState(false);
  const [uploadError, setUploadError] = useState(null);
  const [uploadedEvidence, setUploadedEvidence] = useState(null);
  const fileInputRef = useRef(null);

  const handleSubmit = (event) => {
    event.preventDefault();
    const input = event.target.elements.prompt;
    const value = input.value.trim();
    if (!value) return;
    onSend(value);
    input.value = '';
  };

  const scopeNote = terminalId
    ? `Scope: ${merchantLabel} · terminal ${terminalId}`
    : `Scope: ${merchantLabel}`;
  const memory = activeMemory && typeof activeMemory === 'object' ? activeMemory : null;
  const selectedEntities = memory?.selected_entities && typeof memory.selected_entities === 'object'
    ? Object.entries(memory.selected_entities).filter(([, value]) => value)
    : [];
  const activeTopics = Array.isArray(memory?.active_topics) ? memory.active_topics.filter(Boolean) : [];
  const activeWindow = memory?.active_window && typeof memory.active_window === 'object' ? memory.active_window : null;
  const outstandingFollowUps = Array.isArray(memory?.outstanding_follow_ups) ? memory.outstanding_follow_ups.filter(Boolean) : [];

  return (
    <div className="chat-layout">
      <aside className="glass-card chat-brief">
        <div className="chat-brief-header">
          <h3>Copilot</h3>
          <span className="badge neutral">{merchantLabel}</span>
        </div>
        <div className="chat-thread-row">
          <span className="badge neutral">Thread {threadScope || 'default'}</span>
          <button type="button" className="secondary ghost-button" onClick={onNewThread} disabled={loading}>
            New thread
          </button>
        </div>
        {memory && (
          <details className="compact-details chat-memory-card">
            <summary>{`Thread memory · ${memory.turn_count || 0} turns`}</summary>
            {selectedEntities.length > 0 && (
              <div className="tag-wrap chat-memory-tags">
                {selectedEntities.map(([key, value]) => (
                  <span key={key} className="evidence-tag">{String(key).replace(/_/g, ' ')}: {value}</span>
                ))}
              </div>
            )}
            {(activeWindow?.from_date || activeWindow?.to_date) && (
              <p className="muted-line">
                {activeWindow?.from_date || 'n/a'} to {activeWindow?.to_date || 'n/a'}
              </p>
            )}
            {activeTopics.length > 0 && (
              <div className="tag-wrap chat-memory-tags">
                {activeTopics.map((topic) => (
                  <span key={topic} className="badge neutral">{topic.replace(/_/g, ' ')}</span>
                ))}
              </div>
            )}
            {outstandingFollowUps.length > 0 && (
              <ul className="follow-up-list">
                {outstandingFollowUps.map((item, idx) => (
                  <li key={idx}>{item}</li>
                ))}
              </ul>
            )}
          </details>
        )}
        <div className="quick-reply-wrap">
          {STARTER_PROMPTS.map((prompt) => (
            <button
              key={prompt}
              type="button"
              className="quick-reply-btn"
              onClick={() => onSend(prompt)}
              disabled={loading}
            >
              {prompt}
            </button>
          ))}
        </div>
      </aside>

      <section className="chat-area">
        <div className="chat-messages">
          {messages.length === 0 && (
            <div className="glass-card chat-welcome-card minimal">
              <h3>Ask anything about payments.</h3>
              <p className="muted-line">{scopeNote}</p>
            </div>
          )}

          {messages.map((msg, idx) => {
            if (msg.role === 'user') {
              const text = msg.text || '';
              const match = text.match(/^I've uploaded the receipt, please verify\. (\{.*\})$/);
              if (match) {
                try {
                  const receiptData = JSON.parse(match[1]);
                  const ex = receiptData.extracted || {};
                  return (
                    <div key={idx} className="message-bubble user">
                      I've uploaded the receipt, please verify.
                      <div className="receipt-evidence-card" style={{ marginTop: '8px', color: 'initial', textAlign: 'left' }}>
                        <div className="sub-card-header">
                          <strong>Receipt Attached</strong>
                          <span className="badge success">✓</span>
                        </div>
                        <ul className="follow-up-list section-list" style={{ marginBottom: 0 }}>
                          <li><strong>Amount:</strong> {ex.amount || 'Unknown'} {ex.currency || ''}</li>
                          <li><strong>Date:</strong> {ex.date || 'Unknown'}</li>
                          <li><strong>TxID:</strong> {ex.transaction_id || 'Unknown'}</li>
                        </ul>
                      </div>
                    </div>
                  );
                } catch (e) {
                  // Fallback if parsing fails
                  return <div key={idx} className="message-bubble user">{msg.text}</div>;
                }
              }
              return (
                <div key={idx} className="message-bubble user">
                  {msg.text}
                </div>
              );
            }

            const sources = Array.isArray(msg.sources) ? msg.sources : [];
            const followUps = Array.isArray(msg.followUps) ? msg.followUps : [];
            const structured = msg.structuredResult && typeof msg.structuredResult === 'object' ? msg.structuredResult : null;
            const actionPreview = msg.actionPreview && typeof msg.actionPreview === 'object' ? msg.actionPreview : null;
            const displayNotice = msg.displayNotice && typeof msg.displayNotice === 'object' ? msg.displayNotice : null;
            const answerSections = msg.answerSections && typeof msg.answerSections === 'object' ? msg.answerSections : null;
            const clarifyingQuestion = msg.clarifyingQuestion && typeof msg.clarifyingQuestion === 'object' ? msg.clarifyingQuestion : null;
            const debug = msg.debug && typeof msg.debug === 'object' ? msg.debug : null;
            const executiveSummary = typeof answerSections?.executive_summary === 'string' ? answerSections.executive_summary : '';
            const keyFindings = Array.isArray(answerSections?.key_findings) ? answerSections.key_findings.filter(Boolean) : [];
            const nextBestAction = typeof answerSections?.next_best_action === 'string' ? answerSections.next_best_action : '';
            const caveats = Array.isArray(answerSections?.caveats) ? answerSections.caveats.filter(Boolean) : [];
            const hasStructuredAnswer = executiveSummary || keyFindings.length > 0 || nextBestAction || caveats.length > 0;
            const promoteKey = `chat_finding:chat-${idx}`;
            const isError = String(msg.text || '').startsWith('Error:');

            return (
              <div key={idx} className={`message-bubble assistant ${isError ? 'error' : ''}`}>
                {msg.intent && (
                  <div className="message-meta-row">
                    <span className="badge neutral">{String(msg.intent).replace(/_/g, ' ')}</span>
                  </div>
                )}

                {msg.intent === 'payout_dispute' && onUploadDisputeReceipt && idx === messages.length - 1 && (
                  <div className="dispute-upload-zone inline-table-card">
                    <div className="section-label">Dispute Evidence</div>
                    <p className="muted-line">Attach a receipt or payment slip to verify this transaction.</p>
                    
                    {!uploadedEvidence ? (
                      <div className="dispute-action-row">
                        <input 
                          type="file" 
                          accept="image/*" 
                          ref={fileInputRef} 
                          style={{ display: 'none' }} 
                          onChange={async (e) => {
                            const file = e.target.files[0];
                            if (!file) return;
                            setUploadingDispute(true);
                            setUploadError(null);
                            try {
                              const metadata = sources.find(s => s.startsWith('settle_')) 
                                ? { settlement_id: sources.find(s => s.startsWith('settle_')) } 
                                : {};
                              const res = await onUploadDisputeReceipt(file, metadata);
                              setUploadedEvidence(res.evidence);
                            } catch (err) {
                              setUploadError(err.message);
                            } finally {
                              setUploadingDispute(false);
                            }
                          }}
                        />
                        <button 
                          className="secondary ghost-button" 
                          onClick={() => fileInputRef.current?.click()}
                          disabled={uploadingDispute || loading}
                        >
                          {uploadingDispute ? 'Extracting receipt...' : 'Select receipt image'}
                        </button>
                        {uploadError && <span className="warning-text">{uploadError}</span>}
                      </div>
                    ) : (
                      <div className="receipt-evidence-card">
                        <div className="sub-card-header">
                          <strong>Receipt Details Extracted</strong>
                          <span className={`badge ${uploadedEvidence.confidence === 'high' ? 'success' : 'warning'}`}>
                            {uploadedEvidence.confidence} confidence
                          </span>
                        </div>
                        <ul className="follow-up-list section-list">
                          <li><strong>Amount:</strong> {uploadedEvidence.extracted.amount || 'Unknown'} {uploadedEvidence.extracted.currency || ''}</li>
                          <li><strong>Date:</strong> {uploadedEvidence.extracted.date || 'Unknown'}</li>
                          <li><strong>TxID:</strong> {uploadedEvidence.extracted.transaction_id || 'Unknown'}</li>
                          <li><strong>Card:</strong> {uploadedEvidence.extracted.card_last4 ? `****${uploadedEvidence.extracted.card_last4}` : 'Unknown'}</li>
                        </ul>
                        <div className="dispute-action-row" style={{ marginTop: '12px' }}>
                          <button 
                            className="primary-button" 
                            onClick={() => {
                              onSend(`I've uploaded the receipt, please verify. ${JSON.stringify(uploadedEvidence)}`);
                              setUploadedEvidence(null);
                            }}
                            disabled={loading}
                          >
                            Verify with agent
                          </button>
                          <button 
                            className="secondary ghost-button" 
                            onClick={() => setUploadedEvidence(null)}
                            disabled={loading}
                          >
                            Remove
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                )}

                {hasStructuredAnswer ? (
                  <div className="answer-sections">
                    {executiveSummary && (
                      <div className="answer-summary-card">
                        <div className="section-label">Executive answer</div>
                        <p>{executiveSummary}</p>
                      </div>
                    )}

                    {keyFindings.length > 0 && (
                      <div className="inline-table-card answer-section-card">
                        <div className="section-label">Key findings</div>
                        <ul className="follow-up-list section-list">
                          {keyFindings.map((item, findingIdx) => (
                            <li key={findingIdx}>{item}</li>
                          ))}
                        </ul>
                      </div>
                    )}

                    {nextBestAction && (
                      <div className="inline-table-card answer-section-card">
                        <div className="section-label">Next best action</div>
                        <p className="section-emphasis">{nextBestAction}</p>
                      </div>
                    )}

                    {caveats.length > 0 && (
                      <div className="inline-table-card answer-section-card caution">
                        <div className="section-label">Caveats</div>
                        <ul className="follow-up-list section-list">
                          {caveats.map((item, caveatIdx) => (
                            <li key={caveatIdx}>{item}</li>
                          ))}
                        </ul>
                      </div>
                    )}
                  </div>
                ) : (
                  <div className="answer-body">
                    {String(msg.text || '')
                      .split(/\n\n+/)
                      .filter(Boolean)
                      .map((paragraph, pIdx) => (
                        <p key={pIdx}>{paragraph}</p>
                      ))}
                  </div>
                )}

                {structured && Array.isArray(structured.rows) && structured.rows.length > 0 && (
                  <div className="inline-table-card">
                    <div className="sub-card-header">
                      <strong>{structured.title || 'Result'}</strong>
                      <span className="badge neutral">{structured.rows.length} row(s)</span>
                    </div>
                    {structured.window && (
                      <p className="muted-line">
                        Window: {structured.window.from || 'n/a'} to {structured.window.to || 'n/a'}
                      </p>
                    )}
                    <div className="table-wrap">
                      <table className="data-table compact">
                        <thead>
                          <tr>
                            {(structured.columns || []).map((column) => (
                              <th key={column}>{String(column).replace(/_/g, ' ')}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {structured.rows.map((row, rowIdx) => (
                            <tr key={rowIdx}>
                              {(structured.columns || []).map((column) => (
                                <td key={column}>{formatCell(row?.[column])}</td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}

                {actionPreview && (
                  <div className="inline-table-card">
                    <div className="sub-card-header">
                      <strong>{actionPreview.title || 'Suggested next action'}</strong>
                      {actionPreview.category && <span className="badge neutral">{actionPreview.category}</span>}
                    </div>
                    {actionPreview.summary && <p className="muted-line">{actionPreview.summary}</p>}
                    {Array.isArray(actionPreview.actions) && actionPreview.actions.length > 0 && (
                      <ul className="action-preview-list">
                        {actionPreview.actions.map((action, actionIdx) => (
                          <li key={actionIdx}>{action?.text || ''}</li>
                        ))}
                      </ul>
                    )}
                  </div>
                )}

                {displayNotice && (
                  <details className="compact-details validation-notice">
                    <summary>{displayNotice.summary || displayNotice.title || 'Validation review available'}</summary>
                    <div className="validation-notice-body">
                      {displayNotice.title && <p className="muted-line">{displayNotice.title}</p>}
                      {displayNotice.validation_summary && <p className="muted-line">{displayNotice.validation_summary}</p>}
                      {Array.isArray(displayNotice.issues) && displayNotice.issues.length > 0 && (
                        <ul className="follow-up-list">
                          {displayNotice.issues.map((issue, issueIdx) => (
                            <li key={issueIdx}>
                              <strong>{issue.type || 'issue'}:</strong> {issue.claim || issue.evidence_id || issue.number || 'Review needed'}
                            </li>
                          ))}
                        </ul>
                      )}
                      {displayNotice.recommended_next_step && (
                        <p className="muted-line"><strong>Next step:</strong> {displayNotice.recommended_next_step}</p>
                      )}
                    </div>
                  </details>
                )}

                {clarifyingQuestion && (
                  <div className="inline-table-card">
                    <div className="sub-card-header">
                      <strong>Need one clarification</strong>
                    </div>
                    <p>{clarifyingQuestion.question}</p>
                    {Array.isArray(clarifyingQuestion.choices) && clarifyingQuestion.choices.length > 0 && (
                      <div className="quick-reply-wrap">
                        {clarifyingQuestion.choices.map((choice, choiceIdx) => (
                          <button
                            key={choiceIdx}
                            type="button"
                            className="quick-reply-btn"
                            onClick={() => onSend(choice)}
                            disabled={loading}
                          >
                            {choice}
                          </button>
                        ))}
                      </div>
                    )}
                  </div>
                )}

                {(msg.verificationStatus || sources.length > 0) && (
                  <div className="answer-footer">
                    {msg.verificationStatus && (
                      <div className="footer-line">
                        <strong>Verification:</strong> {msg.verificationStatus}
                      </div>
                    )}
                    {msg.verificationSummary && (
                      <div className="footer-line">
                        <strong>Validation:</strong> {msg.verificationSummary}
                      </div>
                    )}
                    {sources.length > 0 && (
                      <details className="compact-details">
                        <summary>Sources ({sources.length})</summary>
                        <div className="tag-wrap">
                          {sources.map((item, tagIdx) => (
                            <span key={tagIdx} className="evidence-tag">{item}</span>
                          ))}
                        </div>
                      </details>
                    )}
                  </div>
                )}

                {followUps.length > 0 && (
                  <details className="compact-details">
                    <summary>Suggested follow-ups ({followUps.length})</summary>
                    <ul className="follow-up-list">
                      {followUps.map((item, followIdx) => (
                        <li key={followIdx}>{item}</li>
                      ))}
                    </ul>
                  </details>
                )}

                {typeof onPromoteFinding === 'function' && !isError && (
                  <div className="button-row inline-case-actions">
                    <button
                      type="button"
                      className="secondary"
                      onClick={() =>
                        onPromoteFinding({
                          sourceRef: `chat-${idx}`,
                          title: executiveSummary || msg.prompt || 'Chat finding',
                          summary: String(msg.text || executiveSummary || '').trim(),
                          evidence_ids: sources,
                          sources,
                          answer: msg.text || '',
                          question: msg.prompt || '',
                          terminal_id: terminalId || undefined,
                          case_type: sources.some((item) => String(item).toLowerCase().includes('settlement'))
                            ? 'settlement_shortfall_review'
                            : 'manual_ops_review',
                          priority: sources.some((item) => String(item).toLowerCase().includes('settlement')) ? 'high' : 'medium',
                        })
                      }
                      disabled={loading || promotionBusy === promoteKey}
                    >
                      {promotionBusy === promoteKey ? 'Creating case…' : 'Create ops case'}
                    </button>
                  </div>
                )}

                {debug && (
                  <details className="compact-details debug-trace">
                    <summary>Developer trace</summary>
                    <pre>{JSON.stringify(debug, null, 2)}</pre>
                  </details>
                )}
              </div>
            );
          })}

          {loading && (
            <div className="thinking-indicator">
              <div className="thinking-dots"><span></span><span></span><span></span></div>
              Looking into your data…
            </div>
          )}
        </div>

        <form className="chat-input-area" onSubmit={handleSubmit}>
          <div className="chat-input-wrapper">
            <input
              name="prompt"
              placeholder="Ask a question"
              autoComplete="off"
              disabled={loading}
            />
            <button type="submit" className="send-btn" disabled={loading}>
              Ask
            </button>
          </div>
        </form>
      </section>
    </div>
  );
}

function formatCell(value) {
  if (value === null || value === undefined) return '';
  if (typeof value === 'number') return String(value);
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}
