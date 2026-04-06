import { useEffect, useMemo, useState } from 'react';
import {
  addOpsCaseNote,
  assignOpsCase,
  createOpsCase,
  decideOpsApproval,
  fetchOpsApprovals,
  fetchOpsCaseCopilot,
  fetchOpsCaseDetail,
  fetchOpsQueue,
  requestOpsApproval,
  resolveOpsCase,
  updateOpsCaseMemory,
} from '../api';

function tone(status) {
  const normalized = String(status || '').toUpperCase();
  if (normalized === 'AWAITING_APPROVAL') return 'warning';
  if (normalized === 'BLOCKED') return 'danger';
  if (normalized === 'IN_PROGRESS') return 'info';
  if (normalized === 'RESOLVED' || normalized === 'CLOSED') return 'success';
  return 'neutral';
}

function attentionTone(level) {
  const normalized = String(level || '').toLowerCase();
  if (normalized === 'critical') return 'danger';
  if (normalized === 'high') return 'warning';
  if (normalized === 'warning') return 'info';
  if (normalized === 'resolved') return 'success';
  return 'neutral';
}

function formatLabel(value) {
  return String(value || '').replace(/_/g, ' ');
}

function formatStamp(value) {
  if (!value) return '';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.valueOf())) return String(value);
  return parsed.toLocaleString([], {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
}

export default function OpsConsoleView({
  merchantId,
  merchantLabel,
  terminalId,
  lane,
  role,
  refreshSeed = 0,
  onRefreshed,
}) {
  const [queue, setQueue] = useState([]);
  const [queueSummary, setQueueSummary] = useState({});
  const [approvals, setApprovals] = useState([]);
  const [selectedCaseId, setSelectedCaseId] = useState('');
  const [caseDetail, setCaseDetail] = useState(null);
  const [busyKey, setBusyKey] = useState('');
  const [noteBody, setNoteBody] = useState('');
  const [statusMessage, setStatusMessage] = useState('');
  const [copilot, setCopilot] = useState(null);
  const [copilotLoading, setCopilotLoading] = useState(false);
  const [copilotError, setCopilotError] = useState('');
  const [memorySettlementId, setMemorySettlementId] = useState('');
  const [memoryStartDate, setMemoryStartDate] = useState('');
  const [memoryEndDate, setMemoryEndDate] = useState('');
  const [memoryEvidenceText, setMemoryEvidenceText] = useState('');

  const selectedCase = caseDetail?.work_item || null;
  const connectorRuns = Array.isArray(caseDetail?.connector_runs) ? caseDetail.connector_runs : [];
  const latestConnectorRun = connectorRuns[0] || null;

  const loadQueue = async (keepCaseId) => {
    if (!merchantId) return;

    const [queuePayload, approvalPayload] = await Promise.all([
      fetchOpsQueue({ merchantId, lane, role }),
      fetchOpsApprovals({ merchantId, lane, role }),
    ]);

    const cases = queuePayload.cases || [];
    const approvalItems = approvalPayload.approvals || [];
    const selectableCaseIds = new Set(
      [...cases.map((item) => item.case_id), ...approvalItems.map((item) => item.case_id)].filter(Boolean)
    );
    const preferredCaseId = keepCaseId || selectedCaseId;
    const nextCaseId = preferredCaseId && selectableCaseIds.has(preferredCaseId)
      ? preferredCaseId
      : cases[0]?.case_id || approvalItems[0]?.case_id || '';

    setQueue(cases);
    setQueueSummary(queuePayload.queue_summary || {});
    setApprovals(approvalItems);
    setSelectedCaseId(nextCaseId);

    if (nextCaseId) {
      try {
        const detail = await fetchOpsCaseDetail({ merchantId, caseId: nextCaseId, role });
        setCaseDetail(detail);
      } catch {
        setCaseDetail(null);
        setCopilot(null);
      }
    } else {
      setCaseDetail(null);
      setCopilot(null);
    }

    onRefreshed?.();
  };

  useEffect(() => {
    loadQueue('');
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [merchantId, lane, role, refreshSeed]);

  useEffect(() => {
    if (!merchantId || !selectedCaseId) return;
    fetchOpsCaseDetail({ merchantId, caseId: selectedCaseId, role })
      .then((detail) => setCaseDetail(detail))
      .catch(() => setCaseDetail(null));
  }, [merchantId, selectedCaseId, role]);

  useEffect(() => {
    const memory = caseDetail?.memory && typeof caseDetail.memory === 'object' ? caseDetail.memory : {};
    const pinned = memory.pinned_entities && typeof memory.pinned_entities === 'object' ? memory.pinned_entities : {};
    const windowState = memory.active_window && typeof memory.active_window === 'object' ? memory.active_window : {};
    const evidence = Array.isArray(memory.confirmed_evidence_ids) ? memory.confirmed_evidence_ids : [];

    setMemorySettlementId(String(pinned.settlement_id || ''));
    setMemoryStartDate(String(windowState.start_date || ''));
    setMemoryEndDate(String(windowState.end_date || ''));
    setMemoryEvidenceText(evidence.join('\n'));
  }, [selectedCaseId, caseDetail]);

  const loadCopilot = async (caseId = selectedCaseId) => {
    if (!merchantId || !caseId) return;
    setCopilotLoading(true);
    setCopilotError('');

    try {
      const detail = await fetchOpsCaseCopilot({ merchantId, caseId, role });
      setCopilot(detail.copilot || null);
    } catch (error) {
      setCopilot(null);
      setCopilotError(error.message);
    } finally {
      setCopilotLoading(false);
    }
  };

  useEffect(() => {
    if (!merchantId || !selectedCaseId) {
      setCopilot(null);
      setCopilotError('');
      return;
    }

    loadCopilot(selectedCaseId);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [merchantId, selectedCaseId, role, refreshSeed]);

  const handleManualCase = async () => {
    setBusyKey('manual-case');
    try {
      const response = await createOpsCase({
        merchantId,
        terminalId,
        lane,
        role,
        caseType: 'manual_ops_review',
        title: `Manual ${lane} case for ${merchantId}`,
        summary: `Opened from the Bank Foundry console for merchant ${merchantId}.`,
        priority: lane === 'operations' ? 'high' : 'medium',
        evidenceIds: [],
        links: [{ link_type: 'ops_console', ref: merchantId, label: 'Manual console case' }],
      });
      setStatusMessage(response.reused ? 'Reused an existing active case.' : 'Opened a new ops case.');
      await loadQueue(response.work_item?.case_id || response.case_id || '');
    } finally {
      setBusyKey('');
    }
  };

  const handleAssign = async () => {
    if (!selectedCase) return;
    setBusyKey('assign');
    try {
      const detail = await assignOpsCase({
        merchantId,
        caseId: selectedCase.case_id,
        role,
        owner: role,
      });
      setCaseDetail(detail);
      await loadQueue(selectedCase.case_id);
    } finally {
      setBusyKey('');
    }
  };

  const handleNote = async () => {
    if (!selectedCase || !noteBody.trim()) return;
    setBusyKey('note');
    try {
      const detail = await addOpsCaseNote({
        merchantId,
        caseId: selectedCase.case_id,
        role,
        body: noteBody.trim(),
      });
      setCaseDetail(detail);
      setNoteBody('');
      await loadQueue(selectedCase.case_id);
    } finally {
      setBusyKey('');
    }
  };

  const handleApprovalRequest = async () => {
    if (!selectedCase) return;
    setBusyKey('approval');
    try {
      const detail = await requestOpsApproval({
        merchantId,
        caseId: selectedCase.case_id,
        role,
        actionType: 'SETTLEMENT_ESCALATION',
        payloadSummary: `Escalate ${selectedCase.title}`,
        payload: {
          case_id: selectedCase.case_id,
          merchant_id: merchantId,
          lane,
          evidence_ids: selectedCase.evidence_ids || [],
        },
      });
      setCaseDetail(detail);
      await loadQueue(selectedCase.case_id);
    } finally {
      setBusyKey('');
    }
  };

  const handleUseApprovalDraft = async () => {
    const approvalDraft = copilot?.drafts?.approval_request;
    if (!selectedCase || !approvalDraft || approvalDraft.status !== 'ready') return;
    setBusyKey('approval-draft');
    try {
      const detail = await requestOpsApproval({
        merchantId,
        caseId: selectedCase.case_id,
        role,
        actionType: approvalDraft.action_type || 'SETTLEMENT_ESCALATION',
        payloadSummary: approvalDraft.payload_summary || `Escalate ${selectedCase.title}`,
        payload: approvalDraft.payload || {
          case_id: selectedCase.case_id,
          merchant_id: merchantId,
          lane,
          evidence_ids: selectedCase.evidence_ids || [],
        },
      });
      setCaseDetail(detail);
      setStatusMessage('Submitted the copilot approval draft.');
      await loadQueue(selectedCase.case_id);
    } finally {
      setBusyKey('');
    }
  };

  const handleSaveMemory = async () => {
    if (!selectedCase) return;

    const normalizedSettlementId = String(memorySettlementId || '').trim();
    const normalizedStartDate = String(memoryStartDate || '').trim();
    const normalizedEndDate = String(memoryEndDate || '').trim();
    const normalizedEvidenceIds = Array.from(
      new Set(
        String(memoryEvidenceText || '')
          .split(/[\n,]+/)
          .map((item) => item.trim())
          .filter(Boolean)
      )
    );

    setBusyKey('memory-save');
    try {
      const detail = await updateOpsCaseMemory({
        merchantId,
        caseId: selectedCase.case_id,
        role,
        settlementId: normalizedSettlementId,
        startDate: normalizedStartDate,
        endDate: normalizedEndDate,
        evidenceIds: normalizedEvidenceIds,
      });
      setCaseDetail(detail);
      setStatusMessage('Updated pinned case context.');
    } catch (error) {
      setStatusMessage(error.message);
    } finally {
      setBusyKey('');
    }
  };

  const handleClearMemory = async () => {
    if (!selectedCase) return;
    setBusyKey('memory-clear');
    try {
      const detail = await updateOpsCaseMemory({
        merchantId,
        caseId: selectedCase.case_id,
        role,
        evidenceIds: [],
        clearPinnedContext: true,
        clearWindow: true,
        clearEvidence: true,
      });
      setCaseDetail(detail);
      setStatusMessage('Cleared pinned case context.');
    } catch (error) {
      setStatusMessage(error.message);
    } finally {
      setBusyKey('');
    }
  };

  const handleResolve = async () => {
    if (!selectedCase) return;
    setBusyKey('resolve');
    try {
      const detail = await resolveOpsCase({
        merchantId,
        caseId: selectedCase.case_id,
        role,
        resolutionNote: 'Resolved from the Bank Foundry console.',
      });
      setCaseDetail(detail);
      await loadQueue(selectedCase.case_id);
    } finally {
      setBusyKey('');
    }
  };

  const handleApprovalDecision = async (approvalId, decision) => {
    setBusyKey(`${approvalId}:${decision}`);
    try {
      await decideOpsApproval({
        merchantId,
        approvalId,
        lane,
        role,
        decision,
        notes: `${decision.toLowerCase()} in Bank Foundry console`,
      });
      await loadQueue(selectedCaseId);
    } finally {
      setBusyKey('');
    }
  };

  const stepProgress = useMemo(() => {
    const steps = Array.isArray(caseDetail?.runbook_steps) ? caseDetail.runbook_steps : [];
    if (steps.length === 0) return '';
    const completed = steps.filter((step) => String(step.status || '').toUpperCase() === 'DONE').length;
    return `${completed}/${steps.length} complete`;
  }, [caseDetail]);

  const selectedCaseApprovals = useMemo(
    () => approvals.filter((item) => item.case_id && item.case_id === selectedCase?.case_id),
    [approvals, selectedCase]
  );

  const copilotSections = copilot?.answer_sections && typeof copilot.answer_sections === 'object' ? copilot.answer_sections : {};
  const copilotFindings = Array.isArray(copilotSections.key_findings) ? copilotSections.key_findings : [];
  const copilotCaveats = Array.isArray(copilotSections.caveats) ? copilotSections.caveats : [];
  const copilotToolCalls = Array.isArray(copilot?.tool_calls) ? copilot.tool_calls : [];
  const copilotEvidence = Array.isArray(copilot?.evidence_ids) ? copilot.evidence_ids : [];
  const copilotAgents = Array.isArray(copilot?.agents) ? copilot.agents : [];
  const copilotDrafts = copilot?.drafts && typeof copilot.drafts === 'object' ? copilot.drafts : {};
  const operatorNoteDraft = copilotDrafts.operator_note && typeof copilotDrafts.operator_note === 'object' ? copilotDrafts.operator_note : null;
  const approvalDraft = copilotDrafts.approval_request && typeof copilotDrafts.approval_request === 'object' ? copilotDrafts.approval_request : null;
  const caseMemory = caseDetail?.memory && typeof caseDetail.memory === 'object' ? caseDetail.memory : {};
  const pinnedEntities = caseMemory.pinned_entities && typeof caseMemory.pinned_entities === 'object' ? caseMemory.pinned_entities : {};
  const activeWindow = caseMemory.active_window && typeof caseMemory.active_window === 'object' ? caseMemory.active_window : {};
  const memoryEvidence = Array.isArray(caseMemory.confirmed_evidence_ids) ? caseMemory.confirmed_evidence_ids : [];

  return (
    <div className="mail-surface ops-mail-surface">
      <aside className="mail-sidebar ops-mail-sidebar">
        <div className="mail-toolbar">
          <div>
            <h3>Ops inbox</h3>
            <p>{`${merchantLabel || merchantId} · ${formatLabel(lane)} · ${formatLabel(role)}`}</p>
          </div>
          <button type="button" onClick={handleManualCase} disabled={busyKey === 'manual-case'}>
            {busyKey === 'manual-case' ? 'Opening...' : 'New case'}
          </button>
        </div>

        <div className="mail-toolbar-meta">
          <span className="mail-meta-chip">{`${queueSummary.open || 0} open`}</span>
          <span className="mail-meta-chip">{`${approvals.length} approvals`}</span>
          <span className="mail-meta-chip">{`${queueSummary.sla_breached || 0} SLA risk`}</span>
          <span className="mail-meta-chip">{`${queueSummary.unassigned || 0} unassigned`}</span>
        </div>

        {statusMessage && <div className="mail-inline-note">{statusMessage}</div>}

        <div className="mail-list-sections">
          <section className="mail-list-section">
            <div className="mail-list-header">
              <span>Approvals</span>
              <span>{approvals.length}</span>
            </div>

            {approvals.length === 0 ? (
              <p className="mail-empty-inline">No pending approvals.</p>
            ) : (
              <div className="mail-list">
                {approvals.map((item) => {
                  const isSelected = selectedCaseId === item.case_id;
                  return (
                    <article key={item.approval_id} className={`mail-row-panel ${isSelected ? 'active' : ''}`}>
                      <button
                        type="button"
                        className="mail-row-main"
                        onClick={() => item.case_id && setSelectedCaseId(item.case_id)}
                      >
                        <div className="mail-row-top">
                          <span className="mail-row-tag ops">Approval</span>
                          <span className={`mail-row-status ${tone(item.status)}`}>{formatLabel(item.status)}</span>
                        </div>
                        <strong className="mail-row-subject">{item.payload_summary}</strong>
                        <p className="mail-row-snippet">{item.case_title || 'Pending approval request'}</p>
                        <div className="mail-row-meta">
                          {item.case_id && <span>{`Case ${item.case_id}`}</span>}
                          {item.created_at && <span>{formatStamp(item.created_at)}</span>}
                        </div>
                      </button>
                      <div className="mail-approval-actions">
                        <button
                          type="button"
                          onClick={() => handleApprovalDecision(item.approval_id, 'APPROVED')}
                          disabled={busyKey === `${item.approval_id}:APPROVED`}
                        >
                          {busyKey === `${item.approval_id}:APPROVED` ? 'Approving...' : 'Approve'}
                        </button>
                        <button
                          type="button"
                          className="secondary"
                          onClick={() => handleApprovalDecision(item.approval_id, 'REJECTED')}
                          disabled={busyKey === `${item.approval_id}:REJECTED`}
                        >
                          {busyKey === `${item.approval_id}:REJECTED` ? 'Rejecting...' : 'Reject'}
                        </button>
                      </div>
                    </article>
                  );
                })}
              </div>
            )}
          </section>

          <section className="mail-list-section">
            <div className="mail-list-header">
              <span>Cases</span>
              <span>{queue.length}</span>
            </div>

            {queue.length === 0 ? (
              <p className="mail-empty-inline">No active cases in this lane.</p>
            ) : (
              <div className="mail-list">
                {queue.map((item) => (
                  <button
                    key={item.case_id}
                    type="button"
                    className={`mail-row ${selectedCaseId === item.case_id ? 'active' : ''}`}
                    onClick={() => setSelectedCaseId(item.case_id)}
                  >
                    <div className="mail-row-top">
                      <span className="mail-row-tag ops">{formatLabel(item.case_type || 'Case')}</span>
                      <span className={`mail-row-status ${tone(item.status)}`}>{formatLabel(item.status)}</span>
                    </div>
                    <strong className="mail-row-subject">{item.title}</strong>
                    <p className="mail-row-snippet">{item.summary}</p>
                    <div className="mail-row-meta">
                      <span>{formatLabel(item.priority || 'normal')}</span>
                      <span>{item.owner ? `Owner ${item.owner}` : 'Unassigned'}</span>
                      {item.sla_breached && <span>Breached</span>}
                      {!item.sla_breached && item.sla_warning && <span>SLA warning</span>}
                      {item.overdue_task_count > 0 && <span>{`${item.overdue_task_count} overdue`}</span>}
                    </div>
                  </button>
                ))}
              </div>
            )}
          </section>
        </div>
      </aside>

      <section className="mail-preview ops-mail-preview">
        {!selectedCase ? (
          <div className="mail-empty">
            <h3>Select a case</h3>
            <p>Pick an approval or case from the left column to inspect details.</p>
          </div>
        ) : (
          <>
            <div className="mail-preview-header ops-preview-header">
              <div>
                <div className="mail-preview-topline">
                  <span className="mail-row-tag ops">{formatLabel(selectedCase.case_type || 'Case')}</span>
                  <span className={`mail-row-status ${tone(selectedCase.status)}`}>{formatLabel(selectedCase.status)}</span>
                  {selectedCase.attention_level && (
                    <span className={`mail-row-status ${attentionTone(selectedCase.attention_level)}`}>
                      {formatLabel(selectedCase.attention_level)}
                    </span>
                  )}
                </div>
                <h3 className="mail-preview-subject">{selectedCase.title}</h3>
                <div className="mail-preview-meta">
                  <span>{formatLabel(selectedCase.priority || 'normal')}</span>
                  <span>{selectedCase.owner ? `Owner ${selectedCase.owner}` : 'Unassigned'}</span>
                  {selectedCase.age_hours != null && <span>{`${selectedCase.age_hours}h old`}</span>}
                  {selectedCase.due_at && <span>{`Due ${formatStamp(selectedCase.due_at)}`}</span>}
                  <span>{`Approval ${formatLabel(caseDetail?.approval_state?.status || selectedCase.approval_state || 'none')}`}</span>
                  {selectedCase.waiting_on && <span>{`Waiting on ${formatLabel(selectedCase.waiting_on)}`}</span>}
                </div>
                {selectedCase.summary && <p className="mail-preview-summary">{selectedCase.summary}</p>}
              </div>

              <div className="mail-preview-actions">
                <button type="button" onClick={handleAssign} disabled={busyKey === 'assign'}>
                  {busyKey === 'assign' ? 'Assigning...' : 'Assign to me'}
                </button>
                <button type="button" className="secondary" onClick={handleApprovalRequest} disabled={busyKey === 'approval'}>
                  {busyKey === 'approval' ? 'Sending...' : 'Request approval'}
                </button>
                <button type="button" className="secondary" onClick={handleResolve} disabled={busyKey === 'resolve'}>
                  {busyKey === 'resolve' ? 'Resolving...' : 'Resolve'}
                </button>
              </div>
            </div>

            <div className="mail-preview-body">
              <div className="mail-preview-grid">
                <section className="mail-preview-section">
                  <div className="mail-preview-section-head">
                    <div className="mail-preview-section-title">Overview</div>
                    {stepProgress && <span className="mail-meta-chip">{stepProgress}</span>}
                  </div>

                  <div className="mail-kv-grid">
                    <div className="mail-kv">
                      <span className="mail-kv-label">Approval</span>
                      <strong>{formatLabel(caseDetail?.approval_state?.status || selectedCase.approval_state || 'none')}</strong>
                    </div>
                    <div className="mail-kv">
                      <span className="mail-kv-label">Connector</span>
                      <strong>{formatLabel(latestConnectorRun?.status || selectedCase.connector_status || 'idle')}</strong>
                    </div>
                    <div className="mail-kv">
                      <span className="mail-kv-label">Open tasks</span>
                      <strong>{selectedCase.open_task_count || 0}</strong>
                    </div>
                    <div className="mail-kv">
                      <span className="mail-kv-label">Overdue</span>
                      <strong>{selectedCase.overdue_task_count || 0}</strong>
                    </div>
                  </div>

                  {selectedCase.blocked_reason && (
                    <p className="mail-preview-copy">{`Blocked: ${selectedCase.blocked_reason}`}</p>
                  )}

                  {Array.isArray(selectedCase.evidence_ids) && selectedCase.evidence_ids.length > 0 && (
                    <div className="mail-tag-row">
                      {selectedCase.evidence_ids.map((item) => (
                        <span key={item} className="mail-tag">{item}</span>
                      ))}
                    </div>
                  )}
                </section>

                <section className="mail-preview-section">
                  <div className="mail-preview-section-head">
                    <div className="mail-preview-section-title">Copilot</div>
                    <div className="mail-actions-inline">
                      {copilot?.verification && (
                        <span className={`mail-row-status ${copilot.verification === 'verified' ? 'success' : 'warning'}`}>
                          {copilot.verification}
                        </span>
                      )}
                      <button type="button" className="secondary" onClick={() => loadCopilot()} disabled={copilotLoading}>
                        {copilotLoading ? 'Refreshing...' : 'Refresh'}
                      </button>
                    </div>
                  </div>

                  {copilotLoading ? (
                    <p className="mail-preview-copy">Refreshing case summary.</p>
                  ) : copilotError ? (
                    <p className="mail-preview-copy">{copilotError}</p>
                  ) : copilot ? (
                    <>
                      {copilotSections.executive_summary && (
                        <p className="mail-preview-copy">{copilotSections.executive_summary}</p>
                      )}
                      {copilotFindings.length > 0 && (
                        <ul className="mail-text-list">
                          {copilotFindings.map((item, index) => (
                            <li key={index}>{item}</li>
                          ))}
                        </ul>
                      )}
                      {copilotSections.next_best_action && (
                        <p className="mail-preview-callout">{copilotSections.next_best_action}</p>
                      )}
                      {(copilotCaveats.length > 0 || copilotToolCalls.length > 0 || copilotAgents.length > 0 || copilotEvidence.length > 0) && (
                        <details className="mail-disclosure">
                          <summary>Trace</summary>
                          <div className="mail-disclosure-body">
                            {copilotCaveats.length > 0 && (
                              <>
                                <div className="mail-preview-section-title">Caveats</div>
                                <ul className="mail-text-list compact">
                                  {copilotCaveats.map((item, index) => (
                                    <li key={index}>{item}</li>
                                  ))}
                                </ul>
                              </>
                            )}
                            {copilotToolCalls.length > 0 && (
                              <>
                                <div className="mail-preview-section-title">Tools</div>
                                <div className="mail-tag-row">
                                  {copilotToolCalls.map((call, index) => (
                                    <span key={`${call.tool_name}:${index}`} className="mail-tag">
                                      {`${call.tool_name} ${call.verification || ''}`.trim()}
                                    </span>
                                  ))}
                                </div>
                              </>
                            )}
                            {copilotAgents.length > 0 && (
                              <>
                                <div className="mail-preview-section-title">Agents</div>
                                <div className="mail-mini-list">
                                  {copilotAgents.map((agent, index) => (
                                    <div key={`${agent.name || 'agent'}:${index}`} className="mail-mini-item">
                                      <div className="mail-mini-title">
                                        <span>{agent.name || 'Agent'}</span>
                                      </div>
                                      {agent.purpose && <p className="mail-mini-copy">{agent.purpose}</p>}
                                    </div>
                                  ))}
                                </div>
                              </>
                            )}
                            {copilotEvidence.length > 0 && (
                              <>
                                <div className="mail-preview-section-title">Evidence</div>
                                <div className="mail-tag-row">
                                  {copilotEvidence.map((item) => (
                                    <span key={item} className="mail-tag">{item}</span>
                                  ))}
                                </div>
                              </>
                            )}
                          </div>
                        </details>
                      )}
                    </>
                  ) : (
                    <p className="mail-preview-copy">No case summary yet.</p>
                  )}
                </section>

                <section className="mail-preview-section">
                  <div className="mail-preview-section-head">
                    <div className="mail-preview-section-title">Pinned context</div>
                    {caseMemory.updated_at && <span className="mail-meta-chip">{`Saved ${formatStamp(caseMemory.updated_at)}`}</span>}
                  </div>

                  {(pinnedEntities.settlement_id || activeWindow.start_date || memoryEvidence.length > 0) && (
                    <div className="mail-tag-row">
                      {pinnedEntities.settlement_id && <span className="mail-tag">{`Settlement ${pinnedEntities.settlement_id}`}</span>}
                      {activeWindow.start_date && activeWindow.end_date && (
                        <span className="mail-tag">{`${activeWindow.start_date} to ${activeWindow.end_date}`}</span>
                      )}
                      {memoryEvidence.map((item) => (
                        <span key={`memory:${item}`} className="mail-tag">{item}</span>
                      ))}
                    </div>
                  )}

                  <div className="mail-detail-grid">
                    <input
                      value={memorySettlementId}
                      placeholder="Settlement id"
                      onChange={(event) => setMemorySettlementId(event.target.value)}
                    />
                    <input
                      type="date"
                      value={memoryStartDate}
                      onChange={(event) => setMemoryStartDate(event.target.value)}
                    />
                    <input
                      type="date"
                      value={memoryEndDate}
                      onChange={(event) => setMemoryEndDate(event.target.value)}
                    />
                    <textarea
                      rows="4"
                      value={memoryEvidenceText}
                      placeholder="Evidence ids, one per line or comma-separated"
                      onChange={(event) => setMemoryEvidenceText(event.target.value)}
                    />
                  </div>

                  <div className="mail-action-row">
                    <button type="button" className="secondary" onClick={handleSaveMemory} disabled={busyKey === 'memory-save'}>
                      {busyKey === 'memory-save' ? 'Saving...' : 'Save context'}
                    </button>
                    <button type="button" className="secondary" onClick={handleClearMemory} disabled={busyKey === 'memory-clear'}>
                      {busyKey === 'memory-clear' ? 'Clearing...' : 'Clear'}
                    </button>
                  </div>
                </section>

                <section className="mail-preview-section">
                  <div className="mail-preview-section-head">
                    <div className="mail-preview-section-title">Drafts</div>
                  </div>

                  <div className="mail-mini-list">
                    {operatorNoteDraft?.body && (
                      <div className="mail-mini-item">
                        <div className="mail-mini-title">
                          <span>Operator note</span>
                        </div>
                        <pre className="mail-draft-block">{operatorNoteDraft.body}</pre>
                        <div className="mail-action-row">
                          <button
                            type="button"
                            className="secondary"
                            onClick={() => setNoteBody(operatorNoteDraft.body || '')}
                          >
                            Use as note
                          </button>
                        </div>
                      </div>
                    )}

                    {approvalDraft && (
                      <div className="mail-mini-item">
                        <div className="mail-mini-title">
                          <span>Approval draft</span>
                          <span>{approvalDraft.status}</span>
                        </div>
                        {approvalDraft.status === 'ready' ? (
                          <>
                            <p className="mail-mini-copy">{approvalDraft.payload_summary || approvalDraft.action_type}</p>
                            <div className="mail-action-row">
                              <button
                                type="button"
                                className="secondary"
                                onClick={handleUseApprovalDraft}
                                disabled={busyKey === 'approval-draft'}
                              >
                                {busyKey === 'approval-draft' ? 'Submitting...' : 'Use draft'}
                              </button>
                            </div>
                          </>
                        ) : (
                          <p className="mail-mini-copy">{approvalDraft.reason || 'Approval draft is not available right now.'}</p>
                        )}
                      </div>
                    )}

                    {!operatorNoteDraft?.body && !approvalDraft && (
                      <p className="mail-preview-copy">No drafts prepared for this case.</p>
                    )}
                  </div>
                </section>

                <section className="mail-preview-section">
                  <div className="mail-preview-section-head">
                    <div className="mail-preview-section-title">Approvals</div>
                    <span className="mail-meta-chip">{selectedCaseApprovals.length}</span>
                  </div>

                  {selectedCaseApprovals.length === 0 ? (
                    <p className="mail-preview-copy">No approval requests tied to this case.</p>
                  ) : (
                    <div className="mail-mini-list">
                      {selectedCaseApprovals.map((item) => (
                        <div key={item.approval_id} className="mail-mini-item">
                          <div className="mail-mini-title">
                            <span>{item.payload_summary}</span>
                            <span>{formatLabel(item.status)}</span>
                          </div>
                          {item.case_title && <p className="mail-mini-copy">{item.case_title}</p>}
                          <div className="mail-action-row">
                            <button
                              type="button"
                              onClick={() => handleApprovalDecision(item.approval_id, 'APPROVED')}
                              disabled={busyKey === `${item.approval_id}:APPROVED`}
                            >
                              {busyKey === `${item.approval_id}:APPROVED` ? 'Approving...' : 'Approve'}
                            </button>
                            <button
                              type="button"
                              className="secondary"
                              onClick={() => handleApprovalDecision(item.approval_id, 'REJECTED')}
                              disabled={busyKey === `${item.approval_id}:REJECTED`}
                            >
                              {busyKey === `${item.approval_id}:REJECTED` ? 'Rejecting...' : 'Reject'}
                            </button>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </section>

                <section className="mail-preview-section">
                  <div className="mail-preview-section-head">
                    <div className="mail-preview-section-title">Runbook</div>
                    {stepProgress && <span className="mail-meta-chip">{stepProgress}</span>}
                  </div>

                  {Array.isArray(caseDetail?.runbook_steps) && caseDetail.runbook_steps.length > 0 ? (
                    <div className="mail-mini-list">
                      {caseDetail.runbook_steps.map((step) => (
                        <div key={step.step_id} className="mail-mini-item">
                          <div className="mail-mini-title">
                            <span>{step.title}</span>
                            <span>{formatLabel(step.status)}</span>
                          </div>
                          {step.description && <p className="mail-mini-copy">{step.description}</p>}
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="mail-preview-copy">No runbook steps attached.</p>
                  )}
                </section>

                <section className="mail-preview-section">
                  <div className="mail-preview-section-head">
                    <div className="mail-preview-section-title">Tasks</div>
                    <span className="mail-meta-chip">{(caseDetail?.tasks || []).length}</span>
                  </div>

                  {(caseDetail?.tasks || []).length > 0 ? (
                    <div className="mail-mini-list">
                      {(caseDetail?.tasks || []).map((task) => (
                        <div key={task.task_id} className="mail-mini-item">
                          <div className="mail-mini-title">
                            <span>{task.title}</span>
                            <span>{formatLabel(task.status)}</span>
                          </div>
                          {task.description && <p className="mail-mini-copy">{task.description}</p>}
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="mail-preview-copy">No open tasks.</p>
                  )}
                </section>

                <section className="mail-preview-section">
                  <div className="mail-preview-section-head">
                    <div className="mail-preview-section-title">Timeline</div>
                    <span className="mail-meta-chip">{(caseDetail?.timeline || []).length}</span>
                  </div>

                  <div className="mail-scroll-list">
                    {(caseDetail?.timeline || []).length === 0 ? (
                      <p className="mail-preview-copy">No case activity yet.</p>
                    ) : (
                      (caseDetail?.timeline || []).map((event) => (
                        <div key={event.event_id} className="mail-mini-item">
                          <div className="mail-mini-title">
                            <span>{formatLabel(event.event_type)}</span>
                            <span>{formatStamp(event.created_at)}</span>
                          </div>
                          {event.body && <p className="mail-mini-copy">{event.body}</p>}
                          <div className="mail-row-meta">
                            <span>{formatLabel(event.actor_role)}</span>
                            {event.actor_id && <span>{event.actor_id}</span>}
                          </div>
                        </div>
                      ))
                    )}
                  </div>

                  <textarea
                    rows="4"
                    className="mail-preview-textarea"
                    value={noteBody}
                    placeholder="Add operator note"
                    onChange={(event) => setNoteBody(event.target.value)}
                  />

                  <div className="mail-action-row">
                    <button type="button" onClick={handleNote} disabled={busyKey === 'note' || !noteBody.trim()}>
                      {busyKey === 'note' ? 'Saving...' : 'Add note'}
                    </button>
                  </div>
                </section>

                <section className="mail-preview-section">
                  <div className="mail-preview-section-head">
                    <div className="mail-preview-section-title">Connector runs</div>
                    <span className="mail-meta-chip">{connectorRuns.length}</span>
                  </div>

                  {connectorRuns.length === 0 ? (
                    <p className="mail-preview-copy">No connector execution yet.</p>
                  ) : (
                    <div className="mail-mini-list">
                      {connectorRuns.map((run) => (
                        <div key={run.run_id} className="mail-mini-item">
                          <div className="mail-mini-title">
                            <span>{run.connector_name}</span>
                            <span>{formatLabel(run.status)}</span>
                          </div>
                          <div className="mail-row-meta">
                            {run.action_type && <span>{formatLabel(run.action_type)}</span>}
                            {run.connector_mode && <span>{formatLabel(run.connector_mode)}</span>}
                            {run.receipt_ref && <span>{`Receipt ${run.receipt_ref}`}</span>}
                            {run.external_ref && <span>{`External ${run.external_ref}`}</span>}
                          </div>
                          {run.error_message && <p className="mail-mini-copy">{run.error_message}</p>}
                        </div>
                      ))}
                    </div>
                  )}
                </section>
              </div>
            </div>
          </>
        )}
      </section>
    </div>
  );
}
