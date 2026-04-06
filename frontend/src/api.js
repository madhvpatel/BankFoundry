const API_BASE = import.meta.env.VITE_API_BASE || 'http://127.0.0.1:8000';

async function request(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, options);
  if (!response.ok) {
    throw new Error(await response.text());
  }
  return response.json();
}

export async function askAgent({ merchantId, prompt, terminalId, threadScope, history = [], debug = false }) {
  return request('/api/v1/ask', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
      prompt,
      terminal_id: terminalId || undefined,
      thread_scope: threadScope || undefined,
      history,
      debug,
    }),
  });
}

export async function uploadDisputeReceipt({ merchantId, file, context = {} }) {
  const formData = new FormData();
  formData.append('file', file);
  if (merchantId) formData.append('merchant_id', merchantId);
  if (Object.keys(context).length > 0) {
    formData.append('context', JSON.stringify(context));
  }
  
  const response = await fetch(`${API_BASE}/api/v1/dispute/upload-receipt`, {
    method: 'POST',
    body: formData,
  });
  
  if (!response.ok) {
    throw new Error(await response.text());
  }
  return response.json();
}

export async function fetchMerchantOptions() {
  return request('/api/v1/merchants/options');
}

export async function fetchMerchantSnapshot({ merchantId, terminalId, days = 30 }) {
  const params = new URLSearchParams();
  if (merchantId) params.set('merchant_id', merchantId);
  if (terminalId) params.set('terminal_id', terminalId);
  params.set('days', String(days));
  return request(`/api/v1/merchant/snapshot?${params}`);
}

export async function fetchDashboardMetrics({ merchantId, terminalId }) {
  const params = new URLSearchParams();
  if (merchantId) params.set('merchant_id', merchantId);
  if (terminalId) params.set('terminal_id', terminalId);
  return request(`/api/v1/analytics/dashboard?${params}`);
}

export async function fetchMerchantReports({ merchantId, terminalId, days = 30 }) {
  const params = new URLSearchParams();
  if (merchantId) params.set('merchant_id', merchantId);
  if (terminalId) params.set('terminal_id', terminalId);
  params.set('days', String(days));
  return request(`/api/v1/merchant/reports?${params}`);
}

export async function refreshProactive({ merchantId, days = 30, force = true }) {
  return request('/api/v1/copilot/proactive/refresh', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
      days,
      force,
    }),
  });
}

export async function updateProactiveCardState({ merchantId, dedupeKey, state, cardNotes }) {
  return request('/api/v1/copilot/proactive/card/state', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
      dedupe_key: dedupeKey,
      state,
      card_notes: cardNotes || undefined,
    }),
  });
}

export async function previewProactiveCardAction({ merchantId, dedupeKey }) {
  return request('/api/v1/copilot/proactive/card/preview-action', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
      dedupe_key: dedupeKey,
    }),
  });
}

export async function confirmProactiveCardAction({ merchantId, dedupeKey, confirmationToken }) {
  return request('/api/v1/copilot/proactive/card/confirm-action', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
      dedupe_key: dedupeKey,
      confirmation_token: confirmationToken,
    }),
  });
}

export async function previewAction({ merchantId, actionType, payload }) {
  return request('/api/v1/actions/preview', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
      action_type: actionType,
      payload: payload || {},
    }),
  });
}

export async function confirmAction({ merchantId, confirmationToken }) {
  return request('/api/v1/actions/confirm', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
      confirmation_token: confirmationToken,
    }),
  });
}

export async function updateActionStatus({ merchantId, actionId, status }) {
  return request('/api/v1/actions/status', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
      action_id: actionId,
      status,
    }),
  });
}

export async function updateActionDetails({ merchantId, actionId, owner, notes, blockedReason, followUpDate }) {
  return request('/api/v1/actions/details', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
      action_id: actionId,
      owner: owner || undefined,
      notes: notes || undefined,
      blocked_reason: blockedReason || undefined,
      follow_up_date: followUpDate || undefined,
    }),
  });
}

export async function cleanupLegacyActions({ merchantId }) {
  return request('/api/v1/actions/cleanup-legacy', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
    }),
  });
}

export async function fetchOpsQueue({ merchantId, lane = 'operations', role = 'acquiring_ops', status, owner, limit = 25 }) {
  const params = new URLSearchParams();
  if (merchantId) params.set('merchant_id', merchantId);
  params.set('lane', lane);
  params.set('role', role);
  if (status) params.set('status', status);
  if (owner) params.set('owner', owner);
  params.set('limit', String(limit));
  return request(`/api/v1/ops/queue?${params}`);
}

export async function fetchOpsCaseDetail({ merchantId, caseId, role = 'acquiring_ops' }) {
  const params = new URLSearchParams();
  if (merchantId) params.set('merchant_id', merchantId);
  params.set('role', role);
  return request(`/api/v1/ops/cases/${encodeURIComponent(caseId)}?${params}`);
}

export async function createOpsCase({
  merchantId,
  terminalId,
  lane = 'operations',
  role = 'acquiring_ops',
  caseType,
  title,
  summary,
  priority = 'medium',
  severity,
  owner,
  evidenceIds = [],
  links = [],
}) {
  return request('/api/v1/ops/cases', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
      terminal_id: terminalId || undefined,
      lane,
      role,
      case_type: caseType,
      title,
      summary,
      priority,
      severity: severity || undefined,
      owner: owner || undefined,
      evidence_ids: evidenceIds,
      links,
    }),
  });
}

export async function promoteOpsCase({ merchantId, lane = 'operations', role = 'acquiring_ops', sourceType, sourceRef, sourcePayload = {} }) {
  return request('/api/v1/ops/cases/promote', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
      lane,
      role,
      source_type: sourceType,
      source_ref: sourceRef || undefined,
      source_payload: sourcePayload,
    }),
  });
}

export async function assignOpsCase({ merchantId, caseId, role = 'acquiring_ops', owner }) {
  return request(`/api/v1/ops/cases/${encodeURIComponent(caseId)}/assign`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
      role,
      owner,
    }),
  });
}

export async function addOpsCaseNote({ merchantId, caseId, role = 'acquiring_ops', body }) {
  return request(`/api/v1/ops/cases/${encodeURIComponent(caseId)}/notes`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
      role,
      body,
    }),
  });
}

export async function fetchOpsCaseCopilot({ merchantId, caseId, role = 'acquiring_ops', prompt }) {
  return request(`/api/v1/ops/cases/${encodeURIComponent(caseId)}/copilot`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
      role,
      prompt: prompt || undefined,
    }),
  });
}

export async function updateOpsCaseMemory({
  merchantId,
  caseId,
  role = 'acquiring_ops',
  settlementId,
  startDate,
  endDate,
  evidenceIds = [],
  clearPinnedContext = false,
  clearWindow = false,
  clearEvidence = false,
}) {
  return request(`/api/v1/ops/cases/${encodeURIComponent(caseId)}/memory`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
      role,
      settlement_id: settlementId ?? undefined,
      start_date: startDate ?? undefined,
      end_date: endDate ?? undefined,
      evidence_ids: evidenceIds,
      clear_pinned_context: clearPinnedContext,
      clear_window: clearWindow,
      clear_evidence: clearEvidence,
    }),
  });
}

export async function requestOpsApproval({ merchantId, caseId, role = 'acquiring_ops', actionType, payloadSummary, payload = {} }) {
  return request(`/api/v1/ops/cases/${encodeURIComponent(caseId)}/approval`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
      role,
      action_type: actionType,
      payload_summary: payloadSummary,
      payload,
    }),
  });
}

export async function resolveOpsCase({ merchantId, caseId, role = 'acquiring_ops', resolutionNote, status = 'RESOLVED' }) {
  return request(`/api/v1/ops/cases/${encodeURIComponent(caseId)}/resolve`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
      role,
      resolution_note: resolutionNote || undefined,
      status,
    }),
  });
}

export async function fetchOpsApprovals({ merchantId, lane = 'operations', role = 'acquiring_ops', status = 'PENDING', limit = 25 }) {
  const params = new URLSearchParams();
  if (merchantId) params.set('merchant_id', merchantId);
  params.set('lane', lane);
  params.set('role', role);
  params.set('status', status);
  params.set('limit', String(limit));
  return request(`/api/v1/ops/approvals?${params}`);
}

export async function decideOpsApproval({ merchantId, approvalId, lane = 'operations', role = 'acquiring_ops', decision, notes }) {
  return request(`/api/v1/ops/approvals/${encodeURIComponent(approvalId)}/decision`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
      lane,
      role,
      decision,
      notes: notes || undefined,
    }),
  });
}
