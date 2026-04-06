const API_BASE = import.meta.env.VITE_API_BASE || 'http://127.0.0.1:8000';

async function request(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, options);
  if (!response.ok) {
    throw new Error(await response.text());
  }
  return response.json();
}

export async function fetchMerchantOptions() {
  return request('/api/v1/merchants/options');
}

export async function askRevenueRecoveryPreview({
  merchantId,
  prompt,
  userRole = 'ops',
  requestedActionLevel = 'read_only',
}) {
  return request('/api/v1/revenue-recovery/preview/ask', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      merchant_id: merchantId || undefined,
      prompt,
      user_role: userRole,
      requested_action_level: requestedActionLevel,
    }),
  });
}
