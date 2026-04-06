import { useState } from 'react';
import { cleanupLegacyActions, updateActionDetails, updateActionStatus } from '../api';

export default function ActionCenterView({ snapshot, merchantId, onChanged, onPromoteAction, promotionBusy }) {
  const [busyKey, setBusyKey] = useState('');
  const [cleanupMessage, setCleanupMessage] = useState('');
  const actions = snapshot?.existing_actions || [];

  const handleStatus = async (actionId, status) => {
    const key = `${actionId}:${status}`;
    setBusyKey(key);
    try {
      await updateActionStatus({ merchantId, actionId, status });
      onChanged?.();
    } finally {
      setBusyKey('');
    }
  };

  const handleDetails = async (action) => {
    const actionId = action.action_id;
    const key = `${actionId}:details`;
    const form = document.getElementById(`action-form-${actionId}`);
    if (!form) return;
    const owner = form.querySelector('[name="owner"]')?.value || '';
    const notes = form.querySelector('[name="notes"]')?.value || '';
    const blockedReason = form.querySelector('[name="blockedReason"]')?.value || '';
    const followUpDate = form.querySelector('[name="followUpDate"]')?.value || '';
    setBusyKey(key);
    try {
      await updateActionDetails({ merchantId, actionId, owner, notes, blockedReason, followUpDate });
      onChanged?.();
    } finally {
      setBusyKey('');
    }
  };

  const handleCleanup = async () => {
    setBusyKey('cleanup');
    try {
      const result = await cleanupLegacyActions({ merchantId });
      setCleanupMessage(`Hidden ${result.hidden_count || 0} legacy or duplicate action(s).`);
      onChanged?.();
    } finally {
      setBusyKey('');
    }
  };

  return (
    <div className="view-stack">
      <div className="glass-card section-card">
        <div className="section-header">
          <div>
            <div className="eyebrow">Queue hygiene</div>
            <h3>Action Center</h3>
          </div>
          <button onClick={handleCleanup} disabled={busyKey === 'cleanup'}>
            {busyKey === 'cleanup' ? 'Cleaning…' : 'Hide legacy and duplicate items'}
          </button>
        </div>
        {cleanupMessage && <p className="info-text">{cleanupMessage}</p>}
      </div>

      {actions.length === 0 ? (
        <div className="glass-card section-card">
          <p className="empty-inline">No persisted actions are present for this merchant yet.</p>
        </div>
      ) : (
        <div className="stack-list">
          {actions.map((action) => {
            const status = String(action.status || 'UNKNOWN').toUpperCase();
            const closed = ['CLOSED', 'RESOLVED', 'DONE'].includes(status);
            return (
              <div key={action.action_id || action.title} className="glass-card section-card">
                <div className="sub-card-header">
                  <div>
                    <h3>{action.title || action.category || 'Action'}</h3>
                    <p className="muted-line">status: {status} | category: {action.category || 'unknown'}</p>
                  </div>
                  <div className="button-row">
                    {!['IN_PROGRESS', 'CLOSED', 'RESOLVED', 'DONE'].includes(status) && (
                      <button
                        onClick={() => handleStatus(action.action_id, 'IN_PROGRESS')}
                        disabled={busyKey === `${action.action_id}:IN_PROGRESS`}
                      >
                        Mark in progress
                      </button>
                    )}
                    {!closed && (
                      <button
                        className="secondary"
                        onClick={() => handleStatus(action.action_id, 'CLOSED')}
                        disabled={busyKey === `${action.action_id}:CLOSED`}
                      >
                        Close action
                      </button>
                    )}
                  </div>
                </div>

                {action.description && <p>{action.description}</p>}
                <div className="inline-meta">
                  {action.source && <span>source {action.source}</span>}
                  {action.owner && <span>owner {action.owner}</span>}
                  {action.follow_up_date && <span>follow-up {action.follow_up_date}</span>}
                </div>

                <form id={`action-form-${action.action_id}`} className="detail-grid" onSubmit={(e) => e.preventDefault()}>
                  <input name="owner" placeholder="Owner" defaultValue={action.owner || ''} />
                  <input name="followUpDate" type="date" defaultValue={action.follow_up_date || ''} />
                  <input name="blockedReason" placeholder="Blocked reason" defaultValue={action.blocked_reason || ''} />
                  <textarea name="notes" rows="3" placeholder="Notes" defaultValue={action.notes || ''} />
                </form>
                <div className="button-row">
                  <button
                    onClick={() => handleDetails(action)}
                    disabled={busyKey === `${action.action_id}:details`}
                  >
                    Save details
                  </button>
                  {typeof onPromoteAction === 'function' && action.action_id != null && (
                    <button
                      className="secondary"
                      onClick={() => onPromoteAction(action)}
                      disabled={promotionBusy === `merchant_action:${action.action_id}`}
                    >
                      {promotionBusy === `merchant_action:${action.action_id}` ? 'Creating case…' : 'Create ops case'}
                    </button>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
