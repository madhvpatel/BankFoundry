import { useState } from 'react';
import { updateProactiveCardState } from '../api';
import { formatCurrency } from '../utils';

function laneLabel(lane) {
  return lane === 'growth' ? 'Growth' : 'Ops';
}

export default function ProactiveView({
  cards,
  loading,
  merchantId,
  onChanged,
  onRefresh,
  refreshStatus,
  onPromoteCard,
  promotionBusy,
}) {
  const [busyKey, setBusyKey] = useState('');
  const [selectedKey, setSelectedKey] = useState('');

  if (loading) {
    return (
      <div className="empty-state">
        <div className="thinking-dots"><span></span><span></span><span></span></div>
        <p>Fetching proactive insights…</p>
      </div>
    );
  }

  const safeCards = Array.isArray(cards) ? cards.filter((card) => card && typeof card === 'object') : [];

  if (safeCards.length === 0) {
    return (
      <div className="empty-state">
        <div className="icon">📬</div>
        <h3>Inbox is clear</h3>
        <p>No proactive merchant signals are active right now.</p>
        <button onClick={onRefresh}>Refresh proactive cards</button>
      </div>
    );
  }

  const selectedCard = safeCards.find((card) => card.dedupe_key === selectedKey) || safeCards[0];
  const growthCount = safeCards.filter((card) => card.lane === 'growth').length;
  const opsCount = safeCards.length - growthCount;
  const linkedCaseCount = safeCards.filter((card) => card.linked_case_id).length;

  const handleState = async (card, state) => {
    const key = `${card.dedupe_key}:${state}`;
    setBusyKey(key);
    const notes = document.getElementById(`card-notes-${card.dedupe_key}`)?.value || '';
    try {
      await updateProactiveCardState({
        merchantId,
        dedupeKey: card.dedupe_key,
        state,
        cardNotes: notes,
      });
      onChanged?.();
    } finally {
      setBusyKey('');
    }
  };

  return (
    <div className="mail-surface">
      <section className="mail-sidebar">
        <div className="mail-toolbar">
          <div>
            <h3>Inbox</h3>
            <p>{safeCards.length} items</p>
          </div>
          <button type="button" className="secondary" onClick={onRefresh}>Refresh</button>
        </div>

        <div className="mail-toolbar-meta">
          <span className="mail-meta-chip">{growthCount} growth</span>
          <span className="mail-meta-chip">{opsCount} ops</span>
          <span className="mail-meta-chip">{linkedCaseCount} linked</span>
          {refreshStatus?.next_refresh_at && (
            <span className="mail-meta-chip">Next {refreshStatus.next_refresh_at}</span>
          )}
        </div>

        <div className="mail-list mail-list-scroll">
          {safeCards.map((card) => {
            const isSelected = selectedCard?.dedupe_key === card.dedupe_key;
            return (
              <button
                key={card.dedupe_key}
                type="button"
                className={`mail-row ${isSelected ? 'active' : ''}`}
                onClick={() => setSelectedKey(card.dedupe_key)}
              >
                <div className="mail-row-top">
                  <span className={`mail-row-tag ${card.lane === 'growth' ? 'growth' : 'ops'}`}>
                    {laneLabel(card.lane)}
                  </span>
                  <span className="mail-row-status">{card.card_state || 'NEW'}</span>
                </div>
                <strong className="mail-row-subject">{card.title || 'Untitled signal'}</strong>
                <p className="mail-row-snippet">{card.body || 'No detail available.'}</p>
                <div className="mail-row-meta">
                  {card.impact_rupees != null && <span>{formatCurrency(card.impact_rupees)}</span>}
                  {card.confidence != null && <span>{Number(card.confidence).toFixed(2)}</span>}
                  {card.linked_case_id && <span>{`Case ${card.linked_case_id}`}</span>}
                </div>
              </button>
            );
          })}
        </div>
      </section>

      <section className="mail-preview">
        <div className="mail-preview-header">
          <div>
            <div className="mail-preview-topline">
              <span className={`mail-row-tag ${selectedCard.lane === 'growth' ? 'growth' : 'ops'}`}>
                {laneLabel(selectedCard.lane)}
              </span>
              <span className="mail-row-status">{selectedCard.card_state || 'NEW'}</span>
            </div>
            <h3 className="mail-preview-subject">{selectedCard.title || 'Untitled signal'}</h3>
            <div className="mail-preview-meta">
              {selectedCard.confidence != null && <span>{`Confidence ${Number(selectedCard.confidence).toFixed(2)}`}</span>}
              {selectedCard.impact_rupees != null && <span>{`Impact ${formatCurrency(selectedCard.impact_rupees)}`}</span>}
              {selectedCard.verification_status && <span>{selectedCard.verification_status}</span>}
              {selectedCard.linked_case_id && <span>{`Linked ${selectedCard.linked_case_id}`}</span>}
            </div>
          </div>
          <div className="mail-preview-actions">
            <button
              type="button"
              onClick={() => handleState(selectedCard, 'ACKNOWLEDGED')}
              disabled={busyKey === `${selectedCard.dedupe_key}:ACKNOWLEDGED`}
            >
              {busyKey === `${selectedCard.dedupe_key}:ACKNOWLEDGED` ? 'Saving…' : 'Acknowledge'}
            </button>
            <button
              type="button"
              className="secondary"
              onClick={() => handleState(selectedCard, 'DISMISSED')}
              disabled={busyKey === `${selectedCard.dedupe_key}:DISMISSED`}
            >
              {busyKey === `${selectedCard.dedupe_key}:DISMISSED` ? 'Saving…' : 'Dismiss'}
            </button>
            {typeof onPromoteCard === 'function' && (
              <button
                type="button"
                className="secondary"
                onClick={() => onPromoteCard(selectedCard)}
                disabled={promotionBusy === `proactive_card:${selectedCard.dedupe_key}` || Boolean(selectedCard.linked_case_id)}
              >
                {selectedCard.linked_case_id
                  ? 'Ops case linked'
                  : promotionBusy === `proactive_card:${selectedCard.dedupe_key}`
                    ? 'Creating…'
                    : 'Create ops case'}
              </button>
            )}
          </div>
        </div>

        <div className="mail-preview-body">
          <div className="mail-preview-section">
            <p className="mail-preview-copy">{selectedCard.body || 'No detail available.'}</p>
          </div>

          <div className="mail-preview-section">
            <div className="mail-preview-section-title">Note</div>
            <textarea
              key={selectedCard.dedupe_key}
              id={`card-notes-${selectedCard.dedupe_key}`}
              rows="7"
              className="mail-preview-textarea"
              placeholder="Add note"
              defaultValue={selectedCard.card_notes || ''}
            />
          </div>
        </div>
      </section>
    </div>
  );
}
