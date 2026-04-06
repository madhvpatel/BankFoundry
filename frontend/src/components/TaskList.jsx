import { useState } from 'react';
import { confirmAction, previewAction } from '../api';

export default function TaskList({ merchantId, tasks, emptyText, title, onChanged }) {
  const [busyKey, setBusyKey] = useState('');
  const [previewState, setPreviewState] = useState({});
  const safeTasks = Array.isArray(tasks) ? tasks.filter((task) => task && typeof task === 'object') : [];

  const handlePreview = async (task, idx) => {
    const key = `${task.action_type || 'task'}-${idx}`;
    setBusyKey(key);
    try {
      const preview = await previewAction({
        merchantId,
        actionType: task.action_type || 'FOLLOW_UP',
        payload: task.payload || {},
      });
      setPreviewState((prev) => ({ ...prev, [key]: preview }));
    } finally {
      setBusyKey('');
    }
  };

  const handleConfirm = async (task, idx) => {
    const key = `${task.action_type || 'task'}-${idx}`;
    const preview = previewState[key];
    if (!preview?.confirmation_token) return;
    setBusyKey(key);
    try {
      const result = await confirmAction({
        merchantId,
        confirmationToken: preview.confirmation_token,
      });
      setPreviewState((prev) => ({ ...prev, [key]: { ...preview, confirm_result: result } }));
      onChanged?.();
    } finally {
      setBusyKey('');
    }
  };

  return (
    <div className="glass-card section-card">
      {title && <h3>{title}</h3>}
      {safeTasks.length === 0 ? (
        <p className="empty-inline">{emptyText}</p>
      ) : (
        <div className="stack-list">
          {safeTasks.map((task, idx) => {
            const key = `${task.action_type || 'task'}-${idx}`;
            const preview = previewState[key];
            return (
              <div key={key} className="sub-card">
                <div className="sub-card-header">
                  <strong>{task.title || 'Untitled task'}</strong>
                  {task.priority && <span className="badge neutral">{task.priority}</span>}
                </div>
                {task.description && <p>{task.description}</p>}
                <div className="inline-meta">
                  {task.confidence !== undefined && <span>confidence {Number(task.confidence).toFixed(2)}</span>}
                  {task.priority_score !== undefined && <span>score {Number(task.priority_score).toFixed(1)}</span>}
                </div>
                <div className="button-row">
                  <button onClick={() => handlePreview(task, idx)} disabled={busyKey === key}>
                    {busyKey === key ? 'Loading…' : 'Preview action'}
                  </button>
                  {preview?.confirmation_token && (
                    <button className="secondary" onClick={() => handleConfirm(task, idx)} disabled={busyKey === key}>
                      Create action
                    </button>
                  )}
                </div>
                {preview && (
                  <details className="compact-details">
                    <summary>Action payload</summary>
                    <pre>{JSON.stringify(preview, null, 2)}</pre>
                  </details>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
