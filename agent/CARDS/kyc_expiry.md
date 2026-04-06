# CARD: KYC Expiry Risk
severity: info
icon: 🧾
confidence_metric: signal_confidence
trigger:
  tool: compute_kpis
  condition: kyc_expiring_14d_count >= 1 or kyc_overdue_count >= 1
copy:
  title: "KYC documents need attention"
  explanation: "{kyc_expiring_14d_count} document(s) expire in 14 days and {kyc_overdue_count} are already overdue."
actions:
  - "Submit renewal documents before expiry to avoid interruptions."
  - "Track verification status after submission."
  - "Escalate pending verification if due date is near."
