# CARD: Chargeback Deadline Approaching
severity: warning
icon: ⚖️
impact_metric: impact_chargeback_at_risk
confidence_metric: signal_confidence
trigger:
  tool: compute_kpis
  condition: chargeback_due_48h_count >= 1
copy:
  title: "{chargeback_due_48h_count} chargeback case(s) due in 48h"
  explanation: "Open chargebacks due within 48h put ₹{chargeback_due_48h_gmv:,.0f} at immediate risk."
actions:
  - "Prepare and submit evidence pack for due cases now."
  - "Prioritize high-value cases by amount first."
  - "Track submission acknowledgements to avoid auto-loss."
