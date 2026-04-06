# CARD: High-Value Failed Transactions
severity: warning
icon: 💸
impact_metric: impact_high_value_failed
confidence_metric: signal_confidence
trigger:
  tool: compute_kpis
  condition: high_value_failed_count >= 1
copy:
  title: "{high_value_failed_count} high-value failed transaction(s)"
  explanation: "Failures above the high-ticket threshold (₹{high_value_ticket_threshold:,.0f}) total ₹{high_value_failed_gmv:,.0f}."
actions:
  - "Attempt assisted retry for top failed high-ticket payments."
  - "Route high-value payment attempts through the most reliable mode."
  - "Collect issuer/failure-code evidence for rapid escalation."
