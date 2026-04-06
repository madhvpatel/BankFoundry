# CARD: Settlement Delay / Hold
severity: warning
icon: 🏦
impact_metric: impact_settlement_blocked
confidence_metric: signal_confidence
trigger:
  tool: compute_kpis
  condition: settlement_delayed_count >= 1
copy:
  title: "{settlement_delayed_count} settlement(s) delayed or on hold"
  explanation: "Delayed settlements include {settlement_held_count} held cases, with ₹{settlement_delayed_amount:,.0f} awaiting release."
actions:
  - "Review hold reasons and reconciliation exceptions."
  - "Open settlement support review with settlement IDs."
  - "Align expected cashflow with delayed payout window."
