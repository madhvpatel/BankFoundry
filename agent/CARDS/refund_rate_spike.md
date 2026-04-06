# CARD: Refund Rate Spike
severity: warning
icon: ↩️
impact_metric: impact_refund_reduction
confidence_metric: signal_confidence
trigger:
  tool: compute_kpis
  condition: refund_count_24h >= 2 and refund_rate_24h >= refund_rate_7d_avg * 1.5
copy:
  title: "Refund rate spike in last 24h"
  explanation: "Refund rate is {refund_rate_24h:.2f}% today vs {refund_rate_7d_avg:.2f}% 7-day average with {refund_count_24h} refunds (₹{refund_gmv_24h:,.0f})."
actions:
  - "Review top refund reasons and timestamp clusters."
  - "Audit cancellation and fulfillment touchpoints."
  - "Set temporary QA checks on high-refund channels."
