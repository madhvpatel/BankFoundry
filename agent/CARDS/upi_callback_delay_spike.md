# CARD: UPI Callback Delay Spike
severity: warning
icon: ⏱️
impact_metric: impact_callback_delay_revenue
confidence_metric: signal_confidence
trigger:
  tool: compute_kpis
  condition: callback_delay_ratio >= 2.0 and callback_delay_p95_ms_today >= 500
copy:
  title: "UPI callback/completion latency spike detected"
  explanation: "UPI p95 latency is {callback_delay_p95_ms_today:.0f} ms today vs {callback_delay_p95_ms_7d_avg:.0f} ms baseline ({callback_delay_ratio:.2f}x)."
actions:
  - "Request callback resync for last 24h UPI transactions."
  - "Watch pending-to-final conversion before escalating declines."
  - "Raise network/provider support ticket with latency evidence."
