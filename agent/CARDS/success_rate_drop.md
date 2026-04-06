# CARD: Success Rate Drop Detected
severity: warning
icon: 📉
impact_metric: impact_sr_drop_revenue
confidence_metric: signal_confidence
trigger:
  tool: compute_kpis
  condition: success_rate_drop_pp >= 1.5 and attempts_24h >= 50
copy:
  title: "Success rate down {success_rate_drop_pp:.2f}pp (24h vs 7d)"
  explanation: "Last 24h success is {success_rate_24h:.2f}% vs 7-day baseline {success_rate_7d_avg:.2f}%. Top failure clusters: {top_failure_codes}."
actions:
  - "Run a focused failure drilldown by hour and payment mode."
  - "Request callback resync if UPI status finalization is lagging."
  - "Open support ticket with top failure code evidence."
