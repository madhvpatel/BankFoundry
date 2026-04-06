# CARD: Terminal Failure Anomaly
severity: warning
icon: 🖥️
impact_metric: impact_terminal_anomaly
confidence_metric: signal_confidence
trigger:
  tool: compute_kpis
  condition: top_terminal_attempts >= 20 and terminal_fail_ratio >= 3
copy:
  title: "Terminal {top_terminal_id} failure rate is elevated"
  explanation: "Terminal fail rate is {top_terminal_fail_rate_pct:.2f}% vs merchant average {merchant_fail_rate_pct:.2f}% ({terminal_fail_ratio:.2f}x)."
actions:
  - "Run connectivity/device health checks on terminal {top_terminal_id}."
  - "Shift peak-hour traffic to healthier terminals if available."
  - "Open terminal support case with failure-hour evidence."
