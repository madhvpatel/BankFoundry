def actions_for_failure_codes(top_codes: list[str]) -> list[dict]:
    actions = []

    codes = set(c for c in top_codes if c)
    if "91" in codes:
        actions += [
            {"who": "merchant", "text": "Enable smart retry (3 attempts over 60–90s) for issuer unavailable cases."},
            {"who": "bank", "text": "Monitor issuer outage patterns; consider alternate routing where supported."},
        ]
    if any(c.startswith("U") for c in codes):
        actions += [
            {"who": "merchant", "text": "Show 'Try another UPI app' prompt on UPI failures."},
            {"who": "merchant", "text": "For high-value payments, offer UPI Collect fallback."},
        ]
    if not actions:
        actions += [{"who": "merchant", "text": "Review top failure codes and enable retry/fallback for technical failures."}]
    return actions
