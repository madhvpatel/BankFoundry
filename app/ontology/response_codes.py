from __future__ import annotations

from typing import Any


RESPONSE_CODE_MAP: dict[str, dict[str, str]] = {
    "00": {"meaning": "Approved / Transaction Successful", "category": "Success"},
    "51": {"meaning": "Insufficient Funds", "category": "Customer issue"},
    "55": {"meaning": "Incorrect PIN", "category": "Customer authentication"},
    "57": {"meaning": "Transaction Not Permitted to Cardholder", "category": "Card restriction"},
    "58": {"meaning": "Transaction Not Permitted to Terminal", "category": "Terminal configuration"},
    "61": {"meaning": "Exceeds Withdrawal / Transaction Limit", "category": "Card limit exceeded"},
    "62": {"meaning": "Restricted Card", "category": "Issuer restriction"},
    "70": {"meaning": "PIN Data Required / Invalid", "category": "Security validation"},
    "73": {"meaning": "PIN Tries Exceeded", "category": "Card security"},
    "75": {"meaning": "Allowable PIN Attempts Exceeded", "category": "Card blocked"},
    "91": {"meaning": "Issuer or Switch Inoperative", "category": "Network / issuer outage"},
    "93": {"meaning": "Transaction Cannot Be Completed (Violation of Law or Rules)", "category": "Risk / compliance"},
    "N1": {"meaning": "Suspected Fraud / Issuer Decline", "category": "Fraud prevention"},
    "UPI_FAILURE": {"meaning": "UPI failure (response code unavailable)", "category": "UPI network / issuer issue"},
    "UNMAPPED_FAILURE": {"meaning": "Failure code unavailable", "category": "Unclassified failure"},
    "UNKNOWN": {"meaning": "Failure code unavailable", "category": "Unclassified failure"},
}


def normalize_response_code(code: Any) -> str:
    if code is None:
        return "UNKNOWN"
    normalized = str(code).strip().upper()
    return normalized if normalized else "UNKNOWN"


def lookup_response_code(code: Any) -> dict[str, str] | None:
    normalized = normalize_response_code(code)
    return RESPONSE_CODE_MAP.get(normalized)


def canonical_response_desc(code: Any, fallback_desc: Any = None) -> str:
    info = lookup_response_code(code)
    if info:
        return info["meaning"]

    fallback = str(fallback_desc or "").strip()
    if fallback and fallback.upper() != "UNKNOWN":
        return fallback
    return "Failure code unavailable"


def canonical_response_category(code: Any) -> str:
    info = lookup_response_code(code)
    if info:
        return info["category"]
    return "Unclassified failure"


def format_response_code_label(code: Any) -> str:
    normalized = normalize_response_code(code)
    info = lookup_response_code(normalized)
    if not info:
        return normalized
    return f"{normalized} - {info['meaning']}"
