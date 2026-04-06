"""
dispute_context.py — Formats receipt evidence into a chat context prefix.

The output string is prepended to the merchant's next prompt so the
chat agent (via /api/v1/ask) receives both the evidence and the question
in a single turn without requiring chat history changes.
"""
from __future__ import annotations

from typing import Any


def build_dispute_context_string(
    receipt_evidence: dict[str, Any],
    merchant_question: str = "",
) -> str:
    """Build a structured context prefix to inject before the merchant prompt.

    Output format (plain text, not JSON):

        [RECEIPT EVIDENCE — uploaded by merchant]
        Amount      : ₹4,500.00
        Date        : 2025-03-28
        TxID        : TXN123456
        Card        : ****8821 (VISA)
        Terminal    : TID-004
        Confidence  : high
        Evidence ID : receipt:MERCHANT:HASH
        OCR excerpt : "Sale Amount ... Approved ..."

        Merchant question: <original prompt>

    The agent is then expected to compare this against the database record
    and explain match / mismatch.
    """
    extracted = receipt_evidence.get("extracted") or {}
    evidence_id = receipt_evidence.get("evidence_id", "receipt:unknown")
    confidence = receipt_evidence.get("confidence", "low")
    context_meta = receipt_evidence.get("context") or {}

    amount = extracted.get("amount")
    currency = extracted.get("currency", "INR")
    date = extracted.get("date")
    tx_id = extracted.get("transaction_id")
    card_last4 = extracted.get("card_last4")
    card_scheme = extracted.get("card_scheme")
    terminal_id = extracted.get("terminal_id")
    approval_code = extracted.get("approval_code")
    raw_ocr = (extracted.get("raw_ocr") or "").strip()[:300]

    def _val(v: Any, prefix: str = "") -> str:
        return f"{prefix}{v}" if v and v != "null" else "not detected"

    card_str = "not detected"
    if card_last4 and card_last4 != "null":
        card_str = f"****{card_last4}"
        if card_scheme and card_scheme != "null":
            card_str += f" ({card_scheme})"

    lines = [
        "[RECEIPT EVIDENCE — uploaded by merchant]",
        f"Amount      : {_val(amount, '₹')} {currency if amount and amount != 'null' else ''}".rstrip(),
        f"Date        : {_val(date)}",
        f"TxID        : {_val(tx_id)}",
        f"Approval    : {_val(approval_code)}",
        f"Card        : {card_str}",
        f"Terminal    : {_val(terminal_id)}",
        f"Confidence  : {confidence}",
        f"Evidence ID : {evidence_id}",
    ]

    if context_meta.get("settlement_id"):
        lines.append(f"Settlement  : {context_meta['settlement_id']}")
    if context_meta.get("chargeback_id"):
        lines.append(f"Chargeback  : {context_meta['chargeback_id']}")

    if raw_ocr:
        lines.append(f'OCR excerpt : "{raw_ocr}"')

    lines.append("")

    if merchant_question:
        lines.append(f"Merchant question: {merchant_question}")

    return "\n".join(lines)
