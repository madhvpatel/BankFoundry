"""
Canonical Schema Specification for Bank Foundry
================================================
Defines the exact table structures that Bank Foundry expects.
The schema mapper uses this as the target when translating foreign databases.
"""

CANONICAL_SCHEMA = {
    "transaction_features": {
        "description": (
            "Core payment transaction fact table. Each row represents one payment attempt. "
            "This is the primary table used by the SQL agent, dashboard KPIs, "
            "failure analysis, and all merchant-facing analytics."
        ),
        "required_columns": {
            "merchant_id": {
                "type": "TEXT",
                "description": "Merchant identifier. Every query filters on this.",
            },
            "p_date": {
                "type": "DATE",
                "description": "Transaction date. Used for date-range filters and trend grouping.",
            },
            "status": {
                "type": "TEXT",
                "description": "Transaction outcome. Canonical values: 'SUCCESS' or 'FAILURE'. Legacy rows may use 'FAILED'.",
            },
            "amount_rupees": {
                "type": "NUMERIC",
                "description": "Transaction amount in Indian Rupees. Used for all monetary calculations (GMV, revenue, ticket size).",
            },
        },
        "optional_columns": {
            "transaction_fact_id": {
                "type": "TEXT",
                "description": "Primary key / unique transaction identifier.",
            },
            "terminal_id": {
                "type": "TEXT",
                "description": "POS terminal identifier for terminal-level drilldowns.",
            },
            "payment_mode": {
                "type": "TEXT",
                "description": "Payment channel. Expected values: 'UPI' or 'CARD'.",
            },
            "response_code": {
                "type": "TEXT",
                "description": "Failure reason code (e.g., '51' for Insufficient Funds).",
            },
            "response_desc": {
                "type": "TEXT",
                "description": "Human-readable failure description.",
            },
            "initiated_at": {
                "type": "TIMESTAMPTZ",
                "description": "Timestamp when the transaction was initiated.",
            },
            "completed_at": {
                "type": "TIMESTAMPTZ",
                "description": "Timestamp when the transaction completed.",
            },
            "source_system": {
                "type": "TEXT",
                "description": "Originating gateway or provider name.",
            },
            "source_txn_id": {
                "type": "TEXT",
                "description": "Transaction ID from the source/gateway system.",
            },
            "invoice_nr": {
                "type": "TEXT",
                "description": "Invoice or order reference number.",
            },
            "currency": {
                "type": "TEXT",
                "description": "ISO currency code. Default 'INR'.",
            },
            "amount_paise": {
                "type": "NUMERIC",
                "description": "Raw amount in paise (1/100 of a rupee). Usually derived from amount_rupees * 100.",
            },
            "amount_bucket": {
                "type": "TEXT",
                "description": "Bucketed amount range for histogram analysis.",
            },
            "hour_of_day": {
                "type": "INTEGER",
                "description": "Hour (0-23) extracted from initiated_at. Used for peak-hour analysis.",
            },
            "day_of_week": {
                "type": "INTEGER",
                "description": "Day of week (0=Monday ... 6=Sunday).",
            },
            "is_weekend": {
                "type": "BOOLEAN",
                "description": "Whether the transaction occurred on Saturday or Sunday.",
            },
            "is_night": {
                "type": "BOOLEAN",
                "description": "Whether the transaction occurred between 22:00 and 06:00.",
            },
            "card_network": {
                "type": "TEXT",
                "description": "Card network (Visa, Mastercard, RuPay, etc.).",
            },
            "card_type": {
                "type": "TEXT",
                "description": "Credit or Debit.",
            },
            "sub_mode": {
                "type": "TEXT",
                "description": "Sub-classification of payment mode.",
            },
            "device_type": {
                "type": "TEXT",
                "description": "POS device type or terminal hardware model.",
            },
            "pos_type": {
                "type": "TEXT",
                "description": "Point of sale type (physical, virtual, mPOS, etc.).",
            },
            "pos_entry_mode": {
                "type": "TEXT",
                "description": "How the card was read (chip, swipe, contactless, manual).",
            },
            "upi_app_name": {
                "type": "TEXT",
                "description": "UPI app used (Google Pay, PhonePe, etc.).",
            },
            "upi_channel_code": {
                "type": "TEXT",
                "description": "UPI channel identifier.",
            },
            "upi_txn_type": {
                "type": "TEXT",
                "description": "UPI transaction type (collect, pay, etc.).",
            },
            "mcc": {
                "type": "TEXT",
                "description": "Merchant Category Code.",
            },
            "terminal_txn_count_1h": {
                "type": "INTEGER",
                "description": "Rolling 1-hour transaction count for the terminal.",
            },
            "merchant_txn_count_1h": {
                "type": "INTEGER",
                "description": "Rolling 1-hour transaction count for the merchant.",
            },
            "terminal_success_rate_1h": {
                "type": "NUMERIC",
                "description": "Rolling 1-hour success rate for the terminal.",
            },
            "merchant_success_rate_1h": {
                "type": "NUMERIC",
                "description": "Rolling 1-hour success rate for the merchant.",
            },
        },
    },
    "merchants": {
        "description": (
            "Merchant profile and onboarding data. "
            "Joined to transaction_features on merchants.mid = transaction_features.merchant_id."
        ),
        "required_columns": {
            "mid": {
                "type": "TEXT",
                "description": "Primary key. Matches merchant_id in transaction_features.",
            },
        },
        "optional_columns": {
            "merchant_legal_name": {
                "type": "TEXT",
                "description": "Registered legal entity name.",
            },
            "merchant_trade_name": {
                "type": "TEXT",
                "description": "Display / trading name shown in the UI.",
            },
            "merchant_type": {
                "type": "TEXT",
                "description": "Business entity type.",
            },
            "nature_of_business": {
                "type": "TEXT",
                "description": "Business category (e.g., 'Automobile Parking & Valet Services').",
            },
            "business_city": {
                "type": "TEXT",
                "description": "City where the business operates.",
            },
            "business_state": {
                "type": "TEXT",
                "description": "State where the business operates.",
            },
            "business_pincode": {
                "type": "TEXT",
                "description": "Postal code of the business address.",
            },
            "mcc_code": {
                "type": "TEXT",
                "description": "Merchant Category Code.",
            },
            "merchant_risk_category": {
                "type": "TEXT",
                "description": "Risk tier: 'LOW', 'MEDIUM', or 'HIGH'.",
            },
            "annual_turnover": {
                "type": "NUMERIC",
                "description": "Declared annual business turnover.",
            },
            "expected_monthly_volume": {
                "type": "NUMERIC",
                "description": "Expected monthly transaction volume.",
            },
            "expected_avg_ticket_size": {
                "type": "NUMERIC",
                "description": "Expected average transaction size.",
            },
            "onboarding_date": {
                "type": "DATE",
                "description": "Date the merchant was onboarded.",
            },
            "activation_date": {
                "type": "DATE",
                "description": "Date the merchant was activated for live transactions.",
            },
            "merchant_status": {
                "type": "TEXT",
                "description": "'ACTIVE' or 'INACTIVE'.",
            },
            "gst_number": {
                "type": "TEXT",
                "description": "GST registration number.",
            },
            "pan_number": {
                "type": "TEXT",
                "description": "PAN card number.",
            },
            "franchise_flag": {
                "type": "BOOLEAN",
                "description": "Whether the merchant is a franchise.",
            },
            "aggregator_id": {
                "type": "TEXT",
                "description": "Parent aggregator identifier, if applicable.",
            },
        },
    },
    "settlements": {
        "description": (
            "Settlement / payout records. Each row represents a settlement batch or payout cycle. "
            "Optional table — system degrades gracefully if absent."
        ),
        "required_columns": {
            "merchant_id": {
                "type": "TEXT",
                "description": "Merchant identifier, matches transaction_features.merchant_id.",
            },
        },
        "optional_columns": {
            "settlement_id": {
                "type": "TEXT",
                "description": "Unique settlement batch identifier.",
            },
            "status": {
                "type": "TEXT",
                "description": "Settlement status (SETTLED, PENDING, HELD, etc.).",
            },
            "expected_date": {
                "type": "DATE",
                "description": "Expected settlement / payout date.",
            },
            "settled_at": {
                "type": "TIMESTAMPTZ",
                "description": "Actual settlement timestamp.",
            },
            "amount_rupees": {
                "type": "NUMERIC",
                "description": "Net settlement amount in rupees.",
            },
            "gross_amount": {
                "type": "NUMERIC",
                "description": "Gross transaction amount before deductions.",
            },
            "net_settlement_amount": {
                "type": "NUMERIC",
                "description": "Final net payout after all deductions.",
            },
            "mdr_deducted": {
                "type": "NUMERIC",
                "description": "MDR (Merchant Discount Rate) deducted.",
            },
            "gst_on_mdr": {
                "type": "NUMERIC",
                "description": "GST charged on the MDR.",
            },
            "tds_deducted": {
                "type": "NUMERIC",
                "description": "TDS (Tax Deducted at Source) amount.",
            },
            "chargeback_deductions": {
                "type": "NUMERIC",
                "description": "Amount deducted for chargebacks.",
            },
            "reserve_held": {
                "type": "NUMERIC",
                "description": "Amount held in rolling reserve.",
            },
            "adjustment_amount": {
                "type": "NUMERIC",
                "description": "Miscellaneous adjustments.",
            },
            "hold_reason": {
                "type": "TEXT",
                "description": "Reason if the settlement is held.",
            },
            "payment_mode": {
                "type": "TEXT",
                "description": "Payment mode for the settlement batch.",
            },
            "txn_count": {
                "type": "INTEGER",
                "description": "Number of transactions in the batch.",
            },
            "refund_count": {
                "type": "INTEGER",
                "description": "Number of refunds in the batch.",
            },
        },
    },
}


def schema_to_prompt_text() -> str:
    """Render the canonical schema as a human-readable prompt block for the LLM."""
    lines: list[str] = ["CANONICAL BANK FOUNDRY SCHEMA (target):"]
    lines.append("=" * 60)
    for table_name, table_spec in CANONICAL_SCHEMA.items():
        lines.append("")
        lines.append(f"VIEW: {table_name}")
        lines.append(f"Description: {table_spec['description']}")
        lines.append("Required columns:")
        for col_name, col_spec in table_spec["required_columns"].items():
            lines.append(f"  - {col_name} ({col_spec['type']}) [REQUIRED] — {col_spec['description']}")
        lines.append("Optional columns:")
        for col_name, col_spec in table_spec["optional_columns"].items():
            lines.append(f"  - {col_name} ({col_spec['type']}) — {col_spec['description']}")
    return "\n".join(lines)
