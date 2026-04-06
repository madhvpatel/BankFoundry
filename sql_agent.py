"""
Dedicated SQL Agent for AcquiGuru
=================================
Converts natural language questions to SQL, executes safely, returns raw data.
The main orchestrator calls this as a tool and uses the results to form answers.
"""

import re
import logging
from sqlalchemy import create_engine, text
from app.intelligence.money import get_amount_scale, scale_inr
from app.intelligence.response_codes import (
    canonical_response_category,
    canonical_response_desc,
    normalize_response_code,
)

logger = logging.getLogger("sql_agent")

# ─────────────────────────────────────────────
# TABLE INFO — baked-in schema for the LLM
# ─────────────────────────────────────────────

TABLE_INFO = """
DATABASE SCHEMA:

TABLE: transaction_features
Description: Core payment transaction fact table. Each row = one payment attempt.
Columns:
  - transaction_fact_id (TEXT, PK)
  - source_system (TEXT)
  - source_txn_id (TEXT)
  - merchant_id (TEXT) — FK to merchants.mid. ALWAYS filter on this.
  - terminal_id (TEXT)
  - invoice_nr (TEXT)
  - payment_mode (TEXT) — ONLY valid values: 'UPI' or 'CARD'. No Cash/Wallet/NetBanking.
  - sub_mode (TEXT)
  - card_network (TEXT)
  - card_type (TEXT)
  - status (TEXT) — Canonical values: 'SUCCESS' or 'FAILURE'. Some historical rows may use legacy 'FAILED'.
  - response_code (TEXT) — failure reason code
  - response_desc (TEXT) — failure description
  - currency (TEXT)
  - amount_paise (NUMERIC) — raw amount in paise (DO NOT use for reports)
  - amount_rupees (NUMERIC) — amount in rupees (USE THIS for all monetary calculations)
  - amount_bucket (TEXT)
  - p_date (DATE) — transaction date. Use for date ranges and trends.
  - initiated_at (TIMESTAMPTZ)
  - completed_at (TIMESTAMPTZ)
  - hour_of_day (INTEGER) — 0-23
  - day_of_week (INTEGER) — 0=Monday, 1=Tuesday ... 6=Sunday
  - is_weekend (BOOLEAN)
  - is_night (BOOLEAN)
  - pos_type (TEXT)
  - pos_entry_mode (TEXT)
  - device_type (TEXT)
  - upi_app_name (TEXT)
  - upi_channel_code (TEXT)
  - upi_txn_type (TEXT)
  - terminal_txn_count_1h (INTEGER)
  - merchant_txn_count_1h (INTEGER)
  - terminal_success_rate_1h (NUMERIC)
  - merchant_success_rate_1h (NUMERIC)
  - mcc (TEXT)

TABLE: merchants
Description: Merchant profile and onboarding data. Join on merchants.mid = transaction_features.merchant_id.
Columns:
  - mid (TEXT, PK) — matches merchant_id in transaction_features
  - merchant_legal_name (TEXT)
  - merchant_trade_name (TEXT) — display name
  - merchant_type (TEXT)
  - nature_of_business (TEXT) — business category (e.g. 'Automobile Parking & Valet Services')
  - business_city (TEXT)
  - business_state (TEXT)
  - business_pincode (TEXT)
  - mcc_code (TEXT)
  - merchant_risk_category (TEXT) — 'LOW', 'MEDIUM', or 'HIGH'
  - annual_turnover (NUMERIC)
  - expected_monthly_volume (NUMERIC)
  - expected_avg_ticket_size (NUMERIC)
  - onboarding_date (DATE)
  - activation_date (DATE)
  - merchant_status (TEXT) — 'ACTIVE' or 'INACTIVE'
  - gst_number (TEXT)
  - pan_number (TEXT)
  - franchise_flag (BOOLEAN)
  - aggregator_id (TEXT)

CRITICAL RULES:
- Use amount_rupees for all monetary calculations (NEVER amount or amount_paise).
- The ONLY payment_mode values are 'UPI' and 'CARD'.
- For failed analytics, use status IN ('FAILURE','FAILED').
- ALWAYS filter by merchant_id when the query is about a specific merchant.
- Canonical response code meanings:
  00=Approved, 51=Insufficient Funds, 55=Incorrect PIN, 57=Cardholder Not Permitted,
  58=Terminal Not Permitted, 61=Exceeds Limit, 62=Restricted Card, 70=PIN Invalid,
  73=PIN Tries Exceeded, 75=PIN Attempts Exceeded, 91=Issuer/Switch Inoperative,
  93=Violation of Law/Rules, N1=Suspected Fraud.
- Output ONLY the SQL query. No explanations, no markdown fences, no 'sql' prefix.
"""

# ─────────────────────────────────────────────
# FEW-SHOT EXAMPLES
# ─────────────────────────────────────────────

FEW_SHOT_EXAMPLES = """
EXAMPLES (Question → SQL):

Q: What is the total revenue for this merchant?
SQL: SELECT SUM(amount_rupees) as total_revenue FROM transaction_features WHERE merchant_id = '{mid}' AND status = 'SUCCESS'

Q: How many transactions does this merchant have?
SQL: SELECT COUNT(*) as total_transactions FROM transaction_features WHERE merchant_id = '{mid}'

Q: What is the success rate?
SQL: SELECT ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct FROM transaction_features WHERE merchant_id = '{mid}'

Q: What are the top failure codes?
SQL: SELECT response_code, response_desc, COUNT(*) as count FROM transaction_features WHERE merchant_id = '{mid}' AND status IN ('FAILURE','FAILED') GROUP BY response_code, response_desc ORDER BY count DESC LIMIT 10

Q: Show me the payment mode breakdown
SQL: SELECT payment_mode, COUNT(*) as txn_count, SUM(amount_rupees) as total_amount FROM transaction_features WHERE merchant_id = '{mid}' AND status = 'SUCCESS' GROUP BY payment_mode ORDER BY txn_count DESC

Q: What is the daily revenue trend?
SQL: SELECT p_date, SUM(amount_rupees) as daily_revenue FROM transaction_features WHERE merchant_id = '{mid}' AND status = 'SUCCESS' GROUP BY p_date ORDER BY p_date

Q: What is the average ticket size?
SQL: SELECT ROUND(AVG(amount_rupees)::numeric, 2) as avg_ticket_size FROM transaction_features WHERE merchant_id = '{mid}' AND status = 'SUCCESS'

Q: Which hours have the most transactions?
SQL: SELECT hour_of_day, COUNT(*) as txn_count FROM transaction_features WHERE merchant_id = '{mid}' GROUP BY hour_of_day ORDER BY txn_count DESC

Q: What is the failure rate by day of week?
SQL: SELECT day_of_week, ROUND(100.0 * SUM(CASE WHEN status IN ('FAILURE','FAILED') THEN 1 ELSE 0 END) / COUNT(*), 2) as failure_rate_pct FROM transaction_features WHERE merchant_id = '{mid}' GROUP BY day_of_week ORDER BY day_of_week

Q: Tell me about this merchant's business profile
SQL: SELECT merchant_trade_name, nature_of_business, business_city, business_state, merchant_risk_category, annual_turnover, expected_monthly_volume, merchant_status FROM merchants WHERE mid = '{mid}'

Q: What is the UPI vs CARD split by revenue?
SQL: SELECT payment_mode, COUNT(*) as txn_count, SUM(amount_rupees) as revenue, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct FROM transaction_features WHERE merchant_id = '{mid}' AND status = 'SUCCESS' GROUP BY payment_mode

Q: Show the last 10 failed transactions
SQL: SELECT p_date, payment_mode, amount_rupees, response_code, response_desc FROM transaction_features WHERE merchant_id = '{mid}' AND status IN ('FAILURE','FAILED') ORDER BY initiated_at DESC LIMIT 10
"""

# ─────────────────────────────────────────────
# BLOCKED OPERATIONS
# ─────────────────────────────────────────────
BLOCKED_OPS = re.compile(r"\b(DROP|DELETE|UPDATE|INSERT|ALTER|TRUNCATE|CREATE)\b", re.IGNORECASE)


class SQLAgent:
    """Dedicated text-to-SQL agent using local Ollama model."""

    def __init__(self, llm, database_url: str, merchant_id: str):
        self.llm = llm
        self.engine = create_engine(database_url)
        self.merchant_id = merchant_id
        self.last_sql = None  # for transparency logging

    def _build_prompt(self, question: str) -> str:
        examples = FEW_SHOT_EXAMPLES.replace("{mid}", self.merchant_id)
        return f"""{TABLE_INFO}

{examples}

Now answer this question. Output ONLY the raw SQL query, nothing else.
Remember: filter by merchant_id = '{self.merchant_id}' and use amount_rupees for money.

Q: {question}
SQL:"""

    def generate_sql(self, question: str) -> str:
        """Convert natural language to SQL using the local LLM."""
        prompt = self._build_prompt(question)
        try:
            response = self.llm.invoke(prompt)
            raw = response.content if hasattr(response, 'content') else str(response)

            # Clean up: strip markdown fences, 'sql' prefix, whitespace
            sql = raw.strip()
            sql = re.sub(r"^```(sql)?\s*", "", sql)
            sql = re.sub(r"\s*```$", "", sql)
            sql = sql.strip().rstrip(";") + ";"

            self.last_sql = sql
            logger.info(f"Generated SQL: {sql}")
            return sql
        except Exception as e:
            logger.error(f"SQL generation error: {e}")
            return ""

    def execute_sql(self, sql: str) -> dict:
        """Execute SQL safely and return {columns, rows, error}."""
        # Safety check
        if BLOCKED_OPS.search(sql):
            return {
                "columns": [],
                "rows": [],
                "error": f"⛔ Blocked: destructive operation detected in query."
            }

        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql))
                columns = list(result.keys())
                rows = [list(row) for row in result.fetchall()]
                logger.info(f"Query returned {len(rows)} rows")
                return {"columns": columns, "rows": rows, "error": None}
        except Exception as e:
            logger.error(f"SQL execution error: {e}")
            return {"columns": [], "rows": [], "error": str(e)}

    def query(self, question: str) -> str:
        """
        End-to-end: question → SQL → execute → formatted result string.
        This is what the orchestrator calls as a tool.
        """
        sql = self.generate_sql(question)
        if not sql:
            return "ERROR: Could not generate a valid SQL query for this question."

        result = self.execute_sql(sql)

        if result["error"]:
            return f"SQL ERROR: {result['error']}\nGenerated SQL: {sql}"

        if not result["rows"]:
            return f"Query returned no results.\nSQL used: {sql}"

        # Format as a readable table string for the orchestrator
        cols = result["columns"]
        rows = result["rows"]

        amount_scale = get_amount_scale(self.engine)
        if amount_scale != 1.0:
            money_idx = [
                i for i, c in enumerate(cols)
                if any(k in str(c).lower() for k in ("amount", "revenue", "gmv", "ticket", "impact", "settlement"))
            ]
            if money_idx:
                scaled_rows = []
                for row in rows:
                    new_row = list(row)
                    for i in money_idx:
                        new_row[i] = scale_inr(new_row[i], amount_scale)
                    scaled_rows.append(new_row)
                rows = scaled_rows

        # Enrich response code outputs with canonical mapping.
        if "response_code" in cols:
            code_idx = cols.index("response_code")
            desc_idx = cols.index("response_desc") if "response_desc" in cols else None
            mode_idx = cols.index("payment_mode") if "payment_mode" in cols else None
            include_meaning = "response_meaning" not in cols
            include_category = "response_category" not in cols
            if include_meaning:
                cols.append("response_meaning")
            if include_category:
                cols.append("response_category")

            enriched_rows = []
            for row in rows:
                new_row = list(row)
                code = normalize_response_code(new_row[code_idx] if code_idx < len(new_row) else None)
                if code in {"UNKNOWN", "UNMAPPED_FAILURE"}:
                    mode_val = ""
                    if mode_idx is not None and mode_idx < len(new_row):
                        mode_val = str(new_row[mode_idx] or "").strip().upper()
                    code = "UPI_FAILURE" if mode_val == "UPI" else "UNMAPPED_FAILURE"
                if code_idx < len(new_row):
                    new_row[code_idx] = code

                meaning = canonical_response_desc(
                    code,
                    new_row[desc_idx] if desc_idx is not None and desc_idx < len(new_row) else None,
                )
                category = canonical_response_category(code)

                if desc_idx is not None and desc_idx < len(new_row):
                    new_row[desc_idx] = meaning
                if include_meaning:
                    new_row.append(meaning)
                if include_category:
                    new_row.append(category)
                enriched_rows.append(new_row)
            rows = enriched_rows

        # Build text table
        lines = [f"SQL: {sql}", f"Results ({len(rows)} rows):", ""]
        # Header
        lines.append(" | ".join(str(c) for c in cols))
        lines.append("-" * 60)
        # Rows (limit to 25 for context window sanity)
        for row in rows[:25]:
            lines.append(" | ".join(str(v) for v in row))
        if len(rows) > 25:
            lines.append(f"... and {len(rows) - 25} more rows")

        return "\n".join(lines)
