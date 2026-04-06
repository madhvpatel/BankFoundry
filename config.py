import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    # Default to localhost for local development if not set
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://demo:demo@localhost:5433/payments_demo")
    
    # LLM Provider: "gemini" or "ollama"
    LLM_PROVIDER = os.getenv("LLM_PROVIDER", "gemini" if GEMINI_API_KEY else "ollama")
    OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen3:8b")
    OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

    # CORS (comma-separated list; use "*" for all)
    CORS_ALLOW_ORIGINS = os.getenv("CORS_ALLOW_ORIGINS", "*")

    # DB connection retry settings
    DB_CONNECT_RETRIES = int(os.getenv("DB_CONNECT_RETRIES", "5"))
    DB_CONNECT_RETRY_DELAY_SECONDS = float(os.getenv("DB_CONNECT_RETRY_DELAY_SECONDS", "2"))

    # Anomaly detection safety limit
    ANOMALY_MAX_RECORDS = int(os.getenv("ANOMALY_MAX_RECORDS", "2000"))

    # Time anchoring for relative ranges: "system" or "data_max"
    TIME_ANCHOR_MODE = os.getenv("TIME_ANCHOR_MODE", "system").lower()
    TIME_ANCHOR_AUTO_FALLBACK = os.getenv("TIME_ANCHOR_AUTO_FALLBACK", "false").lower() == "true"

    # Analytics response narrative (LLM) toggle
    ANALYTICS_USE_LLM = os.getenv("ANALYTICS_USE_LLM", "true").lower() == "true"
    # Auto-append warnings/suggestions/followups to analytics narrative.
    # Keep disabled by default to avoid silent post-append behavior.
    ANALYTICS_APPEND_BLOCKS = os.getenv("ANALYTICS_APPEND_BLOCKS", "false").lower() == "true"

    # Optional enrichment for business snapshot responses.
    SNAPSHOT_ENRICH_USE_LLM = os.getenv("SNAPSHOT_ENRICH_USE_LLM", "true").lower() == "true"

    # Default source table for query tool
    QUERY_SOURCE_TABLE = os.getenv("QUERY_SOURCE_TABLE", "transaction_features")
    TRANSACTION_SOURCE_TABLE_CANDIDATES = os.getenv("TRANSACTION_SOURCE_TABLE_CANDIDATES", "").strip()
    SETTLEMENT_SOURCE_TABLE_CANDIDATES = os.getenv("SETTLEMENT_SOURCE_TABLE_CANDIDATES", "").strip()

    # Safe scratchpad trace logs (no chain-of-thought)
    TRACE_SCRATCHPAD = os.getenv("TRACE_SCRATCHPAD", "true").lower() == "true"

    # Proactive inbox refresh cadence for merchant OS.
    PROACTIVE_AUTO_REFRESH_ENABLED = os.getenv("PROACTIVE_AUTO_REFRESH_ENABLED", "true").lower() == "true"
    PROACTIVE_REFRESH_INTERVAL_MINUTES = int(os.getenv("PROACTIVE_REFRESH_INTERVAL_MINUTES", "30"))
    PAYOUT_SHORTFALL_MIN_DIFFERENCE_RUPEES = float(os.getenv("PAYOUT_SHORTFALL_MIN_DIFFERENCE_RUPEES", "1000"))
    SETTLEMENT_OPS_CONNECTOR_ENABLED = os.getenv("SETTLEMENT_OPS_CONNECTOR_ENABLED", "true").lower() == "true"
    SETTLEMENT_OPS_CONNECTOR_MODE = os.getenv("SETTLEMENT_OPS_CONNECTOR_MODE", "simulated").lower()
    SETTLEMENT_OPS_CONNECTOR_BASE_URL = os.getenv("SETTLEMENT_OPS_CONNECTOR_BASE_URL", "").strip()
    SETTLEMENT_OPS_CONNECTOR_ENDPOINT = os.getenv("SETTLEMENT_OPS_CONNECTOR_ENDPOINT", "/v1/settlements/interventions").strip()
    SETTLEMENT_OPS_CONNECTOR_AUTH_MODE = os.getenv("SETTLEMENT_OPS_CONNECTOR_AUTH_MODE", "none").lower().strip()
    SETTLEMENT_OPS_CONNECTOR_BEARER_TOKEN = os.getenv("SETTLEMENT_OPS_CONNECTOR_BEARER_TOKEN", "").strip()
    SETTLEMENT_OPS_CONNECTOR_API_KEY = os.getenv("SETTLEMENT_OPS_CONNECTOR_API_KEY", "").strip()
    SETTLEMENT_OPS_CONNECTOR_API_KEY_HEADER = os.getenv("SETTLEMENT_OPS_CONNECTOR_API_KEY_HEADER", "X-API-Key").strip()
    SETTLEMENT_OPS_CONNECTOR_IDEMPOTENCY_HEADER = os.getenv("SETTLEMENT_OPS_CONNECTOR_IDEMPOTENCY_HEADER", "Idempotency-Key").strip()
    SETTLEMENT_OPS_CONNECTOR_TIMEOUT_SECONDS = float(os.getenv("SETTLEMENT_OPS_CONNECTOR_TIMEOUT_SECONDS", "10"))
    SETTLEMENT_OPS_CONNECTOR_VERIFY_SSL = os.getenv("SETTLEMENT_OPS_CONNECTOR_VERIFY_SSL", "true").lower() == "true"
    SETTLEMENT_OPS_CONNECTOR_PARTNER_ID = os.getenv("SETTLEMENT_OPS_CONNECTOR_PARTNER_ID", "").strip()

    # Intelligence monitoring controls
    INTELLIGENCE_ENABLE_DQ_CHECKS = os.getenv("INTELLIGENCE_ENABLE_DQ_CHECKS", "true").lower() == "true"
    INTELLIGENCE_ENABLE_DRIFT_CHECKS = os.getenv("INTELLIGENCE_ENABLE_DRIFT_CHECKS", "true").lower() == "true"
    INTELLIGENCE_PHASE2_MIN_IMPACT_RUPEES = float(os.getenv("INTELLIGENCE_PHASE2_MIN_IMPACT_RUPEES", "50000"))
    INTELLIGENCE_PHASE2_MIN_NEGATIVE_SIGNALS = int(os.getenv("INTELLIGENCE_PHASE2_MIN_NEGATIVE_SIGNALS", "2"))
    CHAT_REASONING_ENABLED = os.getenv("CHAT_REASONING_ENABLED", "true").lower() == "true"
    CHAT_REASONING_TEMPERATURE = float(os.getenv("CHAT_REASONING_TEMPERATURE", "0.2"))
    CHAT_ROUTER_MODEL = os.getenv("CHAT_ROUTER_MODEL", OLLAMA_MODEL)
    CHAT_ROUTER_TEMPERATURE = float(os.getenv("CHAT_ROUTER_TEMPERATURE", "0.0"))
    CHAT_ROUTER_MIN_CONFIDENCE = float(os.getenv("CHAT_ROUTER_MIN_CONFIDENCE", "0.65"))
    CHAT_ROUTER_SOFT_CONFIDENCE = float(os.getenv("CHAT_ROUTER_SOFT_CONFIDENCE", "0.50"))

    # Monetary normalization at DB ingest layer.
    # mode=fixed -> always use DB_AMOUNT_SCALE_FACTOR.
    # mode=auto  -> use DB_AMOUNT_AUTO_FACTOR when avg successful ticket exceeds DB_AMOUNT_AUTO_THRESHOLD.
    DB_AMOUNT_SCALE_MODE = os.getenv("DB_AMOUNT_SCALE_MODE", "auto").lower()
    DB_AMOUNT_SCALE_FACTOR = float(os.getenv("DB_AMOUNT_SCALE_FACTOR", "1.0"))
    DB_AMOUNT_AUTO_THRESHOLD = float(os.getenv("DB_AMOUNT_AUTO_THRESHOLD", "10000"))
    DB_AMOUNT_AUTO_FACTOR = float(os.getenv("DB_AMOUNT_AUTO_FACTOR", "0.1"))

    # Merchant copilot experimentation controls.
    # In experiment mode, copilot uses freer LLM reasoning for causal chains and actions.
    COPILOT_EXPERIMENT_MODE = os.getenv("COPILOT_EXPERIMENT_MODE", "true").lower() == "true"
    COPILOT_EXPERIMENT_TEMPERATURE = float(os.getenv("COPILOT_EXPERIMENT_TEMPERATURE", "0.35"))

    # Volume-based credit (working capital) recommendation knobs.
    # These are *policy/config*, not merchant-specific hardcoded logic.
    CREDIT_MIN_SUCCESS_GMV_30D = float(os.getenv("CREDIT_MIN_SUCCESS_GMV_30D", "1000000"))  # ₹
    CREDIT_MIN_SUCCESS_RATE_PCT_30D = float(os.getenv("CREDIT_MIN_SUCCESS_RATE_PCT_30D", "95.0"))
    CREDIT_MAX_CHARGEBACKS_30D = int(os.getenv("CREDIT_MAX_CHARGEBACKS_30D", "5"))
    CREDIT_MAX_REFUNDS_RATE_PCT_30D = float(os.getenv("CREDIT_MAX_REFUNDS_RATE_PCT_30D", "5.0"))
    CREDIT_MAX_DAILY_GMV_CV_30D = float(os.getenv("CREDIT_MAX_DAILY_GMV_CV_30D", "2.0"))

    # Copilot personality controls (demo).
    # off -> strictly business
    # dry -> occasional dry humor
    # light -> light, friendly humor (no emojis)
    COPILOT_HUMOR_LEVEL = os.getenv("COPILOT_HUMOR_LEVEL", "dry").lower()

    # Scenario engine experimentation controls.
    # Enables freer what-if reasoning with hypothesis ranges for demo mode.
    SCENARIO_EXPERIMENT_MODE = os.getenv("SCENARIO_EXPERIMENT_MODE", "true").lower() == "true"
    SCENARIO_EXPERIMENT_TEMPERATURE = float(os.getenv("SCENARIO_EXPERIMENT_TEMPERATURE", "0.35"))

    # Global experimental runtime:
    # - one startup LLM bootstrap for context/KPI briefing
    # - single multi-step LangChain agent handles chat/tool use
    # - disables other LLM interactions in the app while enabled
    GLOBAL_EXPERIMENTAL_MODE = os.getenv("GLOBAL_EXPERIMENTAL_MODE", "true").lower() == "true"
    GLOBAL_EXPERIMENTAL_MAX_STEPS = int(os.getenv("GLOBAL_EXPERIMENTAL_MAX_STEPS", "6"))

    # Unified ask runtime.
    UNIFIED_AGENT_MAX_STEPS = int(os.getenv("UNIFIED_AGENT_MAX_STEPS", "4"))
    UNIFIED_AGENT_TOOL_TEMPERATURE = float(os.getenv("UNIFIED_AGENT_TOOL_TEMPERATURE", "0.1"))
    UNIFIED_AGENT_COMPOSER_TEMPERATURE = float(os.getenv("UNIFIED_AGENT_COMPOSER_TEMPERATURE", "0.1"))

    # LangGraph SQL pipeline (project-specific SQL orchestration graph).
    # Keep disabled by default to preserve current runtime behavior.
    SQL_LANGGRAPH_ENABLED = os.getenv("SQL_LANGGRAPH_ENABLED", "false").lower() == "true"
    SQL_GRAPH_MAX_ROWS = int(os.getenv("SQL_GRAPH_MAX_ROWS", "200"))
    SQL_GRAPH_MAX_SQL_RETRIES = int(os.getenv("SQL_GRAPH_MAX_SQL_RETRIES", "1"))
    SQL_GRAPH_REQUIRE_HUMAN_REVIEW = os.getenv("SQL_GRAPH_REQUIRE_HUMAN_REVIEW", "false").lower() == "true"
    SQL_GRAPH_CATALOG_PATH = os.getenv("SQL_GRAPH_CATALOG_PATH", "app/copilot/sql_catalog.json")
    SQL_GRAPH_AUTO_DISCOVER_TABLES = os.getenv("SQL_GRAPH_AUTO_DISCOVER_TABLES", "true").lower() == "true"
    SQL_GRAPH_TABLE_ALLOWLIST = os.getenv("SQL_GRAPH_TABLE_ALLOWLIST", "")
    SQL_GRAPH_DISCOVERY_PREFIXES = os.getenv("SQL_GRAPH_DISCOVERY_PREFIXES", "")
