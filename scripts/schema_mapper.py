#!/usr/bin/env python3
"""
AI-Powered Schema Mapper for Bank Foundry
==========================================
Automatically discovers a foreign database schema, uses an LLM to map it
to Bank Foundry's canonical views, validates the mapping, and optionally
applies the views.

Usage:
    # Dry run (prints generated SQL, does not modify the database)
    python scripts/schema_mapper.py --db-url "postgresql://user:pass@host:5432/db"

    # Apply views after validation
    python scripts/schema_mapper.py --db-url "postgresql://user:pass@host:5432/db" --apply

    # Override the OpenAI model
    python scripts/schema_mapper.py --db-url "postgresql://..." --model gpt-4o --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Ensure project root is importable
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.canonical_schema import CANONICAL_SCHEMA, schema_to_prompt_text  # noqa: E402

load_dotenv(_PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("schema_mapper")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MAX_HEAL_RETRIES = 3
SAMPLE_ROW_LIMIT = 5
CANONICAL_VIEW_NAMES = list(CANONICAL_SCHEMA.keys())

SMOKE_TESTS: dict[str, list[str]] = {
    "transaction_features": [
        "SELECT COUNT(*) AS cnt FROM transaction_features",
        "SELECT merchant_id, status, amount_rupees, p_date FROM transaction_features LIMIT 1",
    ],
    "merchants": [
        "SELECT COUNT(*) AS cnt FROM merchants",
        "SELECT mid FROM merchants LIMIT 1",
    ],
    "settlements": [
        "SELECT COUNT(*) AS cnt FROM settlements",
        "SELECT merchant_id FROM settlements LIMIT 1",
    ],
}


# ===================================================================
# Stage 1 — Schema Discovery
# ===================================================================

def discover_schema(engine: Any) -> dict[str, Any]:
    """Inspect the target database and return a schema manifest."""
    logger.info("Stage 1: Discovering database schema...")
    manifest: dict[str, Any] = {"tables": {}, "foreign_keys": []}

    with engine.connect() as conn:
        # --- Tables & columns ---
        rows = conn.execute(text("""
            SELECT table_name, column_name, data_type, is_nullable,
                   character_maximum_length, numeric_precision
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
        """)).fetchall()

        for row in rows:
            table = str(row[0])
            if table.startswith("pg_") or table.startswith("sql_"):
                continue
            if table not in manifest["tables"]:
                manifest["tables"][table] = {"columns": [], "sample_rows": [], "row_count": 0}
            manifest["tables"][table]["columns"].append({
                "name": str(row[1]),
                "type": str(row[2]).upper(),
                "nullable": str(row[3]).upper() == "YES",
            })

        # --- Foreign keys ---
        try:
            fk_rows = conn.execute(text("""
                SELECT
                    tc.table_name AS source_table,
                    kcu.column_name AS source_column,
                    ccu.table_name AS target_table,
                    ccu.column_name AS target_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND tc.table_schema = 'public'
            """)).fetchall()

            for fk in fk_rows:
                manifest["foreign_keys"].append({
                    "source_table": str(fk[0]),
                    "source_column": str(fk[1]),
                    "target_table": str(fk[2]),
                    "target_column": str(fk[3]),
                })
        except Exception:
            logger.warning("Could not read foreign key constraints (non-critical).")

        # --- Sample rows & row counts ---
        for table_name in list(manifest["tables"].keys()):
            try:
                count_row = conn.execute(
                    text(f'SELECT COUNT(*) FROM "{table_name}"')
                ).fetchone()
                manifest["tables"][table_name]["row_count"] = int(count_row[0]) if count_row else 0
            except Exception:
                manifest["tables"][table_name]["row_count"] = -1

            try:
                sample_rows = conn.execute(
                    text(f'SELECT * FROM "{table_name}" LIMIT {SAMPLE_ROW_LIMIT}')
                ).fetchall()
                col_names = [c["name"] for c in manifest["tables"][table_name]["columns"]]
                manifest["tables"][table_name]["sample_rows"] = [
                    {col_names[i]: _serialize_value(row[i]) for i in range(min(len(col_names), len(row)))}
                    for row in sample_rows
                ]
            except Exception as exc:
                logger.warning("Could not sample rows from %s: %s", table_name, exc)

    table_count = len(manifest["tables"])
    total_cols = sum(len(t["columns"]) for t in manifest["tables"].values())
    logger.info("Discovered %d tables with %d total columns.", table_count, total_cols)
    return manifest


def _serialize_value(value: Any) -> Any:
    """Make a value JSON-serializable."""
    if value is None:
        return None
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, (datetime,)):
        return value.isoformat()
    return str(value)


# ===================================================================
# Stage 2 — LLM Mapping
# ===================================================================

MAPPER_SYSTEM_PROMPT = """You are an expert database architect specializing in payment systems.

Your task is to map a foreign database schema into Bank Foundry's canonical schema
by writing PostgreSQL CREATE OR REPLACE VIEW statements.

RULES:
1. Output ONLY valid PostgreSQL DDL statements. No explanations, no markdown fences.
2. Produce one CREATE OR REPLACE VIEW per canonical table (transaction_features, merchants, settlements).
3. If the foreign database does not have data for a canonical table, SKIP that view entirely.
4. Map ALL required columns. Map as many optional columns as possible.
5. If a required column cannot be mapped, use a sensible derivation:
   - For amount_rupees: if only paise exists, use (amount_paise / 100.0)
   - For p_date: if only a timestamp exists, use DATE(timestamp_column)
   - For status: normalize to 'SUCCESS' or 'FAILURE' using CASE WHEN
6. If data is spread across multiple tables, use JOINs.
7. Use column aliases so the view output matches the canonical column names exactly.
8. For unmappable optional columns, use NULL::type AS column_name.
9. Separate each CREATE OR REPLACE VIEW statement with a semicolon and newline.
10. Do NOT wrap output in markdown code fences or add any commentary.
"""


def build_mapping_prompt(manifest: dict[str, Any]) -> str:
    """Build the user-facing prompt with the foreign schema + canonical target."""
    # Trim sample rows to keep prompt size reasonable
    trimmed_manifest = {}
    for table_name, table_data in manifest["tables"].items():
        trimmed_manifest[table_name] = {
            "columns": table_data["columns"],
            "row_count": table_data["row_count"],
            "sample_rows": table_data["sample_rows"][:3],
        }

    fk_text = ""
    if manifest.get("foreign_keys"):
        fk_lines = ["FOREIGN KEY RELATIONSHIPS:"]
        for fk in manifest["foreign_keys"]:
            fk_lines.append(
                f"  {fk['source_table']}.{fk['source_column']} -> "
                f"{fk['target_table']}.{fk['target_column']}"
            )
        fk_text = "\n".join(fk_lines)

    return f"""FOREIGN DATABASE SCHEMA (source):
{json.dumps(trimmed_manifest, indent=2, default=str)}

{fk_text}

{schema_to_prompt_text()}

Based on the foreign schema above, write CREATE OR REPLACE VIEW statements
that present the foreign data in Bank Foundry's canonical format.
Map required columns first, then as many optional columns as the data supports.
Output ONLY the SQL DDL. No explanations."""


def _create_llm(*, model: str, api_key: str, temperature: float = 0.1) -> ChatOpenAI:
    """Create an OpenAI LLM instance."""
    return ChatOpenAI(
        model=model,
        openai_api_key=api_key,
        temperature=temperature,
    )


def generate_view_ddl(
    manifest: dict[str, Any],
    *,
    model: str,
    api_key: str,
    temperature: float = 0.1,
) -> str:
    """Send the schema to the LLM and get back CREATE VIEW DDL."""
    logger.info("Stage 2: Generating view DDL via OpenAI (%s)...", model)

    llm = _create_llm(model=model, api_key=api_key, temperature=temperature)
    prompt = build_mapping_prompt(manifest)

    response = llm.invoke([
        SystemMessage(content=MAPPER_SYSTEM_PROMPT),
        HumanMessage(content=prompt),
    ])

    raw = getattr(response, "content", str(response))
    # Strip markdown fences if the LLM ignored instructions
    ddl = re.sub(r"```(?:sql|pgsql)?", "", raw, flags=re.IGNORECASE)
    ddl = ddl.replace("```", "").strip()

    logger.info("LLM returned %d characters of DDL.", len(ddl))
    return ddl


# ===================================================================
# Stage 3 — Validation + Self-Healing
# ===================================================================

HEAL_SYSTEM_PROMPT = """You are a PostgreSQL expert. The previous CREATE VIEW statement failed.
Fix the SQL based on the error message. Output ONLY the corrected CREATE OR REPLACE VIEW statement.
No explanations, no markdown fences."""


def parse_ddl_statements(ddl: str) -> list[str]:
    """Split a DDL string into individual CREATE VIEW statements."""
    statements: list[str] = []
    # Split on semicolons but keep the statement intact
    parts = re.split(r";\s*(?=CREATE\b)", ddl, flags=re.IGNORECASE)
    for part in parts:
        cleaned = part.strip().rstrip(";").strip()
        if cleaned and re.match(r"CREATE\s+", cleaned, re.IGNORECASE):
            statements.append(cleaned + ";")
    return statements


def extract_view_name(statement: str) -> str | None:
    """Extract the view name from a CREATE VIEW statement."""
    match = re.search(
        r"CREATE\s+OR\s+REPLACE\s+VIEW\s+(\w+)",
        statement,
        re.IGNORECASE,
    )
    if match:
        return match.group(1).lower()
    match = re.search(r"CREATE\s+VIEW\s+(\w+)", statement, re.IGNORECASE)
    return match.group(1).lower() if match else None


def validate_and_heal(
    engine: Any,
    ddl: str,
    manifest: dict[str, Any],
    *,
    model: str,
    api_key: str,
) -> dict[str, Any]:
    """
    Parse, execute, and validate DDL. On failure, ask the LLM to fix it.
    Returns a report dict with status per view.
    """
    logger.info("Stage 3: Validating generated views...")
    statements = parse_ddl_statements(ddl)
    if not statements:
        return {"status": "error", "message": "No valid CREATE VIEW statements found in LLM output.", "views": {}}

    report: dict[str, Any] = {"status": "ok", "views": {}}

    for stmt in statements:
        view_name = extract_view_name(stmt) or "unknown"
        logger.info("  Processing view: %s", view_name)
        current_stmt = stmt

        for attempt in range(1, MAX_HEAL_RETRIES + 1):
            try:
                with engine.begin() as conn:
                    conn.execute(text(current_stmt))
                logger.info("    ✓ CREATE VIEW succeeded (attempt %d)", attempt)
                break
            except Exception as exc:
                error_msg = str(exc)
                logger.warning("    ✗ CREATE VIEW failed (attempt %d): %s", attempt, error_msg[:200])

                if attempt >= MAX_HEAL_RETRIES:
                    report["views"][view_name] = {
                        "status": "failed",
                        "error": error_msg[:500],
                        "final_ddl": current_stmt,
                    }
                    report["status"] = "partial"
                    break

                # Ask the LLM to fix it
                current_stmt = _heal_statement(
                    current_stmt, error_msg, manifest,
                    model=model, api_key=api_key,
                )
        else:
            continue

        # If the view was created, run smoke tests
        if view_name not in report.get("views", {}):
            smoke_results = _run_smoke_tests(engine, view_name)
            report["views"][view_name] = {
                "status": "ok" if smoke_results["passed"] else "smoke_failed",
                "smoke_tests": smoke_results,
                "final_ddl": current_stmt,
            }
            if not smoke_results["passed"]:
                report["status"] = "partial"

    return report


def _heal_statement(
    failed_stmt: str,
    error_msg: str,
    manifest: dict[str, Any],
    *,
    model: str,
    api_key: str,
) -> str:
    """Ask the LLM to fix a failed CREATE VIEW statement."""
    logger.info("    → Asking LLM to heal the statement...")
    llm = _create_llm(model=model, api_key=api_key, temperature=0.05)

    # Build a compact schema summary for context
    schema_summary = {}
    for table_name, table_data in manifest["tables"].items():
        schema_summary[table_name] = [c["name"] for c in table_data["columns"]]

    heal_prompt = f"""The following CREATE VIEW statement failed:

{failed_stmt}

ERROR: {error_msg[:1000]}

Available tables and columns in the database:
{json.dumps(schema_summary, indent=2)}

Fix the SQL and output ONLY the corrected CREATE OR REPLACE VIEW statement."""

    response = llm.invoke([
        SystemMessage(content=HEAL_SYSTEM_PROMPT),
        HumanMessage(content=heal_prompt),
    ])

    raw = getattr(response, "content", str(response))
    cleaned = re.sub(r"```(?:sql|pgsql)?", "", raw, flags=re.IGNORECASE)
    cleaned = cleaned.replace("```", "").strip()

    # Extract just the CREATE statement
    match = re.search(r"(CREATE\s+OR\s+REPLACE\s+VIEW\s+.+?);?\s*$", cleaned, re.IGNORECASE | re.DOTALL)
    if match:
        return match.group(1).rstrip(";") + ";"

    # Fallback: return cleaned output and hope for the best
    return cleaned.rstrip(";") + ";"


def _run_smoke_tests(engine: Any, view_name: str) -> dict[str, Any]:
    """Run smoke tests against a created view."""
    tests = SMOKE_TESTS.get(view_name, [])
    results: list[dict[str, Any]] = []
    all_passed = True

    for query_str in tests:
        try:
            with engine.connect() as conn:
                row = conn.execute(text(query_str)).fetchone()
                results.append({"query": query_str, "passed": True, "result": str(row)})
        except Exception as exc:
            results.append({"query": query_str, "passed": False, "error": str(exc)[:300]})
            all_passed = False

    return {"passed": all_passed, "tests": results}


# ===================================================================
# Stage 4 — Apply & Report
# ===================================================================

def generate_report(
    manifest: dict[str, Any],
    validation_report: dict[str, Any],
    *,
    applied: bool,
) -> dict[str, Any]:
    """Generate a JSON report summarizing the mapping."""
    mapped_views = []
    for view_name, view_info in validation_report.get("views", {}).items():
        mapped_columns = []
        if view_info.get("status") == "ok":
            ddl = view_info.get("final_ddl", "")
            # Count AS aliases in the DDL as a rough column count
            mapped_columns = re.findall(r"\bAS\s+(\w+)", ddl, re.IGNORECASE)
        mapped_views.append({
            "view_name": view_name,
            "status": view_info.get("status", "unknown"),
            "mapped_columns": mapped_columns,
            "column_count": len(mapped_columns),
        })

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source_tables": list(manifest["tables"].keys()),
        "source_table_count": len(manifest["tables"]),
        "foreign_key_count": len(manifest.get("foreign_keys", [])),
        "target_views": mapped_views,
        "overall_status": validation_report.get("status", "unknown"),
        "applied": applied,
    }


# ===================================================================
# CLI Entry Point
# ===================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="AI-Powered Schema Mapper for Bank Foundry",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--db-url",
        default=os.getenv("DATABASE_URL", ""),
        help="PostgreSQL connection string (default: DATABASE_URL env var)",
    )
    parser.add_argument(
        "--model",
        default=os.getenv("OPENAI_MODEL", "gpt-4o"),
        help="OpenAI model to use for schema mapping (default: OPENAI_MODEL env var or gpt-4o)",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("OPENAI_API_KEY", ""),
        help="OpenAI API key (default: OPENAI_API_KEY env var)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually apply the generated views to the database (default: dry run only)",
    )
    parser.add_argument(
        "--output",
        default=str(_PROJECT_ROOT / "schema_mapping_report.json"),
        help="Path to write the mapping report JSON",
    )
    args = parser.parse_args()

    if not args.api_key:
        logger.error("No OpenAI API key provided. Use --api-key or set OPENAI_API_KEY in .env")
        sys.exit(1)

    if not args.db_url:
        logger.error("No database URL provided. Use --db-url or set DATABASE_URL in .env")
        sys.exit(1)

    engine = create_engine(args.db_url)

    # --- Stage 1: Discover ---
    manifest = discover_schema(engine)
    if not manifest["tables"]:
        logger.error("No tables found in the database. Exiting.")
        sys.exit(1)

    # --- Stage 2: LLM Mapping ---
    ddl = generate_view_ddl(
        manifest,
        model=args.model,
        api_key=args.api_key,
    )

    if not ddl:
        logger.error("LLM returned empty DDL. Exiting.")
        sys.exit(1)

    print("\n" + "=" * 70)
    print("GENERATED DDL")
    print("=" * 70)
    print(ddl)
    print("=" * 70 + "\n")

    if not args.apply:
        logger.info("Dry run complete. Pass --apply to execute the views.")
        # Still generate report in dry-run mode
        report = generate_report(
            manifest,
            {"status": "dry_run", "views": {}},
            applied=False,
        )
        _write_report(report, args.output)
        return

    # --- Stage 3: Validate + Heal ---
    validation_report = validate_and_heal(
        engine, ddl, manifest,
        model=args.model,
        api_key=args.api_key,
    )

    # --- Stage 4: Report ---
    applied = validation_report.get("status") in ("ok", "partial")
    report = generate_report(manifest, validation_report, applied=applied)
    _write_report(report, args.output)

    # Print summary
    print("\n" + "=" * 70)
    print("SCHEMA MAPPING SUMMARY")
    print("=" * 70)
    for view_info in report["target_views"]:
        status_icon = "✓" if view_info["status"] == "ok" else "✗"
        print(f"  {status_icon}  {view_info['view_name']:25s}  →  {view_info['column_count']} columns mapped  [{view_info['status']}]")
    print(f"\n  Overall: {report['overall_status'].upper()}")
    print(f"  Applied: {'YES' if report['applied'] else 'NO (dry run)'}")
    print("=" * 70 + "\n")

    if validation_report.get("status") == "error":
        sys.exit(1)


def _write_report(report: dict[str, Any], path: str) -> None:
    """Write the mapping report to a JSON file."""
    try:
        with open(path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        logger.info("Report written to %s", path)
    except Exception as exc:
        logger.warning("Could not write report: %s", exc)


if __name__ == "__main__":
    main()
