#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
BASE_URL="${BASE_URL:-http://127.0.0.1:8765}"
ENDPOINT="$BASE_URL/test/ask"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/artifacts/sprint1_manual_validation}"
CASES_DIR="$OUT_DIR/cases"
mkdir -p "$CASES_DIR"

resolve_default_merchant() {
  "$PYTHON_BIN" - <<'PY'
from sqlalchemy import create_engine, text
from config import Config
engine = create_engine(Config.DATABASE_URL)
with engine.connect() as conn:
    row = conn.execute(text(f"SELECT merchant_id FROM {Config.QUERY_SOURCE_TABLE} LIMIT 1")).fetchone()
print(str(row[0]).strip() if row and row[0] is not None else "")
PY
}

resolve_default_terminal() {
  local merchant_id="$1"
  "$PYTHON_BIN" - "$merchant_id" <<'PY'
import sys
from sqlalchemy import create_engine, text
from config import Config
merchant_id = sys.argv[1]
if not merchant_id:
    print("")
    raise SystemExit(0)
engine = create_engine(Config.DATABASE_URL)
with engine.connect() as conn:
    row = conn.execute(
        text(
            f"""
            SELECT terminal_id
            FROM {Config.QUERY_SOURCE_TABLE}
            WHERE merchant_id = :mid
              AND terminal_id IS NOT NULL
              AND TRIM(CAST(terminal_id AS TEXT)) <> ''
            ORDER BY terminal_id
            LIMIT 1
            """
        ),
        {"mid": merchant_id},
    ).fetchone()
print(str(row[0]).strip() if row and row[0] is not None else "")
PY
}

write_case_result() {
  local case_id="$1"
  local lane="$2"
  local prompt="$3"
  local merchant_id="$4"
  local terminal_id="${5:-}"
  local outfile="$CASES_DIR/${case_id}.json"

  if [[ -n "$terminal_id" ]]; then
    curl -sS -X POST "$ENDPOINT" \
      -H 'Content-Type: application/json' \
      --data @- > "$outfile" <<JSON
{"merchant_id":"$merchant_id","prompt":"$prompt","lane":"$lane","terminal_id":"$terminal_id"}
JSON
  else
    curl -sS -X POST "$ENDPOINT" \
      -H 'Content-Type: application/json' \
      --data @- > "$outfile" <<JSON
{"merchant_id":"$merchant_id","prompt":"$prompt","lane":"$lane"}
JSON
  fi
}

write_blocked_case() {
  local case_id="$1"
  local lane="$2"
  local prompt="$3"
  local merchant_id="$4"
  local reason="$5"
  local outfile="$CASES_DIR/${case_id}.json"
  cat > "$outfile" <<JSON
{
  "case_id": "$case_id",
  "merchant_id": "$merchant_id",
  "lane": "$lane",
  "prompt": "$prompt",
  "status": "BLOCKED",
  "reason": "$reason"
}
JSON
}

MERCHANT_ID="${MERCHANT_ID:-$(resolve_default_merchant)}"
if [[ -z "$MERCHANT_ID" ]]; then
  echo "Could not determine a default merchant_id from ${ENDPOINT}." >&2
  exit 1
fi

TERMINAL_ID="${TERMINAL_ID:-$(resolve_default_terminal "$MERCHANT_ID")}" 

write_case_result \
  "OPS_SHORTFALL" \
  "operations" \
  "I expected Rs 20,000 settlement but got Rs 19,000. Explain the shortfall." \
  "$MERCHANT_ID"

write_case_result \
  "GR_TOP_OPPS" \
  "growth" \
  "What are my top growth opportunities in the last 30 days?" \
  "$MERCHANT_ID"

if [[ -n "$TERMINAL_ID" ]]; then
  write_case_result \
    "GR_TERMINAL_SCOPE" \
    "growth" \
    "What are my top growth opportunities for this terminal in the last 30 days?" \
    "$MERCHANT_ID" \
    "$TERMINAL_ID"
else
  write_blocked_case \
    "GR_TERMINAL_SCOPE" \
    "growth" \
    "What are my top growth opportunities for this terminal in the last 30 days?" \
    "$MERCHANT_ID" \
    "No terminal_id was discoverable for the selected merchant."
fi

"$PYTHON_BIN" - "$CASES_DIR" "$OUT_DIR/prompt_runs.json" <<'PY'
import json
import os
import sys
from datetime import datetime, timezone

cases_dir, archive_path = sys.argv[1], sys.argv[2]
entries = []
for name in sorted(os.listdir(cases_dir)):
    if not name.endswith('.json'):
        continue
    path = os.path.join(cases_dir, name)
    with open(path, 'r', encoding='utf-8') as fh:
        payload = json.load(fh)
    entries.append({
        "case_id": os.path.splitext(name)[0],
        "response_path": path,
        "response": payload,
    })
archive = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "base_url": os.environ.get("BASE_URL", "http://127.0.0.1:8765"),
    "cases": entries,
}
with open(archive_path, 'w', encoding='utf-8') as fh:
    json.dump(archive, fh, ensure_ascii=False, indent=2)
print(archive_path)
PY

echo "Prompt responses saved to $OUT_DIR"
echo "Review the JSON archive at $OUT_DIR/prompt_runs.json"
