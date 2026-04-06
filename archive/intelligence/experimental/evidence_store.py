# app/intelligence/evidence_store.py
import json, hashlib
import pandas as pd
from sqlalchemy import text

MAX_PREVIEW_ROWS = 25

def _hash_payload(sql_text: str, params: dict, preview: dict) -> str:
    payload = {
        "sql": sql_text,
        "params": params,
        "preview": preview,
    }
    b = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(b).hexdigest()

def save_sql_evidence(engine, merchant_id: str, sql_text: str, params: dict) -> str:
    # Execute with params safely
    df = pd.read_sql(text(sql_text), engine, params=params)

    preview = {
        "columns": list(df.columns),
        "rows": df.head(MAX_PREVIEW_ROWS).values.tolist(),
        "row_count": int(len(df)),
    }
    h = _hash_payload(sql_text, params, preview)
    evidence_id = f"ev_{h[:12]}"

    upsert = text("""
      INSERT INTO evidence_store (evidence_id, merchant_id, source_type, sql_text, sql_params, result_preview, result_hash)
      VALUES (:evidence_id, :merchant_id, 'sql', :sql_text, :sql_params::jsonb, :result_preview::jsonb, :result_hash)
      ON CONFLICT (evidence_id) DO NOTHING
    """)
    with engine.begin() as conn:
        conn.execute(upsert, {
            "evidence_id": evidence_id,
            "merchant_id": merchant_id,
            "sql_text": sql_text,
            "sql_params": json.dumps(params),
            "result_preview": json.dumps(preview, default=str),
            "result_hash": h,
        })
    return evidence_id