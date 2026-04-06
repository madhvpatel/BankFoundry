from __future__ import annotations

from typing import Any, Callable

from sqlalchemy import inspect, text

from app.data.providers import resolve_transaction_provider


def load_merchant_options(
    engine: Any,
    *,
    limit: int = 25,
    query_source_table: str,
    default_merchant_id_loader: Callable[[Any], str | None],
) -> list[dict[str, Any]]:
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    options: list[dict[str, Any]] = []

    if "merchants" in tables:
        cols = {col["name"] for col in inspector.get_columns("merchants")}
        id_col = "merchant_id" if "merchant_id" in cols else ("mid" if "mid" in cols else None)
        if id_col:
            name_col = "merchant_trade_name" if "merchant_trade_name" in cols else None
            city_col = "business_city" if "business_city" in cols else None
            biz_col = "nature_of_business" if "nature_of_business" in cols else None
            select_parts = [f"{id_col} AS merchant_id"]
            if name_col:
                select_parts.append(f"{name_col} AS merchant_trade_name")
            if city_col:
                select_parts.append(f"{city_col} AS business_city")
            if biz_col:
                select_parts.append(f"{biz_col} AS nature_of_business")
            query = text(
                f"""
                SELECT {", ".join(select_parts)}
                FROM merchants
                ORDER BY {id_col}
                LIMIT :limit
                """
            )
            with engine.connect() as conn:
                rows = conn.execute(query, {"limit": int(limit)}).mappings().all()
            for row in rows:
                merchant_id = str(row.get("merchant_id") or "").strip()
                if not merchant_id:
                    continue
                label_parts = [str(row.get("merchant_trade_name") or merchant_id).strip()]
                city = str(row.get("business_city") or "").strip()
                if city:
                    label_parts.append(city)
                options.append(
                    {
                        "merchant_id": merchant_id,
                        "label": " | ".join(label_parts),
                        "merchant_trade_name": str(row.get("merchant_trade_name") or "").strip(),
                        "business_city": city,
                        "nature_of_business": str(row.get("nature_of_business") or "").strip(),
                    }
                )
            if options:
                return options

    provider = resolve_transaction_provider(engine, preferred_table=query_source_table)
    if not provider.has("merchant_id"):
        default_mid = default_merchant_id_loader(engine)
        return [{"merchant_id": default_mid, "label": default_mid}] if default_mid else []

    query = text(
        f"""
        SELECT DISTINCT {provider.value('merchant_id')} AS merchant_id
        FROM {provider.source_table}
        WHERE {provider.value('merchant_id')} IS NOT NULL
        ORDER BY {provider.value('merchant_id')}
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"limit": int(limit)}).fetchall()
    return [{"merchant_id": str(row[0]), "label": str(row[0])} for row in rows if row and row[0] is not None]


def fetch_merchant_context(engine: Any, merchant_id: str) -> dict[str, Any]:
    """Return a compact merchant profile, risk, and KYC snapshot."""
    with engine.connect() as conn:
        merchant = conn.execute(
            text(
                """
                SELECT
                  mid,
                  merchant_trade_name,
                  nature_of_business,
                  business_city,
                  merchant_risk_category,
                  merchant_status,
                  annual_turnover
                FROM merchants
                WHERE mid = :mid
                LIMIT 1
                """
            ),
            {"mid": merchant_id},
        ).mappings().first()

        risk = None
        try:
            risk = conn.execute(
                text(
                    """
                    SELECT risk_score AS score, risk_band AS band, updated_at
                    FROM merchant_risk_profiles
                    WHERE merchant_id = :mid
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """
                ),
                {"mid": merchant_id},
            ).mappings().first()
        except Exception:
            risk = None

        kyc = None
        try:
            kyc = conn.execute(
                text(
                    """
                    SELECT
                      COALESCE(MAX(status), 'UNKNOWN') AS status,
                      MIN(CASE WHEN expiry_at IS NOT NULL THEN expiry_at END) AS next_expiry_at
                    FROM merchant_kyc_documents
                    WHERE merchant_id = :mid
                    """
                ),
                {"mid": merchant_id},
            ).mappings().first()
        except Exception:
            kyc = None

    return {
        "merchant": dict(merchant) if merchant else {"mid": merchant_id},
        "risk_profile": dict(risk) if risk else None,
        "kyc": dict(kyc) if kyc else None,
    }
