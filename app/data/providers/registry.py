from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from sqlalchemy import text

from config import Config

DEFAULT_TRANSACTION_TABLE_CANDIDATES = (
    "transaction_features",
    "payment_transactions",
    "transactions",
)
DEFAULT_SETTLEMENT_TABLE_CANDIDATES = (
    "settlements",
    "settlement_records",
)


@dataclass(frozen=True)
class ResolvedField:
    canonical_name: str
    expr: str | None = None
    source_name: str | None = None
    derived: bool = False


@dataclass(frozen=True)
class ResolvedSource:
    domain: str
    source_table: str | None = None
    columns: frozenset[str] = field(default_factory=frozenset)
    fields: dict[str, ResolvedField] = field(default_factory=dict)
    notes: tuple[str, ...] = ()

    def has(self, canonical_name: str) -> bool:
        field = self.fields.get(canonical_name)
        return bool(field and field.expr)

    def value(self, canonical_name: str) -> str:
        field = self.fields.get(canonical_name)
        if not field or not field.expr:
            raise KeyError(f"{self.domain} source does not expose {canonical_name}")
        return field.expr

    def select(self, canonical_name: str, *, alias: str | None = None, null_if_missing: bool = False) -> str:
        output_name = alias or canonical_name
        if self.has(canonical_name):
            return f"{self.value(canonical_name)} AS {output_name}"
        if null_if_missing:
            return f"NULL AS {output_name}"
        raise KeyError(f"{self.domain} source does not expose {canonical_name}")

    def missing(self, *canonical_names: str) -> list[str]:
        return [name for name in canonical_names if not self.has(name)]


def _candidate_names(preferred_table: str | None, configured_tables: str | None, defaults: Iterable[str]) -> tuple[str, ...]:
    ordered: list[str] = []
    seen: set[str] = set()
    raw_values: list[str] = []
    if preferred_table:
        raw_values.append(str(preferred_table))
    if configured_tables:
        raw_values.extend(str(configured_tables).split(","))
    raw_values.extend(str(item) for item in defaults)
    for value in raw_values:
        normalized = value.strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            ordered.append(normalized)
    return tuple(ordered)


def _raw_field(columns: set[str], canonical_name: str, *source_names: str) -> ResolvedField:
    for source_name in source_names:
        if source_name in columns:
            return ResolvedField(canonical_name=canonical_name, expr=source_name, source_name=source_name)
    return ResolvedField(canonical_name=canonical_name)


def _derived_field(columns: set[str], canonical_name: str, *options: tuple[str, str]) -> ResolvedField:
    for source_name, expr in options:
        if source_name in columns:
            return ResolvedField(canonical_name=canonical_name, expr=expr, source_name=source_name, derived=True)
    return ResolvedField(canonical_name=canonical_name)


def _transaction_fields(columns: set[str]) -> dict[str, ResolvedField]:
    fields = {
        "merchant_id": _raw_field(columns, "merchant_id", "merchant_id", "mid"),
        "p_date": _raw_field(columns, "p_date", "p_date", "transaction_date", "txn_date", "payment_date"),
        "status": _raw_field(columns, "status", "status", "txn_status", "transaction_status"),
        "tx_id": _raw_field(columns, "tx_id", "transaction_fact_id", "transaction_id", "tx_id", "txn_id", "source_txn_id"),
        "invoice_nr": _raw_field(columns, "invoice_nr", "invoice_nr", "invoice_id", "order_id"),
        "initiated_at": _raw_field(columns, "initiated_at", "initiated_at", "created_at"),
        "completed_at": _raw_field(columns, "completed_at", "completed_at", "updated_at"),
        "payment_mode": _raw_field(columns, "payment_mode", "payment_mode", "mode", "payment_channel"),
        "response_code": _raw_field(columns, "response_code", "response_code", "resp_code", "gateway_response_code"),
        "response_desc": _raw_field(columns, "response_desc", "response_desc", "resp_desc", "response_message"),
        "amount_rupees": _raw_field(columns, "amount_rupees", "amount_rupees", "transaction_amount", "txn_amount", "amount"),
        "amount_paise": _raw_field(columns, "amount_paise", "amount_paise"),
        "terminal_id": _raw_field(columns, "terminal_id", "terminal_id", "tid"),
        "source_system": _raw_field(columns, "source_system", "source_system", "provider", "gateway"),
        "source_txn_id": _raw_field(columns, "source_txn_id", "source_txn_id", "gateway_txn_id", "provider_txn_id"),
        "hour_of_day": _raw_field(columns, "hour_of_day", "hour_of_day"),
        "day_of_week": _raw_field(columns, "day_of_week", "day_of_week"),
        "payer_bank_code": _raw_field(columns, "payer_bank_code", "payer_bank_code", "bank_code", "issuer_bank_code"),
        "card_network": _raw_field(columns, "card_network", "card_network", "network"),
        "device_type": _raw_field(columns, "device_type", "device_type", "pos_type"),
        "os_name": _raw_field(columns, "os_name", "os_name", "terminal_os"),
    }
    if not fields["p_date"].expr:
        fields["p_date"] = _derived_field(
            columns,
            "p_date",
            ("created_at", "DATE(created_at)"),
            ("initiated_at", "DATE(initiated_at)"),
        )
    if not fields["source_txn_id"].expr and "transaction_id" in columns:
        fields["source_txn_id"] = ResolvedField(
            canonical_name="source_txn_id",
            expr="transaction_id",
            source_name="transaction_id",
        )
    return fields


def _settlement_fields(columns: set[str]) -> dict[str, ResolvedField]:
    fields = {
        "settlement_id": _raw_field(columns, "settlement_id", "settlement_id", "payout_id", "batch_id"),
        "merchant_id": _raw_field(columns, "merchant_id", "merchant_id", "mid"),
        "status": _raw_field(columns, "status", "status", "settlement_status"),
        "expected_date": _raw_field(columns, "expected_date", "expected_date", "settlement_date", "payout_date"),
        "settled_at": _raw_field(columns, "settled_at", "settled_at", "paid_at"),
        "amount_rupees": _raw_field(columns, "amount_rupees", "amount_rupees", "net_settlement_amount", "amount"),
        "gross_amount": _raw_field(columns, "gross_amount", "gross_amount", "amount_rupees"),
        "net_settlement_amount": _raw_field(columns, "net_settlement_amount", "net_settlement_amount", "amount_rupees"),
        "mdr_deducted": _raw_field(columns, "mdr_deducted", "mdr_deducted"),
        "gst_on_mdr": _raw_field(columns, "gst_on_mdr", "gst_on_mdr"),
        "tds_deducted": _raw_field(columns, "tds_deducted", "tds_deducted"),
        "chargeback_deductions": _raw_field(columns, "chargeback_deductions", "chargeback_deductions"),
        "reserve_held": _raw_field(columns, "reserve_held", "reserve_held"),
        "adjustment_amount": _raw_field(columns, "adjustment_amount", "adjustment_amount"),
        "hold_reason": _raw_field(columns, "hold_reason", "hold_reason"),
        "payment_mode": _raw_field(columns, "payment_mode", "payment_mode"),
        "txn_count": _raw_field(columns, "txn_count", "txn_count"),
        "refund_count": _raw_field(columns, "refund_count", "refund_count"),
    }
    if "reference" in columns and "settlement_utr" in columns:
        fields["reference"] = ResolvedField(
            canonical_name="reference",
            expr="COALESCE(reference, settlement_utr)",
            source_name="reference",
            derived=True,
        )
    elif "reference" in columns:
        fields["reference"] = ResolvedField(canonical_name="reference", expr="reference", source_name="reference")
    elif "settlement_utr" in columns:
        fields["reference"] = ResolvedField(canonical_name="reference", expr="settlement_utr", source_name="settlement_utr")
    else:
        fields["reference"] = ResolvedField(canonical_name="reference")
    if "currency" in columns:
        fields["currency"] = ResolvedField(canonical_name="currency", expr="currency", source_name="currency")
    else:
        fields["currency"] = ResolvedField(canonical_name="currency", expr="'INR'", derived=True)
    if fields["expected_date"].expr and fields["settled_at"].expr:
        fields["scope_date"] = ResolvedField(
            canonical_name="scope_date",
            expr=f"COALESCE({fields['expected_date'].expr}, DATE({fields['settled_at'].expr}))",
            derived=True,
        )
    elif fields["expected_date"].expr:
        fields["scope_date"] = ResolvedField(
            canonical_name="scope_date",
            expr=f"DATE({fields['expected_date'].expr})",
            source_name=fields["expected_date"].source_name,
            derived=True,
        )
    elif fields["settled_at"].expr:
        fields["scope_date"] = ResolvedField(
            canonical_name="scope_date",
            expr=f"DATE({fields['settled_at'].expr})",
            source_name=fields["settled_at"].source_name,
            derived=True,
        )
    else:
        fields["scope_date"] = ResolvedField(canonical_name="scope_date")
    return fields


def _resolve_source(
    engine: Any,
    *,
    domain: str,
    candidates: Iterable[str],
    field_factory: Any,
    required_fields: tuple[str, ...],
) -> ResolvedSource:
    for index, table_name in enumerate(candidates):
        columns = _table_columns(engine, table_name)
        if not columns:
            continue
        fields = field_factory(columns)
        source = ResolvedSource(
            domain=domain,
            source_table=table_name,
            columns=frozenset(columns),
            fields=fields,
            notes=(),
        )
        if not source.missing(*required_fields):
            notes: list[str] = []
            if index > 0:
                notes.append(f"{domain} source fell back to {table_name}")
            return ResolvedSource(
                domain=domain,
                source_table=table_name,
                columns=frozenset(columns),
                fields=fields,
                notes=tuple(notes),
            )
    fallback_table = next(iter(candidates), None)
    return ResolvedSource(
        domain=domain,
        source_table=fallback_table,
        notes=(f"No compatible {domain} source was detected",),
    )


def _table_columns(engine: Any, table: str) -> set[str]:
    table = str(table or "").strip()
    if not table:
        return set()
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = :table_name
                    """
                ),
                {"table_name": table},
            ).fetchall()
        cols = {str(row[0]).lower() for row in rows if row and row[0]}
        if cols:
            return cols
    except Exception:
        pass
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
        return {str(row[1]).lower() for row in rows if len(row) > 1 and row[1]}
    except Exception:
        return set()


def resolve_transaction_provider(engine: Any, *, preferred_table: str | None = None) -> ResolvedSource:
    candidates = _candidate_names(
        preferred_table or str(getattr(Config, "QUERY_SOURCE_TABLE", "transaction_features") or "transaction_features"),
        str(getattr(Config, "TRANSACTION_SOURCE_TABLE_CANDIDATES", "") or ""),
        DEFAULT_TRANSACTION_TABLE_CANDIDATES,
    )
    return _resolve_source(
        engine,
        domain="transactions",
        candidates=candidates,
        field_factory=_transaction_fields,
        required_fields=("merchant_id", "p_date", "status"),
    )


def resolve_settlement_provider(engine: Any, *, preferred_table: str | None = None) -> ResolvedSource:
    candidates = _candidate_names(
        preferred_table or "settlements",
        str(getattr(Config, "SETTLEMENT_SOURCE_TABLE_CANDIDATES", "") or ""),
        DEFAULT_SETTLEMENT_TABLE_CANDIDATES,
    )
    return _resolve_source(
        engine,
        domain="settlements",
        candidates=candidates,
        field_factory=_settlement_fields,
        required_fields=("merchant_id",),
    )
