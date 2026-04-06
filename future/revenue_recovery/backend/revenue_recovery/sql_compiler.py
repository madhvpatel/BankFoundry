from __future__ import annotations

from typing import Iterable

from .models import (
    AggregationOp,
    ColumnRef,
    CompiledSQL,
    FilterOp,
    FilterSpec,
    JoinSpec,
    MetricSpec,
    OrderBySpec,
    OrderTargetKind,
    QueryShape,
    QuerySpec,
    SQLGuardrailResult,
    TableRef,
)
from .queryspec import DEFAULT_METRIC_REGISTRY, SemanticMetricRegistry

ALLOWED_TABLES = {
    "transactions",
    "transaction_features",
    "terminal_health_snapshots",
    "terminal_events",
    "settlements",
    "chargebacks",
    "merchant_config_audit",
    "failure_code_reference",
}

ALLOWED_COLUMNS = {
    "transactions": {
        "txn_id", "merchant_id", "tid", "payment_mode", "attempt_timestamp", "amount",
        "status", "response_code", "issuer", "acquirer", "bank",
    },
    "transaction_features": {
        "transaction_fact_id",
        "source_txn_id",
        "merchant_id",
        "terminal_id",
        "payment_mode",
        "sub_mode",
        "status",
        "response_code",
        "response_desc",
        "amount_rupees",
        "p_date",
        "initiated_at",
        "completed_at",
        "success_flag",
        "retry_indicator",
        "failure_bucket",
        "high_risk_flag",
    },
    "terminal_health_snapshots": {
        "tid",
        "mid",
        "captured_at",
        "printer_status",
        "battery_status",
        "quick_battery_drainage",
        "low_network_strength",
    },
    "terminal_events": {
        "event_id", "tid", "event_type", "description", "performed_by", "event_timestamp",
    },
    "settlements": {
        "settlement_id",
        "mid",
        "tid",
        "settlement_date",
        "gross_amount",
        "mdr_deducted",
        "gst_on_mdr",
        "tds_deducted",
        "chargeback_deductions",
        "reserve_held",
        "adjustment_amount",
        "net_settlement_amount",
        "settlement_status",
        "payment_mode",
        "p_date",
    },
    "chargebacks": {
        "chargeback_id",
        "source_txn_id",
        "mid",
        "tid",
        "chargeback_amount",
        "chargeback_reason_code",
        "chargeback_reason_desc",
        "card_network",
        "chargeback_stage",
        "filed_date",
        "response_due_date",
        "merchant_response_status",
        "resolution_outcome",
        "p_date",
    },
    "merchant_config_audit": {
        "merchant_id", "config_type", "old_value", "new_value", "changed_at", "changed_by",
    },
    "failure_code_reference": {
        "response_code", "network", "bucket", "description", "likely_owner", "recommended_action",
    },
}

FORBIDDEN_SQL_TOKENS = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "TRUNCATE",
    "CREATE",
    "GRANT",
    "REVOKE",
    "UNION",
    "WITH RECURSIVE",
}


class SQLCompileError(ValueError):
    pass


class SQLGuardrails:
    @staticmethod
    def validate_table(table: TableRef) -> None:
        if table.name not in ALLOWED_TABLES:
            raise SQLCompileError(f"Table not allowed: {table.name}")

    @staticmethod
    def validate_column(col: ColumnRef, table_lookup: dict[str, str]) -> None:
        if col.table_alias is None:
            raise SQLCompileError(f"Unqualified column not allowed: {col.column}")
        table_name = table_lookup.get(col.table_alias)
        if not table_name:
            raise SQLCompileError(f"Unknown table alias: {col.table_alias}")
        if col.column not in ALLOWED_COLUMNS.get(table_name, set()):
            raise SQLCompileError(f"Column not allowed: {table_name}.{col.column}")

    @staticmethod
    def validate_query_spec(spec: QuerySpec) -> SQLGuardrailResult:
        violations: list[str] = []
        if spec.base_table.name not in ALLOWED_TABLES:
            violations.append(f"Base table not allowed: {spec.base_table.name}")
        if len(spec.joins) > 2:
            violations.append("Too many joins for v1")
        if spec.limit > 1000:
            violations.append("Limit exceeds cap")
        if spec.shape == QueryShape.detail and spec.limit > 200:
            violations.append("Detail queries capped at 200 rows")
        if not spec.metrics and spec.shape != QueryShape.detail:
            violations.append("Non-detail queries require at least one metric")
        return SQLGuardrailResult(allowed=not violations, violations=violations)


def _normalize_aliases(spec: QuerySpec) -> dict[str, str]:
    alias_lookup: dict[str, str] = {}
    base_alias = spec.base_table.alias or "t0"
    alias_lookup[base_alias] = spec.base_table.name
    seen_aliases = {base_alias}
    for index, join in enumerate(spec.joins, start=1):
        alias = join.right_table.alias or f"t{index}"
        if alias in seen_aliases:
            raise SQLCompileError(f"Duplicate table alias: {alias}")
        seen_aliases.add(alias)
        alias_lookup[alias] = join.right_table.name
    return alias_lookup


def render_column(column: ColumnRef) -> str:
    if column.table_alias is None:
        raise SQLCompileError(f"Unqualified column not allowed: {column.column}")
    return f'{column.table_alias}."{column.column}"'


def _safe_semantic_expression(expression: str) -> str:
    upper = expression.upper()
    for token in FORBIDDEN_SQL_TOKENS:
        if token in upper:
            raise SQLCompileError(f"Forbidden SQL token detected in semantic expression: {token}")
    if ";" in expression or "--" in expression or "/*" in expression:
        raise SQLCompileError("Semantic expressions must not contain SQL control tokens")
    return expression


def render_metric(metric: MetricSpec, alias_lookup: dict[str, str], metric_registry: SemanticMetricRegistry) -> str:
    if metric.semantic_metric_id:
        expression = metric_registry.resolve(metric.semantic_metric_id, alias_lookup)
        return f"({_safe_semantic_expression(expression)}) AS \"{metric.alias}\""

    if metric.aggregation and metric.column:
        column = render_column(metric.column)
        if metric.aggregation == AggregationOp.count_distinct:
            return f"COUNT(DISTINCT {column}) AS \"{metric.alias}\""
        return f"{metric.aggregation.value}({column}) AS \"{metric.alias}\""

    raise SQLCompileError(f"Invalid MetricSpec: {metric.name}")


def render_filter(filter_spec: FilterSpec, params: list) -> str:
    column = render_column(filter_spec.column)
    if filter_spec.op in {FilterOp.is_null, FilterOp.is_not_null}:
        return f"{column} {filter_spec.op.value}"
    if filter_spec.op == FilterOp.between:
        params.extend([filter_spec.value, filter_spec.value_to])
        return f"{column} BETWEEN %s AND %s"
    if filter_spec.op in {FilterOp.in_, FilterOp.not_in}:
        if not isinstance(filter_spec.value, list) or not filter_spec.value:
            raise SQLCompileError("IN/NOT IN filters require non-empty list values")
        placeholders = ", ".join(["%s"] * len(filter_spec.value))
        params.extend(filter_spec.value)
        return f"{column} {filter_spec.op.value} ({placeholders})"
    params.append(filter_spec.value)
    return f"{column} {filter_spec.op.value} %s"


def render_order_by(order_by: Iterable[OrderBySpec], metric_aliases: set[str], dimensions: list[ColumnRef]) -> str:
    dimension_targets = {f"{column.table_alias}.{column.column}" for column in dimensions if column.table_alias}
    parts: list[str] = []
    for item in order_by:
        if item.target_kind == OrderTargetKind.metric_alias:
            if item.target not in metric_aliases:
                raise SQLCompileError(f"Unknown metric alias in ORDER BY: {item.target}")
            parts.append(f'"{item.target}" {item.direction.value}')
            continue
        if item.target not in dimension_targets:
            raise SQLCompileError(f"Unknown dimension target in ORDER BY: {item.target}")
        alias, column = item.target.split(".", 1)
        parts.append(f'{alias}."{column}" {item.direction.value}')
    return ", ".join(parts)


def compile_query_spec(spec: QuerySpec, *, metric_registry: SemanticMetricRegistry = DEFAULT_METRIC_REGISTRY) -> CompiledSQL:
    guard = SQLGuardrails.validate_query_spec(spec)
    if not guard.allowed:
        raise SQLCompileError("; ".join(guard.violations))

    SQLGuardrails.validate_table(spec.base_table)
    for join in spec.joins:
        SQLGuardrails.validate_table(join.right_table)

    alias_lookup = _normalize_aliases(spec)
    base_alias = spec.base_table.alias or "t0"

    for metric in spec.metrics:
        if metric.column:
            SQLGuardrails.validate_column(metric.column, alias_lookup)
    for dimension in spec.dimensions:
        SQLGuardrails.validate_column(dimension, alias_lookup)
    for filter_spec in spec.filters:
        SQLGuardrails.validate_column(filter_spec.column, alias_lookup)
    if spec.time_column:
        SQLGuardrails.validate_column(spec.time_column, alias_lookup)
    for join in spec.joins:
        SQLGuardrails.validate_column(join.on_left, alias_lookup)
        SQLGuardrails.validate_column(join.on_right, alias_lookup)

    select_parts: list[str] = []
    params: list = []
    metric_aliases = {metric.alias for metric in spec.metrics}

    for dimension in spec.dimensions:
        select_parts.append(render_column(dimension))
    for metric in spec.metrics:
        select_parts.append(render_metric(metric, alias_lookup, metric_registry))
    if not select_parts:
        select_parts = ["1"]

    sql_parts = [
        "SELECT",
        ", ".join(select_parts),
        f'FROM "{spec.base_table.name}" {base_alias}',
    ]

    for index, join in enumerate(spec.joins, start=1):
        right_alias = join.right_table.alias or f"t{index}"
        sql_parts.append(
            f'{join.join_type.value} JOIN "{join.right_table.name}" {right_alias} '
            f'ON {render_column(join.on_left)} = {render_column(join.on_right)}'
        )

    if spec.filters:
        sql_parts.append("WHERE " + " AND ".join(render_filter(filter_spec, params) for filter_spec in spec.filters))

    if spec.dimensions:
        sql_parts.append("GROUP BY " + ", ".join(render_column(dimension) for dimension in spec.dimensions))

    if spec.order_by:
        sql_parts.append("ORDER BY " + render_order_by(spec.order_by, metric_aliases, spec.dimensions))

    sql_parts.append("LIMIT %s")
    params.append(spec.limit)

    final_sql = "\n".join(sql_parts)
    upper_sql = final_sql.upper()
    for token in FORBIDDEN_SQL_TOKENS:
        if token in upper_sql:
            raise SQLCompileError(f"Forbidden SQL token detected in compiled query: {token}")

    return CompiledSQL(
        query_id=spec.query_id,
        sql=final_sql,
        params=params,
        tables_used=[spec.base_table.name] + [join.right_table.name for join in spec.joins],
        estimated_shape=spec.shape,
    )
