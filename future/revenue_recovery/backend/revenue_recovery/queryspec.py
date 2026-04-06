from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .models import ColumnRef, FilterOp, FilterSpec, MetricSpec, QueryShape, QuerySpec, TableRef, TimeWindow


MetricResolver = Callable[[dict[str, str]], str]


@dataclass(frozen=True)
class SemanticMetricDefinition:
    metric_id: str
    description: str
    resolver: MetricResolver


class SemanticMetricRegistry:
    def __init__(self) -> None:
        self._definitions: dict[str, SemanticMetricDefinition] = {}

    def register(self, definition: SemanticMetricDefinition) -> None:
        self._definitions[definition.metric_id] = definition

    def resolve(self, metric_id: str, alias_lookup: dict[str, str]) -> str:
        definition = self._definitions.get(metric_id)
        if definition is None:
            raise KeyError(f"Unknown semantic metric: {metric_id}")
        return definition.resolver(alias_lookup)

    def has(self, metric_id: str) -> bool:
        return metric_id in self._definitions


def _alias_for_table(alias_lookup: dict[str, str], *table_names: str) -> str:
    for alias, table_name in alias_lookup.items():
        if table_name in table_names:
            return alias
    return next(iter(alias_lookup.keys()))


def _count_expression(alias: str, table_name: str) -> str:
    id_column = "transaction_fact_id" if table_name == "transaction_features" else "txn_id"
    return f'COUNT({alias}."{id_column}")'


def _status_column(alias: str) -> str:
    return f'{alias}."status"'


DEFAULT_METRIC_REGISTRY = SemanticMetricRegistry()
DEFAULT_METRIC_REGISTRY.register(
    SemanticMetricDefinition(
        metric_id="total_attempts",
        description="Total attempt count",
        resolver=lambda alias_lookup: (
            _count_expression(
                _alias_for_table(alias_lookup, "transaction_features", "transactions"),
                alias_lookup[_alias_for_table(alias_lookup, "transaction_features", "transactions")],
            )
        ),
    )
)
DEFAULT_METRIC_REGISTRY.register(
    SemanticMetricDefinition(
        metric_id="failed_attempts",
        description="Failed attempt count",
        resolver=lambda alias_lookup: (
            "SUM(CASE "
            f"WHEN {_status_column(_alias_for_table(alias_lookup, 'transaction_features', 'transactions'))} <> 'SUCCESS' THEN 1 "
            "ELSE 0 END)"
        ),
    )
)
DEFAULT_METRIC_REGISTRY.register(
    SemanticMetricDefinition(
        metric_id="failed_gmv",
        description="Failed GMV in rupees",
        resolver=lambda alias_lookup: (
            "SUM(CASE "
            f"WHEN {_status_column(_alias_for_table(alias_lookup, 'transaction_features', 'transactions'))} <> 'SUCCESS' "
            f"THEN COALESCE({_alias_for_table(alias_lookup, 'transaction_features', 'transactions')}.\"amount_rupees\", 0) "
            "ELSE 0 END)"
        ),
    )
)
DEFAULT_METRIC_REGISTRY.register(
    SemanticMetricDefinition(
        metric_id="success_rate",
        description="Successful attempts divided by total attempts",
        resolver=lambda alias_lookup: (
            "CASE "
            f"WHEN {_count_expression(_alias_for_table(alias_lookup, 'transaction_features', 'transactions'), alias_lookup[_alias_for_table(alias_lookup, 'transaction_features', 'transactions')])} = 0 THEN 0 "
            f"ELSE SUM(CASE WHEN {_status_column(_alias_for_table(alias_lookup, 'transaction_features', 'transactions'))} = 'SUCCESS' THEN 1 ELSE 0 END)::float "
            f"/ {_count_expression(_alias_for_table(alias_lookup, 'transaction_features', 'transactions'), alias_lookup[_alias_for_table(alias_lookup, 'transaction_features', 'transactions')])} END"
        ),
    )
)


def merchant_scope_filter(table_alias: str, merchant_id: str) -> FilterSpec:
    return FilterSpec(column=ColumnRef(table_alias=table_alias, column="merchant_id"), op=FilterOp.eq, value=merchant_id)


def time_window_filters(column: ColumnRef, window: TimeWindow) -> list[FilterSpec]:
    return [FilterSpec(column=column, op=FilterOp.between, value=window.start, value_to=window.end)]


def build_breakdown_query_spec(
    *,
    query_id: str,
    base_table: str,
    base_alias: str,
    metric: MetricSpec,
    dimension: str,
    time_column: str,
    window: TimeWindow,
    limit: int = 20,
) -> QuerySpec:
    return QuerySpec(
        query_id=query_id,
        shape=QueryShape.breakdown,
        base_table=TableRef(name=base_table, alias=base_alias),
        metrics=[metric],
        dimensions=[ColumnRef(table_alias=base_alias, column=dimension)],
        filters=time_window_filters(ColumnRef(table_alias=base_alias, column=time_column), window),
        limit=limit,
        time_column=ColumnRef(table_alias=base_alias, column=time_column),
    )
