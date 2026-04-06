from datetime import datetime, timezone

import pytest

from app.revenue_recovery.models import (
    AggregationOp,
    ColumnRef,
    FilterOp,
    FilterSpec,
    JoinSpec,
    MetricSpec,
    OrderBySpec,
    OrderTargetKind,
    QueryShape,
    QuerySpec,
    TableRef,
    TimeWindow,
)
from app.revenue_recovery.queryspec import SemanticMetricDefinition, SemanticMetricRegistry
from app.revenue_recovery.sql_compiler import SQLCompileError, compile_query_spec


def _window() -> TimeWindow:
    return TimeWindow(
        start=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
        end=datetime(2026, 3, 31, 0, 0, tzinfo=timezone.utc),
    )


def test_compile_query_spec_allows_aggregate_over_allowlisted_table():
    spec = QuerySpec(
        query_id="q_aggregate",
        shape=QueryShape.aggregate,
        base_table=TableRef(name="transactions", alias="t0"),
        metrics=[
            MetricSpec(
                name="amount_sum",
                alias="amount_sum",
                aggregation=AggregationOp.sum,
                column=ColumnRef(table_alias="t0", column="amount"),
            )
        ],
        filters=[
            FilterSpec(column=ColumnRef(table_alias="t0", column="merchant_id"), op=FilterOp.eq, value="merchant_001"),
            FilterSpec(column=ColumnRef(table_alias="t0", column="attempt_timestamp"), op=FilterOp.between, value=_window().start, value_to=_window().end),
        ],
        order_by=[OrderBySpec(target_kind=OrderTargetKind.metric_alias, target="amount_sum")],
    )

    compiled = compile_query_spec(spec)

    assert 'FROM "transactions" t0' in compiled.sql
    assert "GROUP BY" not in compiled.sql
    assert compiled.params[-1] == 100


def test_compile_query_spec_blocks_non_allowlisted_table():
    spec = QuerySpec(
        query_id="q_bad_table",
        shape=QueryShape.aggregate,
        base_table=TableRef(name="secret_table", alias="t0"),
        metrics=[MetricSpec(name="amount_sum", alias="amount_sum", aggregation=AggregationOp.sum, column=ColumnRef(table_alias="t0", column="amount"))],
    )

    with pytest.raises(SQLCompileError, match="Base table not allowed"):
        compile_query_spec(spec)


def test_compile_query_spec_blocks_non_allowlisted_column():
    spec = QuerySpec(
        query_id="q_bad_column",
        shape=QueryShape.aggregate,
        base_table=TableRef(name="transactions", alias="t0"),
        metrics=[MetricSpec(name="bad_col", alias="bad_col", aggregation=AggregationOp.sum, column=ColumnRef(table_alias="t0", column="secret_amount"))],
    )

    with pytest.raises(SQLCompileError, match="Column not allowed"):
        compile_query_spec(spec)


def test_compile_query_spec_blocks_semantic_metric_with_forbidden_token():
    registry = SemanticMetricRegistry()
    registry.register(
        SemanticMetricDefinition(
            metric_id="bad_metric",
            description="malicious",
            resolver=lambda _aliases: 'SUM(t0."amount"); DROP TABLE transactions',
        )
    )
    spec = QuerySpec(
        query_id="q_bad_metric",
        shape=QueryShape.aggregate,
        base_table=TableRef(name="transactions", alias="t0"),
        metrics=[MetricSpec(name="bad_metric", alias="bad_metric", semantic_metric_id="bad_metric")],
    )

    with pytest.raises(SQLCompileError, match="Forbidden SQL token"):
        compile_query_spec(spec, metric_registry=registry)


def test_compile_query_spec_parameterizes_in_filters_and_group_by():
    spec = QuerySpec(
        query_id="q_breakdown",
        shape=QueryShape.breakdown,
        base_table=TableRef(name="transactions", alias="t0"),
        metrics=[MetricSpec(name="amount_sum", alias="amount_sum", aggregation=AggregationOp.sum, column=ColumnRef(table_alias="t0", column="amount"))],
        dimensions=[ColumnRef(table_alias="t0", column="issuer")],
        filters=[
            FilterSpec(column=ColumnRef(table_alias="t0", column="issuer"), op=FilterOp.in_, value=["Bank X", "Bank Y"]),
        ],
        order_by=[OrderBySpec(target_kind=OrderTargetKind.dimension, target="t0.issuer")],
    )

    compiled = compile_query_spec(spec)

    assert "IN (%s, %s)" in compiled.sql
    assert "GROUP BY t0.\"issuer\"" in compiled.sql
    assert compiled.params[:2] == ["Bank X", "Bank Y"]


def test_compile_query_spec_caps_detail_query_limit():
    spec = QuerySpec(
        query_id="q_detail",
        shape=QueryShape.detail,
        base_table=TableRef(name="transactions", alias="t0"),
        limit=500,
    )

    with pytest.raises(SQLCompileError, match="Detail queries capped at 200 rows"):
        compile_query_spec(spec)


def test_compile_query_spec_blocks_too_many_joins():
    spec = QuerySpec(
        query_id="q_many_joins",
        shape=QueryShape.aggregate,
        base_table=TableRef(name="transactions", alias="t0"),
        metrics=[MetricSpec(name="amount_sum", alias="amount_sum", aggregation=AggregationOp.sum, column=ColumnRef(table_alias="t0", column="amount"))],
        joins=[
            JoinSpec(
                right_table=TableRef(name="transaction_features", alias="t1"),
                join_type="INNER",
                on_left=ColumnRef(table_alias="t0", column="txn_id"),
                on_right=ColumnRef(table_alias="t1", column="txn_id"),
            ),
            JoinSpec(
                right_table=TableRef(name="settlements", alias="t2"),
                join_type="LEFT",
                on_left=ColumnRef(table_alias="t0", column="txn_id"),
                on_right=ColumnRef(table_alias="t2", column="source_txn_id"),
            ),
            JoinSpec(
                right_table=TableRef(name="chargebacks", alias="t3"),
                join_type="LEFT",
                on_left=ColumnRef(table_alias="t0", column="txn_id"),
                on_right=ColumnRef(table_alias="t3", column="source_txn_id"),
            ),
        ],
    )

    with pytest.raises(SQLCompileError, match="Too many joins"):
        compile_query_spec(spec)
