from datetime import datetime

import pytest

from common.silver import (
    finalize_rejections,
    keep_latest_by_key,
    partition_by_reference,
    split_validation_rows,
)
from tests.helpers import rows_by_key


# Splitting keeps valid and invalid rows mutually exclusive.
def test_split_validation_rows_separates_error_arrays(spark):
    df = spark.createDataFrame([("ok", []), ("bad", ["INVALID"])], ["id", "validation_errors"])

    valid, rejected = split_validation_rows(df)

    assert [row.id for row in valid.collect()] == ["ok"]
    assert [row.id for row in rejected.collect()] == ["bad"]


# Latest ingestion wins for every business key.
def test_keep_latest_by_key_uses_descending_order_columns(spark):
    df = spark.createDataFrame(
        [("a", "old", datetime(2026, 1, 1)), ("a", "new", datetime(2026, 1, 2)), ("b", "only", datetime(2026, 1, 1))],
        ["id", "value", "ingested_at"],
    )

    actual = rows_by_key(keep_latest_by_key(df, key_columns=["id"], order_columns=["ingested_at"]), "id")

    assert {key: value["value"] for key, value in actual.items()} == {"a": "new", "b": "only"}


# Reference partitioning reports each missing parent with the requested code.
def test_partition_by_reference_returns_semi_and_anti_join_results(spark):
    children = spark.createDataFrame([("known", 1), ("missing", 2)], ["parent", "id"])
    parents = spark.createDataFrame([("known",)], ["parent"])

    valid, rejected = partition_by_reference(children, parents, join_columns=["parent"], error_code="PARENT_MISSING")

    assert [row.id for row in valid.collect()] == [1]
    assert rejected.collect()[0].validation_errors == ["PARENT_MISSING"]


# Finalized rejects combine all frames and add an audit timestamp.
def test_finalize_rejections_unions_frames_and_adds_timestamp(spark):
    first = spark.createDataFrame([("a", ["A"])], ["id", "validation_errors"])
    second = spark.createDataFrame([("b", ["B"])], ["id", "validation_errors"])

    actual = rows_by_key(finalize_rejections(first, second), "id")

    assert set(actual) == {"a", "b"}
    assert all(row["rejected_at"] is not None for row in actual.values())


# Rejection finalization fails early when no input exists.
def test_finalize_rejections_requires_at_least_one_frame():
    with pytest.raises(ValueError, match="At least one"):
        finalize_rejections()
