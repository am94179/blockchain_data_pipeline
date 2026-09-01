import pytest
from common.silver import (
    finalize_rejections,
    keep_latest_by_key,
    partition_by_reference,
    split_validation_rows,
)
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("silver-helper-tests").getOrCreate()
    yield spark
    spark.stop()


def test_split_validation_rows(spark):
    df = spark.createDataFrame(
        [("valid", []), ("invalid", ["MISSING_FIELD"])],
        ["id", "validation_errors"],
    )

    valid, rejected = split_validation_rows(df)

    assert [row.id for row in valid.collect()] == ["valid"]
    assert [row.id for row in rejected.collect()] == ["invalid"]


def test_keep_latest_by_key(spark):
    df = spark.createDataFrame(
        [("a", 1, "first"), ("a", 2, "latest"), ("b", 1, "only")],
        ["key", "batch_id", "value"],
    )

    result = keep_latest_by_key(
        df,
        key_columns=["key"],
        order_columns=["batch_id"],
    )

    assert {row.key: row.value for row in result.collect()} == {"a": "latest", "b": "only"}


def test_partition_by_reference(spark):
    children = spark.createDataFrame([("a", 1), ("missing", 2)], ["tx_hash", "value"])
    references = spark.createDataFrame([("a",), ("a",)], ["tx_hash"])

    valid, rejected = partition_by_reference(
        children,
        references,
        join_columns=["tx_hash"],
        error_code="TRANSACTION_NOT_FOUND",
    )

    assert [row.tx_hash for row in valid.collect()] == ["a"]
    assert rejected.first().validation_errors == ["TRANSACTION_NOT_FOUND"]


def test_finalize_rejections_adds_timestamp(spark):
    first = spark.createDataFrame([("a", ["FIRST"])], ["id", "validation_errors"])
    second = spark.createDataFrame([("b", ["SECOND"])], ["id", "validation_errors"])

    rejected = finalize_rejections(first, second).collect()

    assert {row.id for row in rejected} == {"a", "b"}
    assert all(row.rejected_at is not None for row in rejected)
