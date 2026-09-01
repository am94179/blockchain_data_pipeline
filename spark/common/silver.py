from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def split_validation_rows(
    df: DataFrame,
    *,
    error_column: str = "validation_errors",
) -> tuple[DataFrame, DataFrame]:
    """Return rows without validation errors followed by rejected rows."""
    rejected = df.filter(F.size(F.col(error_column)) > 0)
    valid = df.filter(F.size(F.col(error_column)) == 0)
    return valid, rejected


def keep_latest_by_key(
    df: DataFrame,
    *,
    key_columns: Sequence[str],
    order_columns: Sequence[str],
) -> DataFrame:
    """Keep the newest ingestion record for each business key."""
    window = Window.partitionBy(*key_columns).orderBy(
        *[F.col(column).desc() for column in order_columns]
    )
    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def partition_by_reference(
    df: DataFrame,
    reference_keys: DataFrame,
    *,
    join_columns: Sequence[str],
    error_code: str,
    error_column: str = "validation_errors",
) -> tuple[DataFrame, DataFrame]:
    """Return referenced rows and rows rejected because their reference is missing."""
    key_columns = list(join_columns)
    known_keys = reference_keys.select(*key_columns).dropDuplicates(key_columns)
    valid = df.join(known_keys, on=key_columns, how="left_semi")
    rejected = (
        df.join(known_keys, on=key_columns, how="left_anti")
        .withColumn(error_column, F.array(F.lit(error_code)))
    )
    return valid, rejected


def finalize_rejections(
    *frames: DataFrame,
    rejected_at_column: str = "rejected_at",
) -> DataFrame:
    """Combine rejection frames and add a common rejection timestamp."""
    if not frames:
        raise ValueError("At least one rejection DataFrame is required.")

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)

    return combined.withColumn(rejected_at_column, F.current_timestamp())
