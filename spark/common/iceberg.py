from typing import Literal

from pyspark.sql import DataFrame

from common.logger import get_logger

WriteMode = Literal["append", "overwrite_partitions", "create_or_replace"]

logger = get_logger("common.iceberg")


def write_iceberg_table(
    df: DataFrame,
    table_name: str,
    *,
    mode: WriteMode = "append",
    merge_schema: bool = False,
) -> None:
    """Write a DataFrame to a fully qualified Iceberg table."""
    writer = df.writeTo(table_name)

    if merge_schema:
        writer = writer.option("mergeSchema", "true")

    table_exists = df.sparkSession.catalog.tableExists(table_name)

    logger.info(
        "Writing Iceberg table: table=%s mode=%s exists=%s",
        table_name,
        mode,
        table_exists,
    )

    if not table_exists:
        writer.create()
        logger.info("Created Iceberg table: table=%s", table_name)
        return

    if mode == "append":
        writer.append()
    elif mode == "overwrite_partitions":
        writer.overwritePartitions()
    elif mode == "create_or_replace":
        writer.createOrReplace()
    else:
        raise ValueError(f"Unsupported Iceberg write mode: {mode}")

    logger.info("Completed Iceberg write: table=%s mode=%s", table_name, mode)