import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from common.logger import get_logger


def parse_bronze_args() -> argparse.Namespace:
    """Parse the common CSV ingestion arguments used by Bronze jobs."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--batch_id", required=True)
    return parser.parse_args()


def add_ingestion_metadata(df: DataFrame, batch_id: str) -> DataFrame:
    """Attach lineage fields shared by all Bronze records."""
    return (
        df.withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withColumn("batch_id", F.lit(batch_id))
    )


def ingest_csv_to_bronze(
    spark: SparkSession,
    *,
    input_path: str,
    batch_id: str,
    target_table: str,
    schema: StructType,
    logger_name: str,
) -> None:
    """Read a schema-bound CSV and create or append its Bronze Iceberg table."""
    logger = get_logger(logger_name)
    raw_df = spark.read.option("header", "true").schema(schema).csv(input_path)
    bronze_df = add_ingestion_metadata(raw_df, batch_id)

    if not spark.catalog.tableExists(target_table):
        bronze_df.writeTo(target_table).create()
        logger.info("Created Bronze table: table=%s", target_table)
        return

    (
        bronze_df.writeTo(target_table)
        .option("mergeSchema", "true")
        .append()
    )
    logger.info("Appended Bronze table: table=%s", target_table)
