
from common.iceberg import write_iceberg_table
from common.logger import get_logger
from common.session import get_spark
from common.silver import (
    finalize_rejections,
    keep_latest_by_key,
    split_validation_rows,
)
from common.tables import bronze, silver, silver_rejects
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

BRONZE_TABLE = bronze("blocks")
SILVER_TABLE = silver("blocks")
SILVER_REJECTS_TABLE = silver_rejects("blocks")
logger = get_logger("silver.ingest_blocks")


def validate_blocks(df: DataFrame) -> DataFrame:
    """Append an error code for every failed block-domain validation."""
    return df.withColumn(
        "validation_errors",
        F.array_compact(
            F.array(
                F.when(
                    F.col("block_hash").isNull()
                    | (F.length(F.trim(F.col("block_hash"))) == 0),
                    F.lit("BLOCK_HASH_MISSING"),
                ),
                F.when(
                    F.col("block_height").isNull()
                    | (F.col("block_height") < 0),
                    F.lit("BLOCK_HEIGHT_INVALID"),
                ),
                F.when(
                    F.col("nonce").isNull() | (F.col("nonce") < 0),
                    F.lit("NONCE_INVALID"),
                ),
                F.when(
                    F.col("size_bytes").isNull() | (F.col("size_bytes") < 0),
                    F.lit("SIZE_INVALID"),
                ),
                F.when(
                    F.col("transaction_count").isNull()
                    | (F.col("transaction_count") < 0),
                    F.lit("TRANSACTION_COUNT_INVALID"),
                ),
                F.when(
                    F.col("difficulty").isNull() | (F.col("difficulty") <= 0),
                    F.lit("DIFFICULTY_INVALID"),
                ),
                F.when(
                    F.col("timestamp").isNull(),
                    F.lit("TIMESTAMP_MISSING"),
                ),
            )
        ),
    )


def main() -> None:
    logger.info(
        "Starting silver ingestion: source=%s target=%s", BRONZE_TABLE, SILVER_TABLE
    )
    spark = get_spark("ingest-silver-blocks")
    try:
        bronze_df = spark.read.table(BRONZE_TABLE)
        validated_df = validate_blocks(bronze_df)

        silver_candidates, rejected_df = split_validation_rows(validated_df)
        rejected_df = finalize_rejections(rejected_df)

        silver_candidates = (
            silver_candidates.drop("validation_errors")
            .withColumn("miner", F.lower(F.col("miner")))
        )

        deduplicated_df = keep_latest_by_key(
            silver_candidates,
            key_columns=["block_hash"],
            order_columns=["ingested_at", "batch_id"],
        )

        # -----------------------------------------
        # 6. Add derived columns
        # -----------------------------------------

        silver_df = (
            deduplicated_df.withColumn("block_date", F.to_date("timestamp"))
            .withColumn("block_year", F.year("timestamp"))
            .withColumn("block_month", F.month("timestamp"))
            .withColumn("block_hour", F.hour("timestamp"))
        )

        """
        TODO: 
        - chain validation step, 
            Deduplicate by block_hash.
            Sort by block_height.
            Use lag(block_hash) to compare against previous_block_hash.
        - store output
        """
        write_iceberg_table(silver_df, SILVER_TABLE, mode="create_or_replace")
        write_iceberg_table(rejected_df, SILVER_REJECTS_TABLE, mode="create_or_replace")
    finally:
        logger.info("Stopping silver ingestion: source=%s", BRONZE_TABLE)
        spark.stop()


if __name__ == "__main__":
    main()
