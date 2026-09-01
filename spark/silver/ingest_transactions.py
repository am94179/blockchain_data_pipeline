from common.iceberg import write_iceberg_table
from common.logger import get_logger
from common.session import get_spark
from common.silver import (
    finalize_rejections,
    keep_latest_by_key,
    partition_by_reference,
    split_validation_rows,
)
from common.tables import bronze, silver, silver_rejects
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

BRONZE_TABLE = bronze("transactions")
SILVER_TABLE = silver("transactions")
SILVER_REJECTS_TABLE = silver_rejects("transactions")
logger = get_logger("silver.ingest_transactions")


def validate_transactions(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "validation_errors",
        F.array_compact(
            F.array(
                F.when(
                    F.col("tx_hash").isNull()
                    | (F.length(F.trim(F.col("tx_hash"))) == 0),
                    F.lit("TX_HASH_MISSING"),
                ),
                F.when(
                    F.col("block_hash").isNull()
                    | (F.length(F.trim(F.col("block_hash"))) == 0),
                    F.lit("BLOCK_HASH_MISSING"),
                ),
                F.when(
                    F.col("timestamp").isNull(),
                    F.lit("TIMESTAMP_MISSING"),
                ),
                F.when(
                    F.col("fee_sats").isNull() | (F.col("fee_sats") < 0),
                    F.lit("FEE_INVALID"),
                ),
                F.when(
                    F.col("size_bytes").isNull() | (F.col("size_bytes") < 0),
                    F.lit("SIZE_INVALID"),
                ),
                F.when(
                    F.col("input_count").isNull() | (F.col("input_count") <= 0),
                    F.lit("INPUT_COUNT_INVALID"),
                ),
                F.when(
                    F.col("output_count").isNull() | (F.col("output_count") <= 0),
                    F.lit("OUTPUT_COUNT_INVALID"),
                ),
                F.when(
                    F.col("total_input_sats").isNull()
                    | (F.col("total_input_sats") < 0),
                    F.lit("TOTAL_INPUT_INVALID"),
                ),
                F.when(
                    F.col("total_output_sats").isNull()
                    | (F.col("total_output_sats") < 0),
                    F.lit("TOTAL_OUTPUT_INVALID"),
                ),
            )
        ),
    )


def main() -> None:
    logger.info(
        "Starting silver ingestion: source=%s target=%s", BRONZE_TABLE, SILVER_TABLE
    )
    spark = get_spark("ingest-silver-transactions")
    try:
        bronze_df = spark.table(BRONZE_TABLE)

        validated_df = validate_transactions(bronze_df)
        transaction_candidates, field_rejects = split_validation_rows(validated_df)
        print(
            f"field rejects: {field_rejects.count()} | transaction candidates: {transaction_candidates.count()}"
        )

        """
        Accounting validation:
            - total_input_sats = total_output_sats + fee_sats
        """
        accounting_validated_df = transaction_candidates.withColumn(
            "validation_errors",
            F.when(
                F.col("total_input_sats")
                != F.col("total_output_sats") + F.col("fee_sats"),
                F.concat(
                    F.col("validation_errors"),
                    F.array(F.lit("ACCOUNTING_MISMATCH")),
                ),
            ).otherwise(F.col("validation_errors")),
        )

        accounting_candidates, accounting_rejects = split_validation_rows(
            accounting_validated_df
        )

        """
        Deduplicate accounting_candidates
            - validated df with no field rejects by tx_hash
        """
        deduplicated_df = keep_latest_by_key(
            accounting_candidates,
            key_columns=["tx_hash"],
            order_columns=["ingested_at", "batch_id", "source_file"],
        )

        silver_blocks = spark.table(silver("blocks"))
        silver_block_keys = silver_blocks.select("block_hash").drop_duplicates(
            ["block_hash"]
        )

        relationship_valid, relationship_rejects = partition_by_reference(
            deduplicated_df,
            silver_block_keys,
            join_columns=["block_hash"],
            error_code="BLOCK_NOT_FOUND",
        )

        assert (
            deduplicated_df.count()
            == relationship_rejects.count() + relationship_valid.count()
        )

        silver_df = (
            relationship_valid.drop("validation_errors")
            .withColumn("tx_hash", F.trim(F.col("tx_hash")))
            .withColumn("block_hash", F.trim(F.col("block_hash")))
        )
        silver_rejects_df = finalize_rejections(
            field_rejects, accounting_rejects, relationship_rejects
        )
        write_iceberg_table(silver_df, SILVER_TABLE, mode="create_or_replace")
        write_iceberg_table(
            silver_rejects_df, SILVER_REJECTS_TABLE, mode="create_or_replace"
        )

    finally:
        logger.info("Stopping silver ingestion: source=%s", BRONZE_TABLE)
        spark.stop()


if __name__ == "__main__":
    main()
