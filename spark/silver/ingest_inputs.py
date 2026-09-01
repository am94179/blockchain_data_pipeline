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

BRONZE_TABLE = bronze("inputs")
SILVER_TABLE = silver("inputs")
SILVER_REJECTS_TABLE = silver_rejects("inputs")
logger = get_logger("silver.ingest_inputs")


def canonicalize_bronze_inputs(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("input_id", F.trim("input_id"))
        .withColumn("tx_hash", F.trim("tx_hash"))
        .withColumn("previous_tx_hash", F.trim("previous_tx_hash"))
        .withColumn("address", F.trim("address"))
        .withColumn("input_index", F.col("input_index").cast("int"))
        .withColumn(
            "previous_output_index",
            F.col("previous_output_index").cast("int"),
        )
        .withColumn("value_sats", F.col("value_sats").cast("bigint"))
    )


def validate_inputs(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "validation_errors",
        F.array_compact(
            F.array(
                F.when(
                    F.col("input_id").isNull()
                    | (F.length(F.trim(F.col("input_id"))) == 0),
                    F.lit("INPUT_ID_MISSING"),
                ),
                F.when(
                    F.col("tx_hash").isNull()
                    | (F.length(F.trim(F.col("tx_hash"))) == 0),
                    F.lit("TX_HASH_MISSING"),
                ),
                F.when(
                    F.col("input_index").isNull(),
                    F.lit("INPUT_INDEX_MISSING"),
                ),
                F.when(
                    F.col("input_index") < 0,
                    F.lit("INPUT_INDEX_INVALID"),
                ),
                F.when(
                    F.col("previous_output_index").isNull()
                    | (F.col("previous_output_index") < 0),
                    F.lit("OUTPUT_INDEX_INVALID"),
                ),
                F.when(
                    F.col("value_sats").isNull() | (F.col("value_sats") <= 0),
                    F.lit("VALUE_SATS_INVALID"),
                ),
            )
        ),
    )


def main() -> None:
    logger.info(
        "Starting silver ingestion: source=%s target=%s", BRONZE_TABLE, SILVER_TABLE
    )
    spark = get_spark("ingest-silver-inputs")
    try:
        bronze_df = spark.table(BRONZE_TABLE)
        canonicalized_df = canonicalize_bronze_inputs(bronze_df)
        validated_df = validate_inputs(canonicalized_df)
        transaction_candidates, field_rejects = split_validation_rows(validated_df)

        logger.debug(
            f"field rejects: {field_rejects.count()} | transaction candidates: {transaction_candidates.count()}"
        )

        """
        Deduplicate accounting_candidates
            - validated df with no field rejects by tx_hash
        """
        deduplicated_df = keep_latest_by_key(
            transaction_candidates,
            key_columns=["tx_hash", "input_index"],
            order_columns=["ingested_at", "batch_id", "source_file"],
        )

        silver_transactions = spark.table(silver("transactions"))
        silver_transactions_keys = silver_transactions.select(
            "tx_hash"
        ).drop_duplicates(["tx_hash"])

        parent_transaction_valid, parent_transaction_rejects = partition_by_reference(
            deduplicated_df,
            silver_transactions_keys,
            join_columns=["tx_hash"],
            error_code="TRANSACTION_NOT_FOUND",
        )

        assert (
            deduplicated_df.count()
            == parent_transaction_rejects.count() + parent_transaction_valid.count()
        )
        """
        Value Reconciliation:
        - sum(input.value_sats) = transaction.total_input_sats
        """

        input_totals = parent_transaction_valid.groupBy("tx_hash").agg(
            F.sum("value_sats").alias("actual_total_input_sats")
        )
        transaction_input_counts = parent_transaction_valid.groupBy("tx_hash").agg(
            F.count("*").alias("actual_input_count")
        )

        input_reconciliation_errors = (
            silver_transactions.select("tx_hash", "total_input_sats", "input_count")
            .join(input_totals, on="tx_hash", how="left")
            .join(transaction_input_counts, on="tx_hash", how="left")
            .fillna({"actual_total_input_sats": 0, "actual_input_count": 0})
            .withColumn(
                "reconciliation_errors",
                F.array_compact(
                    F.array(
                        F.when(
                            F.col("actual_total_input_sats")
                            != F.col("total_input_sats"),
                            F.lit("INPUT_VALUE_TOTAL_MISMATCH"),
                        ),
                        F.when(
                            F.col("actual_input_count") != F.col("input_count"),
                            F.lit("INPUT_COUNT_MISMATCH"),
                        ),
                    )
                ),
            )
            .filter(F.size("reconciliation_errors") > 0)
            .select("tx_hash", "reconciliation_errors")
        )

        reconciliation_rejects = (
            parent_transaction_valid.join(
                input_reconciliation_errors, on="tx_hash", how="inner"
            )
            .drop("validation_errors")
            .withColumnRenamed("reconciliation_errors", "validation_errors")
        )
        silver_df = (
            parent_transaction_valid.join(
                input_reconciliation_errors.select("tx_hash"),
                on="tx_hash",
                how="left_anti",
            )
            .drop("validation_errors")
        )

        silver_rejects_df = finalize_rejections(
            field_rejects, parent_transaction_rejects, reconciliation_rejects
        )

        write_iceberg_table(silver_df, SILVER_TABLE, mode="create_or_replace")
        write_iceberg_table(
            silver_rejects_df,
            SILVER_REJECTS_TABLE,
            mode="create_or_replace",
        )

    finally:
        logger.info("Stopping silver ingestion: source=%s", BRONZE_TABLE)
        spark.stop()


if __name__ == "__main__":
    main()
