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

BRONZE_TABLE = bronze("outputs")
SILVER_TABLE = silver("outputs")
SILVER_REJECTS_TABLE = silver_rejects("outputs")
logger = get_logger("silver.ingest_outputs")


def canonicalize_bronze_outputs(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("output_id", F.trim("output_id"))
        .withColumn("tx_hash", F.trim("tx_hash"))
        .withColumn("address", F.trim("address"))
        .withColumn("output_index", F.col("output_index").cast("int"))
        .withColumn("value_sats", F.col("value_sats").cast("bigint"))
        .withColumn("spent", F.col("spent").cast("boolean"))
    )


def validate_outputs(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "validation_errors",
        F.array_compact(
            F.array(
                F.when(
                    F.col("output_id").isNull()
                    | (F.length(F.trim(F.col("output_id"))) == 0),
                    F.lit("OUTPUT_ID_MISSING"),
                ),
                F.when(
                    F.col("tx_hash").isNull()
                    | (F.length(F.trim(F.col("tx_hash"))) == 0),
                    F.lit("TX_HASH_MISSING"),
                ),
                F.when(
                    F.col("output_index").isNull(),
                    F.lit("OUTPUT_INDEX_MISSING"),
                ),
                F.when(
                    F.col("output_index").isNotNull() & (F.col("output_index") < 0),
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
    spark = get_spark("ingest-silver-outputs")
    try:
        bronze_df = spark.table(BRONZE_TABLE)
        canonicalized_df = canonicalize_bronze_outputs(bronze_df)
        validated_df = validate_outputs(canonicalized_df)
        transaction_candidates, field_rejects = split_validation_rows(validated_df)

        logger.debug(
            f"field rejects: {field_rejects.count()} | transaction candidates: {transaction_candidates.count()}"
        )
        """
        Validate output_id ownership:
          - repeated output_id for the same output is a delivery duplicate
          - one output_id mapped to multiple outputs is a collision
        """
        output_id_collisions = (
            transaction_candidates.groupBy("output_id")
            .agg(
                F.countDistinct("tx_hash", "output_index").alias("distinct_output_keys")
            )
            .filter(F.col("distinct_output_keys") > 1)
            .select("output_id")
        )

        output_id_collision_rejects = transaction_candidates.join(
            output_id_collisions,
            on="output_id",
            how="inner",
        ).withColumn(
            "validation_errors",
            F.array(F.lit("OUTPUT_ID_COLLISION")),
        )

        output_id_valid = transaction_candidates.join(
            output_id_collisions,
            on="output_id",
            how="left_anti",
        )

        """
        Deduplicate
            - validated df with no field rejects by tx_hash
        """
        deduplicated_df = keep_latest_by_key(
            output_id_valid,
            key_columns=["tx_hash", "output_index"],
            order_columns=["ingested_at", "batch_id", "source_file"],
        )

        """
        Parent Validation:
            - verify every output relates to an existing transaction
        """
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
        ## Value Reconciliation:
            - verify sum(output value_sats) == sum(transaction value_sats)
        """
        output_totals = parent_transaction_valid.groupBy("tx_hash").agg(
            F.sum("value_sats").alias("actual_total_output_sats")
        )
        transaction_output_counts = parent_transaction_valid.groupBy("tx_hash").agg(
            F.count("*").alias("actual_output_count")
        )

        output_reconciliation_errors = (
            silver_transactions.select("tx_hash", "total_output_sats", "output_count")
            .join(output_totals, on="tx_hash", how="left")
            .join(transaction_output_counts, on="tx_hash", how="left")
            .fillna({"actual_total_output_sats": 0, "actual_output_count": 0})
            .withColumn(
                "reconciliation_errors",
                F.array_compact(
                    F.array(
                        F.when(
                            F.col("actual_total_output_sats")
                            != F.col("total_output_sats"),
                            F.lit("OUTPUT_VALUE_TOTAL_MISMATCH"),
                        ),
                        F.when(
                            F.col("actual_output_count") != F.col("output_count"),
                            F.lit("OUTPUT_COUNT_MISMATCH"),
                        ),
                    )
                ),
            )
            .filter(F.size("reconciliation_errors") > 0)
            .select("tx_hash", "reconciliation_errors")
        )

        reconciliation_rejects = (
            parent_transaction_valid.join(
                output_reconciliation_errors, on="tx_hash", how="inner"
            )
            .drop("validation_errors")
            .withColumnRenamed("reconciliation_errors", "validation_errors")
        )
        silver_df = (
            parent_transaction_valid.join(
                output_reconciliation_errors.select("tx_hash"),
                on="tx_hash",
                how="left_anti",
            )
            .drop("validation_errors")
        )

        silver_rejects_df = finalize_rejections(
            field_rejects,
            output_id_collision_rejects,
            parent_transaction_rejects,
            reconciliation_rejects,
        )
        # silver_df = (
        #     relationship_valid.drop("validation_errors")
        #     .withColumn("tx_hash", F.trim(F.col("tx_hash")))
        #     .withColumn("block_hash", F.trim(F.col("block_hash")))
        # )
        # silver_rejects_df = (
        #     field_rejects.unionByName(accounting_rejects)
        #     .unionByName(relationship_rejects)
        #     .withColumn("rejected_at", F.current_timestamp())
        # )
        write_iceberg_table(silver_df, SILVER_TABLE, mode="create_or_replace")
        write_iceberg_table(
            silver_rejects_df, SILVER_REJECTS_TABLE, mode="create_or_replace"
        )

    finally:
        logger.info("Stopping silver ingestion: source=%s", BRONZE_TABLE)
        spark.stop()


if __name__ == "__main__":
    main()
