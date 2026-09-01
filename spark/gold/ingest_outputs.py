
from common.iceberg import write_iceberg_table
from common.logger import get_logger
from common.session import get_spark
from common.tables import gold, silver
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SILVER_TRANSACTIONS_TABLE = silver("transactions")
SILVER_OUTPUTS_ENRICHED_TABLE = silver("outputs_enriched")
UTXO_RECONCILIATION_TABLE = silver("utxo_reconciliation")
GOLD_OUTPUTS_TABLE = gold("outputs")
logger = get_logger("gold.ingest_outputs")


def main() -> None:
    logger.info(
        "Starting gold ingestion: source=%s target=%s",
        SILVER_OUTPUTS_ENRICHED_TABLE,
        GOLD_OUTPUTS_TABLE,
    )
    spark = get_spark("ingest-gold-outputs")
    try:
        enriched_outputs_df: DataFrame = spark.table(SILVER_OUTPUTS_ENRICHED_TABLE)
        silver_transactions_df: DataFrame = spark.table(SILVER_TRANSACTIONS_TABLE)
        reconciliation_df: DataFrame = spark.table(UTXO_RECONCILIATION_TABLE)

        spending_transactions = silver_transactions_df.select(
            F.col("tx_hash").alias("spending_tx_hash"),
            F.col("timestamp").alias("spending_timestamp"),
        )
        output_spend_activity = (
            reconciliation_df.filter(F.col("resolution_status") == "resolved")
            .join(
                spending_transactions,
                reconciliation_df.tx_hash == spending_transactions.spending_tx_hash,
                how="left",
            )
            .groupBy("previous_tx_hash", "previous_output_index")
            .agg(
                F.count("*").alias("observed_spend_count"),
                F.min("spending_timestamp").alias("first_spent_timestamp"),
            )
            .select(
                F.col("previous_tx_hash").alias("tx_hash"),
                F.col("previous_output_index").alias("output_index"),
                "observed_spend_count",
                "first_spent_timestamp",
            )
        )

        output_transactions = silver_transactions_df.select(
            "tx_hash",
            F.col("block_hash").alias("creating_block_hash"),
            F.col("timestamp").alias("created_timestamp"),
        )

        gold_outputs_df = (
            enriched_outputs_df.join(
                output_spend_activity,
                on=["tx_hash", "output_index"],
                how="left",
            )
            .join(output_transactions, on="tx_hash", how="left")
            .fillna({"observed_spend_count": 0})
            .withColumn("output_date", F.to_date("created_timestamp"))
            .withColumn("first_spend_date", F.to_date("first_spent_timestamp"))
            .withColumn(
                "has_multiple_spend_attempts",
                F.col("observed_spend_count") > 1,
            )
            .withColumn(
                "time_to_spend_days",
                F.when(
                    F.col("canonical_spent"),
                    F.datediff(
                        F.to_date("first_spent_timestamp"),
                        F.to_date("created_timestamp"),
                    ),
                ).otherwise(F.lit(None).cast("int")),
            )
        )

        write_iceberg_table(
            gold_outputs_df,
            GOLD_OUTPUTS_TABLE,
            mode="create_or_replace",
        )
    finally:
        logger.info("Stopping gold ingestion: source=%s", GOLD_OUTPUTS_TABLE)
        spark.stop()


if __name__ == "__main__":
    main()
