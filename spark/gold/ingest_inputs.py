
from common.iceberg import write_iceberg_table
from common.logger import get_logger
from common.session import get_spark
from common.tables import gold, silver
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SILVER_TRANSACTIONS_TABLE = silver("transactions")
SILVER_INPUTS_TABLE = silver("inputs")
UTXO_RECONCILIATION_TABLE = silver("utxo_reconciliation")
GOLD_INPUTS_TABLE = gold("inputs")
logger = get_logger("gold.ingest_inputs")


def main() -> None:
    logger.info(
        "Starting gold ingestion: source=%s target=%s",
        SILVER_INPUTS_TABLE,
        GOLD_INPUTS_TABLE,
    )
    spark = get_spark("ingest-gold-inputs")
    try:
        silver_inputs_df: DataFrame = spark.table(SILVER_INPUTS_TABLE)
        silver_transactions_df: DataFrame = spark.table(SILVER_TRANSACTIONS_TABLE)
        reconciliation_df: DataFrame = spark.table(UTXO_RECONCILIATION_TABLE)

        reconciliation_fields = reconciliation_df.select(
            "tx_hash",
            "input_index",
            "resolution_status",
            "referenced_tx_hash",
            "referenced_output_index",
            "referenced_output_id",
            "referenced_address",
            "referenced_value_sats",
            "address_matches",
            "value_matches",
            "double_spend_detected",
        )

        spending_transactions = silver_transactions_df.select(
            "tx_hash",
            F.col("block_hash").alias("spending_block_hash"),
            F.col("timestamp").alias("spending_timestamp"),
        )
        referenced_transactions = silver_transactions_df.select(
            F.col("tx_hash").alias("referenced_tx_hash"),
            F.col("timestamp").alias("referenced_transaction_timestamp"),
        )

        gold_inputs_df = (
            silver_inputs_df.join(
                reconciliation_fields,
                on=["tx_hash", "input_index"],
                how="left",
            )
            .join(spending_transactions, on="tx_hash", how="left")
            .join(referenced_transactions, on="referenced_tx_hash", how="left")
            .withColumn(
                "resolution_status",
                F.coalesce(F.col("resolution_status"), F.lit("not_reconciled")),
            )
            .withColumn(
                "double_spend_detected",
                F.coalesce(F.col("double_spend_detected"), F.lit(False)),
            )
            .withColumn(
                "is_genesis_input",
                F.col("previous_tx_hash") == F.lit("genesis"),
            )
            .withColumn("spend_date", F.to_date("spending_timestamp"))
            .withColumn(
                "coin_age_days",
                F.when(
                    F.col("resolution_status") == "resolved",
                    F.datediff(
                        F.to_date("spending_timestamp"),
                        F.to_date("referenced_transaction_timestamp"),
                    ),
                ).otherwise(F.lit(None).cast("int")),
            )
        )

        write_iceberg_table(
            gold_inputs_df,
            GOLD_INPUTS_TABLE,
            mode="create_or_replace",
        )
    finally:
        logger.info("Stopping gold ingestion: source=%s", GOLD_INPUTS_TABLE)
        spark.stop()


if __name__ == "__main__":
    main()
