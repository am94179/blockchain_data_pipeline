from common.iceberg import write_iceberg_table
from common.logger import get_logger
from common.session import get_spark
from common.tables import gold, silver
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SILVER_TRANSACTIONS_TABLE = silver("transactions")
SILVER_INPUTS_TABLE = silver("inputs")
SILVER_OUTPUTS_TABLE = silver("outputs")
GOLD_TRANSACTIONS_TABLE = gold("transactions")
logger = get_logger("gold.ingest_transactions")


def main() -> None:
    logger.info(
        "Starting gold ingestion: source=%s target=%s",
        SILVER_TRANSACTIONS_TABLE,
        GOLD_TRANSACTIONS_TABLE,
    )
    spark = get_spark("ingest-gold-transactions")
    try:
        silver_tx_df: DataFrame = spark.table(SILVER_TRANSACTIONS_TABLE)
        silver_inputs_df: DataFrame = spark.table(SILVER_INPUTS_TABLE)
        silver_outputs_df: DataFrame = spark.table(SILVER_OUTPUTS_TABLE)

        silver_inputs_agg = silver_inputs_df.groupBy("tx_hash").agg(
            F.count("*").alias("observed_input_count"),
            F.sum("value_sats").alias("observed_total_input_sats"),
        )

        silver_outputs_agg = silver_outputs_df.groupBy("tx_hash").agg(
            F.count("*").alias("observed_output_count"),
            F.sum("value_sats").alias("observed_total_output_sats"),
        )
        gold_tx_df = (
            silver_tx_df.join(silver_inputs_agg, on="tx_hash", how="left")
            .join(silver_outputs_agg, on="tx_hash", how="left")
            .fillna(
                0,
                subset=[
                    "observed_input_count",
                    "observed_total_input_sats",
                    "observed_output_count",
                    "observed_total_output_sats",
                ],
            )
        )

        gold_tx_df = (
            gold_tx_df.withColumn(
                "fee_rate_sats_per_byte",
                F.when(F.col("size_bytes") == 0, F.lit(None)).otherwise(
                    F.col("fee_sats") / F.col("size_bytes")
                ),
            )
            .withColumn(
                "input_count_matches",
                F.col("input_count") == F.col("observed_input_count"),
            )
            .withColumn(
                "output_count_matches",
                F.col("output_count") == F.col("observed_output_count"),
            )
            .withColumn(
                "input_value_matches",
                F.col("total_input_sats") == F.col("observed_total_input_sats"),
            )
            .withColumn(
                "output_value_matches",
                F.col("total_output_sats") == F.col("observed_total_output_sats"),
            )
            .withColumn("transaction_date", F.to_date("timestamp"))
        )

        write_iceberg_table(
            gold_tx_df, GOLD_TRANSACTIONS_TABLE, mode="create_or_replace"
        )

    finally:
        logger.info("Stopping gold ingestion: source=%s", GOLD_TRANSACTIONS_TABLE)
        spark.stop()


if __name__ == "__main__":
    main()
