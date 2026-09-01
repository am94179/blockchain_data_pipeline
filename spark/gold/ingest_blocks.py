
from common.iceberg import write_iceberg_table
from common.logger import get_logger
from common.session import get_spark
from common.tables import gold, silver
from pyspark.sql import functions as F

SILVER_TX_TABLE = silver("transactions")
SILVER_BLOCKS_TABLE = silver("blocks")
GOLD_BLOCKS_TABLE = gold("blocks")
logger = get_logger("gold.ingest_blocks")


def main() -> None:
    logger.info(
        "Starting gold ingestion: source=%s target=%s",
        SILVER_TX_TABLE,
        GOLD_BLOCKS_TABLE,
    )
    spark = get_spark("ingest-gold-blocks")
    try:
        silver_block_df = spark.table(SILVER_BLOCKS_TABLE)
        silver_tx = spark.table(SILVER_TX_TABLE)

        silver_tx_agg = silver_tx.groupBy("block_hash").agg(
            F.count("*").alias("observed_transaction_count"),
            F.sum("fee_sats").alias("total_fee_sats"),
            F.sum("size_bytes").alias("total_transaction_size_bytes"),
            F.sum("total_input_sats").alias("total_input_sats"),
            F.sum("total_output_sats").alias("total_output_sats"),
        )

        gold_blocks_df = (
            silver_block_df.join(silver_tx_agg, on="block_hash", how="left")
            .fillna(
                0,
                subset=[
                    "observed_transaction_count",
                    "total_fee_sats",
                    "total_transaction_size_bytes",
                    "total_input_sats",
                    "total_output_sats",
                ],
            )
            .withColumn(
                "transaction_count_matches",
                F.col("transaction_count") == F.col("observed_transaction_count"),
            )
        )
        write_iceberg_table(
            gold_blocks_df,
            GOLD_BLOCKS_TABLE,
            mode="create_or_replace",
        )

    finally:
        logger.info("Stopping gold ingestion: source=%s", GOLD_BLOCKS_TABLE)
        spark.stop()


if __name__ == "__main__":
    main()
