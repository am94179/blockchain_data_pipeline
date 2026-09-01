from common.bronze import ingest_csv_to_bronze, parse_bronze_args
from common.logger import get_logger
from common.session import get_spark
from common.tables import bronze
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


logger = get_logger("bronze.ingest_transactions")



def schema():

    return StructType(
        [
            StructField("tx_hash", StringType(), nullable=False),
            StructField("block_hash", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("fee_sats", IntegerType(), True),
            StructField("size_bytes", IntegerType(), True),
            StructField("input_count", IntegerType(), True),
            StructField("output_count", IntegerType(), True),
            StructField("total_input_sats", IntegerType(), True),
            StructField("total_output_sats", IntegerType(), True),
            # StructField("ingested_at", TimestampType(), nullable=False),
            # StructField("source_file", StringType(), nullable=False),
            # StructField("batch_id", StringType(), nullable=False),
        ]
    )


def main(input_path: str, batch_id: str) -> None:
    logger.info("Starting bronze ingestion: dataset=transactions batch_id=%s input_path=%s", batch_id, input_path)
    spark = get_spark("ingest-transactions")
    try:
        ingest_csv_to_bronze(
            spark,
            input_path=input_path,
            batch_id=batch_id,
            target_table=bronze("transactions"),
            schema=schema(),
            logger_name="bronze.ingest_transactions",
        )
    finally:
        logger.info("Stopping bronze ingestion: dataset=transactions batch_id=%s", batch_id)
        spark.stop()


if __name__ == "__main__":
    args = parse_bronze_args()
    main(args.input_path, args.batch_id)
