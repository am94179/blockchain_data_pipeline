from common.bronze import ingest_csv_to_bronze, parse_bronze_args
from common.logger import get_logger
from common.session import get_spark
from common.tables import bronze
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = get_logger("bronze.ingest_blocks")



def schema():
    return StructType(
        [
            StructField("block_hash", StringType(), nullable=False),
            StructField("block_height", IntegerType(), True),
            StructField("previous_block_hash", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), True),
            StructField("miner", StringType(), nullable=False),
            StructField("difficulty", DoubleType(), True),
            StructField("nonce", LongType(), True),
            StructField("size_bytes", IntegerType(), True),
            StructField("transaction_count", IntegerType(), True),
            # StructField("ingested_at", TimestampType(), nullable=False),
            # StructField("source_file", StringType(), nullable=False),
            # StructField("batch_id", StringType(), nullable=False),
        ]
    )


def main(input_path: str, batch_id: str, dataset: str) -> None:
    logger.info("Starting bronze ingestion: dataset=%s batch_id=%s input_path=%s", dataset, batch_id, input_path)

    spark = get_spark("ingest-blocks")
    try:
        ingest_csv_to_bronze(
            spark,
            input_path=input_path,
            batch_id=batch_id,
            target_table=bronze(dataset),
            schema=schema(),
            logger_name="bronze.ingest_blocks",
        )
    finally:
        logger.info("Stopping bronze ingestion: dataset=%s batch_id=%s", dataset, batch_id)
        spark.stop()


if __name__ == "__main__":
    dataset = "blocks"
    args = parse_bronze_args()
    main(args.input_path, args.batch_id, dataset)
