from common.bronze import ingest_csv_to_bronze, parse_bronze_args
from common.logger import get_logger
from common.session import get_spark
from common.tables import bronze
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

logger = get_logger("bronze.ingest_inputs")



def schema():
    return StructType(
        [
            StructField("input_id", StringType(), nullable=False),
            StructField("tx_hash", StringType(), nullable=False),
            StructField("input_index", IntegerType(), nullable=False),
            StructField("previous_tx_hash", StringType(), nullable=False),
            StructField("previous_output_index", StringType(), nullable=False),
            StructField("address", StringType(), nullable=False),
            StructField("value_sats", IntegerType(), nullable=False),
            # StructField("ingested_at", TimestampType(), nullable=False),
            # StructField("source_file", StringType(), nullable=False),
            # StructField("batch_id", StringType(), nullable=False),
        ]
    )


def main(input_path: str, batch_id: str, DATASET: str) -> None:
    logger.info("Starting bronze ingestion: dataset=%s batch_id=%s input_path=%s", DATASET, batch_id, input_path)
    spark = get_spark(f"ingest-{DATASET}")
    try:
        ingest_csv_to_bronze(
            spark,
            input_path=input_path,
            batch_id=batch_id,
            target_table=bronze(DATASET),
            schema=schema(),
            logger_name="bronze.ingest_inputs",
        )
    finally:
        logger.info("Stopping bronze ingestion: dataset=%s batch_id=%s", DATASET, batch_id)
        spark.stop()


if __name__ == "__main__":
    DATASET = "inputs"
    args = parse_bronze_args()
    main(args.input_path, args.batch_id, DATASET)
