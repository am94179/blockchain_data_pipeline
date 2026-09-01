from common.logger import get_logger
from common.session import get_spark

logger = get_logger("jobs.test_spark_job")


def main() -> None:
    logger.info("Starting local Spark smoke test")
    spark = get_spark("local-spark-smoke-test")

    try:
        dataframe = spark.read.option("header", "true").option("inferSchema", "true").csv(
            "/opt/spark/data/sample.csv"
        )
        dataframe.printSchema()
        dataframe.write.mode("overwrite").parquet("/opt/spark/warehouse/test_output")
    finally:
        logger.info("Stopping local Spark smoke test")
        spark.stop()


if __name__ == "__main__":
    main()
