from pyspark.sql import SparkSession

from common.settings import (
    ICEBERG_CATALOG,
    ICEBERG_CATALOG_IMPL,
    ICEBERG_CATALOG_TYPE,
    ICEBERG_CATALOG_URI,
    ICEBERG_WAREHOUSE,
    SPARK_CORES_MAX,
    SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_CORES,
    SPARK_EXECUTOR_MEMORY,
    SPARK_LOG_LEVEL,
    SPARK_MASTER_URL,
    iceberg_catalog_key,
)


ICEBERG_NAMESPACES = ("bronze", "silver", "silver_rejects", "gold")


def ensure_iceberg_namespaces(spark: SparkSession) -> None:
    """Create the namespaces used by the pipeline in the shared catalog."""
    for namespace in ICEBERG_NAMESPACES:
        spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.{namespace}"
        )


def get_spark(app_name: str) -> SparkSession:
    """Create a Spark session configured for this project's Iceberg catalog."""
    spark = (
        SparkSession.builder.appName(app_name)
        .master(SPARK_MASTER_URL)
        .config("spark.cores.max", SPARK_CORES_MAX)
        .config("spark.executor.cores", SPARK_EXECUTOR_CORES)
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.iceberg.vectorization.enabled", "false")
        .config(iceberg_catalog_key(""), ICEBERG_CATALOG_IMPL)
        .config(iceberg_catalog_key("type"), ICEBERG_CATALOG_TYPE)
        .config(iceberg_catalog_key("uri"), ICEBERG_CATALOG_URI)
        .config(iceberg_catalog_key("warehouse"), ICEBERG_WAREHOUSE)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
    ensure_iceberg_namespaces(spark)
    return spark
