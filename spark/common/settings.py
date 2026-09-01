import os

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_LOG_LEVEL = os.getenv("SPARK_LOG_LEVEL", "WARN")
SPARK_CORES_MAX = os.getenv("SPARK_CORES_MAX", "2")
SPARK_EXECUTOR_CORES = os.getenv("SPARK_EXECUTOR_CORES", "2")
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g")

ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "local")
ICEBERG_CATALOG_IMPL = "org.apache.iceberg.spark.SparkCatalog"
ICEBERG_CATALOG_TYPE = os.getenv("ICEBERG_CATALOG_TYPE", "hive")
ICEBERG_CATALOG_URI = os.getenv(
    "ICEBERG_CATALOG_URI", "thrift://hive-metastore:9083"
)
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "/opt/spark/warehouse")


def iceberg_catalog_key(setting: str) -> str:
    suffix = f".{setting}" if setting else ""
    return f"spark.sql.catalog.{ICEBERG_CATALOG}{suffix}"
