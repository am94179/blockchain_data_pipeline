
from common.iceberg import write_iceberg_table
from common.logger import get_logger
from common.session import get_spark
from common.tables import silver
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

INPUTS_TABLE = silver("inputs")
OUTPUTS_TABLE = silver("outputs")
UTXO_RECONCILIATION_TABLE = silver("utxo_reconciliation")
ENRICHED_OUTPUTS_TABLE = silver("outputs_enriched")
logger = get_logger("silver.reconcile_utxos")


def reconcile_inputs(inputs_df: DataFrame, outputs_df: DataFrame) -> DataFrame:
    output_key_counts = (
        outputs_df.groupBy("tx_hash", "output_index")
        .agg(F.count("*").alias("matching_output_count"))
        .select(
            F.col("tx_hash").alias("referenced_tx_hash"),
            F.col("output_index").alias("referenced_output_index"),
            "matching_output_count",
        )
    )

    unique_outputs = (
        outputs_df.alias("output").join(
            output_key_counts.alias("key_count"),
            (F.col("output.tx_hash") == F.col("key_count.referenced_tx_hash"))
            & (
                F.col("output.output_index")
                == F.col("key_count.referenced_output_index")
            ),
        )
        .filter(F.col("matching_output_count") == 1)
        .select(
            "referenced_tx_hash",
            "referenced_output_index",
            F.col("output.output_id").alias("referenced_output_id"),
            F.col("output.address").alias("referenced_address"),
            F.col("output.value_sats").alias("referenced_value_sats"),
        )
    )

    genesis_inputs = (
        inputs_df.filter(F.col("previous_tx_hash") == "genesis")
        .withColumn("matching_output_count", F.lit(None).cast("long"))
        .withColumn("referenced_tx_hash", F.lit(None).cast("string"))
        .withColumn("referenced_output_index", F.lit(None).cast("int"))
        .withColumn("referenced_output_id", F.lit(None).cast("string"))
        .withColumn("referenced_address", F.lit(None).cast("string"))
        .withColumn("referenced_value_sats", F.lit(None).cast("bigint"))
        .withColumn("resolution_status", F.lit("genesis"))
        .withColumn("address_matches", F.lit(None).cast("boolean"))
        .withColumn("value_matches", F.lit(None).cast("boolean"))
    )

    non_genesis_inputs = inputs_df.filter(F.col("previous_tx_hash") != "genesis")

    non_genesis_reconciliation = (
        non_genesis_inputs.join(
            output_key_counts,
            (F.col("previous_tx_hash") == F.col("referenced_tx_hash"))
            & (
                F.col("previous_output_index")
                == F.col("referenced_output_index")
            ),
            how="left",
        )
        .join(
            unique_outputs,
            on=["referenced_tx_hash", "referenced_output_index"],
            how="left",
        )
        .withColumn(
            "resolution_status",
            F.when(F.col("matching_output_count").isNull(), F.lit("unresolved"))
            .when(F.col("matching_output_count") > 1, F.lit("ambiguous"))
            .otherwise(F.lit("resolved")),
        )
        .withColumn(
            "address_matches",
            F.when(
                F.col("resolution_status") == "resolved",
                F.col("address") == F.col("referenced_address"),
            ).otherwise(F.lit(None).cast("boolean")),
        )
        .withColumn(
            "value_matches",
            F.when(
                F.col("resolution_status") == "resolved",
                F.col("value_sats") == F.col("referenced_value_sats"),
            ).otherwise(F.lit(None).cast("boolean")),
        )
    )

    reconciliation_df = genesis_inputs.unionByName(non_genesis_reconciliation)

    double_spend_keys = (
        reconciliation_df.filter(F.col("resolution_status") == "resolved")
        .groupBy("previous_tx_hash", "previous_output_index")
        .agg(F.count("*").alias("spend_count"))
        .filter(F.col("spend_count") > 1)
        .select("previous_tx_hash", "previous_output_index")
        .withColumn("double_spend_detected", F.lit(True))
    )

    return (
        reconciliation_df.join(
            double_spend_keys,
            on=["previous_tx_hash", "previous_output_index"],
            how="left",
        )
        .fillna({"double_spend_detected": False})
    )


def enrich_outputs(outputs_df: DataFrame, reconciliation_df: DataFrame) -> DataFrame:
    observed_spends = (
        reconciliation_df.filter(F.col("resolution_status") == "resolved")
        .select(
            F.col("previous_tx_hash").alias("tx_hash"),
            F.col("previous_output_index").alias("output_index"),
        )
        .dropDuplicates()
        .withColumn("observed_spent", F.lit(True))
    )

    return (
        outputs_df.withColumnRenamed("spent", "source_spent")
        .join(observed_spends, on=["tx_hash", "output_index"], how="left")
        .fillna({"observed_spent": False})
        .withColumn("canonical_spent", F.col("observed_spent"))
        .withColumn(
            "spend_discrepancy",
            ~F.col("source_spent").eqNullSafe(F.col("observed_spent")),
        )
    )


def main() -> None:
    logger.info(
        "Starting UTXO reconciliation: inputs=%s outputs=%s",
        INPUTS_TABLE,
        OUTPUTS_TABLE,
    )
    spark = get_spark("reconcile-utxos")
    try:
        inputs_df = spark.table(INPUTS_TABLE)
        outputs_df = spark.table(OUTPUTS_TABLE)

        reconciliation_df = reconcile_inputs(inputs_df, outputs_df)
        enriched_outputs_df = enrich_outputs(outputs_df, reconciliation_df)

        write_iceberg_table(
            reconciliation_df,
            UTXO_RECONCILIATION_TABLE,
            mode="create_or_replace",
        )
        write_iceberg_table(
            enriched_outputs_df,
            ENRICHED_OUTPUTS_TABLE,
            mode="create_or_replace",
        )
    finally:
        logger.info("Stopping UTXO reconciliation")
        spark.stop()


if __name__ == "__main__":
    main()
