"""A small cross-job integration check for the Silver UTXO flow."""

from silver.ingest_inputs import canonicalize_bronze_inputs, validate_inputs
from silver.ingest_outputs import canonicalize_bronze_outputs, validate_outputs
from silver.reconcile_utxos import enrich_outputs, reconcile_inputs


# Valid source rows flow from canonicalization through reconciliation and enrichment.
def test_silver_utxo_flow_marks_a_referenced_output_as_spent(spark):
    inputs = spark.createDataFrame(
        [(" input-1 ", "spend-tx", "0", "origin-tx", "0", "alice", "25")],
        ["input_id", "tx_hash", "input_index", "previous_tx_hash", "previous_output_index", "address", "value_sats"],
    )
    outputs = spark.createDataFrame(
        [(" output-1 ", "origin-tx", "alice", "0", "25", "false")],
        ["output_id", "tx_hash", "address", "output_index", "value_sats", "spent"],
    )

    validated_inputs = validate_inputs(canonicalize_bronze_inputs(inputs))
    validated_outputs = validate_outputs(canonicalize_bronze_outputs(outputs))
    reconciliation = reconcile_inputs(validated_inputs.drop("validation_errors"), validated_outputs.drop("validation_errors"))
    enriched = enrich_outputs(validated_outputs.drop("validation_errors"), reconciliation)

    assert validated_inputs.collect()[0].validation_errors == []
    assert validated_outputs.collect()[0].validation_errors == []
    assert reconciliation.collect()[0].resolution_status == "resolved"
    assert enriched.collect()[0].canonical_spent is True
