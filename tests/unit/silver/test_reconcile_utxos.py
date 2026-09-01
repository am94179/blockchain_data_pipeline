from silver.reconcile_utxos import enrich_outputs, reconcile_inputs
from tests.helpers import rows_by_key


def _inputs(spark):
    return spark.createDataFrame(
        [
            ("genesis-input", "spend-1", 0, "genesis", 0, "miner", 50),
            ("resolved", "spend-1", 1, "origin", 0, "alice", 10),
            ("unresolved", "spend-2", 0, "missing", 0, "bob", 3),
            ("ambiguous", "spend-2", 1, "duplicate", 0, "carol", 7),
            ("double-a", "spend-3", 0, "origin", 0, "alice", 10),
            ("double-b", "spend-4", 0, "origin", 0, "alice", 10),
        ],
        ["input_id", "tx_hash", "input_index", "previous_tx_hash", "previous_output_index", "address", "value_sats"],
    )


def _outputs(spark):
    return spark.createDataFrame(
        [
            ("origin-output", "origin", 0, "alice", 10, False),
            ("dup-a", "duplicate", 0, "carol", 7, False),
            ("dup-b", "duplicate", 0, "carol", 7, False),
            ("unspent", "origin", 1, "nobody", 2, True),
        ],
        ["output_id", "tx_hash", "output_index", "address", "value_sats", "spent"],
    )


# Reconciliation resolves genesis, absent, duplicate, and double-spend references.
def test_reconcile_inputs_classifies_reference_outcomes(spark):
    actual = rows_by_key(reconcile_inputs(_inputs(spark), _outputs(spark)), "input_id")

    assert actual["genesis-input"]["resolution_status"] == "genesis"
    assert actual["resolved"]["resolution_status"] == "resolved"
    assert actual["unresolved"]["resolution_status"] == "unresolved"
    assert actual["ambiguous"]["resolution_status"] == "ambiguous"
    assert actual["double-a"]["double_spend_detected"] is True
    assert actual["double-b"]["double_spend_detected"] is True


# Resolved inputs compare source address and value with the referenced output.
def test_reconcile_inputs_populates_match_flags_only_when_resolved(spark):
    actual = rows_by_key(reconcile_inputs(_inputs(spark), _outputs(spark)), "input_id")

    assert actual["resolved"]["address_matches"] is True
    assert actual["resolved"]["value_matches"] is True
    assert actual["unresolved"]["address_matches"] is None
    assert actual["ambiguous"]["value_matches"] is None


# Enrichment treats resolved references as canonical spend observations.
def test_enrich_outputs_marks_observed_spends_and_source_discrepancies(spark):
    reconciliation = reconcile_inputs(_inputs(spark), _outputs(spark))
    actual = rows_by_key(enrich_outputs(_outputs(spark), reconciliation), "output_id")

    assert actual["origin-output"]["canonical_spent"] is True
    assert actual["origin-output"]["spend_discrepancy"] is True
    assert actual["unspent"]["canonical_spent"] is False
    assert actual["unspent"]["spend_discrepancy"] is True
