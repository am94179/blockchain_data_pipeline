from datetime import datetime

from gold import ingest_blocks, ingest_inputs, ingest_outputs, ingest_transactions
from tests.conftest import TableSpark
from tests.helpers import rows_by_key


def _run_job(monkeypatch, module, tables, capture_writes):
    captured, capture = capture_writes
    fake_spark = TableSpark(tables)
    monkeypatch.setattr(module, "get_spark", lambda _name: fake_spark)
    monkeypatch.setattr(module, "write_iceberg_table", capture)
    module.main()
    assert fake_spark.stopped is True
    return captured


def _transactions(spark):
    return spark.createDataFrame(
        [("tx-1", "block-1", datetime(2026, 1, 2), 2, 10, 1, 1, 12, 10)],
        ["tx_hash", "block_hash", "timestamp", "fee_sats", "size_bytes", "input_count", "output_count", "total_input_sats", "total_output_sats"],
    )


# Gold blocks computes transaction aggregates and count-quality flags.
def test_gold_blocks_builds_aggregates(monkeypatch, spark, capture_writes):
    blocks = spark.createDataFrame([("block-1", 1), ("empty", 0)], ["block_hash", "transaction_count"])
    captured = _run_job(monkeypatch, ingest_blocks, {ingest_blocks.SILVER_BLOCKS_TABLE: blocks, ingest_blocks.SILVER_TX_TABLE: _transactions(spark)}, capture_writes)

    actual = rows_by_key(captured[ingest_blocks.GOLD_BLOCKS_TABLE], "block_hash")
    assert actual["block-1"]["total_fee_sats"] == 2
    assert actual["block-1"]["transaction_count_matches"] is True
    assert actual["empty"]["observed_transaction_count"] == 0


# Gold transactions exposes observed totals, match flags, and fee rate safely.
def test_gold_transactions_builds_reconciliation_columns(monkeypatch, spark, capture_writes):
    inputs = spark.createDataFrame([("tx-1", 12)], ["tx_hash", "value_sats"])
    outputs = spark.createDataFrame([("tx-1", 10)], ["tx_hash", "value_sats"])
    captured = _run_job(monkeypatch, ingest_transactions, {ingest_transactions.SILVER_TRANSACTIONS_TABLE: _transactions(spark), ingest_transactions.SILVER_INPUTS_TABLE: inputs, ingest_transactions.SILVER_OUTPUTS_TABLE: outputs}, capture_writes)

    row = captured[ingest_transactions.GOLD_TRANSACTIONS_TABLE].collect()[0]
    assert row.fee_rate_sats_per_byte == 0.2
    assert all((row.input_count_matches, row.output_count_matches, row.input_value_matches, row.output_value_matches))
    assert str(row.transaction_date) == "2026-01-02"


# Gold inputs joins reconciliation details and derives coin age for resolved spends.
def test_gold_inputs_enriches_resolved_input(monkeypatch, spark, capture_writes):
    inputs = spark.createDataFrame([("spend", 0, "origin", 0)], ["tx_hash", "input_index", "previous_tx_hash", "previous_output_index"])
    transactions = spark.createDataFrame([("spend", "b2", datetime(2026, 1, 3)), ("origin", "b1", datetime(2026, 1, 1))], ["tx_hash", "block_hash", "timestamp"])
    reconciliation = spark.createDataFrame([("spend", 0, "resolved", "origin", 0, "out", "alice", 5, True, True, False)], ["tx_hash", "input_index", "resolution_status", "referenced_tx_hash", "referenced_output_index", "referenced_output_id", "referenced_address", "referenced_value_sats", "address_matches", "value_matches", "double_spend_detected"])
    captured = _run_job(monkeypatch, ingest_inputs, {ingest_inputs.SILVER_INPUTS_TABLE: inputs, ingest_inputs.SILVER_TRANSACTIONS_TABLE: transactions, ingest_inputs.UTXO_RECONCILIATION_TABLE: reconciliation}, capture_writes)

    row = captured[ingest_inputs.GOLD_INPUTS_TABLE].collect()[0]
    assert row.resolution_status == "resolved"
    assert row.coin_age_days == 2
    assert row.is_genesis_input is False


# Gold outputs calculates spend activity from resolved reconciliation rows.
def test_gold_outputs_adds_spend_timing_and_attempt_count(monkeypatch, spark, capture_writes):
    outputs = spark.createDataFrame([("origin", 0, True)], ["tx_hash", "output_index", "canonical_spent"])
    transactions = spark.createDataFrame([("spend-a", "b2", datetime(2026, 1, 3)), ("spend-b", "b3", datetime(2026, 1, 4)), ("origin", "b1", datetime(2026, 1, 1))], ["tx_hash", "block_hash", "timestamp"])
    reconciliation = spark.createDataFrame([("spend-a", "origin", 0, "resolved"), ("spend-b", "origin", 0, "resolved")], ["tx_hash", "previous_tx_hash", "previous_output_index", "resolution_status"])
    captured = _run_job(monkeypatch, ingest_outputs, {ingest_outputs.SILVER_OUTPUTS_ENRICHED_TABLE: outputs, ingest_outputs.SILVER_TRANSACTIONS_TABLE: transactions, ingest_outputs.UTXO_RECONCILIATION_TABLE: reconciliation}, capture_writes)

    row = captured[ingest_outputs.GOLD_OUTPUTS_TABLE].collect()[0]
    assert row.observed_spend_count == 2
    assert row.has_multiple_spend_attempts is True
    assert row.time_to_spend_days == 2
