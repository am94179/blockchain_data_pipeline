from datetime import datetime

from silver.ingest_blocks import validate_blocks
from silver.ingest_inputs import canonicalize_bronze_inputs, validate_inputs
from silver.ingest_outputs import canonicalize_bronze_outputs, validate_outputs
from silver.ingest_transactions import validate_transactions
from tests.helpers import rows_by_key


# Block validation emits every applicable scalar error for a malformed row.
def test_validate_blocks_collects_all_domain_errors(spark):
    df = spark.createDataFrame(
        [("bad", " ", -1, "parent", None, 0.0, -1, -1, -1), ("typed", "ok", 1, "parent", datetime(2026, 1, 1), 1.0, 1, 1, 1)],
        ["id", "block_hash", "block_height", "previous_block_hash", "timestamp", "difficulty", "nonce", "size_bytes", "transaction_count"],
    )

    errors = validate_blocks(df).where("id = 'bad'").collect()[0].validation_errors

    assert errors == ["BLOCK_HASH_MISSING", "BLOCK_HEIGHT_INVALID", "NONCE_INVALID", "SIZE_INVALID", "TRANSACTION_COUNT_INVALID", "DIFFICULTY_INVALID", "TIMESTAMP_MISSING"]


# Transaction validation accepts a balanced, fully populated record.
def test_validate_transactions_accepts_valid_record(spark):
    df = spark.createDataFrame(
        [("tx-1", "block-1", datetime(2026, 1, 1), 1, 100, 1, 1, 10, 9)],
        ["tx_hash", "block_hash", "timestamp", "fee_sats", "size_bytes", "input_count", "output_count", "total_input_sats", "total_output_sats"],
    )

    assert validate_transactions(df).collect()[0].validation_errors == []


# Transaction validation distinguishes missing keys from invalid measures.
def test_validate_transactions_reports_missing_and_invalid_values(spark):
    df = spark.createDataFrame(
        [(" ", None, None, -1, -1, 0, 0, -1, -1), ("typed", "block", datetime(2026, 1, 1), 1, 1, 1, 1, 2, 1)],
        ["tx_hash", "block_hash", "timestamp", "fee_sats", "size_bytes", "input_count", "output_count", "total_input_sats", "total_output_sats"],
    )

    assert validate_transactions(df).where("tx_hash = ' '").collect()[0].validation_errors == ["TX_HASH_MISSING", "BLOCK_HASH_MISSING", "TIMESTAMP_MISSING", "FEE_INVALID", "SIZE_INVALID", "INPUT_COUNT_INVALID", "OUTPUT_COUNT_INVALID", "TOTAL_INPUT_INVALID", "TOTAL_OUTPUT_INVALID"]


# Input canonicalization trims keys and converts delivery strings to numeric types.
def test_canonicalize_inputs_normalizes_strings_and_casts_numbers(spark):
    df = spark.createDataFrame([(" i ", " tx ", "2", " prev ", "3", " addr ", "4")], ["input_id", "tx_hash", "input_index", "previous_tx_hash", "previous_output_index", "address", "value_sats"])

    row = canonicalize_bronze_inputs(df).collect()[0]

    assert (row.input_id, row.tx_hash, row.previous_tx_hash, row.address) == ("i", "tx", "prev", "addr")
    assert (row.input_index, row.previous_output_index, row.value_sats) == (2, 3, 4)


# Input validation records both missing and negative index violations.
def test_validate_inputs_reports_expected_error_codes(spark):
    df = spark.createDataFrame([(" ", " ", -1, "prev", -1, "addr", 0)], ["input_id", "tx_hash", "input_index", "previous_tx_hash", "previous_output_index", "address", "value_sats"])

    assert validate_inputs(df).collect()[0].validation_errors == ["INPUT_ID_MISSING", "TX_HASH_MISSING", "INPUT_INDEX_INVALID", "OUTPUT_INDEX_INVALID", "VALUE_SATS_INVALID"]


# Output canonicalization trims identity fields and parses boolean source data.
def test_canonicalize_outputs_normalizes_fields(spark):
    df = spark.createDataFrame([(" o ", " tx ", " addr ", "2", "4", "true")], ["output_id", "tx_hash", "address", "output_index", "value_sats", "spent"])

    row = canonicalize_bronze_outputs(df).collect()[0]

    assert (row.output_id, row.tx_hash, row.address, row.output_index, row.value_sats, row.spent) == ("o", "tx", "addr", 2, 4, True)


# Output validation retains separate missing and negative-index error codes.
def test_validate_outputs_reports_expected_error_codes(spark):
    df = spark.createDataFrame([(" ", " ", -1, 0)], ["output_id", "tx_hash", "output_index", "value_sats"])

    assert validate_outputs(df).collect()[0].validation_errors == ["OUTPUT_ID_MISSING", "TX_HASH_MISSING", "OUTPUT_INDEX_INVALID", "VALUE_SATS_INVALID"]
