import pytest
from pyspark.sql.types import BooleanType, IntegerType, LongType, TimestampType

from bronze import ingest_blocks, ingest_inputs, ingest_outputs, ingest_transactions
from common.bronze import add_ingestion_metadata


@pytest.mark.parametrize(
    ("job", "first_field", "expected_type"),
    [
        (ingest_blocks, "block_hash", str),
        (ingest_transactions, "tx_hash", str),
        (ingest_inputs, "input_id", str),
        (ingest_outputs, "output_id", str),
    ],
)
# Each Bronze job exposes a stable, entity-specific input schema.
def test_bronze_schema_has_expected_identity_field(job, first_field, expected_type):
    schema = job.schema()

    assert schema[0].name == first_field
    assert schema[0].dataType.typeName() == "string"
    assert schema[0].nullable is False


# Declared CSV types protect numeric, boolean, and timestamp parsing.
def test_bronze_schemas_include_expected_non_string_types():
    assert isinstance(ingest_blocks.schema()[3].dataType, TimestampType)
    assert isinstance(ingest_blocks.schema()[6].dataType, LongType)
    assert isinstance(ingest_inputs.schema()[2].dataType, IntegerType)
    assert isinstance(ingest_outputs.schema()[5].dataType, BooleanType)
    assert isinstance(ingest_transactions.schema()[2].dataType, TimestampType)


# Ingestion metadata preserves payload columns and records batch lineage.
def test_add_ingestion_metadata_adds_lineage_columns(spark):
    df = spark.createDataFrame([("block-1",)], ["block_hash"])

    row = add_ingestion_metadata(df, "batch-42").collect()[0]

    assert row.block_hash == "block-1"
    assert row.batch_id == "batch-42"
    assert row.ingested_at is not None
