from unittest.mock import MagicMock

import pytest

from common.iceberg import write_iceberg_table


def _writer_dataframe(table_exists):
    df = MagicMock()
    writer = MagicMock()
    df.writeTo.return_value = writer
    df.sparkSession.catalog.tableExists.return_value = table_exists
    return df, writer


# A missing destination table is always created before mode-specific writing.
def test_write_iceberg_table_creates_missing_table():
    df, writer = _writer_dataframe(table_exists=False)

    write_iceberg_table(df, "local.silver.blocks", mode="append")

    writer.create.assert_called_once_with()
    writer.append.assert_not_called()


@pytest.mark.parametrize(
    ("mode", "writer_method"),
    [("append", "append"), ("overwrite_partitions", "overwritePartitions"), ("create_or_replace", "createOrReplace")],
)
# Existing tables use the writer operation selected by the requested mode.
def test_write_iceberg_table_uses_requested_existing_table_mode(mode, writer_method):
    df, writer = _writer_dataframe(table_exists=True)

    write_iceberg_table(df, "local.silver.blocks", mode=mode)

    getattr(writer, writer_method).assert_called_once_with()


# Merge-schema requests are passed to the Spark writer before appending.
def test_write_iceberg_table_enables_merge_schema_when_requested():
    df, writer = _writer_dataframe(table_exists=True)
    writer.option.return_value = writer

    write_iceberg_table(df, "local.silver.blocks", merge_schema=True)

    writer.option.assert_called_once_with("mergeSchema", "true")
    writer.append.assert_called_once_with()


# Unknown write modes fail instead of silently picking a destructive operation.
def test_write_iceberg_table_rejects_unknown_mode():
    df, _writer = _writer_dataframe(table_exists=True)

    with pytest.raises(ValueError, match="Unsupported"):
        write_iceberg_table(df, "local.silver.blocks", mode="unknown")
