"""Shared fixtures for Spark job tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


SPARK_ROOT = Path(__file__).parents[1] / "spark"
if str(SPARK_ROOT) not in sys.path:
    sys.path.insert(0, str(SPARK_ROOT))


@pytest.fixture
def spark() -> SparkSession:
    """Use one quiet local session for all DataFrame-only job tests."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("blockchain-pytest")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def capture_writes(monkeypatch):
    """Capture job output DataFrames without invoking the Iceberg catalog."""
    captured = {}

    def capture(df, table_name, **_kwargs):
        captured[table_name] = df

    return captured, capture


class TableSpark:
    """Minimal Spark facade used to exercise job orchestration locally."""

    def __init__(self, tables):
        self.tables = tables
        self.stopped = False

    def table(self, name):
        return self.tables[name]

    def stop(self):
        self.stopped = True
