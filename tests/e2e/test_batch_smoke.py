"""Opt-in smoke test for the complete batch runner."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).parents[2]
pytestmark = pytest.mark.skipif(
    os.getenv("RUN_E2E") != "1",
    reason="requires the Docker Compose Spark and Hive services",
)


# The checked-in delivery completes the existing Bronze-to-Gold batch command.
def test_sample_batch_completes():
    result = subprocess.run(
        ["make", "ingest-batch", "BATCH=2026-08-31"],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        timeout=600,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "Batch 2026-08-31 completed successfully." in result.stdout
