"""Small assertion helpers for unordered Spark results."""

from __future__ import annotations


def rows_by_key(df, key):
    """Collect a DataFrame into a stable mapping keyed by one column."""
    return {row[key]: row.asDict(recursive=True) for row in df.collect()}


def sorted_rows(df, *columns):
    """Collect rows in a caller-defined, deterministic order."""
    return [row.asDict(recursive=True) for row in df.orderBy(*columns).collect()]
