"""Deterministic post-generation corruption fixtures for the CSV data set."""

import csv
import hashlib
import random
from pathlib import Path


def apply_corruption(
    output_dir: Path, scenario: str, rate: float, seed: int
) -> None:
    """Mutate generated CSVs in place only when a corruption scenario is selected."""
    if scenario == "clean":
        return

    rng = random.Random(seed)
    datasets = {
        name: _read_csv(output_dir / f"{name}.csv")
        for name in ("blocks", "transactions", "inputs", "outputs")
    }

    if scenario in ("data_quality", "mixed"):
        _apply_data_quality_corruption(datasets, rate, rng)
    if scenario in ("relationships", "mixed"):
        _apply_relationship_corruption(datasets, rate, rng)

    for name, (fieldnames, rows) in datasets.items():
        _write_csv(output_dir / f"{name}.csv", fieldnames, rows)


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        return reader.fieldnames or [], list(reader)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _selected_rows(
    rows: list[dict[str, str]], rate: float, rng: random.Random
) -> list[dict[str, str]]:
    if not rows:
        return []
    count = min(len(rows), max(1, round(len(rows) * rate)))
    return rng.sample(rows, count)


def _unknown_hash(rng: random.Random) -> str:
    value = rng.getrandbits(256).to_bytes(32, "big")
    return hashlib.sha256(value).hexdigest()


def _apply_data_quality_corruption(
    datasets: dict[str, tuple[list[str], list[dict[str, str]]]],
    rate: float,
    rng: random.Random,
) -> None:
    blocks = datasets["blocks"][1]
    transactions = datasets["transactions"][1]
    inputs = datasets["inputs"][1]
    outputs = datasets["outputs"][1]

    for row in _selected_rows(blocks, rate, rng):
        row["miner"] = f"  {row['miner'].lower()}  "

    transaction_mutations = (
        lambda row: row.update(tx_hash=""),
        lambda row: row.update(block_hash="   "),
        lambda row: row.update(timestamp="not-a-timestamp"),
        lambda row: row.update(fee_sats="not-a-number"),
    )
    for index, row in enumerate(_selected_rows(transactions, rate, rng)):
        transaction_mutations[index % len(transaction_mutations)](row)

    input_mutations = (
        lambda row: row.update(input_id=""),
        lambda row: row.update(address="   "),
        lambda row: row.update(value_sats="not-a-number"),
    )
    for index, row in enumerate(_selected_rows(inputs, rate, rng)):
        input_mutations[index % len(input_mutations)](row)

    output_mutations = (
        lambda row: row.update(output_id=""),
        lambda row: row.update(value_sats="not-a-number"),
        lambda row: row.update(spent="not-a-boolean"),
    )
    for index, row in enumerate(_selected_rows(outputs, rate, rng)):
        output_mutations[index % len(output_mutations)](row)

    _append_duplicate(blocks)
    _append_duplicate(transactions)
    _append_duplicate(inputs)
    _append_duplicate(outputs)


def _append_duplicate(rows: list[dict[str, str]]) -> None:
    if rows:
        rows.append(rows[0].copy())


def _apply_relationship_corruption(
    datasets: dict[str, tuple[list[str], list[dict[str, str]]]],
    rate: float,
    rng: random.Random,
) -> None:
    transactions = datasets["transactions"][1]
    inputs = datasets["inputs"][1]
    outputs = datasets["outputs"][1]

    for index, row in enumerate(_selected_rows(transactions, rate, rng)):
        if index % 3 == 0:
            row["block_hash"] = _unknown_hash(rng)
        elif index % 3 == 1:
            row["input_count"] = str(int(row["input_count"]) + 1)
        else:
            row["total_output_sats"] = str(int(row["total_output_sats"]) + 1)

    for index, row in enumerate(_selected_rows(inputs, rate, rng)):
        if index % 3 == 0:
            row["tx_hash"] = _unknown_hash(rng)
        elif index % 3 == 1:
            row["previous_tx_hash"] = _unknown_hash(rng)
        else:
            row["address"] = "bc1corruptedaddress"

    for row in _selected_rows(outputs, rate, rng):
        row["tx_hash"] = _unknown_hash(rng)

    referenced_outputs = {
        (row["previous_tx_hash"], row["previous_output_index"])
        for row in inputs
        if row["previous_tx_hash"] != "genesis"
    }
    for row in outputs:
        if (row["tx_hash"], row["output_index"]) in referenced_outputs:
            row["spent"] = "False"
            break
