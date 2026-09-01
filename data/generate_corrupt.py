#!/usr/bin/env python3

#!/usr/bin/env python3

import argparse
import csv
import hashlib
import random
from datetime import datetime, timedelta
from pathlib import Path
from corrupt_fixture import apply_corruption
from uuid import uuid4


# --------------------------------------------------
# Command Line Arguments
# --------------------------------------------------

parser = argparse.ArgumentParser(
    description="Generate deterministic corrupted Bitcoin blockchain CSV datasets."
)

parser.add_argument(
    "-t",
    "--transactions",
    type=int,
    default=100_000,
    help="Number of transactions to generate.",
)

parser.add_argument(
    "--avg-transactions-per-block",
    type=int,
    default=2000,
    help="Average transactions per block.",
)

parser.add_argument(
    "--genesis-utxos", type=int, default=5000, help="Initial spendable outputs."
)

parser.add_argument(
    "--start-height", type=int, default=800000, help="Starting block height."
)

parser.add_argument(
    "--start-date", default="2025-01-01T00:00:00", help="Starting ISO8601 timestamp."
)

parser.add_argument("--seed", type=int, default=42, help="Random seed.")

parser.add_argument(
    "--min-inputs", type=int, default=1, help="Minimum transaction inputs."
)

parser.add_argument(
    "--max-inputs", type=int, default=4, help="Maximum transaction inputs."
)

parser.add_argument(
    "--min-outputs", type=int, default=2, help="Minimum transaction outputs."
)

parser.add_argument(
    "--max-outputs", type=int, default=4, help="Maximum transaction outputs."
)

parser.add_argument(
    "-o", "--output-dir", default="data/raw", help="Directory for generated CSV files."
)
parser.add_argument(
    "--corruption-scenario",
    choices=("clean", "data_quality", "relationships", "mixed"),
    default="mixed",
    help="Corruption fixture to add after generating the clean baseline.",
)

parser.add_argument(
    "--corruption-rate",
    type=float,
    default=0.01,
    help="Fraction of rows targeted by the selected fixture (0 < rate <= 1).",
)

parser.add_argument(
    "--corruption-seed",
    type=int,
    default=None,
    help="Seed for selecting corrupt rows; defaults to --seed.",
)

args = parser.parse_args()

if not 0 < args.corruption_rate <= 1:
    parser.error("--corruption-rate must be greater than 0 and no greater than 1")

random.seed(args.seed)

INGEST_DATE = datetime.now().strftime("%Y-%m-%d")
OUTPUT_DIR = Path(args.output_dir) / f"corrupt_ingest_{INGEST_DATE}"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

START_TIME = datetime.fromisoformat(args.start_date)

MIN_INPUTS = args.min_inputs
MAX_INPUTS = args.max_inputs
MIN_OUTPUTS = args.min_outputs
MAX_OUTPUTS = args.max_outputs

NUM_TRANSACTIONS = args.transactions
AVG_TX_PER_BLOCK = args.avg_transactions_per_block
GENESIS_UTXOS = args.genesis_utxos
START_HEIGHT = args.start_height


# --------------------------------------------------
# Helpers
# --------------------------------------------------


def sha():
    return hashlib.sha256(uuid4().hex.encode()).hexdigest()


def address():
    return "bc1" + sha()[:30]


# --------------------------------------------------
# CSV Writers
# --------------------------------------------------

blocks = open(OUTPUT_DIR / "blocks.csv", "w", newline="")
transactions = open(OUTPUT_DIR / "transactions.csv", "w", newline="")
inputs = open(OUTPUT_DIR / "inputs.csv", "w", newline="")
outputs = open(OUTPUT_DIR / "outputs.csv", "w", newline="")

block_writer = csv.writer(blocks)
tx_writer = csv.writer(transactions)
input_writer = csv.writer(inputs)
output_writer = csv.writer(outputs)

block_writer.writerow(
    [
        "block_hash",
        "block_height",
        "previous_block_hash",
        "timestamp",
        "miner",
        "difficulty",
        "nonce",
        "size_bytes",
        "transaction_count",
    ]
)

tx_writer.writerow(
    [
        "tx_hash",
        "block_hash",
        "timestamp",
        "fee_sats",
        "size_bytes",
        "input_count",
        "output_count",
        "total_input_sats",
        "total_output_sats",
    ]
)

input_writer.writerow(
    [
        "input_id",
        "tx_hash",
        "input_index",
        "previous_tx_hash",
        "previous_output_index",
        "address",
        "value_sats",
    ]
)

output_writer.writerow(
    [
        "output_id",
        "tx_hash",
        "output_index",
        "address",
        "value_sats",
        "spent",
    ]
)


# --------------------------------------------------
# Initial UTXO Set
# --------------------------------------------------

utxos = []

for i in range(GENESIS_UTXOS):
    utxos.append(
        {
            "tx_hash": "genesis",
            "output_index": i,
            "address": address(),
            "value": random.randint(500_000, 20_000_000),
            "spent": False,
        }
    )


miners = [
    "Foundry",
    "AntPool",
    "ViaBTC",
    "F2Pool",
    "Luxor",
    "MARA",
]


prev_block = "0" * 64
block_height = START_HEIGHT
current_time = START_TIME

remaining = NUM_TRANSACTIONS

while remaining > 0:
    txs_this_block = min(
        remaining,
        random.randint(
            int(AVG_TX_PER_BLOCK * 0.8),
            int(AVG_TX_PER_BLOCK * 1.2),
        ),
    )

    block_hash = sha()
    total_block_size = 0

    for _ in range(txs_this_block):
        available = [u for u in utxos if not u["spent"]]

        if len(available) < MAX_INPUTS:
            raise RuntimeError(
                "Not enough unspent outputs remaining. "
                "Increase --genesis-utxos or generate more outputs."
            )

        tx_hash = sha()

        input_count = random.randint(MIN_INPUTS, min(MAX_INPUTS, len(available)))

        output_count = random.randint(MIN_OUTPUTS, MAX_OUTPUTS)

        selected = random.sample(available, input_count)

        total_input = sum(u["value"] for u in selected)

        fee = random.randint(500, 5000)

        spendable = total_input - fee

        remaining_value = spendable
        output_values = []

        for i in range(output_count - 1):
            max_value = max(1000, remaining_value // (output_count - i))

            value = random.randint(1000, max_value)

            output_values.append(value)
            remaining_value -= value

        output_values.append(remaining_value)

        size = random.randint(250, 900)

        tx_writer.writerow(
            [
                tx_hash,
                block_hash,
                current_time.isoformat(),
                fee,
                size,
                input_count,
                output_count,
                total_input,
                spendable,
            ]
        )

        total_block_size += size

        # --------------------
        # Inputs
        # --------------------

        for idx, utxo in enumerate(selected):
            utxo["spent"] = True

            input_writer.writerow(
                [
                    sha()[:16],
                    tx_hash,
                    idx,
                    utxo["tx_hash"],
                    utxo["output_index"],
                    utxo["address"],
                    utxo["value"],
                ]
            )

        # --------------------
        # Outputs
        # --------------------

        for idx, value in enumerate(output_values):
            spent = random.random() < 0.6

            addr = address()

            output_writer.writerow(
                [
                    sha()[:16],
                    tx_hash,
                    idx,
                    addr,
                    value,
                    spent,
                ]
            )

            utxos.append(
                {
                    "tx_hash": tx_hash,
                    "output_index": idx,
                    "address": addr,
                    "value": value,
                    "spent": spent,
                }
            )

    block_writer.writerow(
        [
            block_hash,
            block_height,
            prev_block,
            current_time.isoformat(),
            random.choice(miners),
            round(random.uniform(70.0, 90.0), 2),
            random.randint(1, 2**32),
            total_block_size,
            txs_this_block,
        ]
    )

    prev_block = block_hash
    block_height += 1
    current_time += timedelta(minutes=random.randint(8, 12))

    remaining -= txs_this_block


blocks.close()
transactions.close()
inputs.close()
outputs.close()

apply_corruption(
    OUTPUT_DIR,
    args.corruption_scenario,
    args.corruption_rate,
    args.seed if args.corruption_seed is None else args.corruption_seed,
)

print("\nGeneration complete")
print(f"Corruption  : {args.corruption_scenario}")
print(f"Transactions : {NUM_TRANSACTIONS:,}")
print(f"Blocks       : {block_height - START_HEIGHT:,}")
print(f"Output Dir   : {OUTPUT_DIR.resolve()}")
