# Synthetic Data Generation

The `data/` scripts create the four CSV files that form the pipeline's Raw
layer: `blocks.csv`, `transactions.csv`, `inputs.csv`, and `outputs.csv`.
They generate a compact Bitcoin-style transaction graph for local ingestion,
testing, and visualization demos.

## Scripts

| Script | Purpose | Output directory |
| --- | --- | --- |
| [`generate.py`](../data/generate.py) | Creates a clean synthetic delivery. | `data/raw/ingest_<YYYY-MM-DD>/` |
| [`generate_corrupt.py`](../data/generate_corrupt.py) | Creates the same baseline, then applies a selected corruption fixture. | `data/raw/corrupt_ingest_<YYYY-MM-DD>/` |
| [`corrupt_fixture.py`](../data/corrupt_fixture.py) | Library used by the corrupt generator to mutate already-written CSVs. | Rewrites the four CSV files in place. |

## Clean-generation behavior

`generate.py` first seeds an in-memory set of `--genesis-utxos` spendable
outputs. It then creates blocks until it has written exactly `--transactions`
transactions.

- Each block contains a random transaction count in the range of 80–120% of
  `--avg-transactions-per-block`; the final block is capped at the remaining
  transaction count.
- Block height starts at `--start-height`, while block timestamps start at
  `--start-date` and advance by a random 8–12 minutes per block.
- Each transaction chooses 1–4 inputs and 2–4 outputs by default, spends
  available UTXOs, assigns a 500–5,000 satoshi fee, and distributes the
  remaining input value to its outputs.
- An output's generated `spent` flag controls whether it remains available for
  later selection as an input. The flag is intentionally retained as source
  evidence for the later spent-state reconciliation.

The clean generator enforces the accounting relationship
`total_input_sats = total_output_sats + fee_sats` for each generated
transaction.

## Options and reproducibility

Common options are:

| Option | Default | Effect |
| --- | --- | --- |
| `-t`, `--transactions` | `100000` | Exact number of transactions to write. |
| `--avg-transactions-per-block` | `2000` | Center of the variable transaction count per block. |
| `--genesis-utxos` | `5000` | Initial pool of spendable seeded outputs. |
| `--start-height` | `800000` | First generated block height. |
| `--start-date` | `2025-01-01T00:00:00` | First generated block timestamp. |
| `--min-inputs` / `--max-inputs` | `1` / `4` | Input-count range per transaction. |
| `--min-outputs` / `--max-outputs` | `2` / `4` | Output-count range per transaction. |
| `--seed` | `42` | Seed for Python's random simulation choices. |
| `-o`, `--output-dir` | `data/raw` | Parent directory for the delivery. |

The seed makes random choices such as block sizes, values, and miners
repeatable. Hashes, addresses, and row identifiers are derived from `uuid4()`,
however, so independent runs are not byte-for-byte identical even with the
same seed.

If the script reports **Not enough unspent outputs remaining**, increase
`--genesis-utxos` or adjust the transaction shape to produce more outputs than
the workload consumes.

## Running a clean delivery

```bash
python data/generate.py \
  --transactions 20000 \
  --genesis-utxos 10000
```

The output uses the current calendar date in its directory name. Pass that date
to the batch runner after generation:

```bash
make ingest-batch BATCH=<YYYY-MM-DD>
```

See [Synthetic Data Model](synthetic_data_model.md) for the field-level CSV
contract, and [Corruption Fixtures](corruption_fixtures.md) for intentional
failure scenarios.
