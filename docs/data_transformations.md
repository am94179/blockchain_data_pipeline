# Data Transformations

This guide describes the implemented path from generated blockchain-style CSV
files to the Gold Iceberg tables queried by Trino and visualized in Superset.
It is intended as a companion to the project architecture: the jobs prioritize
traceability and observable data quality over silently correcting source data.

## Flow at a glance

```text
Raw CSV files
  -> Bronze: typed copies + lineage
  -> Silver: validation, deduplication, and relationship checks
  -> Silver reconciliation: input-to-output and spent-state evidence
  -> Gold: analysis-ready transaction, block, input, and output facts
```

The pipeline uses Iceberg tables in a shared Hive Metastore catalog. Bronze
tables append each delivery; Silver and Gold tables are fully rebuilt from their
upstream tables during the current batch workflow.

## Raw to Bronze

The generator creates four linked CSV files: `blocks`, `transactions`, `inputs`,
and `outputs`. Each Bronze job applies the corresponding Spark schema, writes
to `bronze.<entity>`, and adds these lineage fields:

- `source_file`
- `ingested_at`
- `batch_id`

Bronze is a minimally transformed landing layer. It is intentionally the place
to retain the source record and its delivery context before business validation
or relational checks occur.

## Bronze to Silver

Silver tables hold trusted, typed records. Invalid rows are not discarded:
they are written to the corresponding `silver_rejects.<entity>` table with a
`validation_errors` array and the original lineage fields.

| Entity | Implemented validation and transformation |
| --- | --- |
| `blocks` | Requires a block hash, non-negative height/nonce/size/transaction count, positive difficulty, and timestamp. Miner names are lowercased; duplicate block hashes retain the latest ingested record. Reporting date/hour fields are derived. |
| `transactions` | Requires transaction and block hashes, non-negative fee/size/count/value fields, and the accounting rule `total_input_sats = total_output_sats + fee_sats`. Duplicate transaction hashes retain the latest record; transactions must reference a Silver block. |
| `inputs` | Trims identifiers, casts indices and values, validates required IDs and positive values, deduplicates by `(tx_hash, input_index)`, confirms the parent transaction, and reconciles observed input counts/values with transaction-level totals. |
| `outputs` | Trims identifiers, casts indices, values, and spent status, validates required IDs and positive values, detects output-ID collisions, deduplicates by `(tx_hash, output_index)`, confirms the parent transaction, and reconciles observed output counts/values with transaction-level totals. |

The `silver_rejects` tables make intentionally corrupt generator scenarios
inspectable without contaminating the curated tables.

## UTXO reconciliation

`silver.reconcile_utxos` is run after the Silver input and output jobs. It
links each input's `(previous_tx_hash, previous_output_index)` to an output and
publishes two additional Silver tables:

- `silver.utxo_reconciliation` records `genesis`, `resolved`, `unresolved`, or
  `ambiguous` reference status, referenced output details, address/value match
  results, and detected double-spend claims.
- `silver.outputs_enriched` preserves the source `spent` flag, derives an
  observed/canonical spent state from resolved inputs, and flags disagreement as
  `spend_discrepancy`.

The `genesis` previous-transaction sentinel is treated as a known special case,
not as a missing output reference.

## Silver to Gold

Gold tables preserve useful Silver context while adding facts and reconciliation
fields designed for SQL and dashboards. All monetary values remain in satoshis.

| Gold table | Grain | Key enrichments |
| --- | --- | --- |
| `gold.transactions` | One transaction | Observed input/output counts and values, fee rate, date, and four declared-versus-observed reconciliation flags. |
| `gold.blocks` | One block | Observed transaction count, total fees, transaction bytes, input/output values, and a reported-versus-observed transaction-count flag. |
| `gold.inputs` | One input | UTXO-resolution details, referenced-output fields, address/value matches, double-spend flag, genesis indicator, spend date, and resolved-input coin age. |
| `gold.outputs` | One output | Source and canonical spent state, discrepancy flag, observed spend count, first-spend date, multiple-spend indicator, and time-to-spend days. |

These tables are the intended sources for the Trino query layer and Apache
Superset. The dashboard uses `gold.transactions` and `gold.blocks` for the
primary transaction-activity visualizations.

## Execution order

Run the full dependency sequence for a delivery with:

```bash
make ingest-batch BATCH=2026-08-18
```

The batch runner loads all Bronze entities, then Blocks and Transactions in
Silver, then Inputs and Outputs in Silver, followed by UTXO reconciliation and
the four Gold jobs. It stops on the first failure so a downstream layer does not
run against an incomplete upstream result.

For individual job commands, see the [Spark Jobs section](../README.md#spark-jobs)
in the README.
