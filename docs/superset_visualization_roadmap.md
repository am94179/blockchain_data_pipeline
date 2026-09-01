# Superset Visualization Roadmap

## Goal and scope

Create a small, stable Apache Superset layer that demonstrates the full local
flow:

```text
CSV files -> Bronze (raw + ingestion metadata) -> Silver (validated + reconciled) -> Gold (analytics-ready) -> Trino -> Superset
```

The first version should contain two focused dashboards, a handful of datasets,
and static screenshots for the README. Do not create physical Trino views or
tables for this phase: the Trino Iceberg catalog is intentionally read-only.
Use Superset physical datasets for Gold tables and virtual datasets only for
small, presentation-specific summaries.

## Current data profile

The roadmap is based on the currently loaded catalog:

| Dataset | Rows | Current date coverage | Notes |
| --- | ---: | --- | --- |
| `gold.blocks` | 997 | 2025-01-01 to 2025-01-07 | Sufficient block volume for daily and miner comparisons. |
| `gold.transactions` | 20,499 | 2025-01-01 to 2025-01-07 | Primary fact table for the first dashboard. |
| `gold.inputs` | 54,105 | 2025-01-01 to 2025-01-07 | Input and UTXO-resolution analysis source. |
| `gold.outputs` | 61,455 | 2025-01-01 to 2025-01-07 | Lifecycle and spent-state reconciliation source. |
| `silver.utxo_reconciliation` | 54,105 | n/a | The reconciliation source for Gold inputs/outputs. |
| `silver_rejects.transactions` | 1 | n/a | One duplicate/invalid transaction record was rejected. |

Important implications:

- Prefer the daily trend alongside distribution, composition, ranking, and
  reconciliation charts; the seven-day range is sufficient for a README view.
- The current quality flags are meaningful demo data: there is one
  transaction/block reconciliation mismatch, no double spends, and 54,777
  outputs with a source/canonical spent-state discrepancy. Show these as
  reconciliation results, not as unexplained network failures.

## Layer review

### Bronze: raw ingestion audit only

Tables: `bronze.blocks`, `bronze.transactions`, `bronze.inputs`, and
`bronze.outputs`.

Each keeps the source fields plus `ingested_at`, `source_file`, and `batch_id`.
Bronze is not a business-facing visualization source. If it is exposed in
Superset, make one optional *Ingestion Audit* dataset with row counts grouped by
`batch_id` and `source_file`. This is useful as proof of ingestion lineage, but
should not be on the README dashboard.

### Silver: validation and reconciliation audit

Tables include validated entities in `silver.*`, rejects in
`silver_rejects.*`, `silver.utxo_reconciliation`, and
`silver.outputs_enriched`.

Use Silver only for an optional quality dashboard. It demonstrates the
engineering work that Gold hides:

- `silver_rejects.*`: rejected-row count by entity, batch, and validation
  reason when corrupt fixtures are run.
- `silver.utxo_reconciliation`: UTXO resolution status, address/value matches,
  and double-spend detection.
- `silver.outputs_enriched`: source-versus-observed spent status.

Do not mix Bronze/Silver fields into the primary business dashboard; it makes
the portfolio story harder to follow.

### Gold: primary Superset source

Gold joins and aggregates the Silver records into analysis-ready tables.

| Gold table | Grain | Important fields | Best use |
| --- | --- | --- | --- |
| `gold.transactions` | one transaction | `transaction_date`, `fee_sats`, `size_bytes`, input/output counts and values, `fee_rate_sats_per_byte`, four reconciliation-match flags | transaction activity, fee behavior, value flow |
| `gold.blocks` | one block | `block_date`, `miner`, `difficulty`, reported/observed transaction counts, total fees, total transaction size, `transaction_count_matches` | block-level KPI and future time trends |
| `gold.inputs` | one input | `spend_date`, `resolution_status`, referenced value, `coin_age_days`, match flags, `double_spend_detected` | UTXO resolution and spend quality |
| `gold.outputs` | one output | `output_date`, `canonical_spent`, `spend_discrepancy`, `first_spend_date`, `time_to_spend_days`, multiple-spend flag | output lifecycle and reconciliation |

## Datasets to create in Superset

Create these through **Data -> Datasets** from the already configured `Trino
Iceberg` connection. Set the listed temporal column so Superset filters work
consistently.

### Required physical datasets

1. **Gold Transactions** — `iceberg.gold.transactions`
   - Temporal column: `transaction_date`.
   - Keep as the main fact dataset.
   - Useful metrics: `COUNT(*)`, `SUM(fee_sats)`, `AVG(fee_sats)`,
     `AVG(fee_rate_sats_per_byte)`, `SUM(total_output_sats)`.
   - Useful dimensions: `transaction_date`, `fee_sats`, `size_bytes`,
     `input_count`, `output_count`, and all four `*_matches` flags.

2. **Gold Outputs** — `iceberg.gold.outputs`
   - Temporal column: `output_date`.
   - Useful metrics: `COUNT(*)`, `SUM(value_sats)`,
     `COUNT_IF(spend_discrepancy)`, `COUNT_IF(canonical_spent)`.
   - Useful dimensions: `canonical_spent`, `spend_discrepancy`,
     `has_multiple_spend_attempts`, `time_to_spend_days`.

3. **Gold Inputs** — `iceberg.gold.inputs`
   - Temporal column: `spend_date`.
   - Useful metrics: `COUNT(*)`, `SUM(value_sats)`,
     `COUNT_IF(double_spend_detected)`.
   - Useful dimensions: `resolution_status`, `is_genesis_input`,
     `address_matches`, `value_matches`, `coin_age_days`.

4. **Gold Blocks** — `iceberg.gold.blocks`
   - Temporal column: `block_date`.
   - Useful metrics: `COUNT(*)`, `SUM(total_fee_sats)`,
     `SUM(observed_transaction_count)`, `AVG(difficulty)`.
   - Useful dimensions: `miner`, `transaction_count_matches`.

### Optional virtual datasets

Add these only after the four physical datasets work. They simplify charts but
remain read-only Superset metadata, not new warehouse objects.

1. **Output Reconciliation Summary**
   - Group `gold.outputs` by `canonical_spent` and `spend_discrepancy`.
   - Fields: `canonical_spent`, `spend_discrepancy`, `output_count`,
     `total_value_sats`.
   - Purpose: one clean chart explaining output-state reconciliation.

2. **UTXO Resolution Summary**
   - Group `gold.inputs` by `resolution_status` and `is_genesis_input`.
   - Fields: `resolution_status`, `is_genesis_input`, `input_count`,
     `total_value_sats`, `double_spend_count`.
   - Purpose: a compact quality chart without exposing input-level identifiers.

The physical Gold Transactions dataset already has `transaction_date`, so no
virtual dataset is needed for daily trends.

## Dashboard 1: Bitcoin Transaction Activity

This is the main portfolio dashboard and should be the README screenshot.

| Priority | Visualization | Dataset | Configuration | Why it belongs |
| --- | --- | --- | --- | --- |
| 1 | KPI row | Gold Transactions / Gold Blocks | Total transactions; total fee sats; average fee rate; blocks processed | Gives immediate context and proves Gold aggregations. |
| 2 | Daily network trend | Gold Transactions | Time: `transaction_date`; bars: `COUNT(*)`; line: `SUM(fee_sats)` or `AVG(fee_rate_sats_per_byte)` | Shows the expanded seven-day data flow and makes the time-series capability visible. |
| 3 | Fee-rate histogram | Gold Transactions | X: `fee_rate_sats_per_byte`; Y: count of transactions; 15-25 bins | A stable distribution chart. |
| 4 | Fee versus transaction size scatter plot | Gold Transactions | X: `size_bytes`; Y: `fee_sats` or `fee_rate_sats_per_byte`; one point per transaction | Shows granular analytics, not only headline KPIs. |
| 5 | Input/output composition bar chart | Gold Transactions | Group by a calculated `input_count`/`output_count` bucket; metric: count | Demonstrates transaction-shape analysis. |
| 6 | Top-fee transaction table | Gold Transactions | Sort by `fee_sats` descending; show shortened `tx_hash`, fee, size, fee rate, counts | Makes the dashboard inspectable and useful. |

Add a native date filter bound to `transaction_date` so each chart can be
viewed for one or more of the seven loaded dates.

## Dashboard 2: Pipeline Reconciliation and UTXO Quality

Keep this smaller than Dashboard 1. It demonstrates the Silver-to-Gold value
of the project rather than trying to look like a production operations center.

| Priority | Visualization | Dataset | Configuration | Current interpretation |
| --- | --- | --- | --- | --- |
| 1 | UTXO resolution status | UTXO Resolution Summary or Gold Inputs | Donut/bar by `resolution_status`; metric: input count | Shows the current genesis/resolved split. |
| 2 | Output spent-state reconciliation | Output Reconciliation Summary or Gold Outputs | Stacked bar by `canonical_spent`, split by `spend_discrepancy` | Makes the 54,777 discrepancy records visible and attributable. |
| 3 | Reconciliation KPI row | Gold Transactions / Gold Blocks / Gold Inputs | Mismatched transaction counts; mismatched value/count records; double-spend count | Establishes the current baseline: one count/value mismatch and no double spends. |
| 4 | Output lifecycle distribution | Gold Outputs | Histogram of `time_to_spend_days`; exclude nulls | The seven-day range makes spend-duration comparisons meaningful. |

When corrupt fixtures are intentionally ingested, add an optional `Silver
Rejects by Entity` bar chart based on the `silver_rejects` tables. Keep it off
the README dashboard; the current one-row rejection is not visually useful.

## Build order in Superset

1. Confirm **Trino Iceberg** is the database connection and browse the `gold`
   schema.
2. Create the four physical Gold datasets and mark their temporal columns.
3. Build the six Dashboard 1 charts; validate each result against Trino SQL.
4. Build the two highest-value Dashboard 2 charts: UTXO resolution and output
   spent-state reconciliation.
5. Add date filters and short chart descriptions, then save both dashboards.
6. Export the dashboard/dataset assets from Superset and keep the export as the
   future bootstrap seed. Bootstrap automation is intentionally deferred.

## README presentation plan

Use static screenshots rather than embedding the local Superset server.

1. Add one screenshot of **Bitcoin Transaction Activity** after the project
   architecture/setup section. Caption it: “Gold-layer transaction analytics
   queried through Trino and visualized in Apache Superset.”
2. Optionally add a smaller screenshot of **Pipeline Reconciliation and UTXO
   Quality** near the transformation/reconciliation explanation. Caption it:
   “Gold-layer reconciliation fields make input resolution and output-state
   checks queryable.”
3. Do not show raw hashes in the screenshot at full width; use the top-fee
   table with a shortened hash or crop the dashboard to the analytical charts.
4. After screenshots are stable, export the Superset assets and use those
   exports as the input to the future bootstrap-init service.

## Definition of done for this phase

- Four physical Gold datasets exist and query successfully through Trino.
- Dashboard 1 contains the KPI row, daily trend, fee distribution, scatter
  plot, transaction composition chart, and top-fee table.
- Dashboard 2 contains UTXO resolution and output spent-state reconciliation.
- Each dashboard has a clear title and a date filter spanning the loaded
  seven-day fixture.
- One or two static README screenshots are selected.
- Superset exports are saved only after the visualizations are considered
  stable; bootstrap loading is a later task.
