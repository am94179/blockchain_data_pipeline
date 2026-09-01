# Corruption Fixtures

`data/generate_corrupt.py` creates a clean synthetic dataset, then calls
[`corrupt_fixture.py`](../data/corrupt_fixture.py) to rewrite selected CSV rows
with controlled validation and relationship failures. It is intended to
exercise Silver rejects and UTXO reconciliation rather than the normal clean
ingestion demo.

## Running a fixture

```bash
python data/generate_corrupt.py \
  --transactions 20000 \
  --genesis-utxos 10000 \
  --corruption-scenario mixed \
  --corruption-rate 0.01
```

`--corruption-scenario` accepts `clean`, `data_quality`, `relationships`, or
`mixed` (the default). `--corruption-rate` must be greater than zero and no
greater than one; for every non-empty entity it selects at least one row. The
fixture selection uses `--corruption-seed`, which defaults to `--seed`.

## Scenarios

| Scenario | Mutations |
| --- | --- |
| `clean` | Leaves the generated baseline unchanged. |
| `data_quality` | Normalization cases, missing/invalid scalar fields, and one duplicate row for each entity. |
| `relationships` | Broken block and transaction references, count/value mismatches, a bad referenced output, and a spent-state disagreement. |
| `mixed` | Applies both sets of mutations. |

### Data-quality coverage

- **Blocks:** lowercased and whitespace-padded miner names, plus a duplicate.
- **Transactions:** empty `tx_hash`, whitespace-only `block_hash`, malformed
  timestamp or fee, plus a duplicate.
- **Inputs:** empty ID, whitespace-only address, malformed value, plus a
  duplicate.
- **Outputs:** empty ID, malformed value or Boolean `spent`, plus a duplicate.

### Relationship coverage

- Transactions may reference an unknown block, declare an incorrect input
  count, or violate the input/output/fee accounting total.
- Inputs may reference an unknown parent transaction or prior output, or carry
  an address that disagrees with the referenced output.
- Outputs may reference an unknown creating transaction. One referenced output
  is marked unspent in the source to produce a source-versus-observed spent
  discrepancy.

## Batch-runner note

The corrupt generator writes to `corrupt_ingest_<YYYY-MM-DD>`, whereas
`make ingest-batch` expects `ingest_<YYYY-MM-DD>`. The current batch runner
therefore does not consume a corrupt fixture directly. Rename or copy the
fixture directory to the expected `ingest_` name before running the standard
batch command, or run the relevant jobs with a matching custom input path.

For clean-generator behavior and options, see
[Synthetic Data Generation](data_generation.md).
