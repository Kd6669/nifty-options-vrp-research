# SPAN Backfill Audit

- Status: **FAIL**
- Range: `2026-02-01` through `2026-02-01`
- Date/slot cells: `6` / `6` accounted; `6` terminal
- Downloaded valid states: `0`
- Official endpoint returned no slot: `6`
- Ambiguous unclassified 404 cells: `6`
- Failed/incomplete cells: `6`
- Raw integrity failures: `0`
- Downloaded without valid extraction: `0`
- Compacted months/rows: `0` / `0`
- Duplicate compacted natural keys: `0`
- Unmanifested raw files/fragments: `0` / `0`

## Gates

| Gate | Result |
|---|---|
| Final outcome | FAIL_INCOMPLETE |
| Complete durable matrix | PASS |
| Raw archives match manifest | PASS |
| Every downloaded archive extracted/compacted | PASS |
| Compacted natural keys unique | PASS |

`not_returned_http_404` remains source-response evidence only; without independent calendar or source-boundary classification it prevents acceptance.
Unknown SPAN effective times remain unknown and must not be used to introduce EOD lookahead.

## Compacted months

| Month | Rows | Sources | SHA-256 | Issue |
|---|---:|---:|---|---|
