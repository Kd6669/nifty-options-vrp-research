# Code and data provenance

## Code source

The initial implementation was assembled from the clean tracked tree of:

- Repository: `Goyal-Dedhia-Capital/dhan-data-fetch-and-stream`
- Local source branch: `kd-codex/dhan-phase2-3`
- Source commit: `516d31b` (`Record terminal SPAN timing release evidence`)
- Imported scope: tracked `src/`, `tests/`, operator scripts, official source evidence, rule
  dimensions, and existing upstream audit/lineage documentation
- Excluded scope: credentials, `.env`, caches, logs, raw reports, and multi-GB generated datasets

The NSE SPAN acquisition/extraction implementation was imported from the focused tracked tree of:

- Repository: `Goyal-Dedhia-Capital/groww-margin-charges-model`
- Branch: `kd-codex/span-phase1-baseline`
- Commit: `b862d6bf7590ff62097ac373d40fd85e1be6480f`
- Imported scope: `src/robs_live/span`, its focused CLI/dependencies, SPAN tests/scripts, and Phase 1
  source-gap/release evidence
- Packaging-only change: imported `robs_live.*` modules were renamed to the unique `nifty_span.*`
  namespace to avoid collision with unrelated installed `robs_live` checkouts; logic is unchanged
- Excluded scope: the dirty untracked `data/` tree, credentials, raw archives, and unrelated repo data

## Data source

The audit and sample are derived from the local hash-pinned BOD SPAN gold release:

`reports/nifty_gold_span_bod_20210101_20260715/version=1.4.0/gold`

The BOD convenience release is not relabelled as strict point-in-time SPAN. The related strict and
six-slot releases remain separate and are documented in the main README and audit.

## Research brief

The repository copy of the take-home brief is byte-for-byte copied from:

`docs/task/quant-research-task.md`

Its SHA-256 is recorded in `docs/provenance_manifest.json`.
