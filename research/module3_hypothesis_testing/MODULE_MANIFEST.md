# Module 3 manifest

## Checkpoint

- Module: normalized intraday VRP hypothesis testing
- State: closed
- Decision: reject as a standalone intraday defined-risk entry rule for the current dataset
- Position basis: one historical exchange lot per completed trade
- Maximum diagnostic horizon: 180 minutes

## Canonical contracts and outputs

- `module.yaml`: ownership, universe, stages, outputs, and must-not boundaries
- `contracts/hypotheses.json`: all frozen and post-hoc claims with their final states
- `results/closeout.json`: machine-readable consolidated evidence
- `results/closeout_report.md`: human research closeout
- `results/manifest.json`: SHA-256 lineage across sources, code, documents, and outputs

## Preserved full evidence

Compact audit JSON and complete CSV trade/leg books are stored under `audit/`. Large Parquet
inputs and observation panels remain local and Git-ignored. Detailed protocols and research notes
are stored under `docs/research/`. The module result manifest identifies the exact compact source
summaries and implementation files used to generate the closure packet.
