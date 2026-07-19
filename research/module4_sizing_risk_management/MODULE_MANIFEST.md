# Module 4 manifest

## Checkpoint

- Module: sizing and risk management for the gated upper-85 short-iron-fly candidate
- State: closed; frozen forward-shadow candidate
- Deployment decision: not approved
- Capital basis: ₹10,00,000 initial equity
- Exit basis: 60-minute fixed horizon

## Canonical contracts and outputs

- `module.yaml`: ownership, stages, output paths, and prohibited interpretations
- `contracts/strategy.json`: signal, score, execution, sizing, and acceptance contract
- `results/closeout.json`: machine-readable consolidated decision
- `results/closeout_report.md`: human-readable research closeout
- `results/trades/recommended_trade_sheet.csv`: complete signal/trade ledger
- `results/curves/`: equity, monthly return, and drawdown tables
- `results/diagnostics/`: cost, rank, regime, profile, and parameter-neighborhood tables
- `results/exploration/sizing_grid.csv.gz`: complete sizing search
- `results/visualizations/`: deterministic SVG research figures
- `results/manifest.json`: SHA-256 lineage over every published member

## Canonical ownership

Phase 8 owns the gated capital simulator. Phase 9 owns confidence scoring and rank tests. Phase 10
owns the policy grid and profile selection. Module 4 owns only the frozen contract, reconciliation,
packaging, presentation, and integrity verification. This avoids maintaining divergent copies of
the economic calculations.

## Preserved input boundary

The repository preserves compact audit outputs, the exact-lot fly cost surface, and the complete
policy grid. Full minute observation Parquets remain local because they are research data archives,
not source code. Their required paths and rebuild sequence are explicit in `docs/runbook.md`.
