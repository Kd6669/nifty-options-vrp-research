# Module 3 runbook

## Rebuild the individual experiments

The source runners remain deliberately separated so a later rerun cannot silently change every
research degree of freedom at once:

```powershell
python -m research.phase3.run_full_strategy_backtest --help
python -m research.phase3.run_tail_percentile_backtests --help
python -m research.phase4.run_cost_aware_discovery --help
python -m research.phase4.run_multiday_vrp_feasibility --help
python -m research.phase5.build_final_attempt_dataset --help
python -m research.phase5.run_final_attempt_strategy --help
python -m research.phase6.run_vrp_reversal_test --help
python -m research.phase7.run_180min_signal_comparison --help
```

These commands require the local Phase 2 Parquet evidence and gold dataset root documented in
their manifests. Large Parquet observations are intentionally Git-ignored; compact JSON, CSV,
code, protocols, reports, and SHA-256 manifests are preserved in the repository.

## Rebuild the consolidated closeout

From the repository root:

```powershell
python -m research.module3_hypothesis_testing.run build
python -m research.module3_hypothesis_testing.run verify
```

`build` reads the frozen Phase 2–7 JSON evidence, extracts the decision metrics without
re-estimating parameters, and deterministically writes the closeout JSON, Markdown report, and
integrity manifest. `verify` checks every declared source, implementation, document, and output
hash.

## Acceptance checks

```powershell
python -m ruff check .
python -m pytest -q
git diff --check
```

Module 3 is closed when those checks pass, the manifest verifies, and the generated decision is
`REJECT_INTRADAY_VRP_AS_STANDALONE_DEFINED_RISK_ENTRY_RULE_FOR_CURRENT_DATASET`.
