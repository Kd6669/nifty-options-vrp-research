# Module 4 runbook

Run all commands from the repository root with the project environment active.

## Required local data for a full research rerun

The Phase 8–10 runners require these local/Git-ignored observation files in addition to the tracked
CSV evidence:

- `audit/phase4_cost_aware_observations.parquet`
- `audit/phase2_vrp_session_curve_features.parquet`
- `audit/phase2_intraday_iv_surface.parquet`

The tracked `audit/phase4_cost_aware_tradebook.csv` and Module 3 manifests bind their upstream
lineage. Do not silently substitute a different gold release.

## Full calculation chain

```powershell
python -m research.phase8.run_gated_capital_backtest
python -m research.phase9.run_confidence_sizing
python -m research.phase10.run_sizing_exploration
```

Phase 8 must complete before Phase 9, and Phase 9 before Phase 10. Do not run these concurrently
because each phase consumes the prior phase's outputs.

## Rebuild Module 4 presentation outputs

```powershell
python -m research.module4_sizing_risk_management.run build
python -m research.module4_sizing_risk_management.run verify
```

`build` performs an independent reconciliation against profile config 5628. It fails if the
selected row, 35% margin cap, 4% cost-reserved risk cap, 40% score floor, or the published
gross/cost/net/turnover totals change.

## Tests

```powershell
python -m pytest `
  tests/test_nifty_execution.py `
  tests/test_phase8_gated_capital.py `
  tests/test_phase9_confidence_sizing.py `
  tests/test_phase10_sizing_exploration.py `
  tests/test_module4_sizing_risk_management.py -q
```

Repository-wide checks:

```powershell
make lint
make test
git diff --check
```

## Frozen forward-shadow handoff

Only the strategy in `contracts/strategy.json` may be shadowed. Record every signal, rejection,
order quantity, quote age, bid/ask, fill, charge, margin update, intratrade MTM, exit, and overlap.
Do not change gates, score weights, 40% quality floor, 35% margin cap, 4% risk cap, structure, or
60-minute horizon without opening a new research version.
