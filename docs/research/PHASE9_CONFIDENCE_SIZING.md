# Phase 9 — confidence-ranked sizing diagnostic

## Verdict

**FAIL** under the frozen rank-correlation and economic gates.

The rank test uses one-lot outcomes before confidence controls quantity. This prevents an
endogenous correlation between a higher score, more lots, and larger rupee P&L.

## Frozen score and sizing contract

- Primary score: 50% gate cushion, 15% inverse IV percentile, 15% inverse RV percentile,
  10% inverse DTE percentile, and 10% entry-time score.
- Comparator: gate-cushion percentile only.
- Every percentile transform uses the 2021–2023 discovery distribution only.
- Score quintile risk ladder: 0%, 0.5%, 1.0%, 1.5%, and 2.0% of current equity.
- Lots remain capped by 50% entry SPAN and the 76-lot discovery capacity ceiling.
- Exact quantity-aware costs are recomputed at the selected integer lot count.

## Rank correlation

| Score | Split | N | Spearman rho: net | 95% bootstrap CI | One-sided p | Rho: risk return |
|---|---|---:|---:|---:|---:|---:|
| gate_cushion_only | discovery_2021_2023 | 72 | 0.265 | [0.006, 0.514] | 0.0114 | 0.403 |
| gate_cushion_only | validation_2024 | 18 | 0.204 | [-0.270, 0.660] | 0.2087 | 0.483 |
| gate_cushion_only | confirmation_2025_2026 | 42 | 0.113 | [-0.226, 0.454] | 0.2393 | 0.269 |
| gate_cushion_only | holdout_2024_2026 | 60 | 0.165 | [-0.096, 0.420] | 0.1025 | 0.339 |
| gate_cushion_only | full_sample | 132 | 0.222 | [0.040, 0.398] | 0.0066 | 0.365 |
| regime_composite | discovery_2021_2023 | 72 | 0.337 | [0.100, 0.551] | 0.0029 | 0.452 |
| regime_composite | validation_2024 | 18 | 0.427 | [-0.077, 0.826] | 0.0417 | 0.530 |
| regime_composite | confirmation_2025_2026 | 42 | 0.101 | [-0.237, 0.450] | 0.2622 | 0.230 |
| regime_composite | holdout_2024_2026 | 60 | 0.214 | [-0.066, 0.484] | 0.0472 | 0.341 |
| regime_composite | full_sample | 132 | 0.271 | [0.093, 0.437] | 0.0014 | 0.387 |

## Capital-path comparison

| Policy | Split | Eligible | Executed | Net P&L | Mean executed trade | Win rate | Average lots |
|---|---|---:|---:|---:|---:|---:|---:|
| fixed_balanced | full_sample | 132 | 132 | ₹7,696.08 | ₹58.30 | 53.79% | 5.23 |
| fixed_balanced | holdout_2024_2026 | 60 | 60 | ₹-2,412.85 | ₹-40.21 | 48.33% | 5.40 |
| gate_cushion_only | full_sample | 132 | 107 | ₹18,474.18 | ₹172.66 | 51.40% | 3.20 |
| gate_cushion_only | holdout_2024_2026 | 60 | 51 | ₹1,326.61 | ₹26.01 | 41.18% | 3.63 |
| regime_composite | full_sample | 132 | 109 | ₹23,851.35 | ₹218.82 | 55.96% | 3.17 |
| regime_composite | holdout_2024_2026 | 60 | 52 | ₹1,449.42 | ₹27.87 | 44.23% | 3.79 |

## Frozen pass criteria

- [x] combined_holdout_rho_at_least_0_20
- [ ] combined_holdout_bootstrap_ci_low_above_zero
- [x] combined_holdout_one_sided_permutation_p_at_most_0_05
- [x] positive_rho_in_2024_and_2025_2026
- [x] holdout_top_quintile_mean_exceeds_bottom
- [x] holdout_confidence_sizing_net_positive
- [x] holdout_confidence_sizing_beats_fixed_balanced

## Interpretation boundary

The regime directions were identified after inspecting the existing sample. Therefore,
even a statistical pass here would be a research pass rather than pristine OOS evidence.
A deployment claim still requires untouched forward data.

## Reproduce

```powershell
python -m research.phase9.run_confidence_sizing
python -m pytest tests/test_phase9_confidence_sizing.py -q
```
