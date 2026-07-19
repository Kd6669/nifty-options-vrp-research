# Phase 10 — margin-efficient sizing exploration

## Scope

The Phase 9 composite score is frozen. This module changes only sizing and risk mechanics.
All profile selection uses 2021–2023; 2024–2026 is reported afterward without retuning.

- Grid size: 17,640 policies.
- Initial margin/capital pool: ₹10,00,000.
- Margin ceilings: 25%, 35%, 50%, 65%, 80%, and 100%.
- Maximum structural-risk ceilings: 0.5% through 4.0%.
- Score floors, nonlinear score powers, drawdown brakes, and losing-streak brakes.
- Risk cap includes the discovery 95th-percentile round-trip cost reserve for each lot count.

## Discovery-selected profiles

| Profile | Margin cap | Max risk | Floor | Power | DD brake | Streak | Discovery net | Discovery DD | Holdout net | 2024 | 2025–26 | Full return | Full DD |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| active_brake_max_return_dd_1.0% | 50% | 4.0% | 20% | 0.0 | 1.0%×0.50 | none | ₹81,962 | -0.99% | ₹25,285 | ₹8,944 | ₹16,341 | 10.72% | -1.31% |
| low_margin_max_return_dd_1.0% | 35% | 4.0% | 20% | 0.0 | none | none | ₹78,426 | -0.91% | ₹26,931 | ₹9,090 | ₹17,841 | 10.54% | -0.91% |
| max_margin_efficiency_dd_1.5% | 35% | 4.0% | 40% | 0.0 | none | none | ₹70,835 | -0.76% | ₹38,148 | ₹15,113 | ₹23,036 | 10.90% | -0.76% |
| max_return_dd_0.5% | 35% | 2.5% | 40% | 0.0 | none | none | ₹42,501 | -0.46% | ₹26,228 | ₹9,165 | ₹17,063 | 6.87% | -0.65% |
| max_return_dd_1.0% | 50% | 4.0% | 20% | 0.0 | none | none | ₹81,962 | -0.99% | ₹36,856 | ₹8,944 | ₹27,912 | 11.88% | -1.15% |
| max_return_dd_1.5% | 50% | 4.0% | 20% | 0.0 | none | none | ₹81,962 | -0.99% | ₹36,856 | ₹8,944 | ₹27,912 | 11.88% | -1.15% |
| max_return_dd_2.0% | 50% | 4.0% | 20% | 0.0 | none | none | ₹81,962 | -0.99% | ₹36,856 | ₹8,944 | ₹27,912 | 11.88% | -1.15% |
| max_worst_year_dd_2.0% | 65% | 4.0% | 40% | 0.0 | none | none | ₹77,164 | -0.78% | ₹49,046 | ₹13,822 | ₹35,224 | 12.62% | -1.14% |
| smooth_confidence_max_return_dd_1.0% | 35% | 4.0% | 20% | 0.5 | none | none | ₹54,662 | -0.88% | ₹27,304 | ₹8,598 | ₹18,705 | 8.20% | -0.88% |

## Grid robustness

- Discovery-eligible policies: 4,392.
- Positive combined holdout: 58.8%.
- Positive in both 2024 and 2025–2026: 45.8%.
- Discovery-versus-holdout policy-net rank correlation: 0.649.
- Top discovery decile median holdout net: ₹24,963.

## Parameter-neighborhood diagnostics

| Dimension | Value | Policies | Median holdout | Positive holdout | Positive both later | Median full DD |
|---|---:|---:|---:|---:|---:|---:|
| score_floor | 0.0 | 1470 | ₹-145 | 49.3% | 34.7% | -1.00% |
| score_floor | 0.2 | 1764 | ₹1,377 | 55.3% | 41.6% | -0.85% |
| score_floor | 0.4 | 1158 | ₹12,416 | 76.4% | 66.5% | -0.72% |
| margin_fraction | 0.25 | 817 | ₹10,398 | 79.1% | 68.5% | -0.61% |
| margin_fraction | 0.35 | 773 | ₹7,689 | 73.0% | 59.0% | -0.76% |
| margin_fraction | 0.5 | 715 | ₹2,868 | 62.2% | 44.8% | -0.85% |
| margin_fraction | 0.65 | 699 | ₹-83 | 49.2% | 37.5% | -0.92% |
| margin_fraction | 0.8 | 694 | ₹-1,141 | 44.2% | 31.8% | -0.97% |
| margin_fraction | 1.0 | 694 | ₹-2,561 | 40.1% | 28.0% | -1.03% |
| drawdown_brake_threshold | 0.005 | 912 | ₹-1,328 | 41.8% | 17.9% | -0.84% |
| drawdown_brake_threshold | 0.01 | 1362 | ₹3,108 | 56.1% | 48.2% | -0.84% |
| drawdown_brake_threshold | 0.015 | 1408 | ₹6,718 | 66.8% | 56.0% | -0.86% |
| drawdown_brake_threshold | none | 710 | ₹7,142 | 70.1% | 57.0% | -0.86% |

## Interpretation

These are exploratory policies over a score that itself did not pass the strict Phase 9
bootstrap gate. A strong historical profile can be retained for forward shadow testing,
but must not be promoted by selecting whichever row looks best on 2024–2026.

## Reproduce

```powershell
python -m research.phase10.run_sizing_exploration
python -m pytest tests/test_phase10_sizing_exploration.py -q
```
