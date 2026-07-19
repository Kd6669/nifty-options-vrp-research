# Phase 3 upper-tail percentile crossing tear sheet

## Decision

No variant has a positive cost-inclusive sample mean.

This is a post-hypothesis, in-sample diagnostic. Each row trades one historical exchange lot, with no compounding, capital pool, leverage rule, or portfolio risk overlay. Cells overlap across thresholds and must not be summed as independent strategies.

## Frozen experiment

- Signal curve: trailing five-minute median of the causal same-minute-of-day normalized-VRP percentile.
- Thresholds: 70%, 75%, 80%, 85%, 90%, and 95%.
- Directions: first daily strict crossing upward and first daily strict crossing downward.
- Execution: next exact minute, no later than 14:15 IST.
- Structure: the same ATM +/-3 short iron condor for both directions.
- Exit: fixed contracts after exactly 60 minutes.
- Frictions: pinned Groww charges, volume/OI slippage, ATM-IV fallback for missing India VIX, and timestamp-scheduled SPAN margin.

## Base one-lot results

| Variant | Trades | Gross mean pts | Gross total | Mean cost | Net total | Net mean | Win rate | Mean net ROM | ROM 95% CI | Net CVaR 5% | Positive months |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| q70_down | 588 | +0.093 | Rs -328.50 | Rs 268.94 | Rs -158,466.45 | Rs -269.50 | 11.05% | -0.5132% | [-0.5696%, -0.4620%] | Rs -1,476.31 | 3.08% |
| q70_up | 568 | +0.462 | Rs 12,854.25 | Rs 274.06 | Rs -142,812.25 | Rs -251.43 | 13.20% | -0.4873% | [-0.5367%, -0.4389%] | Rs -1,247.35 | 3.08% |
| q75_down | 513 | -0.004 | Rs -3,482.25 | Rs 270.09 | Rs -142,040.18 | Rs -276.88 | 12.67% | -0.5221% | [-0.5854%, -0.4609%] | Rs -1,665.63 | 4.62% |
| q75_up | 475 | +0.548 | Rs 13,055.25 | Rs 277.49 | Rs -118,754.50 | Rs -250.01 | 16.63% | -0.4772% | [-0.5350%, -0.4226%] | Rs -1,370.35 | 6.15% |
| q80_down | 392 | +0.609 | Rs 12,083.00 | Rs 268.91 | Rs -93,328.42 | Rs -238.08 | 20.41% | -0.4624% | [-0.5383%, -0.3901%] | Rs -1,441.85 | 7.69% |
| q80_up | 363 | +0.902 | Rs 18,474.75 | Rs 278.67 | Rs -82,682.73 | Rs -227.78 | 22.87% | -0.4479% | [-0.5241%, -0.3739%] | Rs -1,454.85 | 15.38% |
| q85_down | 296 | +0.883 | Rs 13,454.25 | Rs 266.51 | Rs -65,432.46 | Rs -221.06 | 28.72% | -0.4168% | [-0.5093%, -0.3272%] | Rs -1,518.25 | 11.11% |
| q85_up | 272 | +1.808 | Rs 27,943.50 | Rs 273.51 | Rs -46,452.40 | Rs -170.78 | 33.09% | -0.3259% | [-0.4174%, -0.2377%] | Rs -1,232.41 | 25.40% |
| q90_down | 194 | -0.408 | Rs -1,196.50 | Rs 265.48 | Rs -52,699.10 | Rs -271.64 | 30.41% | -0.5778% | [-0.7585%, -0.4142%] | Rs -2,132.27 | 26.67% |
| q90_up | 181 | +0.628 | Rs 13,121.50 | Rs 270.15 | Rs -35,775.31 | Rs -197.65 | 34.81% | -0.4653% | [-0.6365%, -0.3075%] | Rs -1,492.60 | 33.33% |
| q95_down | 92 | +0.337 | Rs -880.25 | Rs 272.89 | Rs -25,986.41 | Rs -282.46 | 30.43% | -0.5555% | [-0.8291%, -0.2958%] | Rs -2,516.47 | 27.27% |
| q95_up | 94 | +1.139 | Rs 6,438.25 | Rs 271.88 | Rs -19,118.53 | Rs -203.39 | 38.30% | -0.4187% | [-0.7036%, -0.1430%] | Rs -2,009.54 | 39.02% |

## Interpretation rules

A variant is not promoted merely because its sample net mean is positive. Promotion requires a positive cost-inclusive mean, a bootstrap mean-ROM interval above zero, reasonable tail loss and concentration, stability across years/months, and genuinely prospective out-of-sample confirmation. The best row in this table is post-selected.

## Model limitations

- All threshold choices and observations are in-sample and post-hypothesis; no row is prospective OOS evidence.
- Threshold variants overlap in dates and sometimes entry times; they are dependent diagnostics, not an additive portfolio.
- Historical bid/ask is unavailable; fills use the pinned volume/OI synthetic slippage model.
- The pinned slippage function has no submitted-order participation input, so the present experiment is restricted to one lot.
- When modeled slippage would put a sell fill below the model's minimum option tick, the fill is conservatively floored at Rs 0.05 and explicitly counted.
- The slippage stale multiplier is a low volume/OI-turnover proxy, not elapsed quote age.
- SPAN uses the six-slot research reference schedule; BOD is assumed available at 09:15 until ID1.
- The Dhan rolling WEEK history is a nearest-listed-expiry proxy and does not prove actual nearest-weekly identity.
- Missing exact-contract exits are not imputed; the event curve only retains complete next-minute 60-minute paths.

## Reproducibility

The CSV tradebook and legbook retain the exact contracts, entry/exit observations, cost components, slippage components, historical lot size, and SPAN slot. The JSON summary and manifest contain the machine-readable results and SHA-256 evidence.
