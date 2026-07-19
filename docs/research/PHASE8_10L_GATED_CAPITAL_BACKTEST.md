# Phase 8 — ₹10 lakh gated VRP capital backtest

## Decision

The discovery-selected candidate is **short_iron_fly** under the balanced
50%-of-equity SPAN budget and 2%-of-equity defined maximum-loss budget. This is a
capital-aware historical diagnostic, not a clean out-of-sample claim: the gate family was
chosen after inspecting calendar stability even though its numeric thresholds are fitted only
on 2021–2023.

## Frozen contract

- Initial capital: ₹10,00,000.
- Signal: first daily upper-85 VRP percentile upward crossing.
- Gate: all three causal IV/RV conditions must pass.
- Holding period: fixed 60 minutes; nearest weekly expiry; entry legs inside ATM±3.
- Primary sizing: min(discovery capacity cap, 50% SPAN-margin cap, 2% maximum-loss cap).
- Execution: date-aware Groww charges, pinned base slippage, corrected 60-lot-parity
  ladder, and separate square-root volume/OI impact.
- Margin: timestamp-aware joined SPAN slot at entry; multi-lot margin scales linearly for the
  identical defined-risk basket.
- No overlapping positions, compounding occurs only after the 60-minute exit, and there is no
  intratrade stop or mark-to-market margin-call simulation.

## Gate and capacity

- IV change 5m > -0.046817 vol points (decimal -0.00046817).
- IV change 15m > -0.114873 vol points (decimal -0.00114873).
- RV change 5m > -0.02651714.
- Discovery capacity caps: fly 76 lots, condor
  53 lots, put butterfly 21 lots.

## Primary tear sheet

| Metric | Result |
|---|---:|
| Executed trades | 132 |
| Final equity | ₹1,007,696.08 |
| Net profit | ₹7,696.08 |
| Total return | 0.77% |
| CAGR | 0.15% |
| Sharpe, zero rate | 0.174 |
| Gross P&L | ₹101,277.25 |
| Total costs | ₹93,581.17 |
| Charges / slippage | ₹38,682.18 / ₹54,898.99 |
| Base slippage / added impact | ₹46,886.52 / ₹8,012.47 |
| Turnover | ₹16,782,606.04 |
| Turnover / starting capital | 16.78× |
| Win rate | 53.79% |
| Profit factor | 1.066 |
| Maximum drawdown | −₹23,143.85 (-2.29%) |
| Drawdown peak → trough | 2022-02-10 → 2022-08-10 |
| Drawdown recovery | 2024-04-10 |
| Positive active months | 27 / 57 (47.37%) |
| CVaR 5% per trade | ₹-4,153.51 |
| Average lots | 5.23 |
| Average / maximum entry SPAN | ₹279,168.94 / ₹499,983.46 |
| Maximum margin utilization | 49.96% |
| Maximum defined-loss utilization | 2.00% |

## Calendar slices

| Split | Trades | Gross | Costs | Net | Mean net | Win rate |
|---|---:|---:|---:|---:|---:|---:|
| discovery_2021_2023 | 72 | ₹60,103.75 | ₹49,994.82 | ₹10,108.93 | ₹140.40 | 58.33% |
| validation_2024 | 18 | ₹9,593.75 | ₹15,923.83 | ₹-6,330.08 | ₹-351.67 | 33.33% |
| confirmation_2025_2026 | 42 | ₹31,579.75 | ₹27,662.52 | ₹3,917.23 | ₹93.27 | 54.76% |

## Capital-policy sensitivity

| Structure | Policy | Return | Net P&L | Max drawdown | Average lots |
|---|---|---:|---:|---:|---:|
| short_iron_fly | conservative | -1.18% | ₹-11,759.60 | -1.53% | 2.35 |
| short_iron_fly | balanced | 0.77% | ₹7,696.08 | -2.29% | 5.23 |
| short_iron_fly | growth | 2.28% | ₹22,849.29 | -3.62% | 8.15 |
| short_iron_fly | margin_only | 31.55% | ₹315,511.65 | -4.53% | 21.58 |
| short_iron_condor | conservative | -1.54% | ₹-15,361.52 | -1.81% | 2.68 |
| short_iron_condor | balanced | -0.34% | ₹-3,379.61 | -2.22% | 5.80 |
| short_iron_condor | growth | 0.70% | ₹7,011.38 | -3.36% | 9.05 |
| short_iron_condor | margin_only | 18.89% | ₹188,850.42 | -4.03% | 21.44 |
| long_put_butterfly | conservative | -1.63% | ₹-16,347.04 | -2.00% | 2.33 |
| long_put_butterfly | balanced | -1.48% | ₹-14,764.06 | -3.53% | 5.15 |
| long_put_butterfly | growth | -2.68% | ₹-26,817.60 | -5.82% | 7.86 |
| long_put_butterfly | margin_only | 14.33% | ₹143,284.93 | -8.46% | 17.89 |

The margin-only rows are leverage stress tests. They are not deployable recommendations because
the archive cannot simulate intratrade SPAN expansion, margin calls, or forced liquidation, and
the gate is not clean OOS evidence.

## Full cost decomposition

| Component | Rupees | Share of total cost |
|---|---:|---:|
| Brokerage | ₹21,120.00 | 22.57% |
| STT | ₹6,453.56 | 6.90% |
| Exchange charges | ₹5,878.95 | 6.28% |
| GST | ₹4,877.94 | 5.21% |
| Stamp duty | ₹251.04 | 0.27% |
| SEBI + IPFT | ₹100.70 | 0.11% |
| Base slippage | ₹46,886.52 | 50.10% |
| Lot-ladder impact | ₹5,274.09 | 5.64% |
| Volume impact | ₹2,255.98 | 2.41% |
| OI impact | ₹482.41 | 0.52% |

## Descriptive regime leads

These are diagnostics, not permission to add another gate without a new holdout.

| Dimension | Regime | Trades | Net P&L | Mean/trade | Win rate |
|---|---|---:|---:|---:|---:|
| iv_regime | low_iv | 56 | ₹25,105.39 | ₹448.31 | 62.50% |
| iv_regime | high_iv | 38 | ₹-17,159.71 | ₹-451.57 | 47.37% |
| rv_regime | low_rv | 40 | ₹18,693.93 | ₹467.35 | 67.50% |
| rv_regime | high_rv | 44 | ₹-16,293.09 | ₹-370.30 | 43.18% |
| dte_regime | low_dte | 46 | ₹25,844.07 | ₹561.83 | 67.39% |
| dte_regime | high_dte | 42 | ₹-12,090.71 | ₹-287.87 | 50.00% |
| gate_cushion_regime | thin_cushion | 39 | ₹-32,816.47 | ₹-841.45 | 30.77% |
| gate_cushion_regime | wide_cushion | 46 | ₹38,234.69 | ₹831.19 | 65.22% |
| entry_time_regime | 1100_1300 | 71 | ₹-20,961.34 | ₹-295.23 | 50.70% |
| entry_time_regime | after_1300 | 41 | ₹23,484.12 | ₹572.78 | 58.54% |

## Interpretation boundaries

- The 2024 and 2025–2026 labels are calendar holdouts for sizing and numeric thresholds,
  but not pristine OOS evidence because the gate concept was retained after inspecting them.
- SPAN is evaluated at entry only. Intratrade margin expansion and forced-liquidation paths
  are not present in the dataset.
- Historical close, volume, and OI replace observed bid/ask and order-book depth; impact is an
  auditable sensitivity rather than fill-calibrated market truth.
- Regime cells are descriptive and are not additional entry filters.

## Reproduce

```powershell
python -m research.phase8.run_gated_capital_backtest
python -m pytest tests/test_phase8_gated_capital.py -q
```
