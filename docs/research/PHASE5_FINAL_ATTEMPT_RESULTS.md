# Phase 5 — final-attempt VRP feature strategy result

## Final decision

**FAIL — close the IV/RV/VRP hypothesis family for this dataset.**

The final attempt replaced direct VRP crossings with a causal feature model, reduced execution
to two-leg verticals, selected one model/cost gate on calendar 2024, and evaluated one locked
2025–2026 confirmation period. It failed before and after confirmation:

- every one of the 20 frozen validation configurations had negative mean net P&L;
- the locked confirmation lost ₹31,354.53 across 190 non-overlapping one-lot trades;
- gross P&L was already negative before ₹25,224.78 of modeled costs;
- the confidence interval includes materially negative outcomes;
- both confirmation years were negative;
- only 27.78% of populated months were positive;
- the selector relied primarily on low-coverage 180-minute labels;
- predicted gross P&L had negligible confirmation correlation with realized gross P&L.

No further threshold, percentile, structure, horizon, feature, or sizing variant should be
selected on this same sample. Reopening requires materially new data or an independently
motivated forecast.

## Frozen protocol

The protocol was written before Phase 5 outcome computation in
`PHASE5_FINAL_ATTEMPT_PROTOCOL.md`:

- unconditional 15-minute candidate grid;
- nearest-weekly NIFTY options and entry legs within ATM±3;
- bull call and bear put spreads only;
- 60/120/180-minute exact-contract exits;
- causal IV, RV, VRP, ratio, dynamics, skew, Greeks, and entry-liquidity cost features;
- separate ridge gross-P&L models for six structure/horizon cells;
- a predicted-gross-minus-cost-hurdle selector;
- one live position at a time and one historical exchange lot;
- training through 2023, validation in 2024, locked confirmation from 2025;
- dated STT, brokerage, statutory charges, slippage, timestamp-aware SPAN, and no future exit
  liquidity in entry features.

After the label-only availability audit—but before model outcomes—an acceptance gate was added:
any cell contributing at least 10% of confirmation trades must have at least 80% unconditional
exact-contract coverage.

## Dataset

| Item | Count / range |
|---|---:|
| Unconditional candidate timestamps | 27,608 |
| Cost-aware structure/horizon labels | 103,315 |
| Leg observations | 661,563 |
| Date range | 2021-03-31 through 2026-07-14 |
| Train rows | 54,183 |
| Validation rows | 19,713 |
| Confirmation rows | 29,419 |

No `next_*` realized-outcome field is present in the Phase 5 learning table.

### Exact-contract coverage

| Structure | 60m | 120m | 180m |
|---|---:|---:|---:|
| Bear put spread | 81.24% | 62.32% | 43.41% |
| Bull call spread | 81.31% | 62.42% | 43.52% |

Only 60-minute cells clear the 80% availability boundary. The longer-horizon rows remain useful
as disclosed diagnostics but cannot support an accepted strategy if they materially determine
the result.

## Validation selection

The frozen grid contained five ridge penalties and four cost-gate multipliers. Every cell lost
money on calendar 2024.

The selected configuration was:

- ridge penalty: `0.0`;
- cost-gate multiplier: `1.0`;
- non-overlapping validation trades: 164;
- validation gross P&L: −₹3,631.25;
- validation costs: ₹19,526.01;
- validation net P&L: −₹23,157.26;
- validation mean net P&L: −₹141.20/trade.

It was selected because the protocol required at least 50 validation trades and it had the
least-negative mean among eligible configurations. The numerically least-negative configuration
overall had only 35 trades and still lost ₹80.72/trade. Thus validation supplied no positive
strategy candidate to confirm.

## Locked confirmation tear sheet

| Metric | Result |
|---|---:|
| Period | 2025-01-01 through 2026-06-30 |
| Non-overlapping trades | 190 |
| Gross P&L | **−₹6,129.75** |
| Total modeled cost | ₹25,224.78 |
| Net P&L | **−₹31,354.53** |
| Mean net P&L | **−₹165.02/trade** |
| Median net P&L | −₹478.37/trade |
| Net win rate | 26.84% |
| 95% trade-date block-bootstrap mean CI | −₹355.15 to +₹31.75 |
| Mean SPAN margin | ₹39,460.50 |
| Mean net return on margin | −0.3952% |
| Positive months | 5 / 18 = 27.78% |

The selector predicted +2.750 gross points and +0.885 excess points per chosen trade on average,
but realized aggregate gross P&L was negative. The problem is therefore forecast failure plus
cost, not cost alone.

### Composition

| Structure | Horizon | Trades | Share | Coverage | Net P&L |
|---|---:|---:|---:|---:|---:|
| Bear put spread | 60m | 12 | 6.32% | 81.24% | −₹4,788.64 |
| Bear put spread | 120m | 17 | 8.95% | 62.32% | +₹5,633.22 |
| Bear put spread | 180m | 112 | 58.95% | 43.41% | −₹30,402.77 |
| Bull call spread | 60m | 5 | 2.63% | 81.31% | −₹1,856.71 |
| Bull call spread | 120m | 18 | 9.47% | 62.42% | −₹2,548.78 |
| Bull call spread | 180m | 26 | 13.68% | 43.52% | +₹2,609.14 |

The two positive cell totals are low-coverage sensitivities and were not separately selected in
the frozen protocol. The overall chronological selector loses money and materially depends on
180-minute cells, so they cannot be repurposed into a new claim.

### Calendar stability

| Year | Trades | Net P&L |
|---:|---:|---:|
| 2025 | 141 | −₹20,750.73 |
| 2026 through June | 49 | −₹10,603.80 |

Only January, March, and June 2025 plus February and May 2026 were positive. Five positive months
out of 18 is far below the frozen 60% requirement.

## Forecast diagnostics

Confirmation correlations between predicted and realized gross points are economically weak:

| Structure | 60m | 120m | 180m |
|---|---:|---:|---:|
| Bear put spread | +0.094 | +0.024 | −0.019 |
| Bull call spread | +0.012 | +0.029 | +0.061 |

Even the highest predicted-gross decile remains net negative in every cell:

| Structure | Horizon | Predicted gross | Realized gross | Mean net |
|---|---:|---:|---:|---:|
| Bear put spread | 60m | +1.233 pts | +1.086 pts | −₹83.65 |
| Bear put spread | 120m | +2.072 pts | −0.198 pts | −₹182.33 |
| Bear put spread | 180m | +3.790 pts | −2.268 pts | −₹314.39 |
| Bull call spread | 60m | +0.795 pts | −0.694 pts | −₹202.46 |
| Bull call spread | 120m | +1.519 pts | −0.255 pts | −₹167.22 |
| Bull call spread | 180m | +2.098 pts | +0.929 pts | −₹108.10 |

This shows that the tested IV/RV/VRP, local-skew, Greek, spot-return, and cost features do not
forecast enough defined-risk movement to overcome even two-leg execution costs.

## Acceptance gates

| Gate | Result |
|---|---|
| At least 100 confirmation trades | PASS |
| Positive mean net P&L | FAIL |
| Bootstrap CI lower bound above zero | FAIL |
| Positive aggregate net P&L | FAIL |
| At least 60% positive months | FAIL |
| Every confirmation year positive | FAIL |
| Positive-month concentration at most 40% | PASS |
| Material cells have at least 80% coverage | FAIL |

Six of eight gates fail.

## Interpretation and closure

The research result is not “find a slightly better threshold.” It is:

1. Direct VRP zero and tail crossings fail after costs.
2. Four-leg volatility structures have too much leg cancellation and order cost.
3. Quantity scaling does not rescue them after participation impact.
4. The only adequately covered multi-day diagnostic fails.
5. A lower-cost two-leg causal feature model fails validation, gross confirmation P&L,
   forecast calibration, costs, stability, confidence, and coverage.

Accordingly, the current hypothesis family is closed.

A future project may reopen volatility research only with materially different evidence, for
example full-chain bid/ask/depth data, multiple expiries, clean multi-day contract histories,
independent futures/variance-swap proxies, substantially longer untouched data, or a new economic
mechanism specified before observing outcomes.

## Reproduce

```powershell
python -m research.phase5.build_final_attempt_dataset `
  --gold-root "<six-slot-v2.1-gold-root>"

python -m research.phase5.run_final_attempt_strategy

python -m pytest tests/test_phase5_final_attempt.py -q
```

Artifacts:

- `audit/phase5_final_attempt_dataset.parquet`
- `audit/phase5_final_attempt_observations.parquet`
- `audit/phase5_final_attempt_dataset_summary.json`
- `audit/phase5_final_attempt_dataset_manifest.json`
- `audit/phase5_final_attempt_validation_grid.csv`
- `audit/phase5_final_attempt_tradebook.csv`
- `audit/phase5_final_attempt_monthly.csv`
- `audit/phase5_final_attempt_yearly.csv`
- `audit/phase5_final_attempt_calibration.csv`
- `audit/phase5_final_attempt_models.json`
- `audit/phase5_final_attempt_summary.json`
- `audit/phase5_final_attempt_manifest.json`
