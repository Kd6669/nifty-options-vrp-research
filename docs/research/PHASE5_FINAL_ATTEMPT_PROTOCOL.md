# Phase 5 — final-attempt VRP feature strategy protocol

## Status before outcomes

This protocol is frozen before running the Phase 5 strategy result. Phase 4 rejected zero and
tail percentile crossings as standalone signals after costs. Phase 5 is the final attempt to
retain the original economic hypothesis by treating IV, RV, and VRP as causal predictive
features rather than direct trade triggers.

If the locked confirmation result fails the acceptance gates below, the IV/RV/VRP hypothesis
family is closed for this rolling nearest-weekly ATM±10 dataset. No additional percentile,
threshold, structure, or horizon tuning will be performed on the same sample.

## Universe and execution

- NIFTY nearest-weekly options only.
- Entry legs are frozen within ATM±3.
- Candidate entry timestamps are the available 15-minute grid after the causal RV warm-up.
- Structures are restricted to the two lowest-order-count candidates:
  - bull call spread: long `CALL +1`, short `CALL +3`;
  - bear put spread: long `PUT -1`, short `PUT -3`.
- Candidate horizons are fixed at 60, 120, and 180 minutes.
- Exact entry contracts must exist at the fixed exit timestamp.
- One historical exchange lot per trade; no capital pool or dynamic sizing.
- A chronological selector may hold only one position at a time. An entry is skipped while a
  previous selected trade remains open.
- Entry and exit slippage use the pinned volume/OI/DTE model. Missing INDIA VIX falls back to
  same-timestamp ATM IV × 100.
- Actual labels include dated STT, brokerage, GST, exchange/regulatory charges, and modeled
  entry/exit slippage.

## Causal features

Only information available at the candidate timestamp is eligible:

1. ATM IV, causal trailing RV, IV-minus-RV, variance VRP, and `log(IV / RV)`.
2. Time-of-day-conditioned VRP percentile and its quantized value.
3. Five-minute VRP percentile velocity/acceleration and variance-VRP velocity/acceleration.
4. Causal 5/15/30-minute changes in IV, RV, `log(IV/RV)`, and spot returns.
5. Local ATM±3 surface state: put skew, call skew, risk reversal, smile curvature, ATM
   call-put IV gap, and ATM-IV time-of-day percentile.
6. DTE and cyclical time-of-day coordinates.
7. Structure state: entry credit/debit, theoretical max profit/loss, delta, gamma, theta,
   vega, and a causal round-trip cost-hurdle proxy computed from entry liquidity only.

The actual future exit cost is part of the realized label and is never used as an entry
feature.

## Model and selector

- A separate ridge regression predicts gross P&L points for each of the six
  structure/horizon cells.
- Missing values are imputed with training medians; features are standardized using training
  means and standard deviations only.
- The selector computes:

```text
predicted_excess = predicted_gross_points
                   - gate_multiplier * causal_cost_hurdle_points
```

- At each candidate timestamp, it chooses the available cell with the highest predicted
  excess. It enters only if that excess is positive and no earlier position is open.
- No realized label is used to choose between simultaneous structures or horizons.

## Frozen temporal split

| Segment | Dates | Use |
|---|---|---|
| Train | Through 2023-12-31 | Fit feature models |
| Validation | 2024-01-01 through 2024-12-31 | Select one global ridge penalty and gate multiplier |
| Confirmation | From 2025-01-01 | Locked final evaluation |

The entire broader dataset has informed earlier exploratory work, so the confirmation segment
is labelled post-selection walk-forward evidence rather than pristine OOS evidence. Phase 5
does not use its outcomes for tuning.

## Fixed validation search

- Ridge penalties: `0.0, 0.1, 1.0, 10.0, 100.0`.
- Gate multipliers: `1.0, 1.25, 1.5, 2.0`.
- The configuration with the highest validation mean net P&L is selected, subject to at least
  50 non-overlapping validation trades.
- If no configuration has 50 trades, the most active configuration is retained and the
  activity gate is marked failed.
- After selection, models are refit on train plus validation with the frozen ridge penalty;
  the gate multiplier is not changed.

## Confirmation acceptance gates

Every gate must pass:

1. At least 100 non-overlapping confirmation trades.
2. Positive mean net P&L per trade.
3. The 95% trade-date block-bootstrap confidence interval for mean net P&L has a lower bound
   above zero.
4. Aggregate confirmation net P&L is positive after all modeled costs.
5. At least 60% of populated confirmation months have positive net P&L.
6. Every populated confirmation calendar year has positive aggregate net P&L.
7. No single month contributes more than 40% of total positive P&L.
8. Exact-contract label coverage and all missing-data exclusions are reported.
9. Any structure/horizon cell contributing at least 10% of confirmation trades must have at
   least 80% unconditional exact-contract label coverage. This availability gate was added
   after the label-coverage audit but before any Phase 5 model outcome was computed; it prevents
   the selector from converting 120/180-minute rolling-surface attrition into an apparent edge.

SPAN margin and net return on margin are reported for selected confirmation trades, but are
not used to tune or rescue the strategy.

## Closure rule

Failure means the current dataset does not support an executable strategy based on the tested
IV/RV/VRP information set. A future reopening would require materially new evidence, such as
full-chain quotes, observed bid/ask depth, multiple expiries, a longer non-overlapping sample,
or an independently motivated forecast—not another in-sample threshold variant.
