# Phase 4 — cost-aware VRP structure and horizon discovery

## Decision

The simple VRP-tail hypothesis is rejected as a standalone executable strategy in this
dataset. None of the 48 one-lot cells formed by two signals, eight defined-risk structures,
and 60/120/180-minute exits has positive mean P&L after the corrected cost model.

The quantity-aware capacity test also rejects lot scaling as a rescue. For the upper-85%
short iron condor, the least-negative mean occurs at seven lots (−₹125.69/trade). Mean P&L
remains negative at every integer size from one through 100 lots. This result uses the corrected
60-lot-parity impact sensitivity described below; the retired ten-percent-per-lot ladder is not
used.

The strict multi-day diagnostic does not find a viable extension. The only cell with at least
80% exact-contract coverage is the lower-10% long iron condor held one trading session; it has
249 complete observations, −0.136 gross points/trade, and −₹338.72 net/trade. Longer holds and
the upper-tail short trade have too much non-random contract attrition to support inference.

These are in-sample discovery results, not an out-of-sample strategy claim.

## Research contract

- Underlying: NIFTY index options.
- Expiry: nearest weekly expiry at entry.
- Entry surface: frozen entry contracts drawn from ATM ±3.
- Signals: first daily lower-10% downward VRP-percentile crossing and first daily upper-85%
  upward crossing.
- Structures: long/short iron condors, long/short iron flies, long call/put butterflies, bull
  call spreads, and bear put spreads.
- Intraday exits: fixed 60, 120, and 180 minutes, exact entry contracts.
- Sizing: exactly one historical exchange lot per trade; the lot size is date-specific
  (25, 50, 65, or 75 units in this sample).
- Margin: joined timestamp-aware SPAN reconstruction at entry.
- Missing INDIA VIX: same-timestamp ATM IV × 100 is passed to the slippage model.

## Corrected execution model

### Date-aware option STT

Sell-side option STT is selected from the trade date rather than using one rate over the
entire history:

| Effective interval | Option sell STT |
|---|---:|
| Before 2023-04-01 | 0.0500% |
| 2023-04-01 through 2024-09-30 | 0.0625% |
| 2024-10-01 through 2026-03-31 | 0.1000% |
| From 2026-04-01 | 0.1500% |

The current ₹20 per executed F&O order brokerage is retained as the deployable-current
assumption across the historical simulation. This is deliberately distinguished from the
date-regime-correct tax calculation.

### Quantity-aware impact

The recovered historical simulator's ten-percent incremental lot ladder was too aggressive:
it made the deterministic ladder approximately 95% of six-lot impact and brought added impact
up to the entire base-slippage bill near 20 lots. It is retired.

The replacement is an auditable assumption-driven sensitivity anchored so the deterministic
ladder alone equals one base-slippage unit at 60 lots. Minute volume and OI enter separately,
using only submitted quantity above the first lot already represented by base slippage:

```text
incremental_quantity = max(quantity - lot_size, 0)
ladder_ratio = (lots - 1) / (60 - 1)
volume_ratio = sqrt(min(incremental_quantity / max(volume, 1), 1))
oi_ratio = sqrt(min(incremental_quantity / max(open_interest, 1), 1))
impact_per_unit = base_slippage * (ladder_ratio + volume_ratio + oi_ratio)
```

The first lot has zero added participation impact but still pays the calibrated base
slippage. A structure leg with a two-lot weight, such as the butterfly centre leg, correctly
starts above that one-contract-lot baseline. This overlay is explicitly **not fill-calibrated**
and is used only as a capacity sensitivity, never presented as fitted market truth.

For the upper-85% short iron fly, six lots now add ₹60.68 to the ₹448.07 scaled base-slippage
bill: a 13.5% uplift rather than 26.4%. The ladder contributes 62.6% and observed volume/OI
participation contributes 37.4%, replacing the flagged 94.7%/5.3% split. Added impact reaches
approximately the full base bill at 50 lots (₹3,693 versus ₹3,734) and exceeds it at 60 lots,
which places effective parity inside the intended 50–70-lot range.

## Why the four-leg P&L is small

The condor arithmetic reconciles exactly. The small result is economic, not a sign error:

- At the upper-85% signal, the four legs move by 38.70 points in summed absolute terms over
  60 minutes, but the net structure moves only 4.91 points in absolute terms.
- The average cancellation is 76.14% and the median cancellation is 84.49%.
- In the original zero-cross sample, average cancellation is 88.04% and median cancellation
  is 94.38%.
- Same-side wing and inner-option changes are strongly correlated, so the long wings remove
  much of the short options' vega and theta exposure.
- At roughly 6–20 DTE, 60 minutes is a short decay window. A VRP state variable therefore
  need not translate into a large four-leg mark-to-market move.

For the upper-85% 60-minute short condor, the average entry diagnostics are:

| Metric | Mean |
|---|---:|
| Entry credit | 45.386 points |
| Width | 100 points |
| Theoretical max loss | 54.614 points |
| Delta | −0.1666 per unit |
| Gamma | −0.004516 per unit |
| Theta | +2.071 points/day |
| Vega | −6.137 points/vol point |
| Gross P&L | +1.808 points / +₹102.73 |
| Cost hurdle | 5.167 points / ₹272.05 |
| Net P&L | −₹169.32 |
| SPAN margin | ₹55,664.21 |
| Net return on margin | −0.3221% |

The ₹272.05 average bill comprises ₹160 brokerage, ₹65.87 modeled slippage, ₹8.62 dated STT,
and ₹37.56 of GST plus other exchange/regulatory charges. Brokerage alone is charged on eight
executed orders for a four-leg round trip.

## Intraday structure comparison

Values below are mean **net rupees per one-lot trade**. Every cell is negative.

### Lower-10% downward crossing

| Structure | 60m | 120m | 180m |
|---|---:|---:|---:|
| Bear put spread | −165.67 | −180.62 | −244.10 |
| Bull call spread | −129.80 | −113.20 | **−19.22** |
| Long call butterfly | −369.90 | −364.09 | −379.29 |
| Long iron condor | −295.47 | −293.81 | −275.96 |
| Long iron fly | −295.78 | −295.83 | −278.93 |
| Long put butterfly | −354.94 | −361.39 | −355.20 |
| Short iron condor | −331.38 | −333.97 | −342.98 |
| Short iron fly | −359.92 | −360.63 | −366.18 |

The best cell, the 180-minute bull call spread, has 219 observations (75.5% path coverage),
1.976 gross points against a 3.308-point cost hurdle, a 40.6% net win rate, and −₹19.22 mean
net P&L. It is directional and remains negative; it is not evidence of a VRP strategy.

### Upper-85% upward crossing

| Structure | 60m | 120m | 180m |
|---|---:|---:|---:|
| Bear put spread | −212.23 | −254.30 | −227.51 |
| Bull call spread | −162.49 | **−126.46** | **−53.75** |
| Long call butterfly | −159.25 | −164.61 | −241.41 |
| Long iron condor | −374.72 | −380.75 | −308.20 |
| Long iron fly | −421.63 | −422.93 | −363.79 |
| Long put butterfly | **−139.93** | −151.63 | −224.97 |
| Short iron condor | −169.32 | −170.33 | −262.65 |
| Short iron fly | −147.61 | −154.42 | −234.84 |

At 60 minutes, the best gross volatility expressions are the long put butterfly (+2.497
points), short iron fly (+2.416), long call butterfly (+2.365), and short iron condor (+1.808).
Their four-contract round-trip hurdles are about 5.17–5.43 points, so none clears costs.

Longer-horizon coverage declines because the fixed entry contracts leave the rolling surface
or the session ends. For the upper-tail cells, coverage is 100% at 60 minutes, approximately
83% at 120 minutes, and approximately 65% at 180 minutes.

## Capacity curve

The upper-85% 60-minute short-condor curve includes every integer size from one to 100 lots.

| Lots | Mean gross | Mean impact | Mean total cost | Mean net |
|---:|---:|---:|---:|---:|
| 1 | ₹102.73 | ₹0.00 | ₹272.05 | −₹169.32 |
| 2 | ₹205.47 | ₹5.33 | ₹360.63 | −₹155.17 |
| 3 | ₹308.20 | ₹13.25 | ₹451.80 | −₹143.60 |
| 5 | ₹513.67 | ₹37.73 | ₹642.79 | −₹129.12 |
| 7 | ₹719.13 | ₹73.28 | ₹844.83 | **−₹125.69** |
| 10 | ₹1,027.33 | ₹146.61 | ₹1,167.90 | −₹140.56 |
| 20 | ₹2,054.67 | ₹558.17 | ₹2,411.84 | −₹357.17 |
| 50 | ₹5,136.67 | ₹3,272.54 | ₹7,622.79 | −₹2,486.12 |
| 60 | ₹6,164.01 | ₹4,659.57 | ₹9,841.83 | −₹3,677.82 |
| 70 | ₹7,191.34 | ₹6,284.81 | ₹12,298.98 | −₹5,107.64 |
| 100 | ₹10,273.35 | ₹12,579.59 | ₹21,088.95 | −₹10,815.60 |

The old quantity-insensitive arithmetic suggested a formal 11-lot breakeven because fixed
brokerage was amortized. Once participation impact is restored, no tested size breaks even.

## Multi-day feasibility

The exit must match the exact entry expiry, strike, option type, and clock time. No stale
last-quote substitution or synthetic repricing is used.

| Signal / trade | Sessions held | Complete / expected | Coverage | Gross mean | Net mean | Interpretation |
|---|---:|---:|---:|---:|---:|---|
| Lower-10% / long condor | 1 | 249 / 290 | 85.86% | −0.136 pts | −₹338.72 | Eligible diagnostic; fails |
| Lower-10% / long condor | 2 | 181 / 290 | 62.41% | −0.401 pts | −₹355.32 | Biased sensitivity only |
| Lower-10% / long condor | 3 | 140 / 290 | 48.28% | +0.945 pts | −₹299.19 | Biased sensitivity only |
| Lower-10% / long condor | 5 | 46 / 290 | 15.86% | +8.465 pts | +₹133.07 | Severe survivor bias; reject |
| Upper-85% / short condor | 1 | 116 / 272 | 42.65% | −12.544 pts | −₹958.18 | Biased sensitivity only |
| Upper-85% / short condor | 2 | 84 / 272 | 30.88% | −16.664 pts | −₹1,265.56 | Biased sensitivity only |
| Upper-85% / short condor | 3 | 59 / 272 | 21.69% | −18.328 pts | −₹1,336.80 | Biased sensitivity only |
| Upper-85% / short condor | 5 | 34 / 272 | 12.50% | −26.879 pts | −₹1,713.04 | Biased sensitivity only |

The positive five-session lower-tail complete-case result cannot be promoted: 84.1% of the
original events are missing, and attrition is mechanically related to expiry and rolling-chain
membership. Imputing most exits would manufacture the answer rather than verify it.

## What to test next

The evidence says to stop treating a VRP percentile crossing as a complete trade signal.
VRP should instead become one causal feature in a structure-specific expected-P&L model whose
prediction must clear the observed cost hurdle.

The next preregistered discovery should use:

1. **Continuous volatility state:** intraday-normalized ATM IV variance, causal trailing RV
   variance, VRP level, `log(IV / RV)`, and IV-minus-RV divergence.
2. **Dynamics:** 5/15/30-minute slopes and accelerations of IV, RV, VRP, and the IV/RV ratio;
   percentile ranks must be causal and time-of-day/DTE conditioned.
3. **Local surface:** call-put IV skew, ATM±1/±3 slope and curvature, local IV dispersion, and
   each chosen leg's IV relative to the same-timestamp local chain.
4. **Payoff state:** structure credit/debit, width, delta, gamma, theta, vega, DTE, spot return,
   distance-to-strike, base slippage, and predicted total cost.
5. **Targets:** net P&L and net return on SPAN margin separately for verticals, butterflies,
   flies, and condors at 60/120/180 minutes. Do not select a structure after observing its
   realized label.
6. **Acceptance:** walk-forward train/validation/test splits, a locked test set, enough
   non-overlapping entries, positive net mean with a confidence interval above zero, temporal
   stability, and no dependence on low-coverage cells.

The most economical first candidate is a two-leg vertical gated by a directional spot feature
and a volatility-divergence/cost-hurdle model. It halves the four-leg order count and the current
180-minute lower-tail bull-call cell comes closest to break-even. This is a discovery lead, not
a strategy conclusion.

## Reproduce

```powershell
python -m research.phase4.run_cost_aware_discovery `
  --gold-root "<six-slot-v2.1-gold-root>"

python -m research.phase4.run_multiday_vrp_feasibility `
  --gold-root "<six-slot-v2.1-gold-root>"

python -m pytest tests/test_nifty_execution.py `
  tests/test_phase4_discovery.py tests/test_phase4_multiday.py -q
```

Primary evidence:

- `audit/phase4_cost_aware_tradebook.csv` — 11,589 structure/horizon observations.
- `audit/phase4_cost_aware_observations.parquet` — frozen leg-level entry/exit observations.
- `audit/phase4_cost_aware_summary.json` — all 48 cell distributions and coverage.
- `audit/phase4_capacity_curve.csv` — quantity-aware one-through-100-lot curve.
- `audit/phase4_cost_aware_manifest.json` — input/output/code hashes.
- `audit/phase4_multiday_tradebook.csv` — exact-contract complete-case trades.
- `audit/phase4_multiday_coverage.csv` — explicit availability boundary.
- `audit/phase4_multiday_summary.json` — P&L and interpretation gate.
- `audit/phase4_multiday_manifest.json` — input/output/code hashes.

## Limitations

- The option corpus is a rolling nearest-weekly ATM±10 surface, not a full immutable chain.
- Multi-day missingness is non-random and becomes dominant beyond one session.
- There are no observed historical bid/ask quotes; execution is modeled from one-minute close,
  volume, OI, DTE, and VIX/ATM-IV fallback.
- The participation overlay is anchored to a disclosed 60-lot ladder-parity assumption, not a
  fitted impact curve. Order-book or realized-fill data are still required for calibration.
- Current brokerage is applied historically; only option STT is date-regime corrected here.
- The signal and structure comparison is in-sample and multiple-tested.
- No capital pool, portfolio concurrency, dynamic sizing, stop, target, or risk-management rule
  is introduced. Results are one exchange lot per trade type.
