# Phase 2 Defined-Risk Structures Across Corrected VRP States

## Decision summary

The corrected ACT/365 VRP signal is useful as a **state variable**, but its
level, crossing direction, and recent slope are not interchangeable.

The main 60-minute findings are:

- a smoothed crossing from negative to positive VRP favours the short iron
  condor and short iron fly on gross marked P&L;
- a crossing from positive to negative gives the inverse long structures a
  positive mean, but a negative median and a sub-41% win rate; this is a
  convex-tail or insurance profile, not a typical-trade edge;
- the strongest gross short-structure state is the upper causal VRP decile
  after VRP has started decreasing, consistent with entering after an extreme
  premium state begins to mean-revert;
- upper-tail short-structure performance weakens after 60 minutes and its mean
  turns negative by 180 minutes even though the median stays positive;
- a debit or a structure named "long" is not automatically long volatility.
  The inverse iron condor/fly are clean long-convexity counterparts, but a long
  butterfly is a centre-pinning payoff and can profit in the same state as a
  short iron condor.

These are frictionless, overlapping-window diagnostics. They do not establish
an after-cost trading edge.

## Research contract

The entry state is based on:

```text
corrected VRP signal = ATM IV^2 - trailing 60-minute ACT/365 RV^2
```

State definitions are:

- `positive` or `negative`: current signal relative to zero;
- `cross_up` or `cross_down`: a sign change in the trailing five-minute median
  signal, reducing one-minute flicker;
- `upper_10` or `lower_10`: causal same-minute-of-day percentile against prior
  dates only, with at least 60 prior observations;
- `increasing` or `decreasing`: current signal minus its exact 15-minute lag.

All structures use strikes frozen at entry. Exit quotes match the exact entry
strike and option type rather than following the later rolling ATM label.
Every entry leg lies inside ATM +/-3:

| Structure | Entry legs and weights |
|---|---|
| Short iron condor | +ATM-3 PE, -ATM-1 PE, -ATM+1 CE, +ATM+3 CE |
| Long iron condor | Exact inverse of short iron condor |
| Short iron fly | +ATM-3 PE, -ATM PE, -ATM CE, +ATM+3 CE |
| Long iron fly | Exact inverse of short iron fly |
| Bull call spread | +ATM+1 CE, -ATM+3 CE |
| Bear put spread | +ATM-1 PE, -ATM-3 PE |
| Long call butterfly | +ATM-3 CE, -2 ATM CE, +ATM+3 CE |
| Long put butterfly | +ATM-3 PE, -2 ATM PE, +ATM+3 PE |

Marks are one-minute closes with no bid/ask spread, transaction cost, slippage,
margin, or path-dependent stop. Return on risk uses theoretical maximum expiry
loss at entry; it is not SPAN return on margin.

## Coverage and invariants

- 512,460 underlying state rows across 1,371 dates;
- 410,562 entries with a causal 60-minute VRP percentile;
- 1,642,248 unique entry-horizon rows across 15, 60, 120, and 180 minutes;
- approximately 332,200 complete exact-contract 60-minute marks per four-leg
  structure;
- exact long/short condor P&L cancellation error: zero;
- exact long/short iron-fly P&L cancellation error: zero;
- retained condors with invalid maximum profit or loss: zero.

Exact-contract coverage declines normally with horizon:

| Horizon | Complete short-condor marks |
|---:|---:|
| 15m | 390,808 |
| 60m | 332,202 |
| 120m | 253,891 |
| 180m | 175,449 |

An unavailable exact entry contract is left missing. It is not replaced with a
new rolling-offset contract.

## What happens at a zero crossing

The least-overlapping descriptive view takes only the first crossing of each
type per day.

| Crossing | Days | Median ATM IV | Median trailing RV | Short-condor median | Short-condor mean | Win rate | 5th / 95th P&L | Long-condor median | Long-condor mean | Long win rate |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Down | 718 | 15.64% | 16.48% | +0.40 | -0.264 | 60.59% | -7.35 / +5.86 | -0.40 | +0.264 | 39.28% |
| Up | 901 | 16.17% | 15.42% | +0.65 | +0.466 | 65.15% | -5.00 / +6.65 | -0.65 | -0.466 | 34.74% |

The proposed interpretation "VRP turns positive, therefore long defined-risk
structures may be good" is not supported. A cross-up says IV has moved above
trailing realised variance. Over the following 60 minutes, rolling ATM IV
declines by a median 0.17 vol point and the short condor has both a positive
median and mean.

Cross-down is different. The long condor's positive mean comes from rare large
winners, while its median is -0.40 and it wins only 39.28% of first daily
events. That can be useful as a crash-convexity diagnostic, but not as a
standalone expected-return claim.

Minute-grid crossing results tell the same story:

| Crossing | Structure | Windows | Median P&L | Mean P&L | Win rate | P&L 5th / 95th |
|---|---|---:|---:|---:|---:|---:|
| Down | Short condor | 1,385 | +0.50 | -0.297 | 60.29% | -10.10 / +7.18 |
| Down | Long condor | 1,385 | -0.50 | +0.297 | 39.57% | -7.18 / +10.10 |
| Up | Short condor | 1,720 | +0.60 | +0.292 | 64.42% | -5.85 / +7.31 |
| Up | Long condor | 1,720 | -0.60 | -0.292 | 35.35% | -7.31 / +5.85 |

## VRP tails and direction

The 60-minute upper decile is materially different from a mere positive
reading.

| Causal VRP state | Short-condor windows | Median P&L | Mean P&L | Win rate | P&L 5th / 95th | Long-condor median | Long-condor mean |
|---|---:|---:|---:|---:|---:|---:|---:|
| Lower 10%, decreasing | 11,924 | +0.30 | -0.039 | 57.04% | -4.70 / +4.80 | -0.30 | +0.039 |
| Lower 10%, increasing | 10,296 | +0.30 | +0.177 | 57.16% | -4.05 / +4.05 | -0.30 | -0.177 |
| Upper 10%, decreasing | 6,404 | +2.85 | +2.409 | 66.79% | -23.50 / +22.55 | -2.85 | -2.409 |
| Upper 10%, increasing | 12,825 | +1.90 | +1.029 | 63.98% | -20.89 / +17.60 | -1.90 | -1.029 |

The upper-tail/decreasing cell is strongest on gross centre and mean, but its
5th-percentile loss is also large. This is exactly where the later depth,
staleness, slippage, cost, and margin models matter most.

The lower-tail/decreasing long condor again has positive mean but negative
median and only a 42.50% win rate. It behaves like convex insurance, not a
high-frequency long-volatility edge.

## Horizon path

For the upper VRP decile:

| Horizon | Short-condor windows | Median P&L | Mean P&L | Win rate |
|---:|---:|---:|---:|---:|
| 15m | 25,919 | +0.70 | +0.526 | 65.05% |
| 60m | 20,569 | +2.15 | +1.460 | 65.37% |
| 120m | 14,806 | +1.55 | +0.124 | 59.13% |
| 180m | 10,017 | +1.35 | -1.285 | 55.27% |

The centre stays positive longer than the mean because rare adverse moves grow
with horizon. The current evidence supports 60 minutes as the main diagnostic,
120 minutes as robustness, and rejects 180 minutes as the primary short-vol
holding period.

## ATM IV and RV through time

The full ATM IV distribution is 9.09% / 15.84% / 32.24% at the 5th, median,
and 95th percentiles. The corresponding trailing 60-minute ACT/365 RV values
are 9.46% / 18.13% / 39.18%.

| Year | ATM IV 5th | ATM IV median | ATM IV 95th | Trailing RV median | Median VRP variance | Positive VRP rate |
|---:|---:|---:|---:|---:|---:|---:|
| 2021 | 11.57% | 18.71% | 32.47% | 21.49% | -0.0090 | 36.37% |
| 2022 | 12.38% | 20.92% | 37.99% | 21.56% | -0.0015 | 48.04% |
| 2023 | 8.90% | 12.29% | 24.10% | 14.72% | -0.0044 | 35.71% |
| 2024 | 10.28% | 15.16% | 31.09% | 18.23% | -0.0070 | 35.95% |
| 2025 | 7.65% | 13.29% | 24.90% | 15.78% | -0.0069 | 30.56% |
| 2026 through July 15 | 8.91% | 16.59% | 36.17% | 20.01% | -0.0100 | 30.38% |

This non-stationarity is why causal same-time-of-day ranks are preferable to a
single full-sample IV or VRP threshold.

## Local ATM +/-3 IV chain

The local-chain artifact contains 7,167,177 leg observations and 7,144,738
successful research IV solves. Under the nearest-expiry proxy it contains
10,126 strike/type contracts, with a median 623 observations per contract.

Across proxy contracts:

- 5th / median / 95th percentile of each contract's median IV:
  9.72% / 17.07% / 39.92%;
- median within-contract 5th-to-95th IV width: 9.52 vol points;
- 90th percentile of that lifetime width: 25.89 vol points.

The pooled local relative-IV curve is:

| Offset | CE median IV | CE relative to ATM | PE median IV | PE relative to ATM |
|---:|---:|---:|---:|---:|
| -3 | 17.50% | +1.00 vol pt | 17.59% | +1.00 vol pt |
| -2 | 16.99% | +0.61 vol pt | 17.04% | +0.61 vol pt |
| -1 | 16.40% | +0.27 vol pt | 16.42% | +0.27 vol pt |
| 0 | 15.84% | 0.00 | 15.84% | 0.00 |
| +1 | 15.98% | -0.17 vol pt | 15.96% | -0.16 vol pt |
| +2 | 16.16% | -0.27 vol pt | 16.11% | -0.26 vol pt |
| +3 | 16.17% | -0.32 vol pt | 16.09% | -0.31 vol pt |

This confirms a persistent downside skew, while also showing that local
relative IV is not static across years. For example, ATM-3 put relative IV
falls from a median +1.61 vol points in 2021 to +0.58 in 2026 year-to-date.

For the same local contract while it remains observable inside ATM +/-3,
median 60-minute IV changes range from approximately -0.13 to -0.21 vol
points, while the 90th percentile absolute change is approximately 2.2 to 2.7
vol points. A fixed contract is therefore much noisier than the pooled rolling
ATM series.

## Rolling ATM IV versus fixed-leg IV

The 60-minute state-conditioned medians are:

| State | Rolling ATM IV change | Inner +/-1 fixed-leg IV change | +/-3 wing fixed-leg IV change | Short-condor median P&L |
|---|---:|---:|---:|---:|
| Cross down | -0.18 vol pt | -0.10 vol pt | -0.01 vol pt | +0.50 |
| Cross up | -0.18 vol pt | -0.10 vol pt | -0.02 vol pt | +0.60 |
| Lower VRP decile | -0.38 vol pt | -0.30 vol pt | -0.28 vol pt | +0.30 |
| Upper VRP decile | -0.21 vol pt | +0.54 vol pt | +2.52 vol pt | +2.15 |

The upper-tail row demonstrates why rolling ATM IV and fixed-contract IV must
not be conflated. ATM IV declines, while the frozen wings reprice higher in IV
as moneyness and local skew migrate. Structure P&L still improves because it is
the joint result of spot movement, theta, inner-versus-wing repricing, and the
bounded payoff geometry.

Across the upper decile, short-condor P&L has correlations of -0.35 with ATM IV
change and -0.65 with absolute spot return. The VRP state is a conditioning
variable; it does not replace the structure's actual path exposures.

## The predeclared DTE/time boundary is sparse

Inside DTE 0.5-3.5 and 10:45-11:45, the complete 60-minute minute grid contains:

- 2,926 lower-tail windows;
- 683 upper-tail windows.

Taking only the first qualifying event per day leaves 102 lower-tail days but
only 25 upper-tail days. The upper-tail first-event short condor has median
+0.20 points, mean +0.314, and a 52% win rate. Twenty-five independent days are
not enough to promote the cell.

The broad full-day upper-tail result is useful for forming a hypothesis, but
the narrow trading-boundary result is currently a low-count candidate that
must be tested with a predeclared non-overlapping entry rule and OOS split.

## Refined hypotheses

### H1: upper-tail short bounded volatility

When corrected VRP is in its upper causal decile, a 60-minute short iron
condor or short iron fly should outperform its exact inverse before costs. The
decreasing-VRP substate is the primary cell; increasing VRP is the robustness
cell.

### H2: cross-up short bounded volatility

The first daily transition from negative to positive smoothed VRP should favour
the 60-minute short condor over the long condor. This is a weaker but more
frequent event definition than the upper decile.

### H3: cross-down long convexity as insurance, not alpha

Long inverse condors and flies after a cross-down should be evaluated by tail
payoff, conditional expected shortfall, and portfolio protection—not by win
rate. The current positive mean with negative median is consistent with rare
convex payouts and does not support a standalone long-volatility strategy.

## Required next stage

Before accepting any hypothesis:

1. freeze a non-overlapping entry policy and IS/validation/OOS dates;
2. add the existing volume/OI depth, staleness, and slippage model;
3. add transaction charges and taxes;
4. add SPAN and capital usage rather than theoretical max-loss normalization;
5. report weekly and monthly stability, tail loss, concentration, and expiry-day
   results separately;
6. retain exact-contract missing marks as missing and perturb last-quote
   staleness explicitly.

## Limitations

- The Dhan `WEEK`, `expiryCode=1` payload does not expose actual expiry. Contract
  histories use the nearest-listed-expiry research proxy.
- Minute-grid windows overlap and are descriptive, not independent trades.
- One-minute closes are not executable bid/ask marks.
- Costs, slippage, staleness penalties, margin, and path exits are absent.
- Exact-strike exits are observed only while the frozen contract remains inside
  the wider rolling quote universe; no synthetic replacement is made.
- A debit structure, a structure named "long," and a long-volatility exposure
  are not synonyms.
