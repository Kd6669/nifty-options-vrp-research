# Phase 2 VRP Curve, Crossing, and Acceleration Analysis

## Decision first

The VRP percentile curve is useful as an **entry-state and confidence feature**,
but the current results do **not** justify a rule of the form “higher crossed
percentile equals more lots.”

The pooled upper-tail table looks monotonic for short iron condors. The stricter
within-session comparison does not: on dates that reached both levels, the
later, deeper crossing had slightly worse average return. Tail loss also grows
with the percentile threshold. Acceleration adds almost no general linear or
rank information, although fast downward acceleration into the lower 10% tail
is worth retaining as an exploratory long-condor interaction.

Accordingly:

- add VRP curve level, velocity, and acceleration to the research feature set;
- retain the upper-cross short-condor and lower-tail long-condor cells as
  explicitly labelled secondary hypotheses;
- do not convert the pooled percentiles into executable lot multipliers;
- retest the fixed thresholds after charges, slippage, SPAN margin, structural
  breaks, and genuinely prospective data.

## 1. Question tested

Can the intraday path of corrected VRP be traded as a curve?

Specifically, when its causal percentile crosses a fixed level, does the
direction, velocity, or acceleration of that crossing predict the next
60-minute P&L of the fixed-contract ATM +/-1/3 iron condor strongly enough to
support:

1. short condors on upward moves into the upper tail;
2. long condors on downward moves into the lower tail; and
3. progressively more confidence or size at more extreme crossings?

## 2. Curve construction

The corrected variance-rate signal remains:

```text
V_t = ATM_IV_t^2 - RV60_annual_t^2
```

IV and RV retain the independently reconstructed, matched-clock definitions in
the preregistered hypothesis. The VRP curve is not made from vendor IV.

For each minute of each session, `V_t` is ranked only against the same minute on
prior dates. This gives the causal percentile curve `q_t`. A trailing smoother
reduces one-minute flicker:

```text
q5_t = median(q_(t-4), ..., q_t)
```

All five observations must be contiguous. Curve change and acceleration use
exact five-minute lags:

```text
velocity_t     = q5_t - q5_(t-5)
acceleration_t = q5_t - 2*q5_(t-5) + q5_(t-10)
```

For downward crossings, both quantities are sign-adjusted so a positive
directional value always means movement or acceleration further in the crossed
direction.

Acceleration itself is also causally ranked against the same minute on prior
dates. The four fixed bins are 0-25%, 25-50%, 50-75%, and 75-100%.

## 3. Crossing and execution contract

The fixed percentile thresholds are 10%, 25%, 50%, 75%, and 90%.

```text
up crossing:   q5_(t-1) < x and q5_t >= x
down crossing: q5_(t-1) > x and q5_t <= x
```

Only the first valid crossing per date, threshold, and direction is retained.
The signal is known at completed minute `t`; the structure path begins at the
exact next minute `t+1` and ends 60 minutes later. Entries after 14:15 are
excluded. The basket strikes are frozen at entry.

This analysis is still frictionless and uses return on theoretical maximum loss
only as a provisional risk normalizer. It does not include charges, slippage,
SPAN margin, or executable lot sizing.

## 4. Coverage and invariants

- Curve panel: 410,562 rows across 1,306 sessions.
- Causally ranked, smoothed curve rows: 405,301.
- First-crossing observations across all thresholds and directions: 4,744.
- Sessions represented in crossing events: 1,282.
- Dates: 2021-03-31 through 2026-07-14.
- Signal-to-entry delay: exactly one minute for every retained event.
- Duplicate date/threshold/direction identities: zero.
- Maximum gross long-plus-short condor P&L inverse error: zero.

Threshold cells are not mutually independent. The same date may cross several
levels, so the threshold results cannot be added together as independent
trades.

## 5. Upper crossings and the short iron condor

### 5.1 Pooled first-crossing results

| Upward threshold | Dates | Mean points | Median points | Positive | Mean return on max loss | Bootstrap 95% | 5% CVaR return |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 50% | 809 | +0.340 | +0.600 | 67.00% | +1.050% | +0.435% to +1.632% | -24.62% |
| 75% | 475 | +0.548 | +0.700 | 62.74% | +1.188% | +0.263% to +2.038% | -29.70% |
| 90% | 181 | +0.628 | +1.750 | 62.98% | +1.293% | -1.250% to +3.688% | -46.28% |

The pooled mean return rises from 50% to 75% to 90%. However, the increment is
small relative to uncertainty, the 90% confidence interval crosses zero, and
the left tail becomes materially worse as the threshold rises.

### 5.2 Paired same-session test

The relevant leverage question is not whether three differently selected
samples have ordered means. It is whether the deeper crossing improves the
trade on dates that actually reached both levels.

| Deeper crossing | Paired dates | Mean return difference | Median difference | Deeper better | Bootstrap 95% |
|---|---:|---:|---:|---:|---:|
| 75% minus 50% | 328 | -0.617 percentage points | -0.231 pp | 45.43% | -2.003 to +0.782 pp |
| 90% minus 75% | 142 | -1.128 percentage points | -0.665 pp | 44.37% | -4.002 to +1.654 pp |

This rejects the current evidence for mechanically adding lots as the same
session crosses deeper upper percentiles. The pooled monotonicity is primarily
a sample-composition result.

### 5.3 Year instability at the 90% upward crossing

| Year | Dates | Mean short-condor points | Mean return on max loss |
|---:|---:|---:|---:|
| 2021 | 23 | +1.217 | +1.363% |
| 2022 | 56 | +1.921 | +2.964% |
| 2023 | 19 | -2.324 | -3.137% |
| 2024 | 28 | -5.441 | -7.492% |
| 2025 | 30 | +2.353 | +3.637% |
| 2026 partial | 25 | +4.164 | +7.878% |

The sign changes are incompatible with an unconditional leverage multiplier.

## 6. Lower crossings and the long iron condor

| Downward threshold | Dates | Mean points | Median points | Positive | Mean return on max loss | Bootstrap 95% | 5% CVaR return |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 50% | 793 | -0.048 | -0.500 | 36.82% | -0.324% | -1.704% to +1.127% | -42.28% |
| 25% | 590 | -0.034 | -0.500 | 36.95% | -0.534% | -2.327% to +1.375% | -47.43% |
| 10% | 290 | +0.356 | -0.400 | 41.03% | +1.013% | -0.851% to +3.232% | -26.97% |

The lower ladder is not monotonic. Only the 10% tail has positive gross mean,
and it still has negative median, low hit rate, and a confidence interval that
crosses zero. That is the signature of a possible convex tail hedge, not a
proven long-condor alpha rule.

Paired return differences also fail to establish a ladder:

- 25% minus 50%: -0.468 percentage points over 394 paired dates, 95% interval
  -3.324 to +2.435 points.
- 10% minus 25%: +0.942 percentage points over 229 paired dates, 95% interval
  -1.079 to +3.210 points.

## 7. Does acceleration add information?

### 7.1 Broad correlations

Across the complete overlapping minute grid:

- smoothed VRP percentile versus next short-condor P&L: Spearman `+0.1381`;
- percentile velocity versus short-condor P&L: `+0.0147`;
- percentile acceleration versus short-condor P&L: `-0.0012`;
- raw VRP acceleration versus short-condor P&L: `+0.0031`.

Within the upper 90% state, percentile acceleration versus short-condor P&L is
`-0.0075`. At the first upward 90% crossing, the causal directional-
acceleration percentile correlation is `-0.0643`.

Acceleration therefore does not act as a general short-condor confidence
multiplier.

### 7.2 Lower-tail exception worth retaining

At the first downward 10% crossing, the acceleration-percentile correlation
with long-condor P&L is `+0.1096`. In the top directional-acceleration quartile:

- 106 dates;
- mean long-condor P&L `+1.284` points;
- median `-0.025` point;
- positive rate `49.06%`;
- mean return on maximum loss `+3.345%`;
- bootstrap 95% interval `-0.393%` to `+7.714%`.

This is directionally interesting but not conclusive. The result remains
right-tail dependent, has small yearly cells, and was negative in 2022.

## 8. Hypothesis extension to retain

### Secondary H2: upper-curve crossing

> A first daily upward crossing of the causal VRP-percentile curve into the
> 50%, 75%, or 90% level predicts positive next-minute-entered, 60-minute short
> iron-condor return, but a deeper crossing is not assumed to justify more lots
> unless its paired same-session incremental net return is positive and stable.

The pooled positive-return portion is supported gross at 50% and 75%. The
incremental leverage portion is currently unsupported.

### Secondary H3: lower-tail convexity interaction

> A first daily downward crossing into the lower 10% VRP percentile, combined
> with a top-quartile causal acceleration toward the lower tail, improves the
> 60-minute long-condor payoff distribution relative to slower lower-tail
> crossings.

This remains exploratory because its mean confidence interval crosses zero and
its median is approximately flat.

## 9. Sizing implication

Percentile can be retained as a **confidence label**, not yet a leverage lever.
No mapping such as 50%=one lot, 75%=two lots, 90%=three lots is supported by the
paired evidence.

When costs and capital models are integrated, any lot rule must be derived from
the smaller of a margin cap and a tail-loss budget:

```text
lots = min(
    floor(margin_budget / stressed_margin_per_lot),
    floor(session_tail_loss_budget / abs(net_CVaR_per_lot))
)
```

The VRP confidence state may reduce this cap or permit entry only after genuine
OOS validation. It may not override the cap.

## 10. Artifacts

- Machine-readable report: `audit/phase2_vrp_curve_crossings.json`
- Complete session-curve panel: `audit/phase2_vrp_session_curve_features.parquet`
- First-crossing event panel: `audit/phase2_vrp_percentile_crossing_events.parquet`
- Reproducible analysis: `research/phase2/analyze_vrp_curve_crossings.py`

All numerical results in this note are gross exploratory marks. They are not an
after-cost or deployable-capital claim.
