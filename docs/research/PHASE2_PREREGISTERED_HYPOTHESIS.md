# Phase 2 Preregistered Hypothesis

> Historical formulation record. The simplified canonical hypothesis and final
> module decision are now frozen in `FINAL_HYPOTHESIS.md` and
> `research/phase2/final_hypothesis.json`.

## Corrected VRP zero-cross-up in nearest-weekly NIFTY options

**Frozen on:** 2026-07-18

**Historical data already examined through:** 2026-07-15

**Headline event:** first sustained negative-to-positive corrected VRP crossing of the day

**Headline structure:** ATM +/-1 short iron condor protected at ATM +/-3

**Headline holding period:** 60 minutes

**Status:** hypothesis selected after exploratory analysis; not yet an
after-cost or pristine out-of-sample result

## 1. The one-sentence hypothesis

> On regular NIFTY option sessions, the first daily sustained crossing of the
> corrected 60-minute VRP signal from non-positive to positive identifies a
> transition into temporarily overpriced short-dated optionality. Selling the
> nearest-weekly ATM +/-1 iron condor protected by ATM +/-3 wings at the next
> executable minute and closing the same frozen contracts 60 minutes later will
> earn positive out-of-sample P&L net of all charges and conservative slippage,
> measured on entry SPAN margin. The conditional return will increase with the
> crossing's causal same-time-of-day VRP percentile and will outperform both
> the exact inverse and matched non-crossing controls.

This is the single headline claim. It is deliberately precise enough to be
wrong.

## 2. Formal null and alternative

Let `R_net_margin` be the 60-minute basket P&L after all entry and exit costs,
divided by entry SPAN-plus-exposure margin.

### Primary null

```text
H0: E[R_net_margin | first daily zero-cross-up] <= 0
```

or the conditional return does not improve as the post-crossing causal VRP
percentile rises and is no better than matched non-crossing controls.

### Primary alternative

```text
H1: E[R_net_margin | first daily zero-cross-up] > 0
```

and the conditional return is greater than both:

1. the return of the exact inverse long iron condor entered at the same event;
2. the short iron condor entered in matched positive-VRP states without a
   crossing.

A predeclared intensity test additionally requires the combined 75-100%
post-crossing percentile group to outperform the combined 25-75% group. The
headline entry rule itself does not impose a percentile cutoff.

The hypothesis is about **net return on deployable capital**, not return on
premium and not frictionless option points.

## 3. Why the hypothesis is plausible

The exploratory evidence suggests four distinct facts:

1. unconditional intraday ACT/365 VRP is usually negative, so continuously
   selling options is not justified;
2. taking only the first sustained zero-cross-up per day produced 895
   historical events before the 14:15 cutoff, with gross median +0.65 option
   point, mean +0.47, and 65.14% positive;
3. the gross crossing response strengthened with post-crossing causal
   percentile: mean P&L rose from +0.17 in the 25-50% bin to +0.23 in 50-75%,
   +1.08 in 75-90%, and +2.08 in 90-100%;
4. the apparently strongest overlapping upper-tail/decreasing cell does not
   survive the first-event-per-day honesty check: it retains a +1.675 median
   and 64.71% positive rate, but its gross mean becomes -0.825 due to rare
   losses. It is therefore not the headline strategy.

Economically, a zero-cross-up marks the transition at which implied variance
moves above trailing realised variance on the matched clock. The causal
percentile measures how exceptional that transition is relative to the same
minute on prior dates, without imposing a post-selected tail threshold.

This rationale can still be false after next-minute execution, costs,
slippage, margin, structural breaks, and non-overlapping sampling.

## 4. Research universe

### Underlying

- NIFTY 50 index options only.
- No BANKNIFTY or other indices in the headline test.

### Expiry

- The actual nearest eligible NIFTY weekly expiry at entry.
- Historical expiry rules must be applied by date, including the Tuesday
  regime effective 2025-09-01 and previous-session holiday adjustment.
- When the weekly expiry is also the monthly expiry, it remains the nearest
  weekly contract for this test; there is no second-expiry or calendar trade.

### Critical expiry-data precondition

The Dhan historical `WEEK`, `expiryCode=1` response does not itself return an
expiry identity. Existing exploratory work used the nearest-listed-expiry
research proxy because its prices and provider IV were internally consistent.

The confirmatory test must therefore do one of the following:

1. repair and prove the actual nearest-weekly contract identity; or
2. retain the proxy and label the entire result as proxy-based rather than
   claiming proven nearest-weekly execution.

Failure to disclose this distinction invalidates the headline conclusion.

### Entry-leg boundary

Every leg must be inside ATM +/-3 at the actual entry minute:

| Leg | Position | Entry offset |
|---|---:|---:|
| Put wing | Buy 1 | ATM -3 |
| Put short | Sell 1 | ATM -1 |
| Call short | Sell 1 | ATM +1 |
| Call wing | Buy 1 | ATM +3 |

- `ATM` is the nearest valid strike on the contemporaneous strike ladder.
- Strikes and expiry are frozen after entry.
- The exit must not follow later rolling moneyness labels.
- The position is bounded-risk on both sides. No naked short option is ever a
  headline trade.

### Holding period

- Exactly 60 elapsed trading minutes from executed basket entry to exit.
- No overnight holding.
- No hold-to-expiry settlement in the headline test.
- Signals may be formed only after 60 contiguous one-minute spot returns exist.
- Latest entry is 14:15 IST, leaving time for a 15:15 exit and operational
  buffer before the ordinary close.

### Frequency

- Maximum one headline trade per session.
- Use the first qualifying primary event of the day.
- If the entry basket cannot be completed conservatively, record a rejected
  signal; do not search for a later replacement event that day in the headline
  policy.

## 5. Signal definition

### 5.0 Why IV and RV are not taken at face value

Neither provider-chain IV nor a generic daily realised-volatility formula is
used directly in the signal.

Provider IV can embed an unknown spot/forward, rate, expiry clock, solver, and
unit convention. In this dataset, the provider's rolling response also omits
the actual expiry identity. It is retained only as a reconciliation field. The
research IV is independently recovered from observed option prices under one
documented forward, maturity, rate, and ACT/365 convention.

Likewise, the usual intraday RV convention
`sqrt(mean(r^2) * 252 * 375)` cannot be compared directly with ACT/365 option
IV. It assumes only exchange trading minutes exist in the annual clock, while
option IV prices calendar time, overnight gaps, weekends, and expiry-event
variance. The two numbers may both be labelled "annualised volatility" while
being on different scales.

The signal therefore has three explicit layers:

1. reconstruct annualised research IV from option prices;
2. calculate trailing one-minute realised variance and convert it to the same
   ACT/365 clock;
3. project both variance rates onto the common 60-minute research horizon and
   subtract them.

The phrase **intraday IV** in this research means a point-in-time option-implied
annualised volatility observed intraday. It is not a separate provider field
and is not created by arbitrarily dividing annual IV by `sqrt(252)`.

### 5.1 Point-in-time ATM IV

At completed minute `t`:

1. take observed CE and PE prices, strikes, independent NIFTY spot context, and
   the actual nearest-weekly expiry timestamp;
2. infer a common synthetic forward from call-put parity at ATM -1, ATM, and
   ATM +1:

   ```text
   F_t(K) = K + exp(r_t * T_t) * (CE_t(K) - PE_t(K))
   F_t    = median valid F_t(K)
   ```

3. set `T_t` to actual calendar seconds from the completed minute to expiry,
   divided by `365 * 24 * 60 * 60`;
4. invert the discounted Black-76 price separately for the ATM call and put
   using the same `F_t`, `T_t`, and predeclared continuously compounded rate
   input;
5. express both solutions as decimal annualised ACT/365 volatilities;
6. define:

   ```text
   ATM_IV_t = mean(ATM_CE_research_IV_t, ATM_PE_research_IV_t)
   ```

The formative analysis used a documented 10% continuously compounded rate.
The confirmatory run must freeze the rate source before outcomes are read and
show that a reasonable rate perturbation does not create the result.

No provider IV value is substituted when the independent inversion fails.

Invalid no-arbitrage prices, failed IV solutions, missing legs, or unresolved
strike ladders fail closed.

Every ATM +/-3 chain leg used for diagnostics is solved under the same forward
and expiry clock. Local relative IV is then measured as:

```text
local_relative_IV_(offset,type,t) = leg_research_IV_(offset,type,t) - ATM_IV_t
```

Two time series are kept separate:

- **rolling-surface IV:** the current ATM or current local offset at every
  minute;
- **fixed-contract IV:** the same expiry, strike, and option type followed from
  entry through exit.

The former describes the contemporaneous surface; the latter explains the
actual structure's repricing. They are never treated as the same series.

### 5.2 Trailing realised volatility

Using only the previous 60 contiguous NIFTY spot one-minute returns, including
the completed return at `t`:

```text
r_i                  = log(Spot_i / Spot_(i-1))
RV60_integrated_t^2  = sum(r_i^2), for the exact previous 60 minutes
tau_60               = 60 / 525600 ACT/365 years
RV60_annual_t^2      = RV60_integrated_t^2 / tau_60
RV60_annual_t        = sqrt(RV60_annual_t^2)
```

This uses observed variance rather than a daily close-to-close estimator and
puts RV on the same ACT/365 annual clock as option IV. There is no
`252 * 375` trading-session annualisation in the headline signal. Missing-minute
runs are not bridged, filled, or treated as zero returns.

No sample-mean subtraction is used inside a 60-minute window; at this frequency
the squared log-return sum is the realised-variance object being estimated.

### 5.3 Corrected VRP signal

First express the annualised IV rate as an equivalent 60-minute implied
standard-deviation proxy:

```text
IV60_proxy_t       = ATM_IV_t * sqrt(tau_60)
IV60_proxy_var_t   = ATM_IV_t^2 * tau_60
RV60_realised_t    = sqrt(sum(r_i^2))
RV60_realised_var_t= sum(r_i^2)
```

The horizon-normalised 60-minute VRP proxy is:

```text
VRP60_integrated_t = IV60_proxy_var_t - RV60_realised_var_t
                   = tau_60 * (ATM_IV_t^2 - RV60_annual_t^2)
```

For storage and percentile ranking, use the equivalent annualised variance-rate
form:

```text
V_t = ATM_IV_t^2 - RV60_annual_t^2
```

Because `tau_60` is a positive constant, `V_t` and `VRP60_integrated_t` have
identical signs, percentile ranks, crossings, and entry events. The integrated
form makes the 60-minute horizon explicit; the annualised form is numerically
easier to audit against the IV and RV components.

- `V_t > 0`: the 60-minute implied-variance proxy exceeds trailing observed
  60-minute realised variance on the matched clock.
- `V_t < 0`: trailing observed realised variance exceeds the implied proxy.

This is a causal intraday variance-spread state, not an exact 60-minute forward
variance swap. A single option expiry cannot isolate pure 60-minute forward
implied variance. Projecting expiry IV onto 60 minutes assumes the local
variance rate is flat over that short horizon; this is stated as an
approximation and later tested through actual structure P&L rather than treated
as a tradeable variance-swap quote.

### 5.4 Causal percentile

Let `q_t` be the empirical percentile of `V_t` against values observed:

- on prior trade dates only;
- at the same minute of day;
- with at least 60 earlier observations.

The primary implementation uses expanding prior history. A rolling 252-session
history is a predeclared robustness test, not a replacement chosen after seeing
results.

### 5.5 VRP direction

```text
Delta15_t = V_t - V_(t-15 minutes)
```

- `Delta15_t < 0`: VRP is decreasing.
- `Delta15_t > 0`: VRP is increasing.

The lag must be exactly 15 contiguous minutes within the same session.

### 5.6 Crossing smoother

Define the five-minute median corrected VRP state:

```text
V5_t = median(V_(t-4), ..., V_t)
```

All five observations must be present and contiguous. The smoother reduces
one-minute sign flicker without using future data.

### 5.7 Primary event: sustained zero-cross-up

The headline event occurs at the first minute `t` of the session satisfying:

```text
V5_(t-1) <= 0
V5_t > 0
```

Record `q_t`, the causal percentile of the unsmoothed `V_t`, as an intensity
measure. It does not gate the primary entry. Results are predeclared in these
post-crossing bins:

```text
0-10%, 10-25%, 25-50%, 50-75%, 75-90%, 90-100%
```

Empty or low-count bins remain visible. The primary test will not move the
percentile boundary to maximise P&L.

## 6. Execution rule and no-lookahead contract

The signal uses the completed minute `t`; it cannot be filled at that same
close.

1. Compute `V_t`, `q_t`, and `Delta15_t` only after minute `t` is complete.
2. At minute `t+1`, recompute the valid ATM ladder and select the +/-1 and +/-3
   entry strikes.
3. Enter all four legs using conservative leg-level executable fills.
4. Freeze expiry, strike, option type, and quantity.
5. Exit those exact contracts 60 minutes after the executed entry.

If any leg is missing, stale beyond the accepted threshold, outside the
observed contract universe, or fails a quality gate, the basket is not assumed
filled.

The exploratory same-minute close-to-close results are only hypothesis-forming
evidence. They are not the confirmatory backtest.

## 7. Cost, slippage, and margin contract

The headline test must use the repository's existing models rather than adding
an arbitrary flat haircut after seeing gross P&L.

### Costs

Apply per-leg and per-side:

- brokerage;
- exchange transaction charges;
- SEBI charges and stamp duty;
- GST on applicable charges;
- option sell-side STT;
- any expiry-day treatment that applies even though the trade exits intraday.

Lot size and contract multiplier must be point-in-time by trade date.

### Slippage

Use the existing volume/OI model with:

- depth penalty;
- staleness penalty;
- liquidity penalty for thin wings;
- worse-side execution for buys and sells;
- fail-closed behaviour when the model cannot produce a defensible fill.

Base, 1.5x, and 2.0x slippage are predeclared execution-decay scenarios.

### Margin and capital

- Use point-in-time SPAN/exposure or the explicitly documented conservative
  margin proxy available at entry.
- Apply expiry-day ELM treatment where applicable.
- Report return on entry margin and peak margin during the 60-minute path.
- Do not report return on collected credit as the capital-efficiency metric.

The hypothesis is tested at one fixed structure unit. Compounding and dynamic
sizing are excluded from the primary inference.

## 8. Event taxonomy to report

Only the first daily zero-cross-up is the headline strategy. The other events
explain the state transition and try to falsify the mechanism.

| Event | Exact definition | Structure evaluated | Role |
|---|---|---|---|
| Zero cross up | five-minute median `V` moves `<=0` to `>0` | Short iron condor | Primary hypothesis |
| Zero-cross percentile bins | post-crossing `q` in fixed bins | Same short condor | Primary intensity test |
| Upper-tail fade | `V>0`, `q>=90%`, `Delta15<0` | Short iron condor | Tail-event falsification |
| Upper-tail expansion | `V>0`, `q>=90%`, `Delta15>0` | Same short condor | Tail-direction diagnostic |
| Enter upper tail | `q_(t-1)<90%`, `q_t>=90%` | Same short condor | Percentile-crossing event study |
| Exit upper tail | `q_(t-1)>=90%`, `q_t<90%` | Same short condor | Persistence/decay study |
| Zero cross down | five-minute median `V` moves `>=0` to `<0` | Long inverse condor | Convex-tail diagnostic |
| Lower-tail state | `q<=10%` split by `Delta15` sign | Long inverse condor | Insurance diagnostic |

For zero-cross events, report results by the causal VRP percentile occupied
after the crossing:

```text
0-10%, 10-25%, 25-50%, 50-75%, 75-90%, 90-100%
```

This tests whether a zero crossing has information beyond the distributional
rank and prevents all crossings from being pooled into one average.

## 9. Comparators

The primary result must be shown against three fixed comparators.

### Comparator A: exact inverse

Enter the long iron condor with every quantity reversed at the identical event
and fills. Before asymmetric costs, its marked P&L must be the exact negative
of the short structure. This is an implementation invariant.

### Comparator B: matched positive non-crossing control

Sample short-condor entries with `V5_t > 0` but no zero-cross-up at `t`, matched
within the same causal percentile bin and on:

- calendar year/regulatory regime;
- 30-minute time-of-day bucket;
- nearest-weekly DTE bucket;
- expiry day versus non-expiry day.

The control answers whether the transition through zero contains information
beyond being in a positive VRP state with ordinary option theta.

### Comparator C: unconditional feasible entry

Enter the same structure at the first feasible time in the same time and DTE
strata without using VRP. This exposes whether the signal adds value over a
simple scheduled short-premium trade.

## 10. Primary outcome measures

Trade-level:

- gross and net P&L in option points and rupees;
- net return on entry and peak margin;
- hit rate;
- average and median win/loss;
- 5th/1st percentile loss and 95% CVaR;
- maximum adverse and favourable excursion over the 60-minute path;
- cost drag as a percentage of gross P&L;
- rejected-signal and incomplete-basket rates.

Portfolio-level with at most one trade per day:

- total and annualised return on allocated capital;
- volatility, Sharpe, and Sortino, with an explicit fat-tail warning;
- maximum drawdown and duration;
- worst day and week;
- weekly and monthly P&L stability;
- concentration of total P&L in the best five trades/dates;
- turnover and capacity before slippage removes the edge.

Mean P&L alone is not sufficient because short-option results can combine a
positive median with rare large losses.

## 11. Historical evaluation and honest OOS status

The full 2021-01-01 through 2026-07-15 dataset was examined while forming this
hypothesis. Consequently, no period inside that range is a pristine untouched
holdout anymore.

Historical evaluation should still be replayed chronologically:

| Period | Role |
|---|---|
| 2021-2023 | mechanism development and diagnostic baseline |
| 2024 | frozen-rule validation and Nov-2024 structural-break analysis |
| 2025-2026-07-15 | post-selection walk-forward confirmation, explicitly labelled contaminated by prior EDA |

A genuine prospective holdout begins after 2026-07-15. The rule, thresholds,
cost assumptions, and code hash should be frozen before reading those outcomes.
The prospective test should run for at least 120 eligible sessions; if fewer
than 50 primary events occur, the conclusion is **insufficient evidence**, not
acceptance.

Percentile histories may update with prior observations during walk-forward
testing, but no threshold or structure parameter may update based on realised
strategy P&L.

## 12. Promotion gates

The headline hypothesis is promoted only if all of the following hold:

1. prospective or properly labelled walk-forward mean net return on entry
   margin is positive;
2. a date-block bootstrap lower 95% confidence bound for mean net P&L is above
   zero; if sample size is inadequate, the result is inconclusive;
3. the primary zero-cross-up cell beats both the matched positive-VRP
   non-crossing control and the unconditional scheduled-entry control net of
   costs;
4. the exact inverse does not show the same sign after accounting for cost
   asymmetry;
5. base and 1.5x slippage remain positive; 2.0x defines the execution
   break-even boundary;
6. no single date contributes more than 20% of total net P&L and the best five
   dates do not dominate the conclusion;
7. at least 60% of evaluable OOS months are positive, with weekly results and
   low-count months shown rather than hidden;
8. expiry-day, event-day, pre/post-Nov-2024, and pre/post-Sep-2025 results do not
   reveal that the aggregate is entirely one obsolete regime;
9. the worst loss, CVaR, maximum drawdown, and margin path permit a credible
   small-desk size and kill-switch.

These gates are intentionally harder than “gross mean P&L is positive.”

## 13. Falsification and kill conditions

The hypothesis is rejected or shelved if any primary condition occurs:

- mean net P&L or return on margin is non-positive;
- the confidence interval includes an economically material loss;
- the zero-cross-up does not beat the matched positive-VRP non-crossing
  control;
- plausible slippage or charges consume the result;
- profitability exists only in overlapping minute-grid rows and disappears
  with one first event per day;
- the result depends on same-minute entry, rolling-strike substitution, stale
  last quotes, or the unresolved expiry proxy;
- positive mean comes from a handful of left- or right-tail dates while median,
  CVaR, and monthly stability are poor;
- the edge disappears under post-2024 or post-Tuesday-expiry market structure;
- exact-contract availability or liquidity rejects too many real signals to
  implement the rule;
- prospective evidence produces fewer than the minimum event count.

An honest “inconclusive” or “shelved” decision satisfies the research brief if
the reason is demonstrated.

## 14. Predeclared robustness matrix

These variations test fragility. They do not replace the headline rule.

| Dimension | Primary | Robustness only |
|---|---|---|
| Crossing percentile | no entry cutoff; fixed reporting bins | combined 75-100 versus 25-75 intensity contrast |
| VRP tail and slope | not an entry filter | 85th, 90th, 95th tails crossed with 5-, 15-, and 30-minute slopes |
| Zero-cross smoothing | 5-minute median | 1-minute raw, 10-minute median |
| Structure | short +/-1, long +/-3 iron condor | short iron fly with +/-3 wings |
| Holding period | 60 minutes | no alternative promoted; exit-delay sensitivity only |
| Percentile history | expanding prior dates | trailing 252 sessions |
| Entry delay | next minute | two-minute delay |
| Slippage | model base | 1.5x, 2.0x |
| DTE | all eligible nearest-weekly | 0-0.5, 0.5-1.5, 1.5-3.5, 3.5-7 days |
| Regime | pooled | yearly, expiry day, events, regulatory breaks |

Secondary cells must be labelled exploratory or multiplicity-adjusted. A
better-performing robustness cell cannot silently become the headline result.

## 15. “Why this might be fake” before testing

The strongest arguments against the hypothesis are:

1. **Selection after full-history EDA.** The zero-cross-up state was selected
   after observing gross historical behaviour, so the existing history is not
   a pristine holdout.
2. **Same-bar evidence.** Exploratory marks used minute closes; next-minute
   execution may remove much of the effect.
3. **No historical bid/ask.** Volume/OI and staleness penalties remain models,
   particularly for ATM +/-3 wings.
4. **Expiry identity.** The observed Dhan payload does not prove the actual
   contract expiry.
5. **Overlapping observations.** Hundreds of thousands of minute-grid windows
   represent far fewer independent opportunities.
6. **Tail concentration and a failed attractive story.** The exploratory
   upper-tail fade had a positive median and hit rate but a negative first-event
   gross mean. That result warns that apparent minute-grid regularity can be
   overwhelmed by rare losses.
7. **VRP mismatch.** Expiry IV is not pure 60-minute forward implied variance.
8. **Structural breaks.** Weekly rationalisation, lot-size changes, Tuesday
   expiry, taxes, and margin changes may make older observations irrelevant.
9. **Structure geometry.** P&L may be driven by spot pinning, skew migration, or
   theta rather than a harvestable variance premium.
10. **Capacity.** Four-leg fills can lose the edge faster than a theoretical
    mark suggests.

The test should attempt to establish which of these explanations accounts for
the gross pattern.

## 16. Expected directional results, frozen before confirmation

The mechanism predicts:

1. the first daily zero-cross-up gives the short condor positive net mean P&L
   and net return on entry margin;
2. the zero-cross-up cell outperforms both the matched positive-VRP
   non-crossing control and the unconditional scheduled-entry control;
3. the effect is stronger in the combined post-crossing 75-100 percentile band
   than in the 25-75 band, without using percentile as an entry cutoff;
4. upper-tail fade may retain a positive median while failing on net mean after
   rare losses; it cannot be promoted unless that tail result reverses in
   properly independent OOS evidence;
5. zero-cross-down and lower-tail/decreasing states may give the inverse long
   condor positive mean through rare convex payouts, but not necessarily
   positive median or hit rate;
6. fixed-contract wing IV can rise even when rolling ATM IV falls, so structure
   P&L will be more strongly related to absolute spot movement and inner/wing
   repricing than to ATM IV change alone;
7. after costs, the edge will be smaller and may disappear; this is a valid
   falsification outcome.

## 17. The research decision this hypothesis enables

### Post-formulation VRP-curve extension

The causal VRP-percentile curve, its exact-lag velocity, and its acceleration
are retained as secondary research features. The associated extension is
documented in `docs/research/PHASE2_VRP_CURVE_CROSSINGS.md`.

The pooled gross short-condor mean increases across first upward 50%, 75%, and
90% percentile crossings. A paired same-session test does not show that the
deeper crossing improves return, acceleration adds almost no general
correlation, and upper-tail CVaR worsens. Therefore percentile is a confidence
label but **not a lot multiplier** in the frozen headline rule.

The only acceleration interaction retained for further observation is a first
downward crossing into the lower 10% tail with top-quartile causal acceleration
toward that tail, tested with the long iron condor. It remains exploratory and
cannot alter the headline trade or sizing without independent net-cost OOS
confirmation.

The next phase is no longer “find an attractive VRP chart.” It is:

> Determine whether the first daily sustained crossing of normalized intraday
> VRP from non-positive to positive can pay for a conservatively executed ATM
> +/-1/3 nearest-weekly NIFTY iron condor over 60 minutes, on SPAN capital,
> without relying on overlapping entries, stale quotes, one regulatory era, or
> a handful of tail dates.

Only after that test can the project make the task's required decision:
trade it at a constrained size with a kill-switch, continue observing it, or
shelve it.
