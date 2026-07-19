# Final Phase 2 Hypothesis

## Hypothesis-formulation closeout

**Hypothesis ID:** `H1_VRP_LEVEL_DIRECTION_SHORT_CONDOR_60M`

**Frozen:** 2026-07-18

**Status:** formulated and reproducible; not yet confirmed after costs, margin,
or genuine out-of-sample testing

This is the canonical hypothesis produced by the Phase 2 formulation module.
Earlier notes remain evidence and research history; they do not override this
contract.

## 1. The hypothesis

> The causal **level and direction** of normalized intraday VRP contain
> information about the next 60-minute payoff distribution of a bounded-risk
> nearest-expiry NIFTY iron condor. Specifically, the first daily sustained
> transition of normalized VRP from non-positive to positive favours a short
> ATM +/-1 iron condor protected at ATM +/-3 over its exact inverse, when the
> basket is entered at the next minute and the same contracts are closed 60
> minutes later.

In plain language: implied variance moving above recently realised intraday
variance is the entry state. The hypothesis does not require an optimized tail
threshold, acceleration, or a leverage schedule.

### Null

```text
H0:
E[net 60m return on entry margin | first daily VRP cross-up] <= 0
```

or the short condor does not outperform its exact inverse and matched
non-crossing controls.

### Alternative

```text
H1:
E[net 60m return on entry margin | first daily VRP cross-up] > 0
```

and the short condor outperforms its exact inverse and matched non-crossing
controls.

The confirmation outcome is net return on SPAN-plus-exposure capital. Gross
option points and return on theoretical maximum loss are formulation evidence,
not the final decision variable.

## 2. Why the final hypothesis is deliberately simple

The exploration considered:

- raw positive and negative VRP;
- causal VRP percentiles and distribution tails;
- zero crossings;
- five-, fifteen-, and thirty-minute changes;
- percentile-curve velocity and acceleration;
- short and long condors, iron flies, spreads, and butterflies;
- horizons from 15 to 180 minutes;
- percentile-linked confidence and lot ladders.

Three findings determine the final simplification:

1. Matched-clock unconditional 60-minute VRP is negative in most windows, so
   unconditional option selling is not supported.
2. The first daily sustained cross from non-positive to positive VRP gives the
   cleanest frequent gross short-condor result.
3. Tail percentile, acceleration, and leverage extensions are unstable or fail
   paired-session tests. They add research degrees of freedom without improving
   the causal headline rule.

The final hypothesis therefore uses only:

- **level:** normalized VRP relative to zero;
- **direction:** a sustained upward crossing through zero.

The causal percentile is recorded for explanation and stratification. It does
not decide whether to trade or how many lots to use.

## 3. Research universe

| Dimension | Frozen rule |
|---|---|
| Underlying | NIFTY 50 index options only |
| Expiry | actual nearest eligible weekly required for confirmation |
| Formulation expiry | explicitly labelled nearest-listed-expiry proxy |
| Structure | four-leg defined-risk iron condor |
| Entry legs | every leg within ATM +/-3 |
| Horizon | exactly 60 minutes |
| Entry timing | next minute after the completed signal |
| Latest entry | 14:15 |
| Overnight | prohibited |
| Primary frequency | maximum one first cross-up per session |
| Contract tracking | expiry, strike, and option type frozen at entry |

The structure is:

```text
+1 ATM-3 put
-1 ATM-1 put
-1 ATM+1 call
+1 ATM+3 call
```

The exact inverse long condor reverses every weight and is the primary
directional comparator.

## 4. IV is independently reconstructed

Provider IV is not accepted as the research signal.

At completed minute `t`, synchronized call and put prices at ATM-1, ATM, and
ATM+1 produce parity forwards:

```text
F_t(K) = K + exp(r_t * T_t) * (CE_t(K) - PE_t(K))
F_t    = median valid F_t(K)
```

`T_t` is actual calendar seconds from minute `t` to the expiry proxy, divided
by `365 * 24 * 60 * 60`. ATM call and put IV are independently inverted under
discounted Black-76 using the common forward and ACT/365 clock:

```text
ATM_IV_t = mean(ATM_CE_research_IV_t, ATM_PE_research_IV_t)
```

Every local ATM +/-3 leg is solved under the same forward and expiry clock.
Provider IV is reconciliation-only. Invalid no-arbitrage prices, failed
solutions, missing legs, or unresolved strike ladders fail closed.

The formulation run used a documented 10% continuously compounded rate. The
confirmatory run must freeze the rate input before reading outcomes and include
a reasonable perturbation check.

Rolling-surface IV and fixed-contract IV remain separate:

- rolling IV describes the contemporaneous ATM/local surface;
- fixed-contract IV follows the actual expiry, strike, and option type selected
  at entry.

The latter explains the trade. A later row called ATM+3 cannot substitute for
the original ATM+3 strike.

## 5. RV is built on the same intraday clock

Let one-minute NIFTY spot returns be:

```text
r_i = log(Spot_i / Spot_(i-1))
```

For the exact previous 60 contiguous returns, including the completed return at
`t`:

```text
RV60_integrated_variance_t = sum(r_i^2)
tau_60                     = 60 / 525600 ACT/365 years
RV60_annual_t^2            = sum(r_i^2) / tau_60
RV60_annual_t              = sqrt(RV60_annual_t^2)
```

There is no `252 * 375` scaling in the final signal, no missing-minute bridge,
and no treatment of missing returns as zero. Forward RV is an outcome label
only and never enters the signal.

## 6. Normalized intraday VRP

Expiry IV is projected onto the same 60-minute clock:

```text
IV60_proxy_variance_t = ATM_IV_t^2 * tau_60
RV60_realised_variance_t = sum(r_i^2)

VRP60_integrated_t = ATM_IV_t^2 * tau_60 - sum(r_i^2)
```

For stable storage and ranking, use the equivalent annualized variance-rate
form:

```text
V_t = ATM_IV_t^2 - RV60_annual_t^2
```

`tau_60` is positive and constant, so both forms have identical signs,
percentile ranks, and crossings.

This is a normalized intraday variance-spread proxy, not a pure tradeable
60-minute forward variance quote. Projecting expiry IV onto 60 minutes assumes
a locally flat variance rate over the short horizon. The hypothesis is judged
by actual fixed-contract basket P&L rather than treating the proxy as a
variance swap.

## 7. Level and direction signal

The normalized curve is causally smoothed:

```text
V5_t = median(V_(t-4), ..., V_t)
```

All five observations must be present and contiguous.

The primary event is:

```text
V5_(t-1) <= 0
V5_t > 0
```

Only the first qualifying event of the session is retained. Signal formation
uses information through completed minute `t`. Basket entry occurs at exact
minute `t+1`, eliminating same-minute lookahead.

The causal percentile `q_t` ranks current `V_t` against the same minute on
prior dates only, with at least 60 prior observations. It is recorded as the
level's historical intensity but is not an entry cutoff or sizing rule.

## 8. Primary formulation evidence

The final causal recomputation uses the first daily event, next-minute entry,
and an exact 60-minute fixed-contract exit.

| Event | Structure | Dates | Mean points | Median | Positive | P&L 5th / 95th | Mean max-loss return | Bootstrap 95% |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Cross up | Short condor | 895 | +0.473 | +0.600 | 65.81% | -5.06 / +6.75 | +1.133% | +0.583% to +1.640% |
| Cross up | Exact inverse long | 895 | -0.473 | -0.600 | 33.63% | -6.75 / +5.06 | -1.151% | -2.291% to +0.100% |
| Cross down | Long diagnostic | 684 | +0.189 | -0.350 | 40.06% | -5.90 / +6.89 | -0.465% | -2.180% to +1.313% |

The cross-up short condor has positive gross centre, mean, and a positive
bootstrap interval. Its 5% P&L CVaR is -11.879 points and its max-loss-return
CVaR is -21.41%, so the favourable average does not remove material tail risk.

The cross-down long condor has positive gross point mean but negative median,
negative risk-normalized mean, and an interval spanning zero. It is not part of
the primary alpha claim. It remains a convexity and portfolio-insurance
diagnostic.

All six calendar-year cross-up short-condor point means are non-negative in the
formulation sample, but 2026 is partial and near flat. The historical sequence
has already influenced hypothesis selection and is not pristine OOS evidence.

## 9. Dataset and availability summary

- Full weekly-option corpus: 1,371 dates, 2021-01-01 through 2026-07-15.
- Observed minute states: 512,460.
- Independently solved ATM IV states: 511,932, or 99.90%.
- Complete ATM/-3PE/+3CE surfaces: 511,859, or 99.88%.
- Sixty-minute windows are structurally possible on 1,369 of 1,371 dates; two
  shortened sessions cannot support the horizon.
- Unconditional ATM +/-1/3 60-minute windows: 430,665.
- Entry-eligible four-leg baskets: 430,297, or 99.9146%.
- Exact 60-minute endpoints: 429,703, or 99.7766%.
- Strict one-minute paths: 429,267, or 99.6754%.
- Stale-10 terminal coverage: 430,039; 258 entry-eligible windows still require
  a proxy.
- Complete exact-contract 60-minute structure paths in the causal panel:
  approximately 332,202.

The 60-minute, ATM +/-3 rule is therefore well supported but not complete.
Missingness rises when spot migration moves frozen strikes toward the edge of
the rolling surface, so missing exits are tail-correlated rather than random.

## 10. Matched-clock volatility numbers

Across 429,669 valid 60-minute windows:

| Metric | Result |
|---|---:|
| Median ATM IV | 16.10% |
| Median trailing ACT/365 RV | 18.13% |
| Median normalized variance spread | -0.00481 |
| Mean normalized variance spread | -0.01493 |
| Positive VRP rate | 39.54% |

This rejects unconditional short volatility. The final hypothesis is
conditional on the VRP level changing direction through zero.

The full ATM IV distribution is approximately 9.09% / 15.84% / 32.24% at the
5th / median / 95th percentiles. Trailing 60-minute ACT/365 RV is approximately
9.46% / 18.13% / 39.18%. The local surface has persistent downside skew: the
ATM-3 put carries about +1.00 vol point relative to ATM at the median, while the
ATM+3 call is about -0.32 vol point relative to ATM.

## 11. Horizon choice

Upper-decile gross short-condor diagnostics were:

| Horizon | Windows | Median points | Mean points | Positive |
|---:|---:|---:|---:|---:|
| 15m | 25,919 | +0.70 | +0.526 | 65.05% |
| 60m | 20,569 | +2.15 | +1.460 | 65.37% |
| 120m | 14,806 | +1.55 | +0.124 | 59.13% |
| 180m | 10,017 | +1.35 | -1.285 | 55.27% |

Sixty minutes is fixed because it balances contract observability, signal
response, and tail growth. A 120-minute result may be reported as robustness;
180 minutes is not a primary extension.

## 12. Percentile curve and acceleration research note

The separate curve study contains 410,562 rows across 1,306 sessions and 4,744
first threshold-crossing events.

Pooled upward-cross short-condor mean max-loss returns increased from +1.050%
at the 50th percentile to +1.188% at the 75th and +1.293% at the 90th. That did
not validate leverage:

- paired 75% minus 50% return: -0.617 percentage points over 328 dates;
- paired 90% minus 75% return: -1.128 percentage points over 142 dates;
- deeper crossing was better on only 44-45% of paired dates;
- 5% CVaR worsened from -24.62% to -29.70% to -46.28%.

Acceleration was nearly uncorrelated with next short-condor P&L:

```text
Spearman(VRP percentile, short-condor P&L)       = +0.1381
Spearman(percentile velocity, short-condor P&L)  = +0.0147
Spearman(percentile acceleration, short P&L)     = -0.0012
Spearman(raw VRP acceleration, short P&L)        = +0.0031
```

A fast lower-10% cross-down interaction for the long condor remains an
exploratory convexity observation, not part of H1.

Therefore:

- percentile level may be reported as explanatory intensity;
- acceleration is diagnostic only;
- no percentile-to-lot mapping belongs in the final hypothesis.

## 13. Assumptions

1. The independently reconstructed forward and IV are sufficiently accurate
   for state classification.
2. The nearest-listed-expiry proxy adequately describes formulation evidence,
   while confirmation requires actual expiry identity or an explicit proxy
   conclusion.
3. The previous 60 contiguous one-minute spot returns are a reasonable causal
   realized-variance state variable.
4. The locally flat variance-rate projection is acceptable for constructing a
   signal proxy, not for claiming a variance-swap price.
5. Minute-close marks are adequate for formulation, while confirmation must use
   conservative executable prices.
6. Exact-contract missingness remains visible and receives conservative bounds;
   it is not silently imputed.
7. One first event per session is a defensible non-overlapping primary rule.
8. Historical regulatory and expiry changes may alter the distribution and
   must be explicitly stratified.

## 14. Limitations that travel with the hypothesis

- Dhan rolling `WEEK`, `expiryCode=1` history does not return actual expiry
  identity.
- The source is rolling ATM +/-10, not an absolute-strike full chain.
- Frozen contracts can leave the observable surface as ATM migrates.
- Historical bid/ask quotes are unavailable.
- Gross formulation marks omit taxes, charges, slippage, volume/OI depth,
  staleness penalties, and SPAN capital.
- Historical SPAN arrival timestamps are not proven point in time.
- Expiry IV is not a pure 60-minute forward IV.
- The full historical sample influenced selection; genuine confirmation must be
  prospective or explicitly labelled contaminated walk-forward evidence.
- Special sessions and early closes constrain 60-minute availability.
- A positive gross mean can coexist with severe tail loss and concentration.

## 15. Confirmation and falsification

H1 is promoted only if the frozen next-stage implementation shows:

1. positive mean net return on SPAN-plus-exposure margin;
2. a date-block bootstrap lower 95% bound above zero, otherwise inconclusive;
3. outperformance of the exact inverse, matched positive non-crossing control,
   and unconditional scheduled-entry control;
4. positive results under base and 1.5x conservative slippage;
5. acceptable CVaR, maximum drawdown, date concentration, and capacity;
6. weekly and monthly stability rather than only pooled profitability;
7. credibility across expiry days, structural breaks, and recent regimes;
8. sufficient genuinely prospective event count.

Failure of any economically central gate results in rejection, observation
only, or an explicit insufficient-evidence decision. Parameters are not retuned
inside the confirmation module.

## 16. Reproducible module

The frozen machine contract is:

```text
research/phase2/final_hypothesis.json
```

The complete pipeline stages are:

```text
playable_universe
moneyness_horizon
unconditional_coverage
wide_wing_sensitivity
intraday_surface
volatility_regimes
matched_variance
defined_risk_paths
event_summary
curve_crossings
hypothesis_closeout
manifest
```

Run only the closing stages after their dependencies exist:

```powershell
nifty-hypothesis --config research\phase2\hypothesis_formulation.example.json run `
  --from-stage curve_crossings --through-stage hypothesis_closeout

nifty-hypothesis --config research\phase2\hypothesis_formulation.example.json manifest
```

The closeout output is:

```text
audit/phase2_final_hypothesis_closeout.json
```

The manifest hashes code, the frozen hypothesis contract, calendar and dataset
identity, and every declared JSON/Parquet artifact. Large Parquet evidence stays
local and Git-ignored; code, compact JSON reports, contract, tests, and documents
are GitHub-ready.

## 17. Research-note index

- `PHASE2_PLAYABLE_UNIVERSE.md`: unconditional moneyness/horizon boundary.
- `PHASE2_INTRADAY_VOLATILITY.md`: IV, RV, skew, clocks, and expiry proxy.
- `PHASE2_DEFINED_RISK_VRP.md`: bounded structures across VRP states.
- `PHASE2_VRP_CURVE_CROSSINGS.md`: percentile, change, acceleration, and paired
  leverage falsification.
- `PHASE2_PREREGISTERED_HYPOTHESIS.md`: detailed pre-closeout preregistration
  history.
- `FINAL_HYPOTHESIS.md`: canonical simplified hypothesis and module closure.

## Final decision

The hypothesis-formulation module is closed with one testable claim:

> Use the first sustained upward crossing of normalized 60-minute VRP through
> zero as a causal signal for a next-minute-entered, 60-minute, defined-risk
> short NIFTY iron condor. Treat percentile as context, acceleration as a
> diagnostic, and leverage as a later risk-and-capital decision.

This is a plausible gross formulation result. It is not yet a demonstrated
after-cost trading edge.

## Post-formulation Phase 3 outcome

The subsequent one-lot execution test rejects the frozen first daily zero-cross-up as an
after-cost trading rule: average gross P&L was ₹22.60 per trade versus an average ₹271.14
execution bill. This result does not rewrite the preregistration; it records its economic
falsification under the pinned Groww charges, volume/OI slippage, and timestamp-scheduled SPAN
model. The follow-on upper-tail threshold experiment is reported separately in
`PHASE3_TAIL_PERCENTILE_TEAR_SHEET.md` to keep the post-selected extension explicit.

## Final Phase 5 disposition

Phase 4 subsequently rejects all tested defined-risk structure/horizon cells after costs. Phase
5 performs the final preregistered rescue attempt using causal IV/RV/VRP, ratio, dynamics, local
surface, Greeks, spot-return, and entry-cost features with two-leg verticals and a locked
2025–2026 confirmation period. It loses ₹31,354.53 across 190 non-overlapping trades and fails
six of eight acceptance gates. The hypothesis family is therefore closed for the current rolling
nearest-weekly ATM±10 dataset. See `PHASE5_FINAL_ATTEMPT_RESULTS.md`.

An explicitly post-hoc Phase 6 request then tests tail-to-zero reversals with the requested
top-long/bottom-short condor mapping and its exact inverse. The primary 60-minute mapping loses
₹294.12 per trade, both reversal subgroups fail, and the inverse remains negative at every
aggregate horizon. This does not reopen or change the Phase 5 closure. See
`PHASE6_VRP_REVERSAL_RESULTS.md`.

Phase 7 then reprices every previously tested zero-crossing, 70/75/80/85/90/95
percentile-crossing, and tail-reversal signal at one fixed 180-minute horizon. No requested rule
has positive mean net P&L and zero of 32 requested/inverse cells passes the sample, coverage,
profitability, and bootstrap gates. The only positive mean is the post-hoc inverse top-reversal
subgroup at +₹31.18 per trade, based on 96 trades, 50.26% coverage, and a −₹222.31 to +₹263.74
interval. The source contains 191 such signals: 96 are evaluated and 95 have no exact 180-minute
outcome. Those 95 are explicitly unevaluated rather than assigned a synthetic result.
Longer-horizon work remains a justified direction requiring full fixed-contract chain history;
it cannot be inferred beyond 180 minutes from the rolling ATM±10 archive. See
`PHASE7_180MIN_COMPARISON_RESULTS.md`.

## Module 3 formal closure

The entire hypothesis-testing layer is preserved as
`research/module3_hypothesis_testing/`. Its contracts enumerate the zero-crossing, tail
level/direction, velocity/acceleration, structure/horizon, causal-feature, tail-mean-reversion,
and unified 180-minute hypotheses without rewriting their chronological status. The generated
closeout and manifest bind the Phase 2–7 compact evidence to the exact execution and research
implementations.

Select 180-minute cells have materially higher positive gross means—five requested/inverse cells
exceed ₹65 per trade—but only the low-coverage, 96-trade inverse top-reversal subgroup is slightly
positive after cost. Another 95 of its 191 signals are unevaluated at 180 minutes, so it cannot be
verified properly. It is not credible. The formal conclusion is that, within the currently
observable 60–180-minute intraday window, standalone VRP signals do not mature or become realized
in defined-risk option prices consistently enough to cover one-lot execution costs. Research
beyond 180 minutes remains outside the rolling ATM±10 data boundary rather than being declared
futile.
