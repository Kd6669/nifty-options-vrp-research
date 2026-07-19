# Phase 2 Intraday NIFTY Volatility Research Note

## Decision summary

The data supports a focused hypothesis test, but it does not yet support a
clean claim about the official contract expiry.

The defensible strategy-research boundary is:

- NIFTY index options only;
- one rolling Dhan `WEEK`, `expiryCode=1` surface;
- defined-risk structures only;
- every leg inside ATM +/-3 at entry;
- intraday entry and exit, with no multi-expiry, calendar, or multi-day trade;
- 60 minutes as the primary holding horizon;
- 120 minutes as robustness and 180 minutes as the maximum data boundary.

The proposed headline test is a conditional short ATM +/-3 iron condor. It is
not yet a trading result. Costs, slippage, margin, path-dependent exits, and
out-of-sample P&L remain to be applied.

## Critical expiry-identity finding

The Dhan rolling response does not return an actual expiry. The audited gold
layer maps `expiryCode=1` to the second eligible weekly contract, producing a
6.0-21.1 calendar-day DTE range and solved ATM IVs that are inconsistent with
the observed premiums:

| Expiry clock | Observations | Median ATM IV | MAE vs provider ATM IV | Correlation |
|---|---:|---:|---:|---:|
| Audited second-weekly mapping | 512,123 | 6.25% | 10.37 vol points | 0.349 |
| Nearest-listed-expiry proxy | 511,932 | 15.84% | 0.66 vol points | 0.904 |

The nearest-expiry proxy includes the monthly NIFTY expiry when it is the
nearest listed contract. Its inferred DTE distribution is 0.01 days at the
1st percentile, 2.14 days at the median, and 6.25 days at the 99th percentile.

This is strong semantic evidence that the historical prices behave like the
nearest listed expiry, but it is not a provider-returned contract identity.
Accordingly:

- the availability boundary can be finalized at ATM +/-3 and <=180 minutes;
- the IV/RV/VRP analysis below is a labelled research approximation;
- the final published strategy result must either repair the upstream expiry
  identity or carry this approximation as a central limitation;
- the scope should be called **single nearest-listed-expiry proxy**, not
  definitively “nearest weekly,” until the contract identity is proven.

## Method

### IV surface

The study does not use provider IV as the research value. For every minute:

1. infer a common synthetic forward from call-put parity at ATM-1, ATM, and
   ATM+1;
2. use the nearest proven NSE expiry timestamp as the research maturity proxy;
3. solve parity-adjusted Black-76 IV independently for calls and puts;
4. define ATM IV as the mean ATM CE/PE IV;
5. define the downside wing as ATM-3 PE and upside wing as ATM+3 CE.

The resulting ATM CE-minus-PE IV gap has a median indistinguishable from zero
and a 5th-95th percentile range of approximately -0.11 to +0.11 vol points.
That is materially more internally consistent than the audited spot-BSM clock.

### RV and VRP proxies

One-minute spot log returns are annualized using 252 sessions and 375 minutes
per ordinary session.

```text
RV_h = sqrt(mean(next h one-minute squared log returns) * 252 * 375)
```

Two variance spreads are retained:

```text
signal VRP_h = ATM_IV(t)^2 - trailing_RV_h(t)^2
outcome VRP_h = ATM_IV(t)^2 - forward_RV_h(t,t+h)^2
```

Forward RV is outcome-only. It is never used to construct the entry signal.
Intraday causal percentiles rank the current value against prior dates at the
same minute of day, with at least 60 prior observations. Daily percentiles rank
against prior dates only; a daily value must be lagged before it can be used as
a next-day signal.

This is an **intraday variance-spread proxy**, not an expiry-matched variance
swap VRP. Option IV includes overnight, gap, and expiry-event variance that is
absent from same-session RV. The level difference is therefore not itself a
tradable edge.

## Coverage

- 1,371 dates from 2021-01-01 through 2026-07-15;
- 512,460 observed minute rows;
- 511,932 ATM IV rows, or 99.90%;
- 511,859 complete ATM/-3PE/+3CE surfaces, or 99.88%;
- 456,094 rows with India VIX context;
- special sessions remain in daily results; ordinary time-of-day results are
  reported separately.

## Intraday IV and skew

| Metric | 5th percentile | Median | 95th percentile |
|---|---:|---:|---:|
| ATM IV | 9.09% | 15.84% | 32.24% |
| ATM CE IV | 9.09% | 15.84% | 32.22% |
| ATM PE IV | 9.09% | 15.84% | 32.22% |
| ATM-3 PE IV | 9.88% | 17.59% | 41.57% |
| ATM+3 CE IV | 9.09% | 16.17% | 38.33% |
| Put skew: ATM-3 PE minus ATM | +0.22 vol pt | +1.00 vol pt | +12.69 vol pt |
| Call skew: ATM+3 CE minus ATM | -1.45 vol pt | -0.32 vol pt | +10.83 vol pt |
| Risk reversal: put wing minus call wing | -1.34 vol pt | +1.26 vol pt | +5.61 vol pt |
| Smile curvature | -0.02 vol pt | +0.24 vol pt | +11.22 vol pt |

The ordinary surface therefore has a persistent downside skew: the ATM-3 put
usually carries more IV than both ATM and the symmetric ATM+3 call. The very
large upper-tail skew and curvature values require separate microstructure and
event checks before they can become trading signals.

## Clock-matched intraday RV

The initial descriptive RV used the standard intraday convention
`sqrt(mean(r^2) * 252 * 375)`. That is useful for comparing intraday activity
across days, but it is not on the same clock as ACT/365 option IV. For an
intraday interval, ACT/365 RV is 2.3584 times the session-clock RV.

After correcting the clock, the unconditional intraday result reverses:

| Horizon | Windows | Median ATM IV | Median ACT/365 RV | Median variance spread | Positive rate |
|---:|---:|---:|---:|---:|---:|
| 15m | 491,462 | 15.93% | 17.81% | -0.0041 | 42.10% |
| 30m | 470,857 | 16.03% | 18.23% | -0.0050 | 39.84% |
| 60m | 429,669 | 16.10% | 18.13% | -0.0048 | 39.54% |
| 90m | 388,735 | 16.14% | 17.97% | -0.0044 | 39.80% |
| 120m | 347,815 | 16.18% | 17.86% | -0.0042 | 39.89% |
| 180m | 265,992 | 16.27% | 17.85% | -0.0041 | 39.45% |

An unconditional intraday short-volatility hypothesis is therefore not
supported by a fair clock comparison.

## Time-of-day behaviour on ACT/365

For the 60-minute horizon:

| Entry bucket | Median ATM IV | Median ACT/365 RV | Median variance spread | Positive rate |
|---|---:|---:|---:|---:|
| 09:15 | 16.83% | 23.90% | -0.0239 | 18.04% |
| 10:15 | 16.34% | 17.36% | -0.0024 | 43.74% |
| 10:45 | 16.13% | 16.25% | +0.0003 | 50.83% |
| 11:15 | 16.01% | 15.88% | +0.0012 | 53.36% |
| 11:45 | 15.96% | 15.83% | +0.0009 | 52.56% |
| 12:15 | 15.88% | 16.41% | -0.0008 | 47.79% |
| 13:15 | 15.83% | 18.40% | -0.0067 | 35.27% |
| 14:15 | 15.63% | 20.59% | -0.0149 | 23.29% |

The narrow 10:45-11:45 band is the only unconditional clock region with a
slightly positive median spread. The open and late afternoon are realised-vol
dominant.

## Expiry-matched RV including overnight gaps

A second comparison enters at 10:15, freezes the nearest-listed-expiry proxy,
and sums all observed squared spot returns through expiry. It includes the
overnight return between one session's last observation and the next session's
first observation, then annualizes by the actual entry-to-expiry ACT/365 time.

Across 1,363 entries:

- median entry ATM IV: 16.45%;
- median expiry-matched RV: 13.73%;
- median variance spread: +0.0071;
- positive variance-spread rate: 80.26%;
- median overnight share of realised variance: 13.99%;
- 5th-percentile variance spread: -0.0153.

| Proxy DTE | Entries | Median ATM IV | Median expiry RV | Median variance spread | Positive rate | Overnight variance share |
|---|---:|---:|---:|---:|---:|---:|
| 0-0.5d | 289 | 22.24% | 18.74% | +0.0140 | 79.93% | 0.00% |
| 0.5-1.5d | 269 | 16.60% | 13.57% | +0.0087 | 82.53% | 11.38% |
| 1.5-3.5d | 455 | 16.14% | 13.31% | +0.0073 | 83.30% | 20.28% |
| 3.5-7d | 350 | 12.64% | 10.58% | +0.0037 | 74.86% | 25.88% |

This is the fairest available VRP proxy, subject to the unresolved expiry
identity. It shows a positive premium on average, but materially less than the
original session-clock comparison and with a genuine negative tail.

## Standard daily RV

The fixed-10:15 daily view uses standard close-to-close-style returns and
annualizes with 252 sessions:

| Forward sessions | Entries | Median ATM IV | Median RV | Median variance spread | Positive rate |
|---:|---:|---:|---:|---:|---:|
| 1 | 1,362 | 16.45% | 8.61% | +0.0158 | 80.84% |
| 2 | 1,361 | 16.45% | 10.33% | +0.0140 | 80.31% |
| 3 | 1,360 | 16.46% | 11.07% | +0.0130 | 79.71% |
| 5 | 1,358 | 16.48% | 11.48% | +0.0117 | 79.60% |

When maturity is approximately matched to the option proxy, the 5-session cell
has only 54 observations and a 51.85% positive rate. It is not evidence of a
stable five-day premium. The 1-3 session matched cells retain 82-88% positive
rates, but still require proper contract proof and out-of-sample testing.

## Causal signal result

The clock correction makes signal conditioning essential. At 60 minutes, the
causal percentile of `ATM_IV^2 - trailing_ACT365_RV^2` produces:

| Signal quintile | Observations | Median ATM IV | Median forward RV | Median variance spread | Mean variance spread | Positive rate |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 54,968 | 16.52% | 24.55% | -0.0291 | -0.0575 | 10.24% |
| 2 | 75,502 | 13.66% | 17.26% | -0.0097 | -0.0168 | 21.37% |
| 3 | 82,060 | 13.11% | 14.47% | -0.0024 | -0.0072 | 40.33% |
| 4 | 69,489 | 15.76% | 14.13% | +0.0043 | +0.0018 | 65.22% |
| 5 | 50,500 | 24.98% | 19.03% | +0.0245 | +0.0279 | 86.31% |

This monotonic separation is the most credible basis for the next hypothesis.

## Hypotheses worth testing

### H1: conditional short iron condor — primary

> On ordinary sessions, when the nearest-expiry research proxy has DTE between
> 0.5 and 3.5 days, entry time is 10:45-11:45, and the ACT/365 trailing
> 60-minute variance-spread signal is in its top causal time-of-day quintile,
> selling one ATM +/-3 defined-risk iron condor with shorts at +/-1 and wings at
> +/-3 and exiting after 60 minutes earns positive out-of-sample return on SPAN
> margin after all costs and slippage.

This is falsified if the predeclared OOS mean return after costs is <=0, its
confidence interval includes an economically material loss, or the result
fails plausible execution and staleness perturbations.

Predeclared robustness cells are a 120-minute exit, a 180-minute maximum
boundary, the highest risk-reversal quintile included versus suppressed, and
expiry-day results reported separately.

### H2: unconditional short volatility — rejected before backtest

The clock-matched intraday variance spread is negative in roughly 60% of
windows. Selling every day or at every clock is not supported.

### H3: long intraday volatility — diagnostic only

The negative unconditional intraday spread makes a long-volatility diagnostic
reasonable, particularly in the lowest causal signal quintiles. It is not yet
a strategy hypothesis because debit-spread costs, IV repricing, and the
overnight-versus-intraday maturity mismatch can still erase the apparent
advantage.

## Required next test

The next stage should price H1 using frozen exact contracts and the existing
modules for:

- transaction charges and taxes;
- volume/OI depth, staleness, and slippage penalties;
- SPAN/margin capital allocation;
- conservative handling of absent legs;
- non-overlapping entries and predeclared IS/validation/OOS dates.

Until that test is complete, the conclusion is **a plausible and sharply
testable intraday short-volatility hypothesis, not a demonstrated edge**.
