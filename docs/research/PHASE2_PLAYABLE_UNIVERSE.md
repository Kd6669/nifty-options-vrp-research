# Phase 2 Unconditional Playable-Universe Audit

## Status

This is a data-boundary audit, not a strategy result. It answers whether a
defined-risk NIFTY structure can be entered at any feasible minute on every
observed date and then tracked to different intraday horizons without silently
changing contracts.

The primary audit covers:

- all 1,371 weekly-option dates from 2021-01-01 through 2026-07-15;
- each date's actual first-to-last observed weekly-option session envelope,
  including Muhurat and shortened sessions;
- every theoretical minute inside the envelope, including missing minutes;
- horizons of 15, 30, 60, 90, 120, 180, 240, and 300 minutes;
- entry offsets ATM-10 through ATM+10;
- defined-risk iron flies and iron condors only.

No date, time, or row-quality subset is used to construct the denominator.
Entry rows that are absent or unusable remain failures in the denominator.

## Reproduction

```powershell
py -3.11 research/phase2/audit_unconditional_moneyness_horizon.py `
  --gold-root "<gold-root>" `
  --output audit/phase2_unconditional_observed_computed.json `
  --output-dir audit `
  --session-mode observed `
  --entry-offset-source computed
```

The detailed start-time matrix is written to
`audit/phase2_unconditional_start_time_matrix_observed_computed.parquet`.

## Contract and moneyness identity

The source is a rolling ATM +/-10 surface. A later row labelled `ATM+3` is not
necessarily the same strike as the contract that was `ATM+3` at entry.

The audit therefore:

1. computes the entry offset from independent spot, recomputed ATM, and strike;
2. freezes `trade_date, actual_expiry_date, strike, option_type`;
3. tracks later quotes only by that exact identity;
4. rejects duplicate exact-contract timestamps;
5. never substitutes a later rolling-moneyness label.

Computed entry moneyness is used because provider-label disagreements are data
labelling failures, not absent tradable strikes. Entries still require a valid
strike ladder, nonnegative price, and no severe anomaly or proven payload
corruption.

## Availability definitions

- **Entry eligible:** all four frozen legs exist and pass the entry checks.
- **Exact endpoint:** all four legs have a quote at exactly `t+h`.
- **Strict path:** all four legs have an uninterrupted one-minute sequence from
  `t` through `t+h`.
- **Stale-N:** an exact endpoint is absent, but each missing leg has a last
  frozen-contract quote no more than N minutes old.
- **Proxy required:** at least one leg still lacks a quote after the 10-minute
  stale allowance.

Stale quotes are valuation sensitivities, not executable target-time quotes.
Their ages must remain attached to the label.

## Every-date feasibility

A horizon is structurally feasible only when a date's observed first-to-last
span is at least that long. This is separate from quote missingness.

| Horizon | Dates with a feasible window | Structurally excluded dates |
|---:|---:|---|
| 15m | 1,371 | none |
| 30m | 1,371 | none |
| 60m | 1,369 | 2022-10-24, 2024-11-01 |
| 90m | 1,367 | 2021-11-04, 2022-10-24, 2023-11-12, 2024-11-01 |
| 120m | 1,366 | the four above, plus 2025-10-21 |
| 180m | 1,366 | the same five dates |
| 240m | 1,365 | the five above, plus 2024-03-02 |
| 300m | 1,365 | the same six dates |

Therefore, if the hypothesis literally requires one eligible trade on every
dataset date, 30 minutes is the maximum horizon. Longer horizons can still be
tested unconditionally over every date on which that horizon is physically
possible, with the structurally ineligible dates reported rather than silently
dropped.

## ATM +/-3 defined-risk structure

The reference structure below is an iron condor with short legs at +/-1 and
long wings at +/-3. All percentages use every theoretical
date-by-start-minute window in the horizon-specific observed-session
denominator.

| Horizon | Dates | Windows | Entry eligible | Exact endpoint | Strict path | Stale <=5m | Stale <=10m | Proxy after 10m |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 15m | 1,371 | 492,360 | 99.9074% | 99.8489% | 99.8257% | 99.8991% | 99.9033% | 20 (0.0041%) |
| 30m | 1,371 | 471,795 | 99.9114% | 99.8264% | 99.7700% | 99.8809% | 99.8908% | 97 (0.0206%) |
| 60m | 1,369 | 430,665 | 99.9146% | 99.7766% | 99.6754% | 99.8458% | 99.8546% | 258 (0.0599%) |
| 90m | 1,367 | 389,653 | 99.9207% | 99.7423% | 99.5537% | 99.8175% | 99.8306% | 351 (0.0901%) |
| 120m | 1,366 | 348,658 | 99.9286% | 99.7212% | 99.4421% | 99.8182% | 99.8319% | 337 (0.0967%) |
| 180m | 1,366 | 266,698 | 99.9396% | 99.5208% | 99.0926% | 99.6734% | 99.7068% | 621 (0.2328%) |
| 240m | 1,365 | 184,782 | 99.9134% | 99.1071% | 98.5074% | 99.3419% | 99.4047% | 940 (0.5087%) |
| 300m | 1,365 | 102,882 | 99.9028% | 98.5955% | 97.6245% | 98.9580% | 99.0475% | 880 (0.8553%) |

The unconditional result is materially different from a single 10:00 anchor:

- exact endpoint coverage remains above 99% through 240 minutes;
- pooled strict-path coverage remains above 99% through 180 minutes;
- a 10-minute stale allowance takes the terminal label above 99% at 300
  minutes, but does not repair the missing MTM path;
- proxy demand remains below 0.1% through 120 minutes, then rises to 0.23%,
  0.51%, and 0.86% at 180, 240, and 300 minutes.

## Start-time dependence

The next table counts ordinary high-support clock buckets. A bucket has at
least 1,300 date observations. `>=99%` means the four-leg ATM +/-3 structure
meets that availability threshold at that specific entry clock.

| Horizon | Clock buckets | Exact >=99% | Path >=99% | Stale <=10m >=99% |
|---:|---:|---:|---:|---:|
| 15m | 360 | 359 | 359 | 359 |
| 30m | 345 | 344 | 344 | 344 |
| 60m | 315 | 314 | 314 | 314 |
| 90m | 285 | 284 | 283 | 284 |
| 120m | 255 | 254 | 253 | 254 |
| 180m | 195 | 194 | 155 | 194 |
| 240m | 135 | 114 | 18 | 134 |
| 300m | 75 | 11 | 0 | 62 |

The repeated low-support clock is 09:15. Entry eligibility there is only about
95.5%, so staleness cannot repair it: the entry itself is absent. For a
research rule intended to start at arbitrary ordinary-session times, 09:15
must be either excluded in advance or retained as a documented missing-entry
state. Starting at 09:16 avoids this systematic first-minute hole.

There is a separate closing-boundary issue:

- 853 dates end at 15:29 and 511 dates contain a 15:30 timestamp;
- on the 511-date group, a horizon ending exactly at 15:30 has only 313/511
  exact four-leg endpoints for ATM +/-3;
- carrying the last frozen-contract quote from 15:29 recovers all 511 within
  five minutes;
- two special sessions permit a 19:00 start for a 15-minute horizon; exact
  coverage is 1/2 and stale coverage is 2/2.

An exact-quote headline should therefore target exits no later than 15:29.
Using the final observed timestamp requires an explicitly labelled stale-close
sensitivity.

## Day-weighted reliability

Pooled percentages can hide bad days. The table below gives the day-weighted
exact-endpoint distribution for ATM +/-3. The denominator contains every
feasible minute on each feasible date.

| Horizon | Median day | 5th-percentile day | Worst day | Dates >=99% | Dates >=95% |
|---:|---:|---:|---:|---:|---:|
| 15m | 100.0000% | 99.7228% | 21.6667% | 1,360/1,371 | 1,368/1,371 |
| 30m | 100.0000% | 99.7110% | 13.9130% | 1,364/1,371 | 1,367/1,371 |
| 60m | 100.0000% | 99.6830% | 0.6349% | 1,357/1,369 | 1,365/1,369 |
| 90m | 100.0000% | 99.6491% | 0.0000% | 1,349/1,367 | 1,358/1,367 |
| 120m | 100.0000% | 99.6082% | 12.1569% | 1,350/1,366 | 1,353/1,366 |
| 180m | 100.0000% | 99.4872% | 8.7179% | 1,331/1,366 | 1,351/1,366 |
| 240m | 100.0000% | 99.2593% | 0.0000% | 1,324/1,365 | 1,337/1,365 |
| 300m | 100.0000% | 98.6667% | 0.0000% | 1,058/1,365 | 1,332/1,365 |

The median is perfect, but the worst dates are not. These failures remain in
the analysis; they must not be removed by choosing "clean days." Results should
be reported both pooled and date-weighted, with a special-session indicator.

## Moneyness-by-horizon boundary

This table gives the largest symmetric wing for a short +/-1 iron condor that
retains at least 99% four-leg coverage across all feasible dates and start
minutes.

| Horizon | Exact endpoint | Strict path | Stale <=5m | Stale <=10m |
|---:|---:|---:|---:|---:|
| 15m | +/-8 | +/-8 | +/-9 | +/-9 |
| 30m | +/-7 | +/-7 | +/-8 | +/-8 |
| 60m | +/-6 | +/-6 | +/-7 | +/-7 |
| 90m | +/-6 | +/-5 | +/-6 | +/-6 |
| 120m | +/-5 | +/-4 | +/-6 | +/-6 |
| 180m | +/-4 | +/-3 | +/-5 | +/-5 |
| 240m | +/-3 | none | +/-3 | +/-4 |
| 300m | none | none | +/-2 | +/-3 |

`None` means that no tested short-1 four-leg condor reaches the 99% threshold
under that quote treatment. It does not mean every terminal value is absent.

The boundary contracts with horizon because frozen strikes leave the rolling
ATM +/-10 source as ATM migrates. For ATM +/-3 at 60 minutes, the rare
six-or-more-step migration bucket has only 70.29% exact coverage, improving to
86.90% with a 10-minute stale allowance. At 300 minutes that bucket has 67.58%
exact and 75.04% stale-10 coverage. Missingness is therefore tail-correlated,
not missing at random.

## What proxying can and cannot add

If every post-entry quote gap were model-filled, terminal-label coverage could
approach the approximately 99.9% entry-eligible rate at every horizon. That
would be a synthetic-data result, not observed coverage.

The primary result should not silently use a model-generated point quote.
For the residual proxy set:

1. preserve the exact-quote result;
2. report stale-1, stale-2, stale-5, and stale-10 sensitivities with quote age;
3. use maximum defined loss as a conservative missing-trade bound;
4. optionally show a no-arbitrage interval or neighbouring-strike
   interpolation as a secondary sensitivity;
5. stratify all proxy cases by ATM migration because the gaps are
   tail-correlated.

Proxying is small enough for sensitivity work through 120 minutes. It should
not be used to promote 240-300 minute path-dependent research into the primary
universe.

## Defensible research boundary

There is no single boundary until the required label is specified:

### Literal every-date hypothesis

- ATM +/-3 defined-risk structures;
- 15- or 30-minute horizon;
- every observed date is structurally feasible;
- 09:15 missing entries remain explicit;
- exact exit no later than 15:29.

### Arbitrary-time, path-dependent primary research

- ATM +/-3 at entry;
- 15-60 minutes for a strict rule where every high-support clock bucket must
  reach at least 99% path coverage;
- 90-120 minutes as a predeclared extension with clock-bucket controls;
- entry no earlier than 09:16;
- target exit no later than 15:29;
- all feasible dates retained, including special sessions.

### Terminal-label extension

- ATM +/-3 through 180 minutes has above-99% exact coverage at essentially all
  ordinary high-support entry clocks except 09:15;
- 240 minutes clears 99% only in the pooled exact result, not uniformly across
  clocks or for the full path;
- 300 minutes requires stale quotes to clear 99% and is sensitivity-only.

The hypothesis should now be developed inside one of these explicit contracts.
Costs, margin, volume/OI depth penalties, staleness penalties, and the existing
slippage model remain later execution layers and are intentionally excluded
from this availability audit.
