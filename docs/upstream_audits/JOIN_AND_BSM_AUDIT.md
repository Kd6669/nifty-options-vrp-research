# Phase 3 Join and BSM Audit

## 2026-07-17 terminal close

The sections below retain the historical pilot/launch record. The authoritative terminal
state is now:

- quality-patched pre-BSM: PASS, 67 months, 43,018,677 rows;
- independent BSM v2.1: PASS, 33,281,564 solved rows with fixed `r=0.10`, `q=0`;
- BOD-only SPAN options gold: PASS, 67 months, 43,018,677 conserved rows;
- static six-slot SPAN base: PASS, 67 months, 43,018,677 conserved rows;
- strict point-in-time SPAN: PASS, 67 months, 43,018,677 rows, zero historical matches;
- six-slot research SPAN: PASS, 67 months, 43,018,677 rows, all unproven timing reference-only;
- SPAN matched/unmatched: 42,718,832 / 299,845;
- duplicate right keys, BOD policy/join-key violations, lineage violations and partials: all zero;
- exact historical SPAN effective time: unproven; strict selection correctly remains empty;
- base integrity audit SHA-256:
  `eae260f0ff37a9abc046a0dfb564d09270fefe6fa56740204e5da41b250c8bb0`;
- strict/research timing terminal audit SHA-256 after 67/67 resume:
  `cfe46cdcb5b561b39c3741fff4bc79df0a80fb18c6967aebee408ff470b0e1c5`.

All historical effective-time evidence is unknown, so ID1-ID4/EOD are retained only in the
explicitly unsafe-for-contemporaneous-input six-slot research representation. BOD v1.4 remains
the conservative fallback. The producer Phase 1 outcome remains `BLOCKED_SOURCE` with 93
explicit source-boundary cells, while the owner release is `ACCEPTED_WITH_SOURCE_GAPS`.
Historical one-minute futures remain a separate proven source blocker. See
`SPAN_GOLD_AUDIT.md` for the final paths, counts, evidence and rebuild command.

## Mandatory pre-BSM quality boundary

BSM cannot consume the completed but ungated pre-BSM `2.0.0` layer. It requires the separate
`2.1.0` quality overlay after all 67 month manifests and `quality_patch_terminal_audit.json`
PASS. Ladder mismatch/missing ATM is provenance evidence and does not alone block BSM because
pricing uses actual strike and independent spot. Severe provider-spot plus strike divergence is
a hard block. The vectorized solver also requires final `bsm_gate_status=READY`, so a patched
blocked row cannot enter the solver even if its numerical inputs are otherwise finite.

Audit date: 2026-07-16

## Outcome

| Evidence class | Status |
|---|---|
| Implemented | DuckDB pre-BSM v2.1, vectorized BSM v2.1, BOD/static-six-slot SPAN, strict proven-time selection, six-slot reference research, first-seen SHA collection and atomic monthly resume. |
| Validated | All terminal row/hash/cardinality/timing gates, four-month timing pilot, 67/67 resume, point-in-time no-lookahead invariants, and vectorized-vs-scalar BSM fixtures. |
| Scraped | Phase 2 is terminal at 43,018,677 option rows; SPAN consumes the accepted compacted Phase 1 history with 3,993 explicit source gaps. |
| Blocked | No historical created/first-seen SPAN timing evidence exists, so strict historical matches are zero. INDIA VIX gaps remain contextual. Expired FUTIDX minute history remains separately source-blocked. |

## Point-in-time joins

The option timestamp is the observation time. NIFTY spot is the BSM underlying; INDIA VIX is
an independent contextual feature. Each source is matched separately using:

1. exact timestamp;
2. otherwise latest source timestamp not later than the option timestamp;
3. maximum backward lag 60 seconds;
4. identical IST trade date and explicit session key;
5. no future, overnight, or cross-session fill.

Duplicate source keys fail with an explicit join status instead of choosing a row arbitrarily.
Outside-regular-session acquisition rows are not eligible for this preparation contract.

## BSM contract

- European call/put Black-Scholes-Merton with continuously compounded `r=0.10`, `q=0`.
- ACT/365 time from the aware observation timestamp to verified expiry at 15:30 IST.
- Brent root solve bounded to decimal volatility `[0.0001, 5.0]`.
- Intrinsic/discounted no-arbitrage bounds checked before solving.
- No calculations at or after expiry.
- Explicit invalid-input, no-arbitrage, unbracketed-root, iteration, and post-expiry reasons.
- Reconstructed price and signed/absolute close-price residual retained.
- Delta, gamma, theta/year, theta/365, vega/1.00, vega/100, rho/1.00, rho/100 retained.
- Provider IV/Greeks remain provider-prefixed and are never overwritten.

Near-expiry is flagged at the configured threshold; no numerical epsilon is silently injected.

## Mandatory pre-BSM acceptance gate

BSM is not imported or executed by the enrichment module. Each immutable option request file
becomes idempotent `enriched_options/version=1.0.0/trade_date=.../part-<request>.parquet`
parts. Per-input manifests bind option, independent spot/VIX source-manifest, NSE rule and
actual-expiry hashes; audit row conservation, physical cardinality, natural-key duplicates and
required-field nulls; and store explicit exception Parquets.

A row is BSM-eligible when independent NIFTY spot matches without look-ahead, actual expiry and
lot/rule provenance resolve uniquely, close/strike are valid, MTE is positive, and the row is in
the regular session. INDIA VIX remains joined and audited but is contextual: source-unavailable
or otherwise missing VIX alone does not block BSM. The batch gate additionally requires terminal
option acquisition and no unresolved right-side duplicate conflicts. Full BSM materialization
was forbidden until all monthly manifests and the requested terminal audit passed; that gate is
now satisfied by the terminal v2.1 evidence above.

Time definitions are exact: `mte` is fractional calendar minutes from the aware option
timestamp to actual expiry; `dte=mte/1440.0`; `t_years_act365=mte/(365*24*60)`. Integer dates
or trading-day counts are not substituted.

## SPAN boundary and final timing representations

The reader accepts only interface version 1.0 manifests marked `accepted`, with SHA-256,
unique-key counts, duplicate count zero, source path, producer evidence hash, business date,
and slot identity (`i1` BOD through `i5` ID4, `s` EOD). Known effective times must be
timezone-aware and no later than the option observation. Unknown intraday/EOD times are never
guessed; an accepted unknown-time BOD is the sole conservative fallback. EOD is ineligible
before 15:30 IST.

The final release first publishes hash-pinned BOD v1.4 and static six-slot v2.0. It then
publishes two non-interchangeable timing contracts:

1. `point_in_time_strict` uses only exact-contract slots with proven created/first-seen
   effective timestamps no later than the option minute. The accepted history has no such
   evidence, so all 43,018,677 rows are preserved with zero SPAN matches.
2. `six_slot_research` preserves BOD, ID1-ID4 and EOD with the official reference schedule and
   `reference_only` confidence. It is complete for research but unsafe as contemporaneous
   model input.

Both terminal audits report zero duplicates, row loss/multiplication, future/negative ages,
early ID1-ID4/EOD uses, reference-only strict selections and orphan partials. The final command
rerun verified all month/output hashes and resumed 67/67 without rewriting a partition.

## Partition-incremental preparation pilot

The implemented batch contract was run read-only against completed August 2021 partitions:
option request `01570f555e5915f8a83393d807cc1d69fbcc20bac20a24a98a1f2627d0842dc4`
(7,503 rows), 11,844 NIFTY spot rows, and 9,760 INDIA VIX rows. All 7,503 option rows
matched NIFTY spot exactly. INDIA VIX results were 4,717 exact, 1,127 backward within 60
seconds, 750 absent for the session, 808 future-only and therefore forbidden, and 101 outside
tolerance. Every BSM row remained explicitly `blocked/actual_expiry_unverified`; readiness was
`SPAN_PENDING`. No prepared partition was labelled final gold or SPAN-enriched.

That August run predates the authoritative dimension below and is retained as historical join
evidence, not the current gate result.

## Authoritative-dimension pilot

The versioned writer then processed the first real January 2021 option request after the
official rule dimension completed. It conserved 7,500/7,500 rows across 20 dated partitions,
with zero duplicate primary keys, zero right-source duplicate conflicts, and 100% actual-expiry,
lot and MTE coverage. The controlling Dhan v2 definition maps code 1 to the second eligible
contract within exact WEEK/MONTH type; the full dimension has 4,044 unique trade-date/type keys.

Independent NIFTY spot matched 7,499 rows. The sole 09:15 option row rejected the future 09:16
spot candle. INDIA VIX matched zero because January 2021 is in its provider-unavailable period,
so all 7,500 rows correctly remain BSM-blocked. The batch also remains blocked by non-terminal
option acquisition. Manifest SHA-256:
`1e9dadd511bd600253929880e1176d8a12a6b18a92727c818e3f79401bee3524`.
An immediate rerun resumed the hashed part without rewriting; no `.partial` remained.

## DuckDB v2 migration pilots and launch

The final-code January and August pilots conserved 628,498 and 661,210 rows respectively.
Both had zero right-key duplicates, primary-key duplicates, future joins, over-60-second accepted
joins, row multiplication, or orphan partials. January had 628,428 ready rows and 70 rows blocked
only by unavailable independent spot; all VIX values were explicit contextual
`source_unavailable`. August had all 661,210 rows ready, 518,508 matched VIX rows, and 62,829
rows before the proven VIX lower boundary.

Six threads completed both months in 82.16 seconds; eight completed in 67.33 seconds. The two
runs produced identical Parquet SHA-256 values and identical all-column semantic signatures for
both months. A normalized comparison of 37,207 rows from 100 published v1 files covered 58 common
fields with zero unapproved differences; approved VIX/gate policy and enum casing were excluded.

The serial v1 fallback stopped after manifest boundary 67 at 501,304 rows. Its 67 manifests and
1,340 Parquets have zero orphan conflicts or partial files. Full v2 then launched under supervisor
PID 174816, worker PID 180776 and keep-awake PID 188472. The first published month contained
628,498 rows at 21,499 rows/second; initial measured ETA was about 32 minutes. BSM was not launched.
