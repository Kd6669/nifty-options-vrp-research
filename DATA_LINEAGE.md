# Data Lineage

## Pre-BSM quality overlay v2.1.0

The completed 67-month `version=2.0.0` layer is immutable input to a separate monthly
`version=2.1.0` pass. DuckDB preserves every row and provider field, adds the 50-point ladder
audit, independent-spot-derived moneyness, provider-spot divergence and the severe-anomaly gate,
then stages, fsyncs, hashes and atomically publishes one Parquet per month. This pass makes no
Dhan request and executes no BSM calculation. BSM preflight rejects v2.0.0 and requires a
matching v2.1.0 manifest plus the 67-month terminal PASS audit.

## Layers

```text
Dhan official API/public master
  -> bronze exact response bytes + request manifest
  -> silver source-specific normalized Parquet + exceptions
  -> mandatory pre-BSM enriched options + same-session spot/VIX + NSE contract terms
  -> independent BSM only for rows that pass the pre-BSM gate
  -> final hash-pinned static BOD and six-slot SPAN joins
  -> timing evidence derivation from official reference floors plus proven arrival evidence
  -> separate point-in-time strict and six-slot research releases
  -> options gold plus explicit unmatched/source-boundary/timing exceptions
```

`bronze` is immutable. A completed valid response is never overwritten. `silver` and prepared outputs are rebuildable from bronze, request manifests, schema/model versions, and the code commit.

## Source identities

| Source | Identity and scope |
|---|---|
| Dhan rolling options | `/v2/charts/rollingoption`; rolling moneyness surface, not an absolute-strike chain |
| Dhan spot | `/v2/charts/intraday`; NIFTY INDEX security ID 13 |
| Dhan INDIA VIX | `/v2/charts/intraday`; independently resolved official INDEX identity, dataset `india_vix` |
| Dhan futures | `/v2/charts/intraday`; currently active FUTIDX security IDs resolved from the official master |
| Dhan active chain | `/v2/optionchain`; current snapshot for an exact active expiry |
| Dhan instrument master | `https://images.dhan.co/api-data/api-scrip-master.csv`; dated current-tradable snapshot |
| Dhan expiry semantics | Linked v2 Annexure: 0 current/near, 1 next, 2 far; hashed HTML retained in the rule source manifest |
| NSE contract rules | Official FAOP circulars with saved bytes and SHA-256; effective contract-expiry intervals remain distinct from circular dates |
| NSE actual expiries | Official F&O bhavcopies; an expiry is historical-proven only when a NIFTY option record expires on that bhavcopy trade date |
| NSE SPAN | Terminal Phase 1 `BLOCKED_SOURCE` matrix plus hash-verified compacted files; available BOD rows are consumed while every source boundary remains explicit |

## Request identity and immutability

Each request is identified by a canonical SHA-256 of its endpoint and non-secret JSON payload. The manifest records status, attempts, error class, row count, min/max timestamps, response/output paths and hashes. Credentials and headers are excluded.

Writes use a temporary sibling, flush/close, and atomic replace. Resume accepts a completed cell only when its manifest state and stored file hash agree. A mismatch is an exception, not an implicit overwrite.

## Normalization lineage

Parallel response arrays must have equal lengths before flattening. Each normalized row retains enough request metadata to distinguish:

- `WEEK` from `MONTH`;
- expiry code;
- requested rolling moneyness label;
- returned actual strike;
- side (`CALL`/`PUT`);
- provider fields from independently computed fields;
- request/bronze hashes and schema version.

Conflicting natural-key duplicates are quarantined. Missing spot, expiry ambiguity, BSM failures, and future-source gaps remain explicit exceptions.

## Spot and BSM preparation

An option row joins independently to exact same-minute NIFTY spot and INDIA VIX. A backward as-of match is permitted only when it is no more than 60 seconds old and belongs to the same authoritative session and trade date. No future, overnight, or cross-session carry is allowed. INDIA VIX is contextual; it never replaces the NIFTY spot used in BSM.

Pre-BSM v1 retains the per-request fallback layout. Canonical pre-BSM v2 scans immutable
Parquets directly with DuckDB and publishes one deterministic monthly ZSTD Parquet. Compiled
ASOF joins use equality on trade date/session plus a backward timestamp inequality; candidates
older than 60 seconds are nulled. Duplicate right keys are quarantined before the join, so they
cannot multiply option rows. Exact expiry is hash-joined and contract rules are range-joined on
the actual contract expiry. Month manifests bind source filenames/request IDs, every source and
output hash, code/config version, row conservation, key cardinality and coverage. Resume accepts
only the exact validated lineage; stale/corrupt publications are quarantined.

INDIA VIX is always preserved where available. Rows before its proven Dhan lower boundary carry
an explicit contextual null and `source_unavailable` provenance. This does not block BSM because
VIX is not in the formula. The independently joined NIFTY spot remains mandatory.

BSM is computed independently with `r=0.10` continuously compounded, `q=0`, ACT/365, and a separately verified actual expiry timestamp at 15:30 IST. It uses only `t_years_act365` and `independent_nifty_spot`. Dhan-provided IV and Greeks remain provider-prefixed comparison fields.

BSM v2 accepts only exact `pre_bsm.parquet` artifacts whose producer manifest passes the full
row, look-ahead, primary-key, hash and terminal-acquisition gate. Each month is solved in bounded
NumPy/Numba vector batches: Newton is the main path and only its non-convergent tail reaches the
bounded scalar Brent implementation. Monthly outputs, manifests, hashes and status are atomic
and resumable. SPAN is deliberately absent from this materialization and is added only in the
next immutable layer.

Before SPAN enrichment, this layer is `prepared`; it is not the final options gold artifact.

## SPAN gold boundary

The SPAN gold runner validates the BSM terminal PASS audit and accepts the Phase 1
`BLOCKED_SOURCE` outcome only when its 12,132-cell blocked matrix is ready, all blocked-matrix
integrity checks pass, source stability passes, and unresolved missing/non-boundary counts are
zero. It validates the exact daily matrix universe, re-hashes the terminal producer summary and
every monthly BSM/SPAN input, and embeds the exact monthly lineage SHA in every output row.

Because all archived effective timestamps are unproven, only same-date BOD NIFTY CE/PE rows are
eligible for the primary intraday join. The exact right key is date, instrument, actual expiry,
and strike. The writer rejects duplicate SPAN keys, conserves every BSM row, prefixes SPAN
fields, and emits explicit unmatched-contract Parquet. ID1-ID4/EOD are not joined, so EOD can
never enter the primary join. Exact publication time remains unproven, so this is a conservative
BOD policy rather than a measured no-lookahead claim. Monthly output/manifests are atomic and
hash-resumable; crash-pair adoption requires the embedded lineage to match exactly.

This options gold layer does not resolve the separately proven historical-minute-futures source
boundary. Dhan exact expired futures produced daily data but an empty minute response; no spot
substitution or synthetic minute futures are introduced.

## SPAN timing evidence boundary

The official NSE Clearing equity-derivatives schedule supplies reference-price times at 11:00,
12:30, 14:00 and 15:30 IST plus BOD and EOD. The files are run shortly afterward, so the
schedule is not treated as historical arrival evidence. The source PDF, its SHA-256, slot
mapping and activation rules are pinned in `SPAN_TIMING_POLICY.md`.

The accepted SPAN handoff has no non-null file-created or effective timestamp in any of its
24,870,123 rows. Accordingly, the six-slot research release preserves all schedules as
`official_reference_schedule/reference_only`, while the point-in-time strict release selects
none of them. The strict layer still preserves every Dhan/BSM row and records whether a static
contract was available but timing-unproven, source-gapped, or absent.

For future live collection, the bounded first-seen poller accepts only a valid ZIP from an
official NSE/NSE Clearing HTTPS domain, hashes the exact archive bytes, and atomically records
the first successful IST observation keyed by trade date, slot and archive SHA. A strict join
then chooses only the latest exact-contract observation with effective time less than or equal
to the Dhan timestamp. The join remains same-date, backward-only and non-multiplying. ID1-ID4
cannot activate before their reference floors; EOD proof before market close is rejected.
