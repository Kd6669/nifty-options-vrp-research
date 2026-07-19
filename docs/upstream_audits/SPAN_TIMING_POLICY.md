# NSE SPAN timing policy and release split

## Outcome boundary

NSE Clearing's equity-derivatives FAQ states that SPAN parameter files use prices at
11:00, 12:30, 14:00, 15:30, EOD and BOD, and are run shortly afterward. These are
risk-snapshot reference times. They are not historical file-arrival timestamps.

Official source:

- URL: `https://www.nseclearing.in/sites/default/files/2025-08/NCL%20-%20FAQ%20RISK%20MANAGEMENT.pdf`
- locally verified bytes: 474,013
- SHA-256: `ae443f77d0202eeda8b2b07fd17defe344688bc044b265e6880d3197cdd7986c`
- relevant item: Equity Derivatives SPAN frequency, FAQ question 2

The local canonical SPAN handoff contains 24,870,123 rows. A direct aggregate over all
67 monthly Parquets found zero non-null `span_file_created` values, zero non-null
`span_effective_ts_ist` values, and `effective_time_source=unknown` for every row in every
slot. The schedule can therefore label historical research observations, but it cannot prove
when those files became usable by a contemporaneous model.

## Official reference schedule

| Archive name | Canonical slot | Reference policy |
|---|---|---|
| `i1` | `BOD` | Before market; no exact timestamp inferred. |
| `i2` | `ID1` | 11:00 IST reference price. |
| `i3` | `ID2` | 12:30 IST reference price. |
| `i4` | `ID3` | 14:00 IST reference price. |
| `i5` | `ID4` | 15:30 IST reference price. |
| `s` | `EOD` | After market close; no exact timestamp inferred. |

`BOD` and `EOD` intentionally retain null `span_reference_ts_ist` because “before market”
and “after market close” do not identify an exact instant. EOD proof is rejected if its
created/first-seen timestamp precedes 15:30 IST.

## Evidence derivation

For each exact date/slot/archive SHA:

1. `span_file_created` is parsed only when it carries an explicit UTC offset or `Z`.
2. Its local IST date must match the SPAN trade date. Invalid/naive values remain raw and are
   never promoted to arrival evidence.
3. A future live observation is keyed by the exact archive SHA and records the first valid ZIP
   observation timestamp as `span_first_seen_ts_ist`.
4. A proven effective timestamp is the maximum of the applicable official reference floor,
   a valid file-created timestamp, and a valid same-day first-seen timestamp.
5. Activation is rounded forward to the minute boundary. It is never rounded backward.
6. Source observations are audited for timezone/date validity, conflicting created timestamps,
   slot order, monotonicity, reference floors and EOD-before-close violations.

The bounded future collector is:

```powershell
dhan-data span-first-seen `
  --url <exact-official-NSE-archive-url> `
  --trading-date 2026-07-17 `
  --time-slot ID1 `
  --manifest reports\span_first_seen\observations.json `
  --archive-dir reports\span_first_seen\archives
```

It accepts only official NSE/NSE Clearing HTTPS domains, HTTP 200, a valid non-empty ZIP and
clean member CRCs. The manifest update is cross-process locked, atomic and idempotent by
`trading_date,time_slot,source_sha256`.

## Immutable outputs

### Conservative BOD fallback

`reports/nifty_gold_span_bod_20210101_20260715/version=1.4.0`

This retains the explicitly documented same-date BOD research fallback and the exact final
SPAN release lineage. It is not relabelled as measured historical availability.

### Point-in-time strict

`reports/nifty_gold_span_point_in_time_strict_20210101_20260715/version=1.0.0`

This representation preserves every Dhan/BSM row and selects only the latest exact-contract
SPAN observation whose proven effective timestamp is less than or equal to the Dhan minute.
The join is backward-only. No date, session, expiry, strike or option-side substitution is
allowed. Reference-only observations are never selected. With the accepted historical input,
the expected SPAN match count is zero and every static contract match remains explicitly
`timing_unproven` rather than being dropped or filled.

### Six-slot research

`reports/nifty_gold_span_six_slot_research_20210101_20260715/version=2.1.0`

All six slots remain widened under `span_bod_*`, `span_id1_*`, `span_id2_*`, `span_id3_*`,
`span_id4_*` and `span_eod_*`. Historical slots without proof use
`timing_source=official_reference_schedule` and `timing_confidence=reference_only`. They are
available for sensitivity/research analysis, but unsafe as contemporaneous model inputs.

## Timing fields

The strict representation uses unprefixed fields; the six-slot research representation uses
the same fields under each slot prefix.

| Field | Contract |
|---|---|
| `span_reference_ts_ist` | Exact ID1-ID4 official reference timestamp; null for BOD/EOD. |
| `span_file_created_ts_ist` | Parsed, explicitly zoned and date-valid provider creation timestamp. |
| `span_first_seen_ts_ist` | First valid observation of the exact archive SHA by the live collector. |
| `span_effective_ts_ist` | Proven activation after reference/proof maximum and forward-minute rounding. |
| `span_timing_source` | `span_file_created`, `nse_endpoint_first_seen_sha`, `official_reference_schedule`, or `none`. |
| `span_timing_confidence` | `file_created_proven`, `observed_first_seen`, `reference_only`, or `unproven`. |
| `span_time_slot` | Selected strict slot, or the explicit research slot prefix identity. |
| `span_age_seconds` | Non-negative Dhan minute minus proven effective timestamp; null when unavailable. |

Each monthly output is deterministic, ZSTD-compressed, atomically published, SHA-bound and
resumable. Terminal audits report coverage by slot, year, timing source, timing confidence and
join status, plus row conservation, key cardinality, BSM status preservation and orphan
partials.

## Terminal evidence

The 2026-07-17 full release passed both representations at 67/67 months and 43,018,677 rows.
Strict matches, primary-key duplicates, future timestamps, negative ages, reference-only
strict selections, early ID1-ID4/EOD uses and orphan partials are all zero. Research reports
zero raw/valid created timestamps, zero proven effective rows, zero created-time conflicts,
zero slot-order/reference-floor violations, and 8,139 source observations across the monthly
audits. The BSM status distribution is preserved exactly in both outputs: 33,281,564 `ok`,
9,688,658 `no_arbitrage_violation`, 48 `iv_solver_failed`, and 48,407 `blocked`.

The terminal audit is
`reports/nifty_gold_span_point_in_time_strict_20210101_20260715/version=1.0.0/manifests/span_timing_terminal_audit.json`.
After a complete 67/67 hash-resume verification its SHA-256 is
`cfe46cdcb5b561b39c3741fff4bc79df0a80fb18c6967aebee408ff470b0e1c5`.
