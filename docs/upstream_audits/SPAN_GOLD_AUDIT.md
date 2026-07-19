# SPAN-Enriched NIFTY Options Gold Audit

Audit date: 2026-07-17

## Outcome

The immutable Dhan options/spot/BSM dataset has been joined to the available NSE SPAN
history and passed terminal acceptance for the options leg.

- Status: **PASS**
- Range: 2021-01-01 through 2026-07-15 EOD
- Months: 67/67
- Input/output rows: 43,018,677 / 43,018,677
- BOD SPAN matched rows: 42,718,832 (99.302989%)
- Explicit unmatched-contract rows: 299,845
- Duplicate SPAN right keys: 0
- Join-key violations: 0
- BOD policy/join-key violations: 0
- Strict proven-effective SPAN matches: 0
- Research timing: all six slots retained as `official_reference_schedule/reference_only`
- File-created/first-seen historical observations: 0 / 0
- Embedded monthly-lineage violations: 0
- Orphan partials: 0
- Full rerun: 67/67 months resumed; 0 rewritten

Canonical roots:

- conservative BOD fallback: `reports/nifty_gold_span_bod_20210101_20260715/version=1.4.0`;
- static six-slot base: `reports/nifty_gold_span_six_slot_20210101_20260715/version=2.0.0`;
- strict point-in-time: `reports/nifty_gold_span_point_in_time_strict_20210101_20260715/version=1.0.0`;
- six-slot research: `reports/nifty_gold_span_six_slot_research_20210101_20260715/version=2.1.0`.

Independent base integrity audit:

`reports/nifty_gold_span_bod_20210101_20260715/version=1.4.0/manifests/span_release_integrity_audit.json`

SHA-256: `eae260f0ff37a9abc046a0dfb564d09270fefe6fa56740204e5da41b250c8bb0`.

Strict/research timing terminal audit:

`reports/nifty_gold_span_point_in_time_strict_20210101_20260715/version=1.0.0/manifests/span_timing_terminal_audit.json`

SHA-256 after the 67/67 resume verification:
`cfe46cdcb5b561b39c3741fff4bc79df0a80fb18c6967aebee408ff470b0e1c5`.

The BOD fallback gold Parquets contain 43,018,677 rows and 3,743,775,346 bytes; its compact
exception Parquets contain 299,845 rows and 123,832 bytes. Static six-slot gold Parquets are
4,007,574,084 bytes. The strict gold Parquets contain 43,018,677 rows and 3,690,509,237 bytes;
research Parquets contain the same rows and 4,018,138,276 bytes. Each timing representation
has 67 Parquets and 67 monthly manifests. The final rerun rehashed and resumed 67/67 months in
49.438 seconds, rewriting zero months. Earlier BOD v1.3 remains preserved as superseded
evidence; v1.4 is the conservative fallback and the timing releases are the final separated
contemporaneous/research contracts.

## Timing policy and conservative BOD fallback

All archived SPAN effective timestamps are unproven (`effective_time_source=unknown`, null
effective timestamp). BOD v1.4 therefore remains the explicitly documented conservative
session-wide fallback. The strict point-in-time representation does not select BOD or any
intraday/EOD slot without proven effective/first-seen evidence; it preserves all rows with
`timing_unproven`. The research representation widens all six same-date slots but marks the
official schedule as reference-only and unsafe for contemporaneous model input. Every BOD
fallback match was independently checked for:

- `span_time_slot=BOD`;
- unknown/null SPAN effective-time evidence;
- exact trade date;
- exact actual expiry;
- exact strike;
- CALL to CE and PUT to PE mapping.

All BOD policy and join-key violation counts are zero. The strict audit also reports zero
future timestamps, negative ages, reference-only selections, early ID1/ID2/ID3/ID4 uses, EOD
before 15:30, duplicates and orphan partials. The research audit reports zero created-time
conflicts, invalid created timestamps, slot-order violations and reference-floor violations.
This is not a claim that exact SPAN publication time was measured.

## Phase 1 source boundary

The producer outcome remains honestly labelled `BLOCKED_SOURCE`, not rewritten as a clean
full-availability PASS. The consumer accepts it only because the terminal blocked matrix is
ready, every blocked-matrix integrity check passes, all 12,132 cells are accounted, unresolved
missing/non-boundary counts are zero, downloaded extraction and compaction integrity pass, and
the matrix/source artifacts are hash-bound. The 93 source-boundary cells are carried in every
gold row's global producer provenance.

The 299,845 unmatched rows are not caused by a missing BOD date or an unproven BOD source
boundary. They occur on 26 dates where a valid BOD file exists but the exact option
expiry/strike is absent. No row is dropped or filled from another slot.

## BSM and market-data contract

The joined input is the terminal-passing quality-patched BSM v2.1 dataset:

- continuously compounded `r=0.10`, `q=0`;
- ACT/365 to verified actual expiry at 15:30 IST;
- independent same-session NIFTY spot;
- 33,281,564 successful IV/Greek rows;
- 9,688,658 no-arbitrage rejects;
- 48 solver failures;
- 48,407 explicitly blocked inputs with no finite BSM output.

The rolling-options source remains accurately labelled Dhan ATM±10, not an absolute-strike
historical full chain.

## Historical futures boundary

The options gold result does not fabricate historical one-minute futures. Dhan returned 21
daily rows for official expired contract ID 35007 (`NIFTY24JULFUT`) but a valid empty
one-minute response for the same contract/date probe. Exact historical minute futures remain
`BLOCKED_SOURCE_DHAN_EXPIRED_MINUTE_EMPTY`. The documented official alternative is licensed
NSE F&O all-trade-tick history for exact-contract minute OHLCV; historical minute OI remains
unproven and requires written NSE confirmation/source authorization.

## Rebuild and validation

Offline rebuild/resume command:

```powershell
.\scripts\run_span_gold_full.ps1
```

Validation at close:

- full test suite: 125 passed;
- Ruff lint: passed;
- focused changed-file format check: passed;
- repository-wide format check: 29 pre-existing unrelated files would be reformatted;
- compileall: passed;
- `git diff --check`: passed;
- focused post-format timing/first-seen suite: 9 passed;
- CLI smoke: `span-gold`, `span-timing-release`, `span-first-seen` and
  `span-release-verify` loaded;
- secret-free command and artifacts: no credentials required or persisted.
