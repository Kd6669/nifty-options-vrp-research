# Dhan Backfill Audit

## 2026-07-17 authoritative terminal state

Later immutable stages supersede the historical running-state paragraphs below: acquisition
PASSed 8,820/8,820 request cells and 43,018,677 rows; pre-BSM quality patch PASSed all 67
months; BSM v2.1 PASSed all 43,018,677 rows; the conservative BOD v1.4 and static six-slot
v2.0 releases PASSed all 67 months; and the strict v1.0 plus six-slot research v2.1 timing
releases each conserve all 43,018,677 rows. Strict historical SPAN matches are zero because
the accepted archives contain no proven created/first-seen timestamp. BOD v1.4 retains the
42,718,832 matched plus 299,845 explicit unmatched baseline as a conservative fallback. See
`SPAN_GOLD_AUDIT.md`. Historical one-minute futures remain source-blocked and are not part of
the options-gold PASS.

## Post-acquisition quality patch

The acquisition and pre-BSM v2.0.0 artifacts remain immutable. A resumable v2.1.0 monthly overlay
audits the provider strike ladder and provider spot against independent NIFTY spot. The eight
CALL/PUT MONTH ATM-10 rows at 2023-01-06 15:29 IST and 2026-01-12 10:44, 11:08 and 11:09 IST
are exact regression fixtures and hard-blocked. Monthly manifests publish mismatch, missing ATM,
divergence, severe anomaly, eligibility, row/cardinality and hash evidence.

Audit date: 2026-07-16

Repository: `Goyal-Dedhia-Capital/dhan-data-fetch-and-stream`

Branch: `kd-codex/dhan-phase2-3`

Pinned and live starting SHA: `61c11b51e061dc4503fd4c91b3c3c005a28d6b60`

## Outcome by evidence class

| Class | Status |
|---|---|
| Implemented | Resumable acquisition; orphan-partial quarantine; bounded credential fanout; durable supervisor; typed silver; versioned pre-BSM and BSM; hash-pinned BOD/static-six-slot SPAN; strict timing and six-slot research releases; future first-seen SHA capture. |
| Validated | Terminal acquisition, pre-BSM, BSM, BOD/static-six-slot and strict/research timing audits; exact row/hash resume; point-in-time timing invariants; focused and full test suites. |
| Scraped | NIFTY spot, INDIA VIX and rolling options are terminally accounted. Rolling options contain 8,820 cells: 7,560 non-empty, 1,260 valid empty, 43,018,677 retained rows. |
| Blocked | Options gold is terminally published. Historical expired FUTIDX one-minute history remains source-blocked; historical SPAN arrival timestamps remain unproven and therefore unavailable to strict joins. |

The canonical pre-BSM/BSM input is the accepted quality-patched v2.1.0 layer. INDIA VIX gaps
are explicit contextual nulls and did not by themselves block BSM. SPAN is still kept as a
separate final enrichment boundary: strict timing is safe but has zero historical matches;
six-slot research is complete but reference-only and unsafe as contemporaneous model input.

## Repository baseline

The expected checkout did not exist. A clean clone was created at
`<external-data-root>/dhan-data-fetch-and-stream` on the focused branch.
The pinned SHA and live GitHub `main` head were identical. Untouched baseline: 6 tests passed,
all Python compiled, all four original CLI commands loaded, no Makefile, and one pre-existing
Ruff F401 in `core.py` (removed in this branch).

## Acquisition controls

Canonical request SHA-256 IDs exclude secrets. Completed bronze JSON is immutable. Resume
requires manifest status plus matching file hash. Normalizer-version changes rebuild silver
from cached bronze with zero network calls. Writes use a same-directory partial file, fsync,
and atomic replace. Parallel arrays are length-validated. HTTP errors are redacted; manifests
record attempts/status/error class but never headers or tokens. Bounded 5/s rate limiting,
retry/backoff, 100,000/day atomic budgeting, and a three-second current-chain key interval are
implemented.

## Dataset coverage

| Dataset | Requests/status | Retained rows | Returned coverage | Exceptions/notes |
|---|---:|---:|---|---|
| NIFTY spot | 67/67 completed | 576,631 | 2021-01-01 09:16 to 2026-07-15 18:32 IST | 335 quarantined rows: 12 OHLC, 323 negative volume/OI; 62,372 retained outside-session rows; zero natural-key duplicates |
| INDIA VIX | 60 completed, 7 empty | 524,077 | 2021-08-04 10:00 to 2026-07-15 18:35 IST | Jan–Jul 2021 empty; 327 quarantined: 3 OHLC, 324 negative volume/OI; 67,748 outside-session; zero global duplicates |
| Rolling ATM±10 pilot | 84/84 completed | 31,495 | 2026-07-14 09:15–15:29 IST | 31,485 provider rows from `toDate` day explicitly dropped; zero quality exceptions/duplicates |
| Rolling history | 7,560 completed, 1,260 completed-empty, zero remaining | 43,018,677 | 2021-01-01 through 2026-07-15 15:29 IST | 8,820/8,820 canonical; zero failed; terminal artifact/hash audit passed |
| Current futures probe | 3 recent completions, 3 historical empty | 2,247 recent | 2026-07-14–15 for three current IDs | 2021 sentinels empty; expired history unproven |
| Current full chain snapshot | one accepted expiry snapshot | 224 strikes; CE+PE records | 2026-07-21 expiry snapshot | current only |

The Dhan intraday response contains substantial timestamps outside the nominal NSE regular
session, particularly in earlier history. These are preserved with `session_status`, not
deleted or interpreted as exchange-valid. Phase 3 admits only regular-session rows. INDIA VIX
has 1,230 dates with regular-session rows: 1,094 have all 375 minutes on the 09:15–15:29
grid, while 136 are partial (5,493 missing grid minutes in aggregate). Special-session dates
are not classified without a separately audited exchange calendar.

## INDIA VIX observed unsupported period

All seven monthly requests from 2021-01 through 2021-07 returned valid empty payloads. The
first provider timestamp is 2021-08-04 10:00 IST. Therefore this run reports the interval
2021-01-01 through 2021-08-03 as unavailable from the tested intraday identity/contract. It
is not forward-filled, reconstructed, or silently narrowed.

## Storage and lineage

Generated data remains under ignored `reports/` roots and is not committed. Each completed
non-empty request has bronze and silver SHA-256 values; empty responses retain a bronze hash.
Partitioning is independent by dataset/year/month. Quality and request-window exceptions are
separate Parquet artifacts linked through request manifests.

## Official NSE contract dimension and pre-BSM pilot

Official-primary evidence produced 289 historical expiry rows proven by same-day NSE F&O
bhavcopies plus 7 active future expiries visible at the 2026-07-15 cutoff. Sixteen expiries are
holiday-adjusted; calendar date/type duplicates and null lots are both zero. The linked Dhan v2
annexure controls code semantics (`0=current/near`, `1=next`, `2=far`). Code 1 is mapped to the
second eligible contract within exact WEEK/MONTH type for 4,044 trade-date/type keys, with zero
duplicates. Ten lot-rule intervals are keyed by contract-expiry applicability and cite the
establishing circular; circular dates are stored separately. Distinct multiplier, historical
effective-dated tick, and exceptional-session overrides remain explicit null/blockers.

The real pre-BSM pilot processed option request
`05165183578072d3a05af5c2cac364ee952ce68bd13365cf9647525b21119491`: 7,500 input rows,
7,500 conserved canonical rows, 20 trade-date Parquet partitions, zero duplicate primary keys,
and zero right-side timestamp conflicts. Expiry/lot/MTE coverage is 100%. Independent spot
matched 7,499 rows; the 09:15 row correctly rejected the future 09:16 candle. INDIA VIX matched
zero because January 2021 is inside the separately proven unavailable period. The manifest hash
is `1e9dadd511bd600253929880e1176d8a12a6b18a92727c818e3f79401bee3524`; rerun resumed the
file without rewriting it. `bsm_executed=false` and the gate remains blocked.

## Interrupted resume and supervisor evidence

The earlier launch evidence above is historical and must not be read as current process state.
At the verified 2026-07-16 checkpoint no rolling-options process was active. One interrupted
bronze `.partial` was hashed and moved by the acquisition engine to
`exceptions/orphan_partials`; it was never promoted, its canonical sibling did not exist, and
all completed bronze/silver/manifests remained untouched.

A bounded launch with the previously inherited credential returned DH-901. The engine's
parallel scheduler kept at most five cells in flight and stopped submission on the first
credential block. Supervisor PID 177772 and child PID 181576 consumed one request, kept
canonical progress at 5,073/8,820 and rows at 24,913,768, recorded
`authentication:DH-901`, and correctly suppressed restart at count 0. No active partial
remained.

A newly supplied credential was validated without printing or persisting it by a separate
`/v2/profile` request (`HTTP 200`). The same secret-free supervisor command then started
supervisor PID 157996 and acquisition PID 113000 at 2026-07-16 02:30:19 IST. `PASS_RESUMED`
was independently established when canonical accounting advanced from 5,073 to 5,083 and
retained rows advanced from 24,913,768 to 24,968,571, with zero current error codes and zero
active partials. At 02:32:03 IST it had reached 5,168/8,820 canonical cells, 25,217,580 rows,
and a 56.218 cells/minute rolling rate. Atomic live status remains under
`reports/dhan_phase2_backfill_20210101_20260715/supervisor/`; these checkpoint values are not a
terminal-completion claim.

The resumed main pass later stopped at a safe boundary with one non-canonical DH-904 cell.
After cooldown, that exact request alone was retried with one worker at one request/second and
returned 375 rows. The credential-free terminal audit then re-hashed every referenced bronze
and silver artifact. It passed at 2026-07-16 11:23:55 IST with 8,820/8,820 accounted, 7,560
non-empty, 1,260 empty, 43,018,677 retained rows, zero failed, zero integrity errors, zero
partial/canonical conflicts, zero orphan partials and zero unplanned IDs. One previously
quarantined interrupted partial remains evidence-only. The audit artifact SHA-256 is
`5d87204033e64821b863915860ba33e9225faf3a8833f93f18620a44db6aa3e8`.

The full mandatory pre-BSM run launched at 2026-07-16 11:28:56 IST under runner PID 181100,
with keep-awake PID 144504 and zero-byte stderr. Its atomic status expanded from the one-file
pilot to all 7,560 non-empty option inputs and showed fresh manifest growth. BSM remained
disabled while enrichment/coverage audits ran.

That serial v1 fallback was later stopped safely after manifest boundary 67 (501,304 rows) only
after DuckDB v2 migration pilots passed. All 67 v1 manifests, 1,340 Parquets and hashes were
preserved; zero orphan output conflicts and zero partials were found.

The canonical v2 full run launched 2026-07-16 12:39:03 IST with supervisor PID 174816, worker
PID 180776 and bound keep-awake PID 188472. It uses eight threads, an 8 GB DuckDB ceiling,
250,000-row groups and month-level atomic resume. First verified growth was 1/67 months and
628,498 rows at 21,499 rows/s, zero stderr/partials, with an initial ETA near 32 minutes. This
is retained as launch history; pre-BSM and BSM subsequently reached the terminal PASS state
recorded at the top of this audit.

## Remaining blockers

1. Exact expired FUTIDX daily history is proven through Dhan for official contract ID 35007
   (`NIFTY24JULFUT`): 21 rows from 2024-07-01 through 2024-07-30. The same exact ID returned a
   valid empty one-minute payload for 2024-07-15. NSE's paid
   official F&O all-trade-tick archive can produce exact-contract one-minute OHLCV, while the
   free contract archive proves daily OHLC/OI. Historical minute OI and recent Trim-file
   retrospective procurement remain unproven; see `docs/NSE_EXPIRED_FUTURES_FEASIBILITY.md`.
2. The historical SPAN archives expose no credible `span_file_created` timestamp and no
   collector first-seen observation. Official reference times are therefore retained only in
   `six_slot_research`; strict matches remain zero. Future polling can add SHA-bound first-seen
   evidence without retroactively asserting historical arrival.

## Final validation

- `PYTHONPATH=src py -3.11 -m pytest -q`: 125 passed.
- focused post-format timing/first-seen tests: 9 passed.
- `ruff check .`: all checks passed.
- `compileall` for all source/test Python files: passed.
- `git diff --check`: passed.
- CLI help includes `span-timing-release`, `span-first-seen` and `span-release-verify`.
- Secret scan of commit candidates: zero JWT-shaped or literal token-assignment file hits.
- Tracked generated artifacts: none.
