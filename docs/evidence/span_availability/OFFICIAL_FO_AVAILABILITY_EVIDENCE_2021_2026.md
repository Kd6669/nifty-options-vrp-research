# Official NSE F&O calendar evidence, 2021-2026

Evidence captured: 2026-07-15

## Scope and decision rule

This evidence pack supports conservative classification of missing NSE SPAN files. It does not infer exchange closure from HTTP 404 responses or filename patterns. A Saturday/Sunday classification is accepted only under the retained, reviewed NSE F&O weekly contract and remains subordinate to an explicit NSE special-session override.

A missing date/slot may be accepted as absent only when a reviewed official NSE/NSE Clearing artifact explicitly supports the market-state classification. Dates without explicit reviewed evidence remain unresolved. The live 2021-01-01 through 2026-07-15 downloader was not modified, stopped, or used as a writable target while this pack was produced.

NSE F&O Regulation 2.3.1 states that the F&O Segment operates on all days except Saturdays, Sundays, and declared Exchange holidays. Regulation 2.3.2 permits the Exchange to open a day otherwise excluded by Regulation 2.3.1. The identical weekly contract appears in the retained official 2013 regulation and the current 2026 regulation. Therefore the recurring rule is source-backed, not an unproven weekday heuristic, and explicit reviewed special-session dates always override it.

## Official source inventory

All sources were fetched from official NSE endpoints and retained locally. The SHA-256 values below were recomputed after classification; all 18 retained copies matched their declared hashes.

| Evidence | Official URL | Bytes | SHA-256 |
|---|---|---:|---|
| 2021 F&O trading holidays, NSE/FAOP/46625 | https://nsearchives.nseindia.com/content/circulars/FAOP46625.pdf | 75,457 | `dbaa2c14d720bf4902dc1a286d6972ae245b261d75582ea5927b7fcd435b8ca1` |
| 2022 F&O trading holidays, NSE/FAOP/50561 | https://nsearchives.nseindia.com/content/circulars/FAOP50561.pdf | 94,500 | `8f86da811cd7eb59a4c376e729d87ea55f0051834ce36f715c7f30d082fba8d7` |
| 2023 F&O trading holidays, NSE/FAOP/54759 | https://nsearchives.nseindia.com/content/circulars/FAOP54759.pdf | 97,219 | `a371ae8764cf270e563335a3240a75ecdcc4cecfbb17495788828e6c77024e2e` |
| 2023 Bakri Id holiday revision, NSE/FAOP/57286 | https://nsearchives.nseindia.com/content/circulars/FAOP57286.zip | 275,797 | `e6fe044201a8de77dd9dbdcb9c11d2566d1982406edf786d80badc5498946ae4` |
| 2024 F&O trading holidays, NSE/FAOP/59723 | https://nsearchives.nseindia.com/content/circulars/FAOP59723.pdf | 97,789 | `f05edc1dbb8943895f2e2675bb4ca0dfd17538ebb5ca2bb42fad8b6c4607f6e0` |
| January 22, 2024 trading holiday, NSE/CMTR/60338 | https://nsearchives.nseindia.com/content/circulars/CMTR60338.zip | 501,069 | `c70a96e353be65fa1905057300ddd0cb8823fb8398bb3b9e13d95fbe166888b2` |
| Saturday live session on 2024-01-20, NSE/MSD/60340 | https://nsearchives.nseindia.com/content/circulars/MSD60340.pdf | 89,845 | `6d165b8d132b7b430ae79032356ea331fab3d852192b74df4bc9e56722f54cdd` |
| Saturday special live session on 2024-03-02, NSE/MSD/60677 | https://nsearchives.nseindia.com/content/circulars/MSD60677.pdf | 163,493 | `04b67f39314bae95672d86497c28cbed6cea698ad6f029ba47ef74feb9f6da91` |
| Election holiday on 2024-05-20, NSE/FAOP/61517 | https://nsearchives.nseindia.com/content/circulars/FAOP61517.pdf | 67,472 | `0dd363891d13f1186c518ce2a0dadc826b136ec7055eda55470715a3b5b3cfa3` |
| Saturday special live session on 2024-05-18, NSE/MSD/61893 | https://nsearchives.nseindia.com/content/circulars/MSD61893.pdf | 135,194 | `2483f60d67c34231d6fd25024cf5767c031b234196a7a475535d434cd4e758c8` |
| Election holiday on 2024-11-20, NSE/FAOP/64959 | https://nsearchives.nseindia.com/content/circulars/FAOP64959.pdf | 87,616 | `8e473c97c858d346f9adc8a83138a0a6db5799a027cea85be33b3f716d6eeb43` |
| 2025 F&O trading holidays, NSE/FAOP/65588 | https://nsearchives.nseindia.com/content/circulars/FAOP65588.pdf | 134,297 | `e359379d4d7bc0296cea9b9bb1deda6d8460957cf49209208525da5a25ed7629` |
| Saturday Budget live session on 2025-02-01, NSE/FAOP/65730 | https://nsearchives.nseindia.com/content/circulars/FAOP65730.pdf | 97,021 | `d5179d0d0fab72ef3928eb8d8157a1e547751e792ff3040e0301015fc23c9145` |
| Archived F&O Regulations carrying Regulations 2.3.1 and 2.3.2 | https://nsearchives.nseindia.com/web/sites/default/files/inline-files/NSEFOregulations_8.pdf | 549,677 | `cb5afadef88c93a27d5fddfa604130b4c136d3391ea431674aca716d750d816b` |
| Current 2026 F&O Regulations carrying Regulations 2.3.1 and 2.3.2 | https://nsearchives.nseindia.com/web/mediaattachment/2026-04/NSEFORegulations2026_20260410112049.pdf | 588,788 | `a010bd8fd8b497543771ee1d5cec4119056f5daebde88743ebe3591ddfabfa4c` |
| 2026 F&O trading holidays, NSE/FAOP/71777 | https://nsearchives.nseindia.com/content/circulars/FAOP71777.pdf | 176,660 | `5a2079cd78b2e6b536ef0d28300e63b645721bed22cc82a91facf5945f3296ea` |
| Live 2026 NSE trading-holiday API response | https://www.nseindia.com/api/holiday-master?type=trading | 33,691 | `798c545acc5351eb9ed84f353c1fcc665a26967426e3761b7097e7f3c7042424` |
| Sunday Budget live session on 2026-02-01, NSE/FAOP/72352 | https://nsearchives.nseindia.com/content/circulars/FAOP72352.pdf | 90,333 | `7b282150e8cf7757da6944c682fe810189d2ed86fe6b171094bb4cd4d7f1facb` |

The content-addressed retained copies are in `retained_sources/`. They comprise 18 files and 3,355,918 bytes. The original downloaded artifacts, including the unpacked 2023 revision and raw live API response, are in `official_sources/`.

## Reviewed availability import

`reviewed_import_2021_2026.json` uses schema `span-availability-import/v1`, contains one source-backed recurring weekly rule, and contains 122 explicitly reviewed date overrides:

| Market state | Dates |
|---|---:|
| `closed` | 110 |
| `special_trading_session` | 11 |
| `regular_trading_day` | 1 |

Counts by year are 19 for 2021, 18 for 2022, 20 for 2023, 25 for 2024, 19 for 2025, and 21 for 2026. The live API's F&O section contains 20 2026 holiday rows; the import additionally carries explicit special-session overrides.

The 11 reviewed special-session dates are:

- Muhurat sessions: 2021-11-04, 2022-10-24, 2023-11-12, 2024-11-01, 2025-10-21, and 2026-11-08.
- Weekend full/special sessions: 2024-01-20, 2024-03-02, 2024-05-18, 2025-02-01, and 2026-02-01.

The one explicit regular-session override is 2023-06-28. NSE/FAOP/57286 moved the Bakri Id holiday from 2023-06-28 to 2023-06-29, so the original annual-calendar date is not treated as closed.

The January 22, 2024 closure is an intra-year override absent from the annual holiday circular.
NSE/CMTR/60338 explicitly declares the trading holiday, while NSE/MSD/60340 moves the January 22
Equity Derivatives expiry to the full live session held on Saturday, January 20.

Weekend holiday rows explicitly printed in annual circulars remain retained as date-specific reviewed closures. All other Saturdays and Sundays are generated under the reviewed Regulation 2.3.1 weekly contract. Date-specific live-session entries take precedence, implementing the Regulation 2.3.2 exception mechanism.

## Point-in-time downloader snapshot

Classification was run against a read-only snapshot, not the active manifest:

- Artifact: `download_manifest_snapshot_20260715.jsonl`
- Bytes: 208,450
- SHA-256: `0b00e72fc0e26ec96876d3be99d138ffd60b5c41efbce97427d37bfbd7da24c2`
- Journal records: 337
- Latest date/slot cells: 246 across 41 dates, 2021-01-01 through 2021-02-10
- Latest states: 163 `downloaded`, 78 terminal `not_returned_http_404`, and 5 `retrying_transport_error`

Command:

```powershell
uv run span-backfill classify `
  --start-date 2021-01-01 `
  --end-date 2026-07-15 `
  --download-manifest docs\evidence\span_availability\download_manifest_snapshot_20260715.jsonl `
  --availability-import docs\evidence\span_availability\reviewed_import_2021_2026.json `
  --availability-manifest docs\evidence\span_availability\availability_manifest_weekly_snapshot_20260715.jsonl `
  --provenance-root docs\evidence\span_availability\retained_sources `
  --json
```

Point-in-time result:

- Imported explicit/expanded dates in the requested range: 661
- Retained sources: 17
- Terminal missing cells classified: 78, comprising 72 ordinary-weekend cells and all six slots on the official holiday 2021-01-26
- Terminal missing cells unresolved: 0
- Source-boundary classifications: 0
- Command exit status: 0

The 78 accepted events have `classification_outcome=accepted_absence` and `source_availability_boundary_proven=false`. Calendar classifications are either `official_holiday` or `official_weekend`. The sources prove exchange non-trading days, not a historical SPAN archive boundary. No raw 404 body or missing-file observation was used as calendar evidence.

## Audit validation

Confirmed holiday control, 2021-01-26:

- Outcome: `PASS_READY`
- Expected/accounted cells: 6/6
- Ambiguous cells: 0
- Failed or incomplete cells: 0
- Report: `validation/holiday/audit/SPAN_BACKFILL_AUDIT.md`

Pre-contract negative control, 2021-01-02:

- Outcome: `FAIL_INCOMPLETE`
- Expected/accounted cells: 6/6
- Ambiguous cells: 6
- Failed or incomplete cells: 6
- Report: `validation/weekend/audit/SPAN_BACKFILL_AUDIT.md`

This original negative control proves that a weekend plus six HTTP 404 responses is insufficient without independent evidence.

Source-backed weekend control, 2021-01-02:

- Outcome: `PASS_READY`
- Expected/accounted cells: 6/6
- Ambiguous cells: 0
- Failed or incomplete cells: 0
- Report: `validation/weekend_contract/audit/SPAN_BACKFILL_AUDIT.md`

The transition from `FAIL_INCOMPLETE` to `PASS_READY` occurs only after the retained NSE F&O Regulations are added. The focused test suite also verifies that an explicitly notified Saturday/Sunday special session overrides the weekly contract and remains unresolved when its expected SPAN archive is missing.

Real-import special-session override control, 2026-02-01:

- Classifier: 0 accepted cells and 6 unresolved cells
- Audit outcome: `FAIL_INCOMPLETE`
- Ambiguous cells: 6
- Report: `validation/special_override/audit/SPAN_BACKFILL_AUDIT.md`

This uses the actual reviewed import and NSE/FAOP/72352 source, not a unit-test-only calendar fixture.

Code validation:

```text
uv run python -m unittest tests.test_span_availability tests.test_span_backfill_audit tests.test_span_backfill_pipeline
.............
Ran 13 tests in 7.294s
OK

uv run ruff check <modified availability/backfill files and focused tests>
All checks passed!
```

## Remaining gaps and rerun contract

The reviewed market-calendar contract is complete for the 2021-01-01 through 2026-07-15 downloader range, subject to the final downloader-driven checks below.

- The current classification is tied to the immutable snapshot hash above. Re-run `span-backfill classify` against the completed downloader manifest to classify all terminal missing cells.
- Keep retrying/non-terminal transport errors out of availability classification until they reach a terminal downloader state.
- Any later correction circular must override, not merely supplement, the annual list, as demonstrated by the 2023 Bakri Id revision.
- Any downloaded SPAN archive on a date classified `official_weekend` is a hard contradiction that must fail audit and trigger review for a missing special-session circular; it must not be discarded or relabeled absent.
- The live API artifact is a point-in-time 2026 response with SHA-256 `798c545acc5351eb9ed84f353c1fcc665a26967426e3761b7097e7f3c7042424`. Refresh it for future dates or after exchange calendar corrections.

The acceptance gate for the final backfill remains: no unresolved or ambiguous date/slot cells, all accepted absences backed by retained official evidence, and a clean end-to-end audit over the completed manifest.
