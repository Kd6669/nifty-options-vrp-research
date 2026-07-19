# NSE SPAN current official-public corrupt archives — four 2021 cells

## Disposition

`CURRENT_OFFICIAL_PUBLIC_SOURCE_CORRUPTION_PROVEN`

Four exact NSE SPAN archive paths currently return HTTP 200 `application/zip` payloads that begin with ZIP local-file magic but have no usable ZIP central directory. Python `zipfile` rejects all four. For every cell, NSE's reports API returns an inner archive with exactly the same byte length and SHA-256 as the static object, while the other five slot archives in that API response validate.

This proves current official-public source corruption and current usable-archive unavailability. It does **not** prove historical nonpublication, that the objects were malformed when first published, or that no valid member-retained copy exists.

Phase 1 remains fail-closed for these four cells. The evidence action is `DO_NOT_MARK_USABLE_OR_DOWNLOADED`; it does not add dates to `reviewed_import_2021_2026.json` or convert the cells into accepted non-trading absences.

The deterministic companion record is `NSE_SPAN_2021_CORRUPT_OFFICIAL_ARCHIVES_EVIDENCE.json` using schema `span-source-corruption-evidence/v1`.

## Exact current official static objects

The official URL pattern is:

`https://nsearchives.nseindia.com/archives/nsccl/span/nsccl.YYYYMMDD.<suffix>.zip`

| Date | Slot | Suffix | HTTP / media type | Bytes | SHA-256 | `zipfile.is_zipfile` |
|---|---|---|---|---:|---|---|
| 2021-10-11 | EOD | `s` | 200 / `application/zip` | 7,768,558 | `cb0266e876b3ff5f071bd4032a7bb92d395ee15b64d5d7177b2d1b19ef72cc27` | `False` |
| 2021-11-01 | ID4 | `i5` | 200 / `application/zip` | 7,768,558 | `05e8970fc52e81e1f14b579490dfa28e5c489a150f730df5f32e378b8031a8b9` | `False` |
| 2021-12-01 | ID4 | `i5` | 200 / `application/zip` | 3,851,638 | `9c91e58ad48b3ff1e3ba34b0b4c54cc612cc0b169a1f70b4dd023e399cc59173` | `False` |
| 2021-12-30 | ID4 | `i5` | 200 / `application/zip` | 6,005,944 | `220eec6fe7359e8b20b9548431e6fb6f9c424d4f4c87a0bc6be694cde932e676` | `False` |

A 16-byte range request returned HTTP 206, `application/zip`, and the expected total length for every object. The exact `Content-Range` and first 16 bytes are preserved in the companion JSON. Each prefix starts `504b0304`, the ZIP local-file signature. That signature and media type are not sufficient integrity evidence: full central-directory validation fails.

Local-file-header parsing proves that these are truncated mid-deflate, not merely missing a trailing directory after otherwise complete member data:

| Date / suffix | Declared compressed bytes | Declared data end | Actual body end | Bytes short |
|---|---:|---:|---:|---:|
| 2021-10-11 `s` | 10,591,472 | 10,591,550 | 7,768,558 | 2,822,992 |
| 2021-11-01 `i5` | 11,745,450 | 11,745,530 | 7,768,558 | 3,976,972 |
| 2021-12-01 `i5` | 11,764,387 | 11,764,467 | 3,851,638 | 7,912,829 |
| 2021-12-30 `i5` | 10,222,972 | 10,223,052 | 6,005,944 | 4,217,108 |

No central-directory (`PK0102`) or end-of-central-directory (`PK0506`) signature occurs in the final 128 bytes of any affected body. The local member name, declared CRC32, declared compressed and uncompressed lengths, and exact offset arithmetic are retained in the JSON.

## Independent official-path equality

For each target date, a read-only request to `https://www.nseindia.com/api/reports` selected all six equity-derivatives SPAN categories. The affected inner member matched the static object exactly:

| Date / suffix | Reports API inner member | Inner CRC32 in outer pack | Same bytes and SHA as static | Other slot archives |
|---|---|---|---|---|
| 2021-10-11 `s` | `nsccl.20211011.s.zip` | `a0fddc49` | Yes | five valid |
| 2021-11-01 `i5` | `nsccl.20211101.i5.zip` | `e004d7ee` | Yes | five valid |
| 2021-12-01 `i5` | `nsccl.20211201.i5.zip` | `f398b4c5` | Yes | five valid |
| 2021-12-30 `i5` | `nsccl.20211230.i5.zip` | `f4022844` | Yes | five valid |

The reports API rebuilds its outer ZIP container, so the outer response hash is not treated as a stable identifier. The relevant invariant is the affected inner member's exact length and SHA-256 equality with the static official object.

This rules out the static path as a recovery fallback for the reports API payload: both current official public mechanisms expose the same malformed inner bytes.

The 20 sibling controls (five on each date) also match byte length and SHA-256 across the static and reports-API paths. Every sibling passes `zipfile.is_zipfile`, `ZipFile.testzip()`, has its expected sole SPN member and parses to XML root `spanFile`. Their exact sizes and SHA-256 values are retained in the companion JSON.

## Timestamp provenance

The reports API comparisons have exact UTC start/end timestamps from `2026-07-15T17:36:49.945Z` through `2026-07-15T17:41:42.651Z`. The static range reconfirmation has an exact group range of `2026-07-15T17:50:55.215Z` through `2026-07-15T17:50:57.760Z`.

The full-static probe output did not emit exact UTC timestamps. Those exact full-download times cannot be recovered and are intentionally left null rather than inferred or invented. The companion JSON separates that limitation from the exact range and API timestamps.

## Proof boundary and required closure

Proven:

- the four exact current official static objects exist and return HTTP 200 ZIP-typed payloads;
- their complete byte lengths and SHA-256 values are deterministic as recorded;
- all four fail Python ZIP central-directory validation;
- each reports API inner member is byte-for-byte identical to the corresponding static object; and
- the other five returned slot archives validate, isolating each failure to one inner object.

Not proven:

- that an affected archive was never generated or never published;
- that it was malformed at initial publication rather than replaced or damaged later; or
- that no valid member-only or member-retained copy exists.

Cell closure requires a valid archive republished by NSE Clearing, a byte-verifiable valid member/Extranet copy with authoritative provenance, or written NSE Clearing confirmation plus an approved source-boundary treatment. Until then these four cells block Phase 1 acceptance.
