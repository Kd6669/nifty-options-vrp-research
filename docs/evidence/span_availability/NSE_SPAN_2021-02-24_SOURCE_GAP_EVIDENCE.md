# NSE SPAN source gap — 24 February 2021

## Disposition

`STILL_UNRESOLVED_HISTORICAL_NONPUBLICATION`

The four intraday files are proven unavailable through both current official public mechanisms, and the pattern is strongly consistent with NSE Clearing's documented outage. Historical nonpublication is **not** proven: no located official document states that `i2` through `i5` were never generated or published.

Recommended descriptive classification: `official_public_archive_unavailable_documented_incident`.

This evidence is deliberately not an availability-import record. It does not modify `reviewed_import_2021_2026.json`, create availability events, or set `source_availability_boundary_proven=true`.

The deterministic companion record is `NSE_SPAN_2021-02-24_SOURCE_GAP_EVIDENCE.json` using schema `span-source-gap-evidence/v1`.

## Existing immutable run evidence

The authoritative append-only download manifest is:

`data/span/full_20210101_20260715/manifests/download.jsonl`, lines 427–432.

One six-category request to `https://www.nseindia.com/api/reports` returned HTTP 200 `application/zip`, 20,152,723 bytes, with body SHA-256:

`50426777a2e79e646d96b68b91ee2fca1c12af7b22883c3af0e39518d2de2f68`

The outer pack returned only `i1` and `s`. The four other terminal manifest cells are:

| Slot | Suffix | State |
|---|---|---|
| ID1 | `i2` | `slot_not_returned` |
| ID2 | `i3` | `slot_not_returned` |
| ID3 | `i4` | `slot_not_returned` |
| ID4 | `i5` | `slot_not_returned` |

The exact request parameters and the UTF-8 byte hash of the `archives` JSON string are preserved in the companion JSON. The six-category `archives` value is 764 bytes with SHA-256 `89046931925347834c7f3a33adee440bc8eee7d874142e013c5cf07d98360b07`.

## Retained raw archives

| Slot | File | Bytes | SHA-256 | Inner member | Inner CRC32 | Compressed / uncompressed | ZIP integrity |
|---|---|---:|---|---|---|---:|---|
| BOD | `nsccl.20210224.i1.zip` | 10,156,427 | `22f72f91c52e32f4b05142ad65ec2a6709ccc554f0d9386c3e0bce33df5d4ac4` | `nsccl.20210224.i01.spn` | `afcb560f` | 10,156,233 / 61,531,511 | `ZipFile.testzip() = None` |
| EOD | `nsccl.20210224.s.zip` | 9,996,040 | `66140ceaf11b914816e12d9864404376d85b7d168c981d0d793aed4204b886b3` | `nsccl.20210224.s.spn` | `e91c45c9` | 9,995,850 / 61,542,449 | `ZipFile.testzip() = None` |

The outer-pack member CRC32 values recorded by the downloader are `f17073d0` for the retained `i1` ZIP and `2852a55d` for the retained `s` ZIP.

Both archives extracted successfully. `data/span/full_20210101_20260715/manifests/extraction.jsonl`, lines 223–224, records 2,275 NIFTY rows per slot: 1,095 CE, three FUT and 1,177 PE.

## Official reports API recheck

A separate in-memory request selected only the four intraday categories. It warmed `https://www.nseindia.com`, waited 2.5 seconds, then requested:

`GET https://www.nseindia.com/api/reports`

The request used the exact `Accept`, `Accept-Language`, `Connection`, `Referer` and Chrome 124 `User-Agent` headers preserved in the companion JSON.

Timestamp provenance limitation: the interactive recheck did not emit an exact UTC timestamp. The investigation occurred in the 2026-07-15 session, but its exact observation time cannot be recovered and is intentionally left null rather than inferred.

Parameters:

- `date=24-Feb-2021`
- `type=Archives`
- `archives=<the exact compact four-category JSON preserved in the companion file>`

The compact `archives` value is 481 UTF-8 bytes, SHA-256 `46ebda2e95a855b63376193d85e4181630864d2ea4d40cb081ace26ba9f1497a`.

Response:

- HTTP `404`
- `application/json; charset=utf-8`
- 68 bytes
- first 16 bytes `7b226572726f72223a224e6f7420466f`
- SHA-256 `a859901d666e8265f6f4f3a9bf12a8d3e27e9263fa76fbab71bfdf95aa19340b`
- body `{"error":"Not Found, may be some files are unavailable","show":true}`

## Official static archive paths

The official convention was first checked against a known published file. A range request for bytes 0–15 to:

`https://nsearchives.nseindia.com/archives/nsccl/span/nsccl.20260709.i2.zip`

returned HTTP 206 `application/zip`, ZIP magic, and `Content-Range: bytes 0-15/9694422`.

All static probes used method `GET` with `Range: bytes=0-15`, `Accept: */*` and the exact Chrome 124 `User-Agent` preserved in the companion JSON. Their individual exact UTC timestamps were not emitted by the interactive probe. The investigation occurred in the 2026-07-15 session, but the exact times cannot be recovered and are intentionally not invented; every result explicitly references this group-level timestamp limitation.

The same conservative 16-byte range request was then used for the six target-date paths:

| Slot | Exact URL | Status | Content range / length | Signature | Response SHA-256 |
|---|---|---:|---|---|---|
| BOD | `https://nsearchives.nseindia.com/archives/nsccl/span/nsccl.20210224.i1.zip` | 206 | `bytes 0-15/10156427` | `504b03041400000008008da057520f56` | `94f55f0816cedde15fc4715aa19bf33ac2af1170edef19300a263c9c8d8c5227` |
| ID1 | `https://nsearchives.nseindia.com/archives/nsccl/span/nsccl.20210224.i2.zip` | 404 | 3,815 bytes | `3c21444f43545950452068746d6c3e0d` | `a7bb36b894dc0a4db8dca1c046711db8a7c2710dd15475163128a6800edee37f` |
| ID2 | `https://nsearchives.nseindia.com/archives/nsccl/span/nsccl.20210224.i3.zip` | 404 | 3,815 bytes | `3c21444f43545950452068746d6c3e0d` | `a7bb36b894dc0a4db8dca1c046711db8a7c2710dd15475163128a6800edee37f` |
| ID3 | `https://nsearchives.nseindia.com/archives/nsccl/span/nsccl.20210224.i4.zip` | 404 | 3,815 bytes | `3c21444f43545950452068746d6c3e0d` | `a7bb36b894dc0a4db8dca1c046711db8a7c2710dd15475163128a6800edee37f` |
| ID4 | `https://nsearchives.nseindia.com/archives/nsccl/span/nsccl.20210224.i5.zip` | 404 | 3,815 bytes | `3c21444f43545950452068746d6c3e0d` | `a7bb36b894dc0a4db8dca1c046711db8a7c2710dd15475163128a6800edee37f` |
| EOD | `https://nsearchives.nseindia.com/archives/nsccl/span/nsccl.20210224.s.zip` | 206 | `bytes 0-15/9996040` | `504b03041400000008003ca45852c945` | `c34e8815a3b863981fd59bf04c68496457738ff18a1fe6a6604964235d365d0f` |

The positive target-date prefixes and total sizes exactly match the retained `i1` and `s` files. This demonstrates that the path convention is valid and that the official archive distinguishes present same-date objects from missing ones.

## Official incident and schedule evidence

NSE Clearing's [Annual Report 2020-21](https://www.nseclearing.in/sites/default/files/disclosure/Annual_Report_2020_21_NSE_Clearing_Limited.pdf), section 2.3, PDF text pages P15–P16 and printed report pages 12–14, records that:

- its Risk and Clearing & Settlement systems were unavailable;
- NSE halted trading at 11:40 because Risk Management was unavailable;
- trading resumed at 15:30 and closed at 17:00; and
- the primary risk-management system was not operational between 10:06 and 15:30.

NSE Clearing's [FAQ on Risk Management](https://www.nseclearing.in/sites/default/files/2025-07/NCL%20-%20FAQ%20RISK%20MANAGEMENT.pdf), Q2 on printed page 3, says that equity-derivatives SPAN files are based on prices at 11:00, 12:30, 14:00, 15:30, EOD and BOD and are run shortly thereafter.

The missing `i2`, `i3`, `i4` and `i5` pattern is therefore strongly consistent with the documented primary RMS outage. That is an inference from two official documents plus the file evidence; neither document says explicitly that these four files were never generated or published.

## Proof boundary and required closure

Proven:

- the retained six-category response returned only `i1` and `s`;
- the current official API exposes none of the four intraday categories for this date;
- exact official static paths for `i2`–`i5` return 404 while same-date `i1` and `s` return ZIP bytes; and
- the primary RMS outage spans the four scheduled intraday runs.

Not proven:

- that the files were never generated;
- that they were never published through member-only or another official channel; or
- that they did not once exist and later become unavailable.

Definitive closure requires either written NSE Clearing confirmation or recovery/authoritative absence evidence from the member Extranet location `/FAOFTP/FAOCOMMON/Parameter`.
