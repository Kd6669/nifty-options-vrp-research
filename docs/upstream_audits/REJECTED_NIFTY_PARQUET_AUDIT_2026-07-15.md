# Audit: `NIFTY (1).parquet`

Audit date: 2026-07-15 (Asia/Kolkata)
Audited file: `<external-data-root>\NIFTY (1).parquet`
Audit mode: read-only; the source Parquet was not modified

## Executive verdict

**FAIL for direct research, backtesting, cost modelling, or live-parity use.**

The Parquet container is healthy and fully readable, and its OHLC fields pass basic price invariants. The dataset nevertheless has material semantic defects. The most serious is that **899,395 rows across 115 sessions from 2025-08-29 through 2026-02-12 retain Thursday expiries after NIFTY weekly expiries moved to Tuesday**. That makes the embedded contract identity suspect and makes `Minutes_to_Expiry`, `DTE`, IV, and Greeks unsafe for that period.

Other important defects are conflicting duplicate keys, timestamp-level spot disagreement, 16 signed-integer volume overflows, negative synthetic bids, an incomplete final session on 2026-02-12, and a disconnected 2026-03-20 block with stale lot-size and STT metadata.

The file is an exact duplicate of the existing canonical-path file `<external-data-root>\NIFTY.parquet`; fixing only one copy would leave the other equally affected.

## File identity and structural integrity

| Check | Result |
|---|---:|
| Size | 1,660,734,376 bytes (1.547 GiB) |
| SHA-256 | `d520cd840c92a87f389950ef24aa98e91bd31b661926b94dd028224371293353` |
| Exact duplicate | Byte-for-byte identical to `<external-data-root>\NIFTY.parquet` |
| Parquet version | 2.6 |
| Producer | `parquet-cpp-arrow version 22.0.0` |
| Compression | SNAPPY on all 41 columns |
| Row groups | 10 |
| Rows | 9,782,931 |
| Columns | 41 |
| Full scan | Passed; all columns and row groups were readable |
| Physical order | Globally nondecreasing by parsed timestamp and strike |
| Declared sort metadata | None |

The file is not truncated or page-corrupt. The problems below are data/content problems, not Parquet-container corruption.

## Coverage and schema

- Parsed date range: 2021-02-08 through 2026-03-20.
- Distinct session dates: 1,246.
- Distinct minute timestamps: 465,670.
- Ticker: only `NIFTY`.
- Strike range: 13,650 to 26,850; all strikes are multiples of 50.
- Distinct strikes: 265.
- `date` and `time` are stored as strings, not temporal types.
- `Next_Expiry` is a timezone-naive timestamp.
- Price and model columns are doubles; volume/OI/strike/lot are 64-bit integers in the final Parquet schema.
- The schema mixes observed-looking fields (OHLC, volume, OI) with derived fields (IV, Greeks, slippage, bid/ask) without provenance/version metadata.

Annual coverage:

| Year | Rows | Sessions | Minute timestamps | First date | Last date |
|---:|---:|---:|---:|---|---|
| 2021 | 1,756,409 | 223 | 83,404 | 2021-02-08 | 2021-12-31 |
| 2022 | 1,948,533 | 248 | 92,784 | 2022-01-03 | 2022-12-30 |
| 2023 | 1,933,556 | 246 | 92,099 | 2023-01-02 | 2023-12-29 |
| 2024 | 1,945,352 | 249 | 92,663 | 2024-01-01 | 2024-12-31 |
| 2025 | 1,955,519 | 249 | 93,121 | 2025-01-01 | 2025-12-31 |
| 2026 | 243,562 | 31 | 11,599 | 2026-01-01 | 2026-03-20 |

## Critical findings

### 1. Wrong expiry regime after the NSE Tuesday migration

NSE circular NSE/FAOP/68747 introduced NIFTY weekly Tuesday expiries beginning with 2025-09-02. The first affected source/trade date in this file is 2025-08-29, immediately after the last Thursday expiry on 2025-08-28.

The file instead continues assigning Thursday expiries through 2026-02-12:

- Affected rows: **899,395** (9.19% of the full dataset).
- Affected sessions: **115**.
- Embedded expiry range: 2025-09-04 through 2026-02-12, all Thursdays.
- Example: rows on 2025-09-02 point to 2025-09-04 with `DTE` 2.00–2.26 even though the new weekly contract expired on Tuesday 2025-09-02.
- The 2026-03-20 block suddenly uses Tuesday 2026-03-24, indicating a separate/newer generation path.

Impact:

- `Next_Expiry` is wrong for the affected period.
- `Minutes_to_Expiry` and `DTE` are wrong by contract construction.
- IV and all ten Greek fields are derived using the wrong time to expiry and must be recomputed.
- Without the original instrument symbol/token, it is not possible to prove whether OHLC belongs to the real Tuesday contract and was merely mislabelled, or whether the contract-price series itself was assembled incorrectly.

Primary source: [NSE circular NSE/FAOP/68747](https://nsearchives.nseindia.com/content/circulars/FAOP68747.pdf).

### 2. Conflicting duplicate keys and inconsistent spot

Using `(date, time, Ticker, Strike)` as the natural minute-strike key:

- Total rows: 9,782,931.
- Distinct keys: 9,776,095.
- Duplicate excess: **6,836 rows** across 6,600 duplicate groups; every duplicate group had conflicting spot values.
- Duplicate excess is concentrated in 2021: 5,618 rows in 2021, 627 in 2022, 498 in 2023, 44 in 2024, and 49 in 2025.
- No duplicate excess was found in 2026.
- **6,669 timestamps** (1.43% of all timestamps) have more than one spot value across strikes.
- Maximum same-minute spot disagreement: **78.60 NIFTY points**.

Concrete example for 2021-02-18 15:11, strike 14,700: four records exist with spot values 15,121.5, 15,123.5, 15,125.1, and 15,126.4. The records also split CE/PE availability and contain different closes, so `drop_duplicates(keep="first")` is not a defensible repair.

Impact:

- Minute-level features can use different underlying prices for different strikes.
- Cross-strike IV surfaces, parity, moneyness, Greeks, and portfolio aggregation become internally inconsistent.
- A deterministic merge requires either a trusted underlying minute series or re-ingestion from source data.

### 3. 2026 tail is discontinuous and internally inconsistent

- 2026-02-12 ends at 15:02 instead of 15:29/15:30: **28 closing minute slots are missing**.
- There is then a **36-calendar-day data gap** from 2026-02-12 to 2026-03-20.
- 2026-03-20 is a full 376-minute block, but it reverts to `Lot_Size=75` and `STT_Rate=0.0005`.
- NSE had revised the NIFTY lot from 75 to 65 for weekly/monthly contracts after the 2025-12-30 expiry, so 65 is the applicable lot for March 2026.
- Options-sale STT remained 0.1% (`0.001`) through March 2026; the increase to 0.15% only took effect from 2026-04-01. Therefore `0.0005` on 2026-03-20 is stale by two rate regimes.
- The 2026-03-20 price block has float32 fingerprints cast into doubles: 7,402 rows miss an exact 0.05 tick test only by at most 0.00048828125. This is numerical representation noise, not genuine off-tick trading, but it confirms a different upstream source/serialization path.

Primary sources:

- [NSE lot-size circular NSE/FAOP/70616](https://nsearchives.nseindia.com/content/circulars/FAOP70616.pdf).
- [Income Tax Department Finance Bill 2024 memorandum](https://incometaxindia.gov.in/budgets%20and%20bills/2024/memo-2024.pdf).
- [Income Tax Department Budget 2026 STT FAQ](https://incometaxindia.gov.in/Documents/Budget2026/FAQs-Budget-2026.pdf).

### 4. Synthetic bid/ask fields can be negative

For every active leg tested:

- `Bid = Close - Slippage` exactly.
- `Ask = Close + Slippage` exactly.
- No active row deviates from these identities beyond 1e-9.

Therefore `CE_Bid`, `CE_Ask`, `PE_Bid`, and `PE_Ask` are generated quotes, not observed order-book quotes.

The formula creates:

- 38,714 negative CE bids.
- 33,328 negative PE bids.
- **72,039 rows** with at least one negative bid.
- Minimum generated bid: approximately -0.11553 while the corresponding option close is 0.05.

Impact:

- These fields must not be used as historical executable quotes.
- Fill simulation, spread analysis, and transaction-cost estimation using these columns will inherit a modelling assumption and can generate impossible prices.

### 5. Signed 32-bit overflow in volume

There are **16 negative volume rows**: nine CE and seven PE. Values are near -4.25 billion, a classic signed 32-bit wraparound signature.

Adding `2^32 = 4,294,967,296` produces positive values from 6,074,625 to 50,161,425, and all 16 repaired values are exact multiples of the row lot size. No negative OI was found.

These rows occur from 2025-08-14 through 2025-09-30. They should be repaired explicitly and tagged; silent clipping to zero would discard genuine high volume.

## High/medium findings

### 6. IV clipping and unstable near-expiry Greeks

- CE IV floor `0.1`: 881,679 rows (9.03% of active CE legs).
- PE IV floor `0.1`: 823,460 rows (8.43% of active PE legs).
- CE IV cap `500`: 2,714 rows.
- PE IV cap `500`: 2,489 rows.
- CE IV above 100: 147,835 rows; PE IV above 100: 132,405 rows.
- `abs(Theta) > 1,000`: 50,610 CE rows and 37,850 PE rows.
- Maximum absolute theta is about 20,686 per day-equivalent convention used by the generator.

The largest theta values occur when `DTE=0`, `Minutes_to_Expiry=6`, and IV approaches/caps at 500. This shows that the generator applies a minimum time-to-expiry floor, not literal time remaining. Such rows may be numerically explainable but are not robust model inputs without an explicit expiry-day policy.

### 7. One special session points to an already expired contract

All 1,276 rows in the 2021-11-04 evening session (18:15–19:15) have `Next_Expiry` earlier than the row timestamp. Their non-negative MTE is a clamp/derivation artifact, not real time remaining.

That special session must be remapped to the correct next tradable expiry and all expiry-derived fields recomputed.

### 8. Strike-count irregularities

- Expected common surface: 21 distinct strikes per timestamp.
- Timestamps with a count other than 21: **1,234** (0.265%).
- Range: 2 to 23 distinct strikes.
- Relative to a fixed 21-strike expectation, 3,337 distinct minute-strike keys are missing.
- Duplicate/extra rows also produce 6,779 rows above the 21-row expectation across timestamps.

Some of these are sparse illiquid contracts or ingest-window transitions; they still require explicit completeness flags before surface-based modelling.

### 9. Explicit `Is_Jio_Anomaly` block

`Is_Jio_Anomaly=True` for every row on 34 complete sessions from 2023-07-20 through 2023-09-06: **268,319 rows**.

The field is useful, but the dataset has no metadata explaining the intended treatment. A consumer must decide whether to exclude, isolate, or specially model these dates; silently mixing them into training defeats the purpose of the flag.

## Positive checks

- Zero invalid `date` or `time` strings under `%d-%m-%Y` and `%H:%M` parsing.
- Zero nulls in key fields, spot, OHLC, volume, OI, lot, STT, expiry, MTE, or DTE.
- No negative option OHLC prices.
- No CE or PE OHLC ordering violations (`High` contains open/low/close and `Low` contains open/high/close).
- No crossed generated markets (`Bid > Ask`).
- No out-of-range deltas, negative vegas, or negative gammas.
- All 13,840 CE model/quote null sets correspond exactly to all-zero CE legs.
- All 15,346 PE model/quote null sets correspond exactly to all-zero PE legs.
- There are no rows where both CE and PE are entirely missing.
- Strike grid is internally consistent at 50-point intervals.
- Lot-size transitions through 2026-02-12 match the relevant NSE circular regimes: 75→50, 50→25, 25→75, and 75→65. The isolated 2026-03-20 reversion is the defect.
- STT transitions through 2026-02-12 match 0.05%→0.0625% on 2023-04-01 and 0.0625%→0.1% on 2024-10-01. The isolated 2026-03-20 reversion is the defect.

Relevant primary notices:

- [NSE 2021 lot revision 75→50](https://nsearchives.nseindia.com/content/circulars/FAOP47854.pdf)
- [NSE 2024 lot revision 50→25](https://nsearchives.nseindia.com/content/circulars/FAOP61415.pdf)
- [NSE 2024 lot revision 25→75](https://nsearchives.nseindia.com/content/circulars/FAOP64625.pdf)
- [Income Tax Department circular: options STT 0.05%→0.0625% from 2023-04-01](https://incometaxindia.gov.in/communications/circular/circular-1-2024.pdf)

## Special-session observations

The unusual evening/weekend dates are not automatically corrupt:

- 2021-11-04, 2022-10-24, 2023-11-12, 2024-11-01, and 2025-10-21 have special evening/afternoon session shapes.
- 2024-01-20, 2024-03-02, 2024-05-18, 2025-02-01, and 2026-02-01 are weekend sessions.
- The split-session gaps on 2024-03-02 and 2024-05-18 are visible as 89 and 90 internal clock-minute gaps respectively and should be evaluated against the official special-session schedule, not a normal 09:15–15:30 template.

The 2021-11-04 expiry mapping remains wrong even if the special session itself is legitimate.

## Required repair contract

Do not overwrite the only source copy. Build a versioned cleaned dataset and a row-level audit manifest.

1. **Recover contract identity**
   - Join each row to an official NSE instrument/contract master using actual symbol/token and trade date.
   - Correct Tuesday expiries from 2025-08-29 onward.
   - Validate whether the OHLC series is for the real Tuesday contract before retaining it.

2. **Recompute all dependent fields**
   - Recompute `Next_Expiry`, `Minutes_to_Expiry`, `DTE`, IV, delta, gamma, theta, vega, and rho from the corrected contract.
   - Record model version, rate, dividend/carry assumption, IV solver bounds, and expiry-time convention.

3. **Resolve duplicate keys without arbitrary first/last selection**
   - Obtain one authoritative spot per timestamp from the NIFTY underlying minute series.
   - Re-ingest duplicate option rows where possible.
   - If re-ingestion is impossible, quarantine conflicting keys or use a documented leg-level merge rule and preserve all source rows in an exceptions table.

4. **Repair integer overflow**
   - For the 16 identified negative volume cells, add `2^32` and tag `volume_overflow_repaired=true`.
   - Revalidate against source volume if available.

5. **Separate observed and modelled fields**
   - Rename generated quotes to `CE_Bid_Model`, `CE_Ask_Model`, etc.
   - Clamp modelled bids to at least the exchange tick only if the downstream model explicitly requires it; do not misrepresent clamped values as observed quotes.

6. **Quarantine incomplete/disconnected tail data**
   - Mark 2026-02-12 incomplete.
   - Do not bridge the 2026-02-12→2026-03-20 gap.
   - Correct 2026-03-20 to lot 65 and STT 0.1%, then validate the separate source block before merging.

7. **Publish explicit quality flags**
   - `duplicate_conflict`, `spot_conflict`, `surface_incomplete`, `expiry_regime_invalid`, `volume_overflow`, `synthetic_quote`, `iv_floor`, `iv_cap`, `special_session`, `session_incomplete`, and the existing `Is_Jio_Anomaly`.

8. **Acceptance gates for a cleaned release**
   - Unique `(timestamp, ticker, strike, expiry)` key.
   - Exactly one authoritative spot per timestamp.
   - Official contract expiry and lot-size lookup succeeds for every row.
   - Zero negative volume/OI and zero impossible observed bids.
   - Session completeness checked against the official NSE calendar, including special sessions.
   - Recomputed IV/Greeks carry model/version metadata and pass bounded residual tests.
   - Old and cleaned Parquet hashes plus a transformation manifest are retained.

## Usage recommendation

- **Do not use 2025-08-29 through 2026-02-12 for expiry-sensitive research until contract identity is validated and all derived fields are recomputed.**
- **Do not use the 2026-03-20 block until lot/STT/source-provenance corrections are made.**
- Treat `Bid`, `Ask`, and `Slippage` as synthetic model outputs across the entire file.
- For pre-2025-08-29 research, first repair/quarantine duplicates, enforce one spot per minute, repair the 16 overflowed volumes, and apply a documented policy for special sessions and IV clipping.
- Preserve this file under its SHA-256 as the immutable raw input; produce a new cleaned filename rather than editing in place.

## Audit implementation notes

The audit used PyArrow metadata inspection and DuckDB full-column scans. Checks included schema and row-group readability, hashes, parsed date/time validity, uniqueness, row ordering, session shapes, temporal coverage, strike grids, spot consistency, OHLC invariants, null/zero-leg patterns, volume/OI sign and lot multiples, quote identities, IV clipping, Greek bounds/extremes, expiry/MTE residuals, lot/STT transitions, and primary-source comparison against NSE and Income Tax Department notices.
