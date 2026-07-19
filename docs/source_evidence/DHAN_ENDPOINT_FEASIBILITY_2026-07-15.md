# Dhan Endpoint Feasibility — 2026-07-15

No credential value, header, or token is stored in this document, request manifest, raw
response path, or committed artifact.

## Evidence matrix

| Surface | Official identity/contract | Live evidence | Verdict |
|---|---|---|---|
| NIFTY spot | ID 13, `IDX_I`, `INDEX`, `/charts/intraday` | Successful recent, 2021 boundary, and full monthly run | Feasible; requested lower bound 2021-01-01 returned |
| INDIA VIX | ID 21, `IDX_I`, `INDEX`, `/charts/intraday` | Pilot and 67-month run succeeded | Feasible from first returned 2021-08-04 10:00 IST; Jan–Jul 2021 returned empty |
| WEEK/MONTH rolling options | ID 13, `NSE_FNO`, `OPTIDX`, `/v2/charts/rollingoption` | Codes 1/2/3 accepted in bounded probes; code 0 rejected; code-1 ATM±10 pilot and historical run succeeded | Feasible rolling surface; response omits actual expiry |
| Active full chain | ID 13 plus exact active expiry, `/v2/optionchain` | 2026-07-21 snapshot returned 224 strikes, CE and PE records | Feasible for current snapshot, not historical expired chain |
| Current near/next/far FUTIDX | Current official-master IDs, `/charts/intraday` | All three returned 749 recent rows for 2026-07-14–15 | Feasible for current contracts |
| Expired FUTIDX through Dhan | No documented Dhan expired-futures minute endpoint | Official exact ID 35007 returned 21 July-2024 daily rows but a valid empty 2024-07-15 one-minute payload | Exact daily proven for the sentinel; expired minute unavailable in tested probe |
| Expired FUTIDX through NSE | Exact symbol/instrument/expiry in paid F&O all-trade-tick files | Official v1.18 layout proves tick time, price and quantity; free archive proves exact-contract daily OHLC/OI | Paid ticks can produce one-minute OHLCV; historical minute OI remains unproven |

## Official contracts and limits

- Intraday: active instruments, intervals including 1 minute, last five years, no more than
  90 days per request. The implementation uses calendar-month requests.
- Rolling options: last five years, no more than 30 days per request, WEEK/MONTH and
  CALL/PUT parallel arrays. Near-expiry index options document ATM±10.
- Active option chain: exact active expiry; one unique request per three seconds.
- Data APIs: 5 requests/second and 100,000 requests/day. The engine implements bounded fixed
  spacing, retry/backoff, an atomic daily budget, and the stricter option-chain key interval.

Official references:

- https://dhanhq.co/docs/v2/expired-options-data/
- https://dhanhq.co/docs/v2/historical-data/
- https://dhanhq.co/docs/v2/option-chain/
- https://dhanhq.co/docs/v2/instruments/
- https://dhanhq.co/docs/v2/annexure/
- https://docs.dhanhq.co/api/v2/guides/annexure
- https://dhanhq.co/docs/v2/
- https://dhanhq.co/docs/v2/releases/
- https://www.nseindia.com/static/market-data/eod-historical-data-subscription
- https://nsearchives.nseindia.com/web/mediaattachment/2026-05/NSE_Hist_Order_Trade_Data_1.18_20260518122214.pdf
- https://www.nseindia.com/all-reports-derivatives

## Exact identity evidence

The 2026-07-15 detailed official master was fetched from
`https://images.dhan.co/api-data/api-scrip-master-detailed.csv`, SHA-256
`9065b1b9ea25108948d9452638a90644c8ef0b0e8c39d713bbb3974c69b9cc96`.
Its exact INDIA VIX row identifies `NSE,I,21,...,INDEX,21,INDIA VIX,...,India VIX,INDEX`.
Therefore the request is security ID 21, not NIFTY ID 13.

The current compact master also resolved NIFTY ID 13 and dated active FUTIDX IDs 61093,
58072, and 68407 for the July/August/September 2026 contracts. These are a dated current
snapshot and are never used to infer expired identities.

## Live boundary and pilot evidence

The valid bounded probe root is `reports/dhan_phase2_probe_valid_20260715`: 22 manifests,
15 data completions, one metadata-only completion, two code-0 failures, 9,173 retained rows.
The rolling boundary probes returned 2021-01-01 09:15 IST, so the requested lower bound is
inside the provider's actual returned window. No earlier date was requested; the exact source
boundary before 2021-01-01 is not claimed.

The rolling ATM±10 pilot root is `reports/dhan_phase2_atm10_pilot_20260715`: 84/84 requests,
31,495 retained rows for 2026-07-14, all 21 labels for WEEK/MONTH and CALL/PUT. Dhan returned
the documented `toDate` day as well; the normalizer enforced the local half-open request
window and recorded 31,485 dropped provider rows in manifests. Actual returned strikes ranged
from 23,550 to 24,650 and changed as ATM moved. This proves a rolling moneyness surface, not
an absolute-strike chain.

INDIA VIX pilot: one request, 374 rows, 2026-07-14 09:16–15:29 IST, with separate bronze and
silver hashes. The complete 67-month request run retained 524,077 rows. January through July
2021 returned empty; the first row was 2021-08-04 10:00 IST. That observed gap is not filled.

## Required negative claims

- Rolling ATM±10 data is not an absolute-strike full chain.
- Rolling responses do not identify actual expiry; no BSM row may guess it.
- Expired futures minute availability through Dhan remains unproven. Exact expired-contract
  minute OHLCV is available from licensed NSE all-trade-tick files; minute OI is not proven.
- Spot is never used to fabricate futures.
- SPAN enrichment and final gold remain pending the separate audited Phase 1 deliverable.
