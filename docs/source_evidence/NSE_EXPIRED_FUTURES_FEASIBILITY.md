# Expired NIFTY Futures Feasibility

Audit date: 2026-07-16

## Evidence classes

| Surface | Evidence | Verdict |
|---|---|---|
| Dhan active FUTIDX minute | Official `/v2/charts/intraday` contract says active instruments, 1-minute supported, five-year window and at most 90 days/request; near/next/far current IDs returned data. | Proven for active contracts only. |
| Dhan exact expired FUTIDX minute | Dhan has no documented expired-futures analogue of `/v2/charts/rollingoption`. Underlying ID 13 with expiry codes returned empty 2021 minute payloads; official exact ID 35007 returned empty for 2024-07-15. | Tested empty; no minute coverage promise. |
| Dhan exact expired FUTIDX daily | Official exact ID 35007 (`NIFTY24JULFUT`) plus expiry code 0 returned 21 daily OHLCV/OI rows for 2024-07-01 through 2024-07-30. | Proven for this exact expired contract at daily granularity. |
| Dhan relative FUTIDX daily | `/charts/historical` with current contract identities returned 2021 daily futures-shaped rows in bounded probes. | Data exists, but exact-contract versus relative-expiry/continuous semantics remain unproven. |
| NSE free exact-contract daily | NSE's Historical Contract-wise Price Volume Archive exposes `FUTIDX-NIFTY` by actual expiry with daily OHLC, LTP, settlement, contracts, turnover, OI, change in OI and underlying. | Proven daily exact-contract source. |
| NSE paid exact-contract tick | NSE Data & Analytics sells historical F&O Order & Trade data. Specification v1.18 defines F&O trade files as all trade ticks with jiffy time, symbol, instrument (`FUTIDX`), actual expiry, futures marker `FF`, trade price and trade quantity. | Proven source from which exact-contract one-minute OHLCV can be deterministically aggregated. |
| Historical one-minute OI | The all-trade-tick layout contains no OI field. The public pages do not prove retrospective minute-snapshot/OI availability for 2021–2026. | Unproven; obtain written NSE confirmation before promising it. |

## Official NSE paid route

The paid historical product is available for F&O through NSE's online platform. NSE states
that Historical Order & Trade data is available from January 2008 onward for F&O, so the
requested 2021-01-01 start is inside the advertised product history.

Specification v1.18 distinguishes:

- F&O Trim trade files: all trade ticks, available EOD;
- F&O Full trade files: all trade ticks, available after 30 calendar days;
- jiffy timestamps: 65,536 jiffies/second from 1980-01-01 midnight, with an explicit IST
  conversion procedure;
- contract identity: symbol, `FUTIDX`, actual `DDMMMYYYY` expiry, and `FF` futures marker;
- observations: transaction price and transaction quantity in contracts.

Therefore full tick coverage through approximately 2026-06-15 is within the stated 30-day
cooling period at the 2026-07-16 cutoff. Although Trim files are generated EOD, the public
materials do not say whether a new subscriber can buy the immediately preceding 30 days
retrospectively. Coverage for 2026-06-16 through 2026-07-15 requires written confirmation from
NSE Data & Analytics.

Aggregation is straightforward once licensed files exist: group regular-market `RM` trades by
`symbol=NIFTY`, `instrument=FUTIDX`, actual expiry and IST minute; compute first/max/min/last
trade price and sum trade quantity. This yields exact-contract one-minute OHLCV. It does not
yield minute OI.

## Dhan exact-ID probe boundary

An official NSE MII contract master for 2024-07-15 identifies `NIFTY24JULFUT` as
`FinInstrmId=35007`, `UndrlygFinInstrmId=26000`, `FinInstrmNm=FUTIDX`, with a 25-unit lot. The
ID is an official exchange identity and is a suitable candidate for a bounded Dhan
expired-instrument sentinel.
The authenticated Dhan probe must wait for the active rolling-options acquisition to reach a
safe terminal boundary, because that process owns the configured five-request/second budget.
The authenticated bounded probe returned a valid empty one-minute payload for 2024-07-15 and
21 daily rows for 2024-07-01 through 2024-07-30. Bronze hashes are respectively
`95bfee380c6a1b7a25df747018b1aea79f1b2c79eb46fea2040f1f9539f3d89e` and
`e82d385adabfa28f84905332b68ab14835fc8e66c64e3a8dc4bb0e180b52bc6d`;
the daily silver hash is
`385205d04b7692f98ad4c2b68b49adba733df2a8b1bc56b2876c81fe8b48f8db`.
This proves exact expired daily support for the tested contract, not expired minute support.

## Official sources

- Dhan historical data: https://dhanhq.co/docs/v2/historical-data/
- Dhan expired options (options only): https://dhanhq.co/docs/v2/expired-options-data/
- Dhan releases: https://dhanhq.co/docs/v2/releases/
- NSE paid EOD/historical data: https://www.nseindia.com/static/market-data/eod-historical-data-subscription
- NSE Historical Order & Trade Specification v1.18: https://nsearchives.nseindia.com/web/mediaattachment/2026-05/NSE_Hist_Order_Trade_Data_1.18_20260518122214.pdf
- NSE historical-data availability FAQ: https://nsearchives.nseindia.com/web/sites/default/files/inline-files/FPI%20FAQs_Brochure.pdf
- NSE free derivatives reports/contract-wise archive: https://www.nseindia.com/all-reports-derivatives
- Official 2024-07-15 F&O MII contract master used for the sentinel identity: https://nsearchives.nseindia.com/content/fo/NSE_FO_contract_15072024.csv.gz

NSE directs subscription questions to `marketdata@nse.co.in`. The exact request should ask for
F&O Full Trades for 2021-01-01 through 2026-06-15, retrospective Trim availability for
2026-06-16 through 2026-07-15, licensing terms for derived one-minute OHLCV, and any separately
available historical fixed-interval OI archive.
