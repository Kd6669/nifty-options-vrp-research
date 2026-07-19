# NSE NIFTY contract-rule history (2021-01-01 through 2026-07-15)

Status legend used here and in the machine-readable artifacts:

- **Implemented** — encoded as an effective-dated rule or mapping dimension.
- **Validated** — checked against official NSE/NSE Clearing/Dhan primary evidence.
- **Scraped** — source bytes were fetched and SHA-256 recorded.
- **Blocked** — evidence is insufficient; the value must stay null or out of BSM.

## Scope and evidence standard

This dimension covers NIFTY (`FUTIDX`/`OPTIDX`) expiry rules, actual expiries,
market lots, option tick size, trading unit and the expiry cutoff needed for
minute-to-expiry. Official NSE/NSE Clearing evidence is controlling for exchange
contracts. The official DhanHQ v2 documentation is used only to interpret the
rolling endpoint's `expiryFlag` and `expiryCode`.

An actual expiry is **not** accepted from a weekday calculation alone. For each
scheduled Thursday (through August 2025) or Tuesday (from September 2025), the
generator searches the official F&O bhavcopy on that day and up to seven days
back. A row is accepted only if the bhavcopy contains NIFTY option records whose
contract expiry equals that bhavcopy's trade date. The scheduled weekday is
retained separately so holiday moves are auditable.

The source manifest stores the exact URL, byte count and SHA-256 for each saved
circular and every bhavcopy used as evidence. Bhavcopy archives are hashed but
not retained locally; the small circular/API-document source files are retained
under `docs/nse_rules/sources/`.

## Implemented rule timeline

### Expiry weekday

| Contract expiries | Rule | Holiday rule | Primary evidence |
|---|---|---|---|
| 2021-01-01 through 2025-08-31 | Weekly Thursday; monthly/quarterly/half-yearly last Thursday | Previous trading day | [NSE/FAOP/65336](https://nsearchives.nseindia.com/content/circulars/FAOP65336.pdf) confirms no NIFTY change; [NSE/FAOP/67338](https://nsearchives.nseindia.com/content/circulars/FAOP67338.pdf) deferred the proposed Monday change; [NSE/FAOP/68747](https://nsearchives.nseindia.com/content/circulars/FAOP68747.pdf) preserves expiries through August 2025. |
| On/after 2025-09-01 | Weekly Tuesday; monthly/quarterly/half-yearly last Tuesday | Previous trading day | [NSE/FAOP/68589](https://nsearchives.nseindia.com/content/circulars/FAOP68589.pdf), [NSE/FAOP/68685](https://nsearchives.nseindia.com/content/circulars/FAOP68685.pdf), final operational update [NSE/FAOP/68747](https://nsearchives.nseindia.com/content/circulars/FAOP68747.pdf). |

The March 2025 proposal in
[NSE/FAOP/66938](https://nsearchives.nseindia.com/content/circulars/FAOP66938.pdf)
would have moved NIFTY to Monday. It is recorded as a superseded proposal, not
an effective rule, because NSE/FAOP/67338 deferred it before implementation.

### Weekly availability

[NSE/FAOP/64506](https://nsearchives.nseindia.com/content/circulars/FAOP64506.pdf)
made NIFTY the only NSE benchmark index retaining weekly options from
2024-11-20. It discontinued weekly BANKNIFTY, MIDCPNIFTY and FINNIFTY contracts;
it did **not** discontinue NIFTY weekly options.

### Market lot by contract expiry, not observation date

| Expiry family | Old lot and last applicable expiry | New lot and first applicable expiry | Primary evidence |
|---|---|---|---|
| Monthly 2021 | 75 through June 2021 | 50 from July 2021 | [NSE/FAOP/47854](https://nsearchives.nseindia.com/content/circulars/FAOP47854.pdf) |
| Weekly 2021 | 75 through July weekly expiries | 50 from August 2021 weekly expiries | NSE/FAOP/47854 |
| NIFTY 2024 transition | 50 through the preserved 2024-04-25 monthly expiry | 25: first weekly 2024-05-02; first monthly 2024-05-30 | [NSE/FAOP/61415](https://nsearchives.nseindia.com/content/circulars/FAOP61415.pdf) |
| Weekly 2024/25 transition | 25 through 2024-12-19 | 75 from 2025-01-02 | [NSE/FAOP/64625](https://nsearchives.nseindia.com/content/circulars/FAOP64625.pdf) |
| Monthly 2024/25 transition | 25 through 2025-01-30 | 75 from 2025-02-27 | NSE/FAOP/64625 |
| Weekly 2025/26 transition | 75 through 2025-12-23 | 65 from 2026-01-06 | [NSE/FAOP/70616](https://nsearchives.nseindia.com/content/circulars/FAOP70616.pdf) |
| Monthly 2025/26 transition | 75 through 2025-12-30 | 65 from 2026-01-27 | NSE/FAOP/70616 |

Long-dated option transitions are also recorded in the rule-history JSON:
existing NIFTY contracts over three months moved 75 to 50 after the June 2021
expiry; existing quarterly/half-yearly contracts moved 25 to 75 at
2024-12-26 EOD and 75 to 65 at 2025-12-30 EOD. These rules must not be
collapsed into an observation-date lookup.

### Tick, trading unit and expiry timestamp

- **Validated:** the official [NIFTY 50 F&O specification](https://www.nseindia.com/static/products-services/equity-derivatives-nifty50)
  states a NIFTY index-option price step of Rs. 0.05 and that futures and options
  use the same permitted lot for a given underlying.
- **Validated:** NSE's [Equity Derivatives market timings](https://www.nseindia.com/static/market-data/market-timings)
  close the normal market at 15:30 IST. NSE Clearing says final option exercise
  is determined at close of trading hours on expiry day and the final index
  settlement price uses the underlying index close on the last trading day:
  [settlement mechanism](https://www.nseclearing.in/clearing-settlement/equity-derivatives/settlement-mechanism),
  [settlement price](https://www.nseclearing.in/clearing-settlement/equity-derivatives/settlement-price).
- **Implemented:** `actual_expiry_timestamp_ist` is therefore the proven actual
  expiry date at `15:30:00+05:30` for regular sessions.
- **Blocked:** no separate point-value contract multiplier was found in primary
  evidence. The trading unit is one market lot; `contract_multiplier` remains
  null rather than being guessed.
- **Blocked:** a complete effective-dated historical tick schedule is not
  proven. The current Rs. 0.05 option tick is retained as a medium-confidence,
  current-only fact in the narrative rule history; point-in-time calendar/rule
  dimension rows keep `tick_size` null rather than extrapolating it backward.

Exceptional shortened/special sessions are not silently assigned 15:30. They
require a dated session override and remain in the unresolved table until an
official circular is attached.

## Dhan `expiryCode=1` mapping

The current linked [DhanHQ v2 Annexure](https://dhanhq.co/docs/v2/annexure/)
defines `0 = Current/Near`, `1 = Next`, and `2 = Far`. The official
[Expired Options Data](https://dhanhq.co/docs/v2/expired-options-data/) request
uses an explicit `expiryFlag` (`WEEK` or `MONTH`) and its example uses
`expiryCode: 1`.

Accordingly, the code-1 dimension selects the **second** eligible actual expiry
on or after each trade date within the exact flag:

- `WEEK`: second future `weekly` expiry; monthly expiries are excluded.
- `MONTH`: second future `monthly` expiry.

Only bhavcopy-proven actual expiries are eligible. A trade date with fewer than
two proven eligible future contracts is emitted as a blocked exception, not
mapped by weekday extrapolation. A conflicting search/redirect snippet that
called code 1 “near” is documented as a discovery discrepancy; it is not used
because the linked v2 Annexure explicitly calls code 1 “Next Expiry.”

## Machine-readable artifacts

- `docs/nse_rules/nse_contract_rule_history.json` — narrative effective-dated rules.
- `docs/nse_rules/nse_contract_rule_dimension.{json,parquet}` — enrichment-ready lot/rule intervals.
- `docs/nse_rules/nse_actual_expiry_calendar.{json,parquet}` — bhavcopy-proven actual expiries and active future expiries visible at the 2026-07-15 cutoff.
- `docs/nse_rules/dhan_expiry_code_1_mapping.{json,parquet}` — trade-date × exact expiry type mapping for Dhan code 1.
- `docs/nse_rules/source_manifest.json` — official URLs, hashes and evidence descriptions.
- `docs/nse_rules/unresolved_exceptions.json` — explicit blockers and resolved documentation discrepancy.
- `docs/nse_rules/validation_report.json` — counts, duplicate checks and output hashes.

The generator is `research/generate_nse_rule_history.py`. Writes use temporary
files followed by atomic replacement. It does not use a Dhan credential.

## BSM handoff rule

For a rolling option row, join the Dhan mapping by `trade_date`, `NIFTY`, exact
`expiry_type` and `expiry_code=1`; then join the selected actual expiry to the
contract-rule dimension. Compute:

```text
mte = max(0, (actual_expiry_timestamp_ist - option_timestamp_ist) / 60 seconds)
dte = mte / 1440.0
t_years_act365 = mte / (365 * 24 * 60)
```

Rows at or after expiry, missing a code-1 mapping, missing a point-in-time lot,
or requiring an exceptional-session override stay blocked from BSM.
