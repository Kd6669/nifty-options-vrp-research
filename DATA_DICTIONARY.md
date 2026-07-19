# Data Dictionary

## Pre-BSM quality patch v2.1.0 fields

| Field | Meaning |
|---|---|
| `provider_moneyness_offset` | Signed integer parsed from Dhan's label (`ATM-N=-N`, `ATM+N=+N`). |
| `ladder_atm_strike` | ATM peer strike in the same timestamp/date/expiry flag/code/option-side group. |
| `expected_strike` | `ladder_atm_strike + provider_moneyness_offset * 50`. |
| `strike_ladder_valid` | True when ATM peer, offset, and expected actual strike agree. |
| `strike_ladder_failure_reason` | `missing_atm_peer`, `invalid_provider_moneyness_label`, or `strike_mismatch`. |
| `recomputed_atm_strike` | Nearest 50-point strike to independent NIFTY spot, half-up. |
| `computed_moneyness_offset` | `(strike - recomputed_atm_strike) / 50`. |
| `computed_moneyness_label` | Recomputed `ATM`, `ATM+N`, `ATM-N`, or `NON_50_GRID`. |
| `provider_moneyness_matches_computed` | Provider-label equality to the independent-spot-derived label. |
| `provider_spot_signed_diff`, `provider_spot_abs_diff` | Signed and absolute difference from independent spot. |
| `provider_spot_rel_diff`, `provider_spot_divergence_bps` | Absolute relative and signed basis-point differences. |
| `provider_spot_divergence_status` | Exact/within-50-bps/material/severe or missing/invalid status. |
| `proven_severe_payload_corruption` | Exact flag for the eight audited corrupt rows. |
| `quality_severe_anomaly` | Provider spot and strike both severely diverge from independent spot. |
| `base_bsm_gate_status` | Unchanged v2.0.0 gate retained as lineage. |
| `bsm_gate_status` | Final gate; base blocks persist and severe quality anomalies are `BLOCKED`. |

Schema contract version: 1.0.0. Acquisition normalizer version: 1.2.0.

All timestamps below are timezone-aware `Asia/Kolkata` microsecond timestamps. Prices and
Greeks are `float64`; option strike is `decimal128(18,4)`. Raw response JSON is retained in
bronze and is the source of truth for rebuilds.

## Common acquisition fields

| Field | Meaning |
|---|---|
| `schema_version` | Normalizer schema version used for the Parquet row. |
| `request_id` | SHA-256 request identity; contains no credential material. |
| `provider` | Dhan source surface, such as `dhan_intraday` or `dhan_rollingoption`. |
| `timestamp_ist` | Provider candle timestamp converted from epoch to IST. |
| `trade_date` | IST calendar date derived from `timestamp_ist`. |
| `session_status` | `regular_session` for 09:15 through 15:30 inclusive; otherwise `outside_regular_session`. |
| `underlying` | Canonical identity: `NIFTY` or `INDIA_VIX`; never inferred from another security ID. |
| `open`, `high`, `low`, `close` | Provider OHLC values, unadjusted. |
| `volume`, `open_interest` | Provider quantities when present; null when the endpoint omits them. |

## NIFTY rolling options (`options`)

Natural key: `timestamp_ist, trade_date, underlying, expiry_date, expiry_flag,
expiry_code, moneyness_label, strike, option_type`.

| Field | Meaning |
|---|---|
| `expiry_date` | Actual expiry date; null until independently verified because rolling responses omit it. |
| `expiry_flag` | Requested `WEEK` or `MONTH`. |
| `expiry_code` | Requested Dhan code. This run uses code 1. The official v2 annexure currently labels 0 current/near, 1 next and 2 far, while other Dhan references have conflicted; the response does not prove an actual expiry. |
| `moneyness_label` | Requested rolling label `ATM`, `ATM+1`…`ATM+10`, or `ATM-1`…`ATM-10`. |
| `strike` | Actual strike returned by Dhan at that timestamp. Labels can map to different strikes as ATM moves. |
| `option_type` | `CALL` or `PUT`. |
| `provider_iv_raw` | Dhan IV value exactly as normalized from the provider array; not used as the solved BSM IV. |
| `provider_iv_unit` | `provider_raw_unverified`; prevents an undocumented percent/decimal conversion. |
| `provider_spot` | Spot value returned alongside the rolling option candle; preserved for comparison only. |
| `expiry_resolution_status` | Why `expiry_date` is unresolved, normally `not_returned_by_rolling_endpoint`. |

This dataset is a rolling ATM±10 moneyness surface. It is not an absolute-strike full chain.

## NIFTY spot (`spot`)

Natural key: `timestamp_ist, trade_date, underlying`. `security_id` is the official Dhan
NIFTY INDEX identity (13 in the 2026-07-15 master snapshot). The silver layer preserves
outside-session rows with an explicit flag; the point-in-time preparation layer excludes them.

## INDIA VIX (`india_vix`)

Natural key: `timestamp_ist, trade_date, underlying`. This is an independent `INDEX`
instrument resolved from the official Dhan master on every run. It is partitioned separately
and never reuses the NIFTY security ID. OHLC and volume are provider values; no value is
synthesized from NIFTY options.

## NIFTY current futures (`futures`)

Natural key: `timestamp_ist, trade_date, underlying, security_id`.
`futures_expiry_text` and `series_label` preserve the dated master identity. Only current
near/next/far identities are planned. An empty historical response from a current ID is not
evidence that expired futures are available.

## Mandatory pre-BSM enrichment (`enriched_options`, versions 1.0.0 and 2.0.0)

This separate Parquet layer is materialized before any BSM calculation. Its request-file
parts are idempotent and carry source hashes, row/cardinality checks, null coverage, and an
explicit BSM gate. `bsm_executed` is always false in this layer.

| Field | Meaning |
|---|---|
| `provider_spot` | Dhan rolling-response spot, retained unchanged and never substituted for the independent join. |
| `independent_nifty_spot` | NIFTY spot close from the strict point-in-time join. |
| `nifty_spot_timestamp_ist`, `nifty_spot_age_seconds` | Selected source timestamp and non-negative backward age. |
| `nifty_spot_match_method`, `nifty_spot_join_status`, `nifty_spot_join_failure_reason` | Exact/backward method and explicit result. |
| `india_vix` | Independent INDIA VIX close; contextual only, never the BSM underlying. |
| `india_vix_timestamp_ist`, `india_vix_age_seconds` | Selected VIX timestamp and backward age. |
| `india_vix_match_method`, `india_vix_join_status`, `india_vix_join_failure_reason` | Exact/backward method and explicit result. |
| `actual_expiry_date`, `actual_expiry_timestamp_ist` | Bhavcopy-proven NSE contract expiry and verified pricing cutoff, or null. |
| `expiry_type`, `expiry_rule_weekday` | Weekly/monthly classification and effective weekday rule. |
| `expiry_rule_effective_from`, `expiry_rule_effective_to` | Effective-dated expiry-rule bounds. |
| `expiry_holiday_adjusted`, `original_scheduled_expiry` | Actual-vs-scheduled expiry evidence. |
| `contract_lot_size`, `market_lot` | Point-in-time lot applicable to the actual contract expiry, not merely the observation date. |
| `contract_multiplier`, `trading_unit`, `tick_size` | Official contract terms where separately proven; unresolved values remain null. |
| `expiry_*_source_*`, `contract_rule_*` | Rule/circular/source IDs, hashes, confidence, effective dates, and mapping status. |
| `mte` | Positive fractional calendar minutes from option timestamp to actual expiry timestamp. |
| `dte` | `mte / 1440.0`; fractional calendar days. |
| `t_years_act365` | `mte / (365*24*60)`; the only time value admitted to BSM. |
| `canonical_bsm_population` | True only for source rows in the authoritative regular session. |
| `bsm_gate_status`, `bsm_gate_failure_reason` | Row-level readiness; ambiguity, missing joins/rules, and non-positive MTE remain blocked. |

Version 2.0.0 is the canonical bulk layout. It writes one deterministic ZSTD
`pre_bsm.parquet` per calendar month (row groups near 250,000) plus separate duplicate,
source-exception, and primary-key audit Parquets. `source_option_file` preserves the immutable
silver filename; `request_id` preserves request lineage. Month manifests bind every input and
output SHA-256, the code/config hash, row/cardinality checks, and Parquet metadata counts.

`india_vix_join_status=source_unavailable` is an explicit contextual null, with
`india_vix_source_available_from` and `india_vix_source_provenance`. INDIA VIX is not a BSM
formula input and its absence alone does not block `bsm_gate_status`. Valid independent NIFTY
spot, strike/close, actual expiry, contract rule/lot, regular session, and positive time remain
mandatory.

## Vectorized BSM (`bsm`, version 2.0.0)

| Field | Meaning |
|---|---|
| `bsm_price_input_field` | Always `close`; provider OHLC remains unchanged. |
| `bsm_iv_close`, `bsm_iv_unit` | Independently solved annual IV and explicit `decimal` unit. |
| `bsm_solver_method`, `bsm_solver_iterations`, `bsm_solver_converged` | Bounded vector Newton result or sparse Brent fallback diagnostics. |
| `bsm_no_arbitrage_lower`, `bsm_no_arbitrage_upper` | Discounted model bounds checked before solving. |
| `bsm_price_reconstructed`, `bsm_price_residual_signed`, `bsm_price_residual_abs` | Repriced close and solver residual. |
| `bsm_delta`, `bsm_gamma` | Standard BSM delta and gamma. |
| `bsm_theta_per_year`, `bsm_theta_per_day_365` | Theta per ACT/365 year and divided by 365. |
| `bsm_vega_per_1`, `bsm_vega_per_100` | Vega per 1.00 decimal-vol move and per 0.01 move. |
| `bsm_rho_per_1`, `bsm_rho_per_100` | Rho per 1.00 rate move and per 0.01 move. |
| `bsm_provider_iv_delta_decimal` | Solved-minus-provider IV only when the provider explicitly declares decimal units. |
| `bsm_status`, `bsm_failure_reason`, `bsm_near_expiry` | Explicit convergence/gate/failure result and near-expiry flag. |

## Dhan gold-preparation fields

`spot` and `india_vix` are joined independently: exact timestamp first, otherwise the latest
backward observation no more than 60 seconds old within the same trade date/session. Status,
matched timestamp, and lag are retained for both.

BSM outputs use `r=0.10` continuously compounded, `q=0`, ACT/365, and an independently
verified expiry timestamp at exactly 15:30 IST. `bsm_iv_close` is a decimal IV. Vega and rho
are stored both per 1.00 and divided by 100; theta is stored per year and divided by 365.
`bsm_status` and `bsm_failure_reason` preserve solver/no-arbitrage/expiry failures. Provider
fields remain separate.

The immutable BSM layer remains SPAN-free. The next `span-gold` layer adds:

| Field | Meaning |
|---|---|
| `span_join_policy` | `BOD_CONSERVATIVE_UNKNOWN_EFFECTIVE_TIME`; no intraday/EOD time is guessed. |
| `span_enrichment_status` | `matched` or `unmatched`; every input row is retained. |
| `span_unmatched_reason` | Currently `contract_not_in_bod_span` when the exact expiry/strike is absent. |
| `span_phase1_outcome` | Terminal producer outcome, including `BLOCKED_SOURCE`. |
| `span_phase1_source_boundary_cells` | Global count of explicitly proven Phase 1 source-boundary cells. |
| `span_phase1_completion_sha256` | Hash binding the row to the terminal Phase 1 evidence contract. |
| `span_<source field>` | Every original SPAN value and lineage field with a `span_` prefix. |

The exact join key is `trade_date`, mapped CE/PE side, `actual_expiry_date`, and strike against
same-date NIFTY BOD. Unmatched rows also appear in a compact exception Parquet. ID1-ID4/EOD are
not columns in the primary joined baseline because their effective times are unproven.

## Final SPAN release and timing representations

The final hash-pinned SPAN consumer publishes three separate contracts instead of conflating
reference schedules with arrival evidence:

- BOD conservative fallback `version=1.4.0`;
- point-in-time strict `version=1.0.0`;
- six-slot research `version=2.1.0`.

The strict output preserves all Dhan/BSM columns and rows. It selects the latest exact-contract
SPAN slot only when a proven effective timestamp is no later than `timestamp_ist`. Historical
reference-only slots leave SPAN values null and use `span_join_status=timing_unproven`. The
research output widens all slots under `span_bod_*`, `span_id1_*`, `span_id2_*`, `span_id3_*`,
`span_id4_*` and `span_eod_*`.

| Field | Meaning |
|---|---|
| `span_reference_ts_ist` | Official reference-price timestamp for ID1-ID4; null for BOD/EOD because “before market”/“after close” is not an exact instant. |
| `span_file_created_ts_ist` | Parsed provider value only when explicitly zoned, same-date and slot-valid. Raw `span_file_created` remains separate. |
| `span_first_seen_ts_ist` | First valid live observation of the exact archive SHA. |
| `span_effective_ts_ist` | Maximum of applicable reference floor and valid proof timestamps, rounded forward to a Dhan minute. |
| `span_timing_source` | `span_file_created`, `nse_endpoint_first_seen_sha`, `official_reference_schedule`, or `none`. |
| `span_timing_confidence` | `file_created_proven`, `observed_first_seen`, `reference_only`, or `unproven`. |
| `span_time_slot` | Strict selected slot; research uses its explicit slot prefix/time-slot field. |
| `span_age_seconds` | Non-negative option-minute minus effective timestamp; null without proven availability. |
| `span_static_available_slot_count` | Count of the six same-date exact-contract static matches, independent of timing eligibility. |
| `span_static_source_gap_slot_count` | Count of slots blocked by an accepted producer source gap. |
| `span_static_unmatched_contract_slot_count` | Count of available slot archives lacking the exact option contract. |

In the six-slot research output, each timing field is prefixed by its slot, for example
`span_id2_reference_ts_ist` and `span_eod_timing_confidence`. See
`SPAN_TIMING_POLICY.md` for source hash, activation rules and the strict acceptance boundary.
