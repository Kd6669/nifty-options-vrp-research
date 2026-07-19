# NIFTY gold dataset audit

Audit verdict: **PASS_WITH_DECLARED_LIMITATIONS**

This is a read-only independent scan of the hash-pinned BOD-SPAN convenience gold release. The
audit does not promote BOD SPAN to strict point-in-time data; the historical archive has no proven
arrival timestamp.

## Identity and coverage

| Field | Value |
|---|---:|
| Release | `nifty_gold_span_bod_20210101_20260715/version=1.4.0` |
| Monthly Parquet files | 67 |
| Rows | 43,018,677 |
| Bytes | 3,743,775,346 |
| Dataset corpus SHA-256 | `aee5dbebcc1195882c48d57695ca7dd47deb470d4b1fc7c5e112def607caa252` |
| First timestamp | 2021-01-01 09:15:00+05:30 |
| Last timestamp | 2026-07-15 15:29:00+05:30 |
| Trade dates | 1,371 |
| Schema variants | 1 |

### Annual coverage

| year | rows | trade_dates | first_trade_date | last_trade_date |
|---|---|---|---|---|
| 2021 | 7778949 | 248 | 2021-01-01 | 2021-12-31 |
| 2022 | 7785385 | 248 | 2022-01-03 | 2022-12-30 |
| 2023 | 7722792 | 246 | 2023-01-02 | 2023-12-29 |
| 2024 | 7793886 | 249 | 2024-01-01 | 2024-12-31 |
| 2025 | 7815587 | 249 | 2025-01-01 | 2025-12-31 |
| 2026 | 4122078 | 131 | 2026-01-01 | 2026-07-15 |

## Trading-session completeness

Expected sessions are generated for the observed dataset range from the retained, source-backed NSE
F&O calendar: ordinary Monday-Friday sessions, less explicitly declared holidays, plus explicitly
notified special weekend and Muhurat sessions.

| Field | Value |
|---|---:|
| Audited range | 2021-01-01 through 2026-07-15 |
| Expected NSE F&O sessions | 1,371 |
| Observed trade dates | 1,371 |
| Matched expected sessions | 1,371 |
| Missing trading sessions | **0** |
| Unexpected observed sessions | 0 |
| Session coverage | 100.0% |
| Calendar evidence SHA-256 | `2cb0f6e8c3920b7409a507a29dfff8eaa8bd63906291d977db93f6e51d01afe2` |

### Session coverage by year

| year | expected_sessions | observed_sessions | matched_expected_sessions | missing_sessions | unexpected_observed_sessions | coverage_pct |
|---|---|---|---|---|---|---|
| 2021 | 248 | 248 | 248 | 0 | 0 | 100.0 |
| 2022 | 248 | 248 | 248 | 0 | 0 | 100.0 |
| 2023 | 246 | 246 | 246 | 0 | 0 | 100.0 |
| 2024 | 249 | 249 | 249 | 0 | 0 | 100.0 |
| 2025 | 249 | 249 | 249 | 0 | 0 | 100.0 |
| 2026 | 131 | 131 | 131 | 0 | 0 | 100.0 |

### Missing trading-session list

_No rows._

### Unexpected observed-session list

_No rows._

## Integrity gates

| check | violations |
|---|---|
| null_primary_fields | 0 |
| negative_volume | 0 |
| negative_open_interest | 0 |
| ohlc_violations | 0 |
| invalid_lot_size | 0 |
| future_spot_joins | 0 |
| over_tolerance_spot_joins | 0 |
| successful_bsm_nonpositive_time | 0 |
| successful_bsm_nonfinite | 0 |
| delta_range_violations | 0 |
| negative_gamma | 0 |
| negative_vega | 0 |
| bsm_parameter_violations | 0 |
| severe_anomaly_solved | 0 |
| blocked_rows_with_bsm_values | 0 |
| bod_intraday_asof_join_rows | 0 |
| bod_publication_time_claim_rows | 0 |
| bod_effective_timestamp_rows | 0 |
| non_bod_rows_in_bod_release | 0 |
| primary_key_duplicate_excess_rows | 0 |

All integrity gates must be zero. The Parquet metadata row total, full DuckDB scan, partition count,
and schema fingerprint must also reconcile.

## BSM outcome

| status | rows |
|---|---|
| blocked | 48407 |
| iv_solver_failed | 48 |
| no_arbitrage_violation | 9688658 |
| ok | 33281564 |

| Residual statistic | Value |
|---|---:|
| Solved rows | 33,281,564 |
| p50 absolute price residual | 5.4569682106375694e-12 |
| p95 absolute price residual | 6.259142537601292e-09 |
| p99 absolute price residual | 2.0631716779462295e-08 |
| Maximum absolute price residual | 1.049021989274479e-07 |

Blocked, no-arbitrage, and solver-failed rows remain visible. They are not silently discarded or
assigned fabricated Greeks.

## Join coverage

### Independent NIFTY spot

| status | rows |
|---|---|
| BLOCKED | 29925 |
| MATCHED | 42988752 |

### INDIA VIX

| status | rows |
|---|---|
| BLOCKED | 195285 |
| MATCHED | 38262338 |
| source_unavailable | 4561054 |

### Conservative BOD SPAN

| status | rows |
|---|---|
| matched | 42718832 |
| unmatched_contract | 299845 |

## Expiry mechanics

After the Tuesday migration, the actual expiry weekday distribution—including explicitly tagged
holiday adjustments—is:

| actual_expiry_weekday | expiry_holiday_adjusted | rows |
|---|---|---|
| Monday | True | 598127 |
| Tuesday | False | 6587253 |

The expiry and lot-size fields come from the saved official rule dimensions, not from the stale
rejected NIFTY Parquet.

## Declared limitations

- Observed historical bid/ask present: **False**.
- Historical expired-futures minute data present: **False**.
- Historical SPAN arrival times proven: **False**.
- Option surface: Dhan ATM plus/minus 10 rolling moneyness; not an absolute-strike full chain.
- Transaction-cost model included: **False**.
- Rows without a usable effective-dated tick size: **43,018,677**.

These limitations do not invalidate OHLC/spot/contract/BSM hypothesis work, but they constrain later
execution, margin, and capacity claims. Slippage and fee assumptions must be conservative and tested
separately. Strict point-in-time SPAN users must consume the separate strict release, whose historical
SPAN matches are intentionally zero until arrival evidence exists.

## Readiness decision

| component | status |
|---|---|
| options_spot_contract_bsm | READY_WITH_ROW_LEVEL_STATUS_GATES |
| bod_span | STATIC_CONSERVATIVE_FALLBACK_ONLY |
| strict_point_in_time_span | USE_SEPARATE_STRICT_RELEASE |
| six_slot_span | REFERENCE_ONLY_SENSITIVITY_INPUT |
| execution_cost_calibration | EXTERNAL_CONSERVATIVE_MODEL_REQUIRED |
| historical_expired_futures_1m | SOURCE_BLOCKED |

**Decision:** the data foundation may proceed to hypothesis design only if these limitations are
accepted as part of the research contract. They must not be hidden or retroactively filled with
synthetic source data.
