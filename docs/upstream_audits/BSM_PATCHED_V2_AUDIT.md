# Phase 3 BSM on the quality-patched input

## Authoritative input

- Root: `reports/dhan_phase3_pre_bsm_quality_patch_20210101_20260715/enriched_options/version=2.1.0`
- Months: 67
- Rows: 43,018,677
- READY: 42,970,270
- BLOCKED: 48,407
- Severe anomalies eligible: 0
- Pre-BSM terminal audit SHA-256: `30a6fda40ed5124a39fc6c7d09c4fd8ebc6d7083c16753dba41aa9728a1b69b1`

The preflight refuses v2.0.0, verifies the accepted month-manifest hash through the terminal
audit, and requires `bsm_launch_authorized=true`.

## Numerical contract

- Option price: minute close.
- Spot: `independent_nifty_spot` only.
- Time: `t_years_act365` using the verified `actual_expiry_timestamp_ist`.
- Rate/dividend yield: continuously compounded `r=0.10`, `q=0`.
- IV: vectorized bounded Newton first; bounded Brent only for the non-convergent eligible tail.
- Provider IV is provenance/comparison only and never initializes or replaces independent IV.
- BLOCKED rows remain in output with explicit source and BSM failure statuses and null IV/price/Greeks.
- Severe rows are forbidden from solver eligibility and from any finite BSM output.
- Successful CALL delta must be in `[0,1]`; successful PUT delta in `[-1,0]`.
- Successful gamma and vega must be nonnegative.

## Output roots

- Pilot: `reports/dhan_phase3_bsm_quality_patch_pilot_202101_202301_202601/version=2.1.0`
- Full: `reports/dhan_phase3_bsm_quality_patch_20210101_20260715/version=2.1.0`

The earlier ungated BSM trees are preserved under `reports/_quarantine/noncanonical_ungated_bsm`
and are never resumable or publishable.

## Patched-input pilot acceptance

The January 2021, January 2023, and January 2026 pilot completed with terminal status `PASS`.

- Input/output rows: 1,919,576 / 1,919,576
- READY/BLOCKED: 1,919,267 / 309
- Severe anomalies/named corrupt rows: 160 / 8; rows solved from either group: 0
- Solver methods: Newton 1,283,506; Brent 220,957; none 415,113
- Statuses: success 1,504,461; no-arbitrage reject 414,804; solver failure 2; blocked 309
- Absolute residual: p50 5.153e-12; p95 6.106e-9; p99 1.798e-8; max 9.655e-8
- Duplicate, blocked-solved, severe-solved, success-nonfinite, delta-range, negative-gamma,
  negative-vega, and orphan-partial violations: 0
- Terminal-audit SHA-256: `2e2f7b1fe6539180b7c61d851aa936d67fd6407c7691c93f8dfc653766fa3d1f`

An immediate resume rerun accounted for all three months without rewriting a month manifest;
manifest hashes, manifest modification times, and embedded output hashes remained unchanged.

## Full-run terminal acceptance

The credential-free terminal audit completed in approximately 103.5 seconds with status `PASS`.

- Months and rows: 67 / 67; 43,018,677 / 43,018,677
- READY/BLOCKED: 42,970,270 / 48,407
- Statuses: success 33,281,564; no-arbitrage reject 9,688,658; solver failure 48;
  blocked 48,407
- Solver methods: Newton 28,196,221; Brent 5,085,391; none 9,737,065
- Severe-quality/proven-corrupt rows: 602 / 8; rows solved from either group: 0
- Absolute residual: p50 5.575e-12; p95 6.261e-9; p99 2.082e-8; max 1.049e-7
- Monthly row conservation, Parquet metadata, output hash, primary-key duplicate,
  blocked-solved, severe-solved, success-nonfinite, delta-range, negative-gamma,
  negative-vega, and orphan-partial violations: 0
- Audit JSON: `reports/dhan_phase3_bsm_quality_patch_20210101_20260715/version=2.1.0/manifests/bsm_v2_terminal_audit.json`
- Audit JSON SHA-256: `a398c656a9954d4646240ca5e843991af4d0555b6a87bef68ccf6bb57dfbbdd6`
- Audit Markdown SHA-256: `0c602eab02f53acd6bc18e8163ff120eb482792b36b0abb1041771ce95a39a3f`

The solver configuration remains unchanged at price tolerance `1e-8` and IV tolerance `1e-10`.
The residual p95 is below the price tolerance; p99 and maximum are above it. A read-only
reconciliation found 599,664 successful rows above `1e-8`, all using Brent and none using
Newton. Brent's existing contract can accept the IV solution when the IV bracket meets the
`1e-10` tolerance even if the final price residual is slightly above `1e-8`; neither tolerance
nor the terminal acceptance gate was changed for this audit.
