# Pre-BSM Quality Patch Audit

## Contract

- Immutable input: `reports/dhan_phase3_pre_bsm_v2_20210101_20260715/enriched_options/version=2.0.0`
- Patched output: `reports/dhan_phase3_pre_bsm_quality_patch_20210101_20260715/enriched_options/version=2.1.0`
- Month audit: `dhan_pre_bsm_quality_patch_month` `1.0.0`
- Terminal audit: `dhan_pre_bsm_quality_patch_terminal_audit` `1.0.0`
- Provider label is preserved; computed moneyness uses independent NIFTY spot.
- Ladder mismatch/missing ATM is audited but is not alone a BSM block.
- Severe provider-spot and strike divergence is hard-blocked.
- BSM is unauthorized until 67/67 manifests and the terminal audit PASS.

## Exact regression anomalies

- 2023-01-06 15:29 IST, MONTH ATM-10 C/P: strike 41,200; provider spot 42,218; independent 17,863.5.
- 2026-01-12 10:44 IST, MONTH ATM-10 C/P: strike 67,100; provider spot 68,101.9; independent 25,563.15.
- 2026-01-12 11:08 IST, MONTH ATM-10 C/P: strike 67,000; provider spot 67,957.7; independent 25,546.3.
- 2026-01-12 11:09 IST, MONTH ATM-10 C/P: strike 66,900; provider spot 67,923.35; independent 25,541.4.

## Pilot

January 2021/2023/2026 conserved 1,919,576 rows. The corrected pilot found 344 ladder
mismatches, 39 missing-ATM rows, all eight named corrupt rows, 160 total severe spot-plus-strike
anomalies and zero severe anomalies BSM-eligible. Full totals come only from the terminal JSON.

## Terminal result

- Status: **PASS**
- Months: 67/67
- Input/output rows: 43,018,677 / 43,018,677
- Ready/blocked rows: 42,970,270 / 48,407
- Ladder mismatches: 33,589
- Missing ATM peers: 3,192
- Provider versus computed moneyness mismatches: 95,750
- Severe provider spot-and-strike anomalies: 602
- Named regression anomalies: 8
- Severe anomalies BSM-eligible: 0
- Primary-key duplicate excess rows: 0
- Row-multiplication excess rows: 0
- Orphan partials: 0
- Terminal audit: `manifests/quality_patch_terminal_audit.json`
- Terminal audit SHA-256: `30a6fda40ed5124a39fc6c7d09c4fd8ebc6d7083c16753dba41aa9728a1b69b1`

A transient Windows sharing lock prevented one atomic status-file rename after valid month
publication. No monthly data was lost. The run resumed from 44 manifest boundaries after bounded
atomic-rename retry hardening; the failure log is retained under `exceptions/run_events`.
No BSM process was launched as part of this patch or terminal audit.
