# Module map

## Dhan acquisition and archives

| Capability | Primary modules |
|---|---|
| Active-chain historical and live REST/TBT capture | `src/dhan_data_fetch_stream/core.py` |
| Resumable spot, VIX, rolling-option, and futures acquisition | `src/dhan_data_fetch_stream/acquisition.py` |
| Bronze/silver schemas and preparation | `schemas.py`, `preparation.py` |
| CLI and operator commands | `cli.py`, `supervisor.py`, `scripts/` |

## Cleaning, joining, and model enrichment

| Capability | Primary modules |
|---|---|
| Source normalization and point-in-time joins | `enrichment.py`, `joins.py` |
| Month-incremental pre-BSM enrichment | `pre_bsm_duckdb.py`, `pre_bsm_runner.py` |
| Severe-quality overlay and READY/BLOCKED gate | `pre_bsm_quality_patch.py` |
| BSM IV/Greeks | `bsm.py`, `bsm_vectorized.py`, `bsm_v2_runner.py` |
| Credential-free terminal verification | `v2_terminal_audit.py` |

## NSE SPAN acquisition and cleaning

The imported SPAN package uses the unique `nifty_span` namespace to avoid collisions with other
local `robs_live` installations.

| Capability | Primary modules |
|---|---|
| Bounded resumable downloader | `src/nifty_span/span/backfill_downloader.py` |
| Safe download and archive contracts | `downloader.py`, `availability.py`, `durable_jsonl.py` |
| Streaming extraction and monthly compaction | `streaming_extractor.py`, `extractor.py` |
| Recovery and source-gap classification | `corrupt_recovery.py`, `backfill_audit.py` |
| Terminal finalization and release | `phase1_finalizer.py`, `phase1_release.py` |
| Reader and static margin structures | `parquet.py`, `contracts.py`, `margin_model_a.py` |

## Gold and SPAN representation boundaries

| Capability | Primary modules |
|---|---|
| BOD fallback gold | `dhan_data_fetch_stream/span_gold.py` |
| SPAN handoff interface | `span_interface.py` |
| Hash-pinned BOD/static-six-slot release | `span_release.py`, `span_release_verify.py` |
| Strict and six-slot research timing releases | `span_timing_release.py` |
| Future first-seen timestamp evidence | `span_first_seen.py` |

## Repository audit and sample

| Capability | Primary modules/artifacts |
|---|---|
| Full 43-million-row read-only audit | `tools/audit_gold_dataset.py` |
| Deterministic research-facing sample | `tools/create_sample.py` |
| Sample integrity check | `tools/audit_sample.py` |
| Human-readable audit | `AUDIT_REPORT.md` |
| Machine-readable audit | `audit/gold_dataset_audit.json` |

## Phase 2 hypothesis-formulation evidence

| Capability | Primary modules/artifacts |
|---|---|
| Dataset-bound configuration and stage contracts | `src/nifty_hypothesis/config.py`, `contracts.py` |
| Reproducible orchestration and CLI | `pipeline.py`, `cli.py` |
| Artifact identity, hashes, and Parquet row counts | `validation.py` |
| Playable universe and volatility evidence builders | `research/phase2/` |
| Module usage and research boundaries | `docs/research/HYPOTHESIS_FORMULATION_MODULE.md` |
| Frozen hypothesis contract | `research/phase2/final_hypothesis.json` |
| Curve and paired-threshold diagnostics | `research/phase2/analyze_vrp_curve_crossings.py` |
| Final next-minute closeout | `research/phase2/close_hypothesis_formulation.py` |
| Canonical hypothesis and decision | `docs/research/FINAL_HYPOTHESIS.md` |

## Closed hypothesis, sizing, and submission modules

| Capability | Primary modules/artifacts |
|---|---|
| Cost-inclusive hypothesis testing and 60–180-minute closeout | `research/module3_hypothesis_testing/` |
| ₹10 lakh sizing, risk, capacity, trades and capital curves | `research/module4_sizing_risk_management/` |
| Final robustness, trade sheet, tearsheet and submission decision | `research/module5_final_submission/` |
| Eight-page paper, one-page highlights and workbook | `submission/` |
| Compact end-to-end rerun | `scripts/reproduce_compact.ps1`, `REPRODUCE.md` |
| Deterministic credential-free team ZIP | `tools/team_bundle.py`, `release/` |
