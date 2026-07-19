# NIFTY Options Data Foundation

Audited, reproducible data infrastructure for the NIFTY short-dated options research task in
[`docs/task/quant-research-task.md`](docs/task/quant-research-task.md).

## Public submission package

This is the clean public research edition. The downloadable ZIP, eight-page memo, one-page
highlights paper, Excel tearsheet, checksum and member manifest are published together under the
[`v1.0.0` release](https://github.com/Kd6669/nifty-options-vrp-research/releases/tag/v1.0.0).
The repository contains no credentials or full multi-gigabyte provider dataset. Research use only;
the retained candidate is shadow-only and has zero live-capital approval.

## Final submission — start here

The research is complete. The clean reviewer surface is
[`research/module5_final_submission/README.md`](research/module5_final_submission/README.md), with
the final [PDF and workbook handoff](submission/README.md). Rebuild and verify the compact headline
results with one command:

```powershell
.\research\module5_final_submission\scripts\run_submission.ps1
```

For the complete compact result chain, environment lock and the honest full-data boundary, use
[`REPRODUCE.md`](REPRODUCE.md). Build the deterministic, credential-free team-share ZIP from a
clean commit with:

```powershell
.\scripts\build_team_bundle.ps1
```

The archive, SHA-256 checksum and member manifest are written under ignored `dist/`; the release
contract and verifier are versioned under [`release/`](release/README.md).

The decision is deliberately conservative: **the standalone 60–180-minute VRP hypothesis is
rejected net of costs; the later positive gated/sized result remains shadow-only and receives zero
live capital until a new forward sample passes frozen promotion gates.**

This repository began as **Phase 1**, the audited data foundation, and now contains the complete
research chain through the final net-of-cost decision, robustness tests, sizing diagnostics,
trade sheet, workbook and eight-page memo. Historical phase documents retain their original
as-of status; the current conclusion is in Module 5.

The repo also contains a separate, dataset-bound **Phase 2 hypothesis-formulation evidence
module**. It reproduces the playable-universe, IV/RV/VRP, defined-risk, and tail/crossing analyses
and closes with a frozen machine-readable hypothesis. It does not claim the later cost/OOS result. See
[`docs/research/HYPOTHESIS_FORMULATION_MODULE.md`](docs/research/HYPOTHESIS_FORMULATION_MODULE.md).

## What is included

- Dhan historical rolling-options, NIFTY spot, INDIA VIX, active futures, current-chain, REST,
  TBT, and Redis/Parquet capture utilities.
- Immutable bronze, typed silver, pre-BSM, independently recomputed BSM, SPAN, and final join
  modules.
- Official NSE expiry, lot-size, calendar, and SPAN source evidence.
- Hash-pinned BOD, strict point-in-time, and six-slot research SPAN release builders.
- A reproducible full-dataset auditor and deterministic small Parquet sample.
- The research brief, data dictionary, lineage, archive-layer contract, and audit reports.
- Phase 2 playable-universe, IV/RV/VRP, defined-risk structure, and preregistration notes.

The full multi-GB datasets are intentionally excluded from Git. Only the small, non-secret sample
and machine-readable audit evidence are versioned.

## Dataset layers

```text
Dhan/NSE sources
  -> bronze: immutable provider responses and request manifests
  -> silver: typed, source-specific normalized Parquet plus exceptions
  -> gold: point-in-time spot/VIX enrichment, official contract rules, BSM, and separated SPAN views
```

See [`docs/ARCHIVE_LAYERS.md`](docs/ARCHIVE_LAYERS.md) and
[`DATA_LINEAGE.md`](DATA_LINEAGE.md) for the complete contract. The exact code ownership map is in
[`docs/MODULE_MAP.md`](docs/MODULE_MAP.md).

## Canonical releases

The final local corpus contains 67 monthly partitions and 43,018,677 option rows from
2021-01-01 through 2026-07-15:

1. **BSM v2.1** — quality-gated option/spot/VIX/contract-rule data with independent BSM values.
2. **BOD SPAN v1.4** — conservative session-wide BOD fallback; useful for static margin research,
   but historical arrival time is not proven.
3. **Strict SPAN v1.0** — only backward as-of joins from proven effective timestamps. Historical
   SPAN matches are intentionally zero because the source lacks arrival evidence.
4. **Six-slot research v2.1** — BOD, ID1-ID4, and EOD retained as reference-only sensitivity data;
   it is not contemporaneous execution evidence.

The repo sample is extracted from the BOD convenience gold and retains the timing/provenance flags
needed to prevent it from being mistaken for strict point-in-time SPAN.

## Setup

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -e .[dev]
```

Dhan credentials are environment-only. Never commit `.env`:

```powershell
$env:DHAN_ACCESS_TOKEN = "<token>"
$env:DHAN_CLIENT_ID = "<client-id>"  # optional when embedded in the token
```

## SPAN acquisition and cleaning

The focused Phase 1 downloader/extractor is exposed independently of the Dhan commands:

```powershell
span-backfill download `
  --start-date 2021-01-01 --end-date 2026-07-15 `
  --raw-root data\span\raw --parquet-root data\span\extracted

span-backfill extract `
  --start-date 2021-01-01 --end-date 2026-07-15 `
  --raw-root data\span\raw --parquet-root data\span\extracted

span-backfill audit `
  --start-date 2021-01-01 --end-date 2026-07-15 `
  --raw-root data\span\raw --parquet-root data\span\extracted
```

Use `span-backfill --help` for the exact current flags. The downloader uses bounded concurrency,
atomic partial writes, hash/ZIP validation, durable manifests, safe extraction, source-gap
classification, and resumable partitioned compaction.

## Reproduce the audit

Run against the local BOD gold root:

```powershell
nifty-data-audit `
  --dataset-root "<path>\nifty_gold_span_bod_20210101_20260715\version=1.4.0\gold" `
  --session-calendar docs\evidence\span_availability\reviewed_import_2021_2026.json `
  --output-json audit\gold_dataset_audit.json `
  --output-markdown AUDIT_REPORT.md
```

The audit reconciles every observed trade date to the retained official NSE F&O calendar. It reports
the expected-session count, exact missing-session list, unexpected observed dates, annual coverage,
and the SHA-256 of the calendar evidence used.

Create the deterministic sample from the same root:

```powershell
nifty-data-sample `
  --dataset-root "<path>\nifty_gold_span_bod_20210101_20260715\version=1.4.0\gold" `
  --output samples\nifty_gold_sample.parquet `
  --manifest samples\nifty_gold_sample.manifest.json
```

## Validation

```powershell
make lint
make test
make audit-sample
```

If GNU Make is unavailable on Windows, run the equivalent commands from the `Makefile` directly.

## Research readiness boundary

The options/spot/contract/BSM corpus is ready for the preregistered Phase 2 test with explicit
blocked rows and source limitations. The canonical final hypothesis is documented in
[`docs/research/FINAL_HYPOTHESIS.md`](docs/research/FINAL_HYPOTHESIS.md), with its detailed
pre-closeout history in
[`docs/research/PHASE2_PREREGISTERED_HYPOTHESIS.md`](docs/research/PHASE2_PREREGISTERED_HYPOTHESIS.md).
The post-formulation percentile-curve, velocity, acceleration, and paired leverage test is in
[`docs/research/PHASE2_VRP_CURVE_CROSSINGS.md`](docs/research/PHASE2_VRP_CURVE_CROSSINGS.md).

The corpus does **not** contain observed historical bid/ask quotes or proven historical
expired-futures minute data. Checkpoint 3 supplies the pinned conservative execution-cost,
quantity-aware slippage and SPAN-margin engines; Modules 3–5 apply them to the tested entries,
exits and sizes. The timestamp-aware SPAN join retains its source slot and timing provenance. See
[`docs/research/CHECKPOINT3_EXECUTION_CAPITAL_MODELS.md`](docs/research/CHECKPOINT3_EXECUTION_CAPITAL_MODELS.md).

The resulting one-lot cost-inclusive strategy test and complete gross-to-net attribution are in
[`docs/research/PHASE3_FULL_STRATEGY_TEAR_SHEET.md`](docs/research/PHASE3_FULL_STRATEGY_TEAR_SHEET.md).
That checkpoint rejects the first daily zero-cross signal for executable economic viability.
The subsequent 12-cell, one-lot upper-tail test at 70/75/80/85/90/95 percent in both crossing
directions is documented in
[`docs/research/PHASE3_TAIL_PERCENTILE_TEAR_SHEET.md`](docs/research/PHASE3_TAIL_PERCENTILE_TEAR_SHEET.md).
Its reproducible runner is `research/phase3/run_tail_percentile_backtests.py`; the audit directory
contains the corresponding tradebook, legbook, JSON tear sheet, and SHA-256 manifest.

Phase 4 corrects option STT by historical date, restores a quantity-aware volume/OI capacity
sensitivity, and compares eight defined-risk structures at 60/120/180 minutes. It also audits
exact-contract multi-day holds. All 48 intraday cells remain negative after one-lot costs, no
integer size from one through 20 rescues the upper-tail short condor, and the only multi-day cell
with at least 80% coverage also fails. See
[`docs/research/PHASE4_COST_AWARE_VRP_DISCOVERY.md`](docs/research/PHASE4_COST_AWARE_VRP_DISCOVERY.md).

The final Phase 5 attempt preregisters a causal IV/RV/VRP feature model, reduces execution to
two-leg verticals, selects one configuration on calendar 2024, and evaluates a locked 2025–2026
confirmation period. It loses gross and net, fails six of eight acceptance gates, and closes the
hypothesis family for this dataset. See the frozen
[`protocol`](docs/research/PHASE5_FINAL_ATTEMPT_PROTOCOL.md) and final
[`result`](docs/research/PHASE5_FINAL_ATTEMPT_RESULTS.md).

One explicitly post-hoc Phase 6 check tests the requested VRP tail-reversal mapping—top-to-zero
long condors and bottom-to-zero short condors—against its exact inverse. The primary mapping loses
₹294.12 per trade and the inverse also fails at every aggregate horizon. See the frozen
[`reversal protocol`](docs/research/PHASE6_VRP_REVERSAL_PROTOCOL.md) and
[`result`](docs/research/PHASE6_VRP_REVERSAL_RESULTS.md).

Phase 7 puts every previously tested zero-crossing, 70/75/80/85/90/95 percentile-crossing, and
tail-reversal event on one fixed 180-minute, one-lot execution basis. None of the 32
requested/inverse cells passes the economic and coverage gates. The sole positive mean is a
96-trade, 50.26%-coverage inverse subgroup with a confidence interval spanning zero; 95 of its
191 signals have no evaluated 180-minute outcome. Those missing outcomes remain explicitly
unevaluated. The result
also records why horizons beyond 180 minutes are a future full-chain-data question rather than an
inference supported by this rolling ATM±10 archive. See the frozen
[`180-minute protocol`](docs/research/PHASE7_180MIN_COMPARISON_PROTOCOL.md) and
[`comparison result`](docs/research/PHASE7_180MIN_COMPARISON_RESULTS.md).

The complete execution and hypothesis-testing history is now closed as the self-contained
[`Module 3 VRP hypothesis-testing package`](research/module3_hypothesis_testing/README.md). It
contains frozen hypothesis contracts, explicit module boundaries, reproducible Phase 3–7 code
lineage, preserved compact results, a generated closeout report, and a SHA-256 integrity manifest.
The conclusion is bounded to this dataset: the tested 60–180-minute VRP rules do not realize
enough defined-risk payoff consistently to clear one-lot per-trade costs; horizons beyond 180
minutes require full fixed-contract chain data.

Phase 8 is an explicitly post-hoc capital diagnostic for the gated upper-85 crossing candidate.
It applies exact-lot Groww charges, corrected quantity-aware impact, timestamp-aware entry SPAN,
and defined-loss sizing to a ₹10 lakh account across conservative, balanced, growth, and
margin-only policies. The discovery-selected balanced short iron fly finishes slightly positive
over the full sample, but fails in calendar 2024 and does not constitute clean OOS evidence. See
the reproducible [`₹10 lakh gated capital tear sheet`](docs/research/PHASE8_10L_GATED_CAPITAL_BACKTEST.md).

Phase 9 tests whether discovery-ranked gate cushion and causal IV/RV/DTE/time regime scores can
control the ₹10 lakh fly position size. It measures Spearman rank correlation against unscaled
one-lot net P&L before applying the score, then recomputes exact quantity-aware costs at the chosen
integer lot count. The composite improves the historical capital path but fails the frozen
bootstrap-confidence gate, so it remains a forward-testing candidate rather than a passed sizing
model. See the [`confidence-sizing diagnostic`](docs/research/PHASE9_CONFIDENCE_SIZING.md).

Phase 10 keeps that score frozen and explores 17,640 sizing and risk-control policies against the
same ₹10 lakh pool. It varies margin ceilings, cost-reserved maximum risk, score floors and
curvature, drawdown brakes, and losing-streak brakes; profile selection is restricted to 2021–23
before later-period evaluation. The strongest result is a broad hard-quality-switch plateau rather
than an isolated optimizer cell. See the reproducible
[`margin-efficient sizing exploration`](docs/research/PHASE10_SIZING_EXPLORATION.md).

The complete post-hypothesis capital work is now closed as the self-contained
[`Module 4 sizing and risk-management package`](research/module4_sizing_risk_management/README.md).
It freezes the 35%-margin, 4%-cost-reserved-risk, 40%-quality-switch candidate; reconciles its
one-lot-to-quantity economics against the exact cost surface; and publishes the full signal/trade
sheet, business-day equity and drawdown curves, monthly returns, cost and regime diagnostics, all
17,640 sizing policies, deterministic figures, tests, rerun contracts, and SHA-256 lineage. The
historical ₹10 lakh result is positive, but the Phase 9 confidence-rank bootstrap gate failed, so
the strategy is explicitly limited to an unchanged forward shadow test and is not deployment
approved.
