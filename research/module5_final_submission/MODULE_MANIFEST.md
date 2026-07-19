# Module 5 manifest

## Inputs

- Module 3 hypothesis closeout and preserved Phase 3–7 artifacts.
- Module 4 selected-profile trade sheet, equity/drawdown curves and diagnostics.
- `audit/phase9_scored_events.csv` for causal entry-time features.
- `audit/phase10_fly_cost_surface.parquet` for exact 1–100-lot capacity economics.
- `contracts/event_calendar.csv` for frozen public event dates and sources.

## Implementations

- `analysis.py` — final aggregation, execution decay, structural/event tests, capacity and autopsy.
- `run.py` — `build` and `verify` commands.
- `scripts/build_workbook.mjs` — Excel build with `@oai/artifact-tool`.
- `scripts/build_pdf.py` — evidence-figure generation and two-pass LaTeX compilation.
- `../../submission/NIFTY_VRP_Research_Memo.tex` — versioned eight-page paper source.
- `../../submission/NIFTY_VRP_Research_Highlights.tex` — versioned one-page executive summary.
- `scripts/run_submission.ps1` — single analytical command.

## Outputs

All compact analytical outputs are under `results/` and SHA-256-covered by `results/manifest.json`.
Reviewer-facing TeX/PDF/XLSX artifacts and vector paper figures are under `submission/`. Raster
renders used only for visual QA are kept under `tmp/pdfs/` and excluded from the final package.

## Acceptance contract

- exactly 132 candidate signals, 86 executed and 46 explicit skips;
- gross minus charges minus modeled slippage equals net for every executed trade;
- final net equals the last daily-equity observation less ₹10 lakh;
- execution-decay baseline multiplier 1.0 equals the frozen Module 4 net result;
- every manifest member exists and matches its hash;
- no result upgrades the decision beyond shadow-only.
