# Module 5 — Final submission and robustness closeout

This is the single review surface for the completed NIFTY short-dated VRP research. It packages,
but does not rewrite, the canonical work in Modules 1–4.

## Decision

**No live allocation. Shadow only.** The preregistered VRP level/direction rules fail at 60–180
minutes after one-lot costs. A later gated upper-85 short iron fly with frozen ₹10 lakh sizing is
historically positive, but it is post-hoc, weak in 2024, and fails the frozen Phase 9 resampled
rank-correlation gate. It is a forward-shadow candidate, not an accepted strategy.

## Reproduce

From the repository root after `pip install -e .[dev,submission]`:

```powershell
python -m research.module5_final_submission.run build
python -m research.module5_final_submission.run verify
python -m pytest -q
```

Or run the wrapper:

```powershell
.\research\module5_final_submission\scripts\run_submission.ps1
```

The LaTeX figure builder uses the `submission` Python extra and requires `pdflatex` from MiKTeX or
TeX Live. Workbook regeneration uses the Codex-provided private
`@oai/artifact-tool` runtime; if it is unavailable, the one-command wrapper retains and hash-checks
the versioned workbook instead of silently producing a different XLSX implementation.

The analytical build consumes only frozen compact artifacts already versioned in the repository.
Raw multi-gigabyte source data is intentionally not required to regenerate the headline result.

## Review order

1. [`docs/end_to_end_process.md`](docs/end_to_end_process.md) — extraction through decision.
2. [`docs/research_note.md`](docs/research_note.md) — hypothesis, methods, results and limitations.
3. [`results/tearsheet.md`](results/tearsheet.md) — headline economics and robustness.
4. [`../../submission/NIFTY_VRP_Research_Memo.tex`](../../submission/NIFTY_VRP_Research_Memo.tex) —
   reviewable source for the exactly eight-page research paper.
5. [`../../submission/NIFTY_VRP_Research_Highlights.pdf`](../../submission/NIFTY_VRP_Research_Highlights.pdf) —
   one-page executive companion to the full paper.
6. [`results/trades/final_trade_sheet.csv`](results/trades/final_trade_sheet.csv) — all 132 signals,
   including the 46 deliberately skipped entries.
7. [`docs/live_monitoring.md`](docs/live_monitoring.md) — shadow protocol and kill-switches.
8. [`../../submission/README.md`](../../submission/README.md) — final PDF/XLSX handoff.

## Boundary

Owned here: final aggregation, formal break/event/execution sensitivities, reviewer artifacts and
the deployment decision. Upstream data, IV/RV/VRP, cost, SPAN, signal and sizing implementations
remain owned by their original modules. Hashes for every generated compact result are recorded in
`results/manifest.json`.
