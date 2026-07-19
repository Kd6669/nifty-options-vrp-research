# Final submission artifacts

The public downloadable package is also available from the
[`v1.0.0` GitHub release](https://github.com/Kd6669/nifty-options-vrp-research/releases/tag/v1.0.0).

The centrepiece files are generated from the frozen compact results in this repository:

- `NIFTY_VRP_Research_Memo.pdf` — exactly eight-page LaTeX internal research paper.
- `NIFTY_VRP_Research_Memo.tex` — reviewable paper source; figures are under `figures/`.
- `NIFTY_VRP_Research_Highlights.pdf` — matching one-page executive research summary.
- `NIFTY_VRP_Research_Highlights.tex` — reviewable one-page source.
- `NIFTY_VRP_Research_Tearsheet.xlsx` — formula-backed tearsheet, all signals/trades, curves,
  drawdowns, costs, capacity, execution decay, break/event diagnostics and monitor specification.

Rebuild the analytical sources with:

```powershell
.\research\module5_final_submission\scripts\run_submission.ps1
```

The memo and workbook builders are retained under `research/module5_final_submission/scripts/`.
The PDF builder regenerates evidence figures from the compact CSV/JSON outputs, compiles the
versioned TeX source twice with `pdflatex`, and writes the reviewer PDF. MiKTeX or TeX Live must be
available on `PATH`. The workbook input is `results/workbook_payload.json`, which is hash-covered
by the module manifest. `manifest.json` pins the final TeX/PDF, figures, XLSX, builders and
immediate analytical inputs.
