# Reproduction contract

This repository supports two distinct rerun levels. Keeping them separate prevents a compact
review packet from pretending to contain the multi-gigabyte historical corpus.

## 1. Compact result reproduction — self-contained

The Git repository and team ZIP contain every input needed to rebuild and verify the reported
Module 3 hypothesis closeout, Module 4 sizing/risk packet and Module 5 final submission metrics.
They also contain the final trade sheet, curves, robustness tables, PDFs, workbook, code, tests,
manifests and a deterministic gold-data sample.

Create a Python 3.11 environment and install the frozen reviewer dependencies:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements\research-release.txt
.\.venv\Scripts\python.exe -m pip install -e . --no-deps
```

Then run the complete compact chain:

```powershell
.\scripts\reproduce_compact.ps1 -Python .\.venv\Scripts\python.exe
```

This audits the sample, rebuilds and hash-verifies Modules 3–5, and runs the test suite. The
Module 5 analytical build consumes frozen compact upstream artifacts rather than raw Parquet.

To rebuild the two LaTeX PDFs as well, install MiKTeX or TeX Live so `pdflatex` is on `PATH`, then
run:

```powershell
.\research\module5_final_submission\scripts\run_submission.ps1
```

The versioned workbook remains hash-verifiable without the private workbook-authoring runtime;
see `submission/README.md` for that boundary.

## 2. Full-data pipeline reproduction — external data required

Acquisition through final gold is code-reproducible but not data-self-contained. It requires:

- Dhan credentials supplied only through environment variables;
- provider/NSE availability for the requested historical dates;
- the retained bronze/SPAN archives or permission to fetch them again;
- sufficient local storage for the multi-gigabyte silver, BSM and gold layers;
- explicit dataset roots passed to every command.

Install the full project environment:

```powershell
.\.venv\Scripts\python.exe -m pip install -e .[dev,submission]
```

The ordered ownership and runbooks are:

1. Dhan acquisition and archive preparation — `src/dhan_data_fetch_stream/` and `scripts/`;
2. normalization, point-in-time joins and pre-BSM gates — `docs/ARCHIVE_LAYERS.md` and
   `DATA_LINEAGE.md`;
3. BSM recomputation — `src/dhan_data_fetch_stream/bsm_v2_runner.py`;
4. SPAN acquisition, extraction and timing releases — `src/nifty_span/` and
   `docs/research/CHECKPOINT3_EXECUTION_CAPITAL_MODELS.md`;
5. gold audit and sample — the `nifty-data-audit` and `nifty-data-sample` commands in `README.md`;
6. Phase 2–10 research — `research/phase2/` through `research/phase10/`, with exact full-data
   commands in each module runbook.

Exact dataset-root examples are deliberately placeholders because the full corpus is not in Git.
No command silently falls back to the sample for a full-data claim.

## Integrity and expected decision

Each closed module has a SHA-256 manifest. A valid rerun must preserve the current bounded
decision: the standalone 60–180-minute intraday VRP rule is rejected net of costs, while the
post-hoc gated upper-85 candidate remains shadow-only and receives no live allocation until a
new frozen forward sample passes its promotion gates.
