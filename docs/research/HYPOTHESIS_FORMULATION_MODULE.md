# Hypothesis Formulation Evidence Module

## Purpose

`nifty_hypothesis` packages every analysis used to move from the audited NIFTY
gold dataset to the frozen Phase 2 volatility hypothesis. It regenerates the
evidence, encodes the final hypothesis contract, and produces a machine-readable
closeout:

- playable moneyness and horizon boundaries;
- unconditional and wider-wing quote availability;
- parity-forward Black-76 ATM and local-chain IV;
- intraday, expiry-matched, and daily RV on explicit clocks;
- causal VRP percentiles, zero crossings, tails, and direction;
- frozen-contract defined-risk structure marks;
- compact event tables and a hash/row-count manifest.
- causal VRP-curve level, velocity, acceleration, and paired crossing tests;
- the final level-and-direction hypothesis and its closeout decision.

The package is intentionally separate from the later cost, slippage, SPAN
capital, OOS, and portfolio-decision modules.

## Package layout

```text
src/nifty_hypothesis/
  config.py       # dataset and path contract
  contracts.py    # stage order, dependencies, canonical outputs
  pipeline.py     # orchestration and manifest generation
  validation.py   # input identity and artifact checks
  cli.py          # plan, validate, run, closeout, and manifest commands

research/phase2/
  audit_*                         # playable-universe evidence
  analyze_intraday_volatility.py  # parity-forward IV/RV/VRP surface
  summarize_intraday_volatility.py
  analyze_matched_realized_variance.py
  analyze_defined_risk_vrp.py     # exact-strike bounded structures
  summarize_defined_risk_vrp.py   # tail and crossing events
  analyze_vrp_curve_crossings.py  # curve level/change/acceleration tests
  close_hypothesis_formulation.py # final causal evidence and closeout
  final_hypothesis.json           # frozen machine-readable hypothesis
```

The original scripts remain executable directly and are also importable as
`research.phase2` modules.

## Configuration

The committed example is
[`research/phase2/hypothesis_formulation.example.json`](../../research/phase2/hypothesis_formulation.example.json).
It uses an environment variable for the large local dataset:

```powershell
$env:ENDOVIA_GOLD_ROOT = `
  "C:\path\to\nifty_gold_span_bod_20210101_20260715\version=1.4.0\gold"
```

The loader resolves relative paths from `repo_root`, expands environment
variables, and validates that `audit/gold_dataset_audit.json` names the same
gold root. This prevents silently running the hypothesis analysis on a nearby
or stale dataset release.

## Stage contract

| Order | Stage | Main evidence |
|---:|---|---|
| 1 | `playable_universe` | Exact/stale structure availability |
| 2 | `moneyness_horizon` | Moneyness x horizon boundary |
| 3 | `unconditional_coverage` | All-date/all-clock coverage matrix |
| 4 | `wide_wing_sensitivity` | +/-3 versus +/-5, +/-7, +/-9 support |
| 5 | `intraday_surface` | ATM/local IV, skew, RV, and causal labels |
| 6 | `volatility_regimes` | Time, DTE, year, and percentile regimes |
| 7 | `matched_variance` | ACT/365 and daily-clock reconciliation |
| 8 | `defined_risk_paths` | Exact-contract structure MTM paths |
| 9 | `event_summary` | VRP tails, zero crossings, and daily events |
| 10 | `curve_crossings` | Causal curve level/change, acceleration, and paired threshold tests |
| 11 | `hypothesis_closeout` | Frozen hypothesis plus next-minute first-crossing evidence |
| 12 | `manifest` | Data, code, contract, calendar hashes and artifact row counts |

## Commands

Install the repo in editable mode, then inspect the plan before running:

```powershell
py -3.11 -m pip install -e .[dev]

nifty-hypothesis --config `
  research\phase2\hypothesis_formulation.example.json plan
```

Validate the exact dataset identity and currently available artifacts:

```powershell
nifty-hypothesis --config `
  research\phase2\hypothesis_formulation.example.json validate
```

Run all evidence stages and produce the manifest:

```powershell
nifty-hypothesis --config `
  research\phase2\hypothesis_formulation.example.json run
```

Resume a partially completed local run without repeating a stage whose complete
declared output set exists:

```powershell
nifty-hypothesis --config `
  research\phase2\hypothesis_formulation.example.json run --resume
```

The manifest is always regenerated, including on a resumed run, so it cannot
silently retain stale hashes after an upstream artifact or source file changes.

Run or inspect a bounded stage range:

```powershell
nifty-hypothesis --config `
  research\phase2\hypothesis_formulation.example.json run `
  --from-stage intraday_surface --through-stage event_summary
```

Hash and row-count the current evidence without recomputing it:

```powershell
nifty-hypothesis --config `
  research\phase2\hypothesis_formulation.example.json manifest
```

Rebuild only the final curve and closeout stages when their dependencies are
already present:

```powershell
nifty-hypothesis --config `
  research\phase2\hypothesis_formulation.example.json run `
  --from-stage curve_crossings --through-stage hypothesis_closeout
```

Or regenerate only the closeout from the existing curve evidence:

```powershell
nifty-hypothesis --config `
  research\phase2\hypothesis_formulation.example.json closeout
```

The canonical human-readable result is
[`FINAL_HYPOTHESIS.md`](FINAL_HYPOTHESIS.md). The frozen contract is
`research/phase2/final_hypothesis.json`, and the generated machine closeout is
`audit/phase2_final_hypothesis_closeout.json`.

## Reproducibility rules

- `observed` sessions and `computed` entry moneyness are the canonical coverage
  settings for this dataset.
- Forward RV and future structure P&L are outcome labels only.
- Causal percentiles use prior dates at the same minute of day.
- Entry legs are selected inside rolling ATM +/-3 and then frozen by strike and
  option type for the exit mark.
- Missing exact exit contracts remain missing; the pipeline does not substitute
  a later rolling-offset leg.
- The nearest-listed-expiry clock remains a documented research proxy because
  Dhan rolling history does not return the actual contract expiry identity.
- Full Parquet artifacts remain local and Git-ignored. JSON evidence, code,
  configuration, frozen hypothesis contract, tests, and documentation are the
  pushable module.

## What belongs in the later hypothesis test

The module now ends with the hypothesis frozen and formulation explicitly
closed. A later confirmation module must separately implement:

- the already frozen headline structure and entry event without retuning;
- non-overlapping entry selection;
- IS/validation/OOS dates;
- volume/OI, staleness, slippage, and charges;
- SPAN capital and sizing;
- tail, drawdown, weekly/monthly stability, capacity, and kill-switch gates.
