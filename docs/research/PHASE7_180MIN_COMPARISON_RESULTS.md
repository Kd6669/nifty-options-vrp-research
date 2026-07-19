# Phase 7 — unified 180-minute VRP signal comparison

## Decision

**NO CREDIBLE 180-MINUTE EDGE. The Phase 5 hypothesis-family closure remains in force.**

All previously tested VRP event definitions were repriced at one fixed 180-minute horizon with
the same one-lot execution, cost, slippage, and timestamp-aware SPAN implementation. No requested
signal produces positive mean net P&L. The only positive cell in the exact-inverse diagnostics is
too small, has only 50.26% exact-contract coverage, and has a confidence interval spanning large
losses and gains. Of 191 top-reversal signals, 96 are evaluated and 95 have no exact 180-minute
outcome. Those 95 are unevaluated—not wins, losses, or imputed observations.

## What was compared

The frozen universe contains 6,099 signal memberships at 5,536 unique entry timestamps:

- first daily normalized-VRP zero crossing upward and downward;
- first daily causal-percentile crossing at 70%, 75%, 80%, 85%, 90%, and 95%, in both directions;
- first daily top-tail and bottom-tail reversal from Phase 6.

Each event enters at the next minute and holds the exact expiry/strike/type contracts for 180
minutes. The basket is the ATM±1 iron condor protected at ATM±3. Identical timestamps are priced
once, while overlapping signal cells remain separate. They must not be summed into a portfolio.

Historical/requested mappings are zero-up short, zero-down long, every percentile-crossing short,
top-reversal long, and bottom-reversal short. The inverse diagnostic reverses the structure on the
same event.

## Requested mappings

All rupee figures are per one historical exchange lot and per completed trade.

| Signal | Events | Evaluated | Unevaluated | Coverage | Gross ₹ | Cost ₹ | Net ₹ | Net win | Mean ROM | Bootstrap mean-net 95% CI |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Zero up / short condor | 895 | 768 | 127 | 85.81% | +27.36 | 267.72 | **−240.37** | 22.66% | −0.4561% | −290.11 to −192.82 |
| Zero down / long condor | 684 | 337 | 347 | 49.27% | −0.15 | 273.08 | **−273.22** | 15.43% | −0.5901% | −344.47 to −197.92 |
| q70 up / short condor | 568 | 359 | 209 | 63.20% | −8.37 | 276.77 | **−285.14** | 28.69% | −0.5172% | −384.54 to −190.85 |
| q70 down / short condor | 588 | 437 | 151 | 74.32% | +0.96 | 259.91 | **−258.95** | 24.03% | −0.4520% | −335.06 to −189.06 |
| q75 up / short condor | 475 | 303 | 172 | 63.79% | −55.06 | 281.01 | **−336.07** | 31.02% | −0.5976% | −456.98 to −226.12 |
| q75 down / short condor | 513 | 383 | 130 | 74.66% | +30.37 | 261.09 | **−230.72** | 30.03% | −0.3991% | −319.18 to −146.59 |
| q80 up / short condor | 363 | 247 | 116 | 68.04% | −75.39 | 280.90 | **−356.29** | 34.41% | −0.6585% | −514.75 to −206.28 |
| q80 down / short condor | 392 | 305 | 87 | 77.81% | +42.99 | 257.12 | **−214.13** | 36.39% | −0.3383% | −335.47 to −98.13 |
| q85 up / short condor | 272 | 176 | 96 | 64.71% | +22.78 | 285.44 | **−262.65** | 36.36% | −0.5338% | −436.47 to −95.98 |
| q85 down / short condor | 296 | 226 | 70 | 76.35% | +69.88 | 258.32 | **−188.44** | 42.48% | −0.3094% | −340.00 to −47.03 |
| q90 up / short condor | 181 | 121 | 60 | 66.85% | +74.64 | 272.88 | **−198.23** | 46.28% | −0.5396% | −417.08 to +9.74 |
| q90 down / short condor | 194 | 137 | 57 | 70.62% | +8.68 | 252.59 | **−243.91** | 45.26% | −0.5196% | −457.24 to −36.43 |
| q95 up / short condor | 94 | 56 | 38 | 59.57% | −202.34 | 291.73 | **−494.07** | 35.71% | −1.2612% | −847.04 to −144.80 |
| q95 down / short condor | 92 | 60 | 32 | 65.22% | +30.95 | 266.68 | **−235.74** | 41.67% | −0.6480% | −531.56 to +49.45 |
| Top reversal / long condor | 191 | 96 | **95** | 50.26% | −279.90 | 248.56 | **−528.46** | 17.71% | −1.0768% | −758.15 to −276.51 |
| Bottom reversal / short condor | 301 | 146 | 155 | 48.50% | +10.67 | 310.57 | **−299.91** | 13.01% | −0.5608% | −372.10 to −225.69 |

The best requested mean is q85-down short at −₹188.44 per trade. The q90-up short cell has the
largest requested gross mean, +₹74.64, but its ₹272.88 average execution bill still leaves
−₹198.23 net. No requested rule is close to clearing costs consistently.

## Exact inverse diagnostics

| Signal | Inverse structure | Events | Evaluated | Unevaluated | Coverage | Gross ₹ | Cost ₹ | Net ₹ | Bootstrap mean-net 95% CI |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Zero up | Long condor | 895 | 768 | 127 | 85.81% | −27.36 | 267.71 | **−295.06** | −341.80 to −245.51 |
| Zero down | Short condor | 684 | 337 | 347 | 49.27% | +0.15 | 273.07 | **−272.92** | −350.14 to −200.56 |
| q70 up | Long condor | 568 | 359 | 209 | 63.20% | +8.37 | 276.80 | **−268.43** | −362.03 to −166.72 |
| q70 down | Long condor | 588 | 437 | 151 | 74.32% | −0.96 | 259.94 | **−260.91** | −330.25 to −185.19 |
| q75 up | Long condor | 475 | 303 | 172 | 63.79% | +55.06 | 281.08 | **−226.02** | −335.30 to −102.97 |
| q75 down | Long condor | 513 | 383 | 130 | 74.66% | −30.37 | 261.10 | **−291.47** | −375.76 to −202.90 |
| q80 up | Long condor | 363 | 247 | 116 | 68.04% | +75.39 | 280.97 | **−205.58** | −354.86 to −46.09 |
| q80 down | Long condor | 392 | 305 | 87 | 77.81% | −42.99 | 257.13 | **−300.12** | −413.62 to −181.27 |
| q85 up | Long condor | 272 | 176 | 96 | 64.71% | −22.78 | 285.42 | **−308.20** | −473.18 to −137.31 |
| q85 down | Long condor | 296 | 226 | 70 | 76.35% | −69.88 | 258.31 | **−328.18** | −466.67 to −177.63 |
| q90 up | Long condor | 181 | 121 | 60 | 66.85% | −74.64 | 272.79 | **−347.44** | −551.05 to −126.83 |
| q90 down | Long condor | 194 | 137 | 57 | 70.62% | −8.68 | 252.62 | **−261.31** | −464.20 to −50.36 |
| q95 up | Long condor | 94 | 56 | 38 | 59.57% | +202.34 | 291.84 | **−89.49** | −430.86 to +259.48 |
| q95 down | Long condor | 92 | 60 | 32 | 65.22% | −30.95 | 266.64 | **−297.59** | −577.06 to −3.28 |
| Top reversal | Short condor | 191 | 96 | **95** | 50.26% | +279.90 | 248.71 | **+31.18** | −222.31 to +263.74 |
| Bottom reversal | Long condor | 301 | 146 | 155 | 48.50% | −10.67 | 310.56 | **−321.23** | −388.80 to −249.77 |

The inverse top-reversal short condor is the sole positive mean. It fails three central gates:
96 evaluated trades is below 100, 50.26% coverage is far below 80%, and the confidence interval
crosses zero widely. The remaining 95 top-reversal signals have no evaluated 180-minute outcome;
they are neither wins nor losses and are not imputed. The cell was also identified as an inverse
subgroup after the primary reversal rule had failed. It is not a strategy candidate.

## Coverage boundary and longer horizons

The Phase 6 aggregate inverse reversal diagnostic improved from +₹2.71 gross / −₹288.71 net at
60 minutes to +₹42.32 / −₹247.02 at 120 and +₹104.60 / −₹181.43 at 180. This monotonic improvement
is a legitimate reason to investigate whether some VRP effects operate more slowly.

It does **not** prove that holding longer makes the strategy profitable or that the pattern applies
to every signal. Aggregate requested reversals worsen with horizon, every aggregate inverse result
remains net negative, and exact-contract reversal coverage falls from 86.99% at 60 minutes to
49.19% at 180. In this unified test, only zero-up reaches the 80% coverage gate; every other
180-minute signal cell is below 78%.

The rolling nearest-weekly ATM±10 archive cannot support an unbiased conclusion beyond 180
minutes. Frozen strikes migrate out of the observed rolling surface, missingness is not random,
and multi-session expiry/contract tracking is incomplete. Longer-horizon and multi-day VRP
research may yield different economics, but it is outside the current data-coverage scope and
requires full fixed-contract chain history.

## Acceptance decision

A credible lead required all four of:

1. at least 100 completed trades;
2. at least 80% exact-contract coverage;
3. positive mean net P&L;
4. a positive date-block-bootstrap lower 95% bound.

**Zero of 32 requested/inverse cells pass.** The 180-minute comparison therefore supplies a
robustness and research-boundary result, not a viable trading rule. No further threshold should be
selected from this same sample.

## Reproduce

```powershell
python -m research.phase7.run_180min_signal_comparison `
  --gold-root "<six-slot-v2.1-gold-root>"

python -m pytest tests/test_phase7_180min_comparison.py -q
```

Artifacts:

- `audit/phase7_180min_membership.csv`
- `audit/phase7_180min_executions.csv`
- `audit/phase7_180min_observations.parquet` (large, local and Git-ignored)
- `audit/phase7_180min_tradebook.csv`
- `audit/phase7_180min_summary.json`
- `audit/phase7_180min_manifest.json`

The manifest pins the protocol, code, upstream Phase 2 event inputs, volatility surface, and every
compact result artifact with SHA-256 hashes.
