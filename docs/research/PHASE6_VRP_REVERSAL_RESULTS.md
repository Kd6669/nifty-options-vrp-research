# Phase 6 — VRP tail-reversal result

## Decision

**FAIL — the Phase 5 closure remains in force.**

The requested rule was tested exactly once after being frozen:

- after a causal VRP percentile reached at least 90%, a 10-point reversal while VRP remained
  positive triggered a long iron condor;
- after it reached at most 10%, a 10-point reversal while VRP remained negative triggered a
  short iron condor;
- entry occurred at the next exact minute;
- the primary exit was 60 minutes;
- the exact inverse mapping and 120/180-minute exits were prespecified diagnostics.

The requested 60-minute mapping has essentially zero gross edge, loses ₹294.12 per one-lot
trade after costs, and fails five of seven acceptance gates. The exact inverse also loses.

## Events and premise

The first-daily state machine produced 492 events from 2021-04-01 through 2026-07-09:

| Reversal | Events |
|---|---:|
| Top tail toward zero | 191 |
| Bottom tail toward zero | 301 |

### Did VRP actually revert to zero?

| Reversal | Horizon | Zero-touch rate | Distance-to-zero reduced | Median minutes to zero when touched |
|---|---:|---:|---:|---:|
| Bottom → zero | 60m | 24.92% | 55.16% | 28 |
| Bottom → zero | 120m | 32.89% | 49.06% | 35 |
| Bottom → zero | 180m | 35.55% | 45.21% | 36 |
| Top → zero | 60m | 46.60% | 24.49% | 6 |
| Top → zero | 120m | 56.02% | 29.84% | 9 |
| Top → zero | 180m | 69.63% | 34.34% | 16 |

Top-tail signals often cross zero quickly but frequently overshoot or move away again by the
fixed exit; hence zero-touch rates exceed endpoint distance-reduction rates. Bottom-tail signals
more often reduce their distance without actually reaching zero. The proposed event is therefore
not a clean, persistent zero-reversion process.

## Requested mapping

| Horizon | Complete trades | Coverage | Gross ₹/trade | Cost ₹/trade | Net ₹/trade | Net win rate | Mean net ROM |
|---|---:|---:|---:|---:|---:|---:|---:|
| **60m primary** | **428** | **86.99%** | **−2.71** | **291.41** | **−294.12** | **8.41%** | **−0.5725%** |
| 120m | 335 | 68.09% | −42.32 | 289.31 | −331.63 | 13.73% | −0.6216% |
| 180m | 242 | 49.19% | −104.60 | 285.97 | −390.57 | 14.88% | −0.7655% |

Primary 60-minute totals:

- gross P&L: −₹1,157.75;
- total cost: ₹124,724.97;
- net P&L: **−₹125,882.72**;
- 95% trade-date block-bootstrap mean-net interval: **−₹331.79 to −₹254.19**;
- mean SPAN margin: ₹54,990.13;
- positive populated months: **3 / 64 = 4.69%**.

The option result is not merely a cost-erased positive edge: gross rupee P&L is already slightly
negative.

### Requested 60-minute subgroups

| Reversal | Trades | Gross points | Gross ₹/trade | Cost ₹/trade | Net ₹/trade | Net win rate |
|---|---:|---:|---:|---:|---:|---:|
| Bottom → zero / short condor | 281 | +0.355 | +6.87 | 306.62 | −299.75 | 3.91% |
| Top → zero / long condor | 147 | −0.222 | −21.00 | 262.35 | −283.35 | 17.01% |

Both sides of the requested symmetric rule fail separately.

## Exact inverse

| Horizon | Trades | Coverage | Gross ₹/trade | Cost ₹/trade | Net ₹/trade | Net win rate |
|---|---:|---:|---:|---:|---:|---:|
| 60m | 428 | 86.99% | +2.71 | 291.41 | −288.71 | 11.92% |
| 120m | 335 | 68.09% | +42.32 | 289.34 | −247.02 | 19.10% |
| 180m | 242 | 49.19% | +104.60 | 286.03 | −181.43 | 30.17% |

The inverse direction has better gross results as the horizon increases, but no aggregate cell
is profitable after costs. Coverage also falls below 50% at 180 minutes.

The isolated top-reversal/inverse 180-minute subgroup has +₹31.18 mean net P&L across 96 complete
trades. It cannot be promoted because it is the opposite of the requested primary, has fewer than
100 observations, belongs to the low-coverage 180-minute sensitivity, and was discovered only
after the aggregate test. The complete inverse 180-minute mapping remains −₹181.43/trade.

## Acceptance gates

| Primary gate | Result |
|---|---|
| At least 100 complete daily trades | PASS |
| Positive mean net P&L | FAIL |
| Positive aggregate net P&L | FAIL |
| Bootstrap lower bound above zero | FAIL |
| Both reversal subgroups positive | FAIL |
| At least 80% exact-contract coverage | PASS |
| At least 60% positive months | FAIL |

Five of seven gates fail.

## Interpretation

The test separates three possible claims:

1. **VRP begins reversing after an extreme:** sometimes true, particularly for top-tail zero
   touches.
2. **The reversal persists toward zero at the fixed exit:** weak and asymmetric.
3. **The event predicts enough long/short defined-risk option P&L to cover execution:** false.

The long/short condor is not a direct trade on the VRP scalar. VRP can fall because IV falls, RV
rises, or both; those decompositions affect long vega and long gamma differently. The symmetric
top-long/bottom-short mapping therefore has no reliable gross payoff relationship in this sample.

## Reproduce

```powershell
python -m research.phase6.run_vrp_reversal_test `
  --gold-root "<six-slot-v2.1-gold-root>"

python -m pytest tests/test_phase6_vrp_reversal.py -q
```

Artifacts:

- `audit/phase6_reversal_events.csv`
- `audit/phase6_reversal_observations.parquet`
- `audit/phase6_reversal_zero_diagnostics.csv`
- `audit/phase6_reversal_tradebook.csv`
- `audit/phase6_reversal_summary.json`
- `audit/phase6_reversal_manifest.json`

This concludes the post-hoc reversal check. No further VRP entry rule should be selected on the
same research sample.
