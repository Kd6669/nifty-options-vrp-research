# Module 3 — VRP hypothesis-testing closeout

## Decision

**REJECT the tested intraday standalone-VRP defined-risk strategy family for the current dataset.**

This closes the economic testing layer. IV and RV were independently normalized onto the intraday clock before constructing VRP; provider chain IV was not accepted at face value. Every economic result below is one historical exchange lot per completed trade, with dated charges, volume/OI slippage, ATM-IV fallback when India VIX is absent, conservative fills, and timestamp-aware SPAN margin.

## Test sequence

| Test | Main evidence | Decision |
|---|---|---|
| Zero crossing | 60m short condor: +₹22.60 gross, −₹248.54 net over 895 trades | Rejected after costs |
| Tail level and direction | Best of 12 at 60m: q85_up at −₹170.78 net/trade; dated-STT rerun −₹169.32 | All 12 rejected |
| Velocity and acceleration | Rank correlations are near zero and deeper crossings fail paired-session ordering | Rejected as confidence/leverage rule |
| Structures and horizons | 0 positive-net cells out of 48 | No structure/horizon rescue |
| Causal feature rescue | 190 locked-confirmation trades, −₹165.02 net/trade | Rejected |
| Tail mean reversion | 492 frozen events; requested and aggregate inverse mappings fail | Rejected |
| Unified 180m | 0 credible positive cells out of 32 requested/inverse cells | No credible edge |

## The material 180-minute gross results

The longest observable horizon does produce several materially larger gross means:

| Mapping | Signal | Signals | Evaluated | Unevaluated | Coverage | Gross/trade | Cost/trade | Net/trade |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| inverse | reversal_top | 191 | 96 | 95 | 50.26% | +₹279.90 | ₹248.71 | +₹31.18 |
| inverse | q95_up | 94 | 56 | 38 | 59.57% | +₹202.34 | ₹291.84 | −₹89.49 |
| inverse | q80_up | 363 | 247 | 116 | 68.04% | +₹75.39 | ₹280.97 | −₹205.58 |
| requested | q90_up | 181 | 121 | 60 | 66.85% | +₹74.64 | ₹272.88 | −₹198.23 |
| requested | q85_down | 296 | 226 | 70 | 76.35% | +₹69.88 | ₹258.32 | −₹188.44 |

This is important evidence of slower payoff maturation, but it is not a profitable strategy result. The only positive-net cell is the inverse top-reversal short condor at +₹31.18 across 96 evaluated trades with 50.26% coverage and a confidence interval spanning −₹222.31 to +₹263.74. The other 95 of 191 signals are unevaluated—not wins, losses, or imputed trades. It fails every robustness condition needed for promotion.

## Economic conclusion

At 60 minutes, the original zero-crossing gross edge is only +₹22.60 against ₹271.14 average cost. Tail selection, direction, velocity, acceleration, alternative defined-risk structures, and a causal feature model do not bridge the hurdle. At 180 minutes, selected gross means rise above ₹65 and sometimes much further, but coverage falls and the payoff remains too inconsistent to clear approximately ₹249–₹311 per-trade cost.

The supported conclusion is therefore economic rather than metaphysical: **within the observable intraday window, these standalone VRP rules do not mature or become realized in defined-risk option prices consistently enough to cover one-lot trading costs.**

## Data boundary

The rolling nearest-weekly ATM±10 archive cannot support an unbiased test beyond 180 minutes. For the apparent top-reversal candidate, 95 of 191 signals have no evaluated 180-minute outcome. Frozen strikes progressively leave the observed surface, exact-contract coverage falls non-randomly, and multi-session contract tracking is incomplete. A longer-horizon or multi-day VRP hypothesis remains open only for a future full fixed-contract chain dataset; no profitability is extrapolated from this module.

## Reproduce and verify

```powershell
python -m research.module3_hypothesis_testing.run build
python -m research.module3_hypothesis_testing.run verify
python -m pytest -q
```

The detailed phase reports and full CSV trade/leg books remain preserved under `docs/research/` and `audit/`. `results/manifest.json` hashes the calculation code, contracts, source summaries, documentation, and generated closeout outputs.
