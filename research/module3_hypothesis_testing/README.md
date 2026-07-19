# Module 3 — VRP hypothesis testing

This is the closed, reproducible economic-testing module for the normalized intraday VRP
hypothesis formulated in Phase 2.

It consolidates, without retuning:

1. first daily zero crossings;
2. 70/75/80/85/90/95 percentile-tail crossings in both directions;
3. VRP percentile level, direction, velocity, and acceleration;
4. eight defined-risk structures across 60/120/180 minutes and multi-day feasibility;
5. the locked causal-feature rescue attempt;
6. top-tail and bottom-tail mean-reversion events for long and short condors;
7. every previously tested signal on one unified 180-minute requested/inverse basis.

## Closed result

The strategy family fails on a per-trade, per-one-lot basis after dated charges and modeled
execution. Select 180-minute cells have materially larger positive gross means—above ₹65 per
trade—but none forms a credible positive-net strategy. The sole positive-net diagnostic is a
96-trade, 50.26%-coverage inverse subgroup with an interval spanning large losses and gains. It
originates from 191 signals: 96 are evaluated and 95 have no observable 180-minute outcome.
Those 95 are explicitly unevaluated rather than classified or imputed.

This supports the bounded conclusion that the tested VRP moves do not mature or become realized
in defined-risk prices consistently enough within 60–180 minutes to cover costs. It does not
settle horizons beyond 180 minutes: those require full fixed-contract chain history rather than
the current rolling ATM±10 archive.

## Layout

- `contracts/`: frozen hypotheses and final states;
- `docs/`: architecture and reproduction runbook;
- `results/`: deterministic closeout JSON, report, and integrity manifest;
- `scripts/`: Windows build-and-verify entrypoint;
- `closeout.py`: evidence consolidation and report generation;
- `run.py`: module CLI.

Canonical experiment implementations remain in `research/phase3/` through `research/phase7/` and
the execution engines remain in `src/nifty_execution/`. The generated manifest binds them into
this module by SHA-256, avoiding duplicated calculation code.

```powershell
python -m research.module3_hypothesis_testing.run build
python -m research.module3_hypothesis_testing.run verify
```
