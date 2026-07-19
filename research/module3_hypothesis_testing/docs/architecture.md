# Module 3 architecture

```text
Phase 2 frozen IV/RV/VRP and fixed-contract paths
        |
        +--> Phase 3: zero crossing and percentile-tail execution tests
        +--> Phase 4: costs, capacity, structures, horizons, multi-day feasibility
        +--> Phase 5: causal feature-model rescue with locked confirmation
        +--> Phase 6: tail-to-zero reversal test
        +--> Phase 7: unified 180-minute requested/inverse comparison
        |
        +--> Module 3 closeout builder
                +--> results/closeout.json
                +--> results/closeout_report.md
                +--> results/manifest.json
```

## Ownership

- `src/nifty_execution/` owns dated Groww charges, volume/OI slippage, fill policy, and
  timestamp-aware SPAN margin selection.
- `research/phase3/` through `research/phase7/` own the frozen calculations. They remain the
  canonical implementations; this module does not duplicate or subtly fork them.
- `audit/phase2_*` through `audit/phase7_*` contain machine results and full trade/leg evidence.
- `research/module3_hypothesis_testing/` owns hypothesis contracts, evidence consolidation,
  integrity verification, and the final decision.

## Boundaries

All headline economics are per completed trade and one historical exchange lot. Margin affects
return-on-margin but no fixed capital pool, compounding, portfolio allocation, or lot-sizing rule
is introduced. Signal variants overlap and are research cells rather than simultaneous positions.

The primary 60-minute hypothesis has high path availability. The 180-minute comparison is a
diagnostic: frozen strikes increasingly leave the rolling ATM±10 surface, making missingness
non-random. The module therefore closes the observed intraday question without claiming that
longer or multi-session VRP trades fail.
