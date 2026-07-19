# Module 4 architecture

## Lineage

```text
Module 3 closeout
  └─ post-hoc upper-85 gated candidate
      ├─ corrected execution layer
      │   ├─ dated Groww charges
      │   ├─ depth/staleness slippage
      │   ├─ quantity ladder + volume/OI impact
      │   └─ timestamp-aware entry SPAN
      ├─ Phase 8: ₹10 lakh capital simulator
      ├─ Phase 9: confidence score and one-lot rank test
      ├─ Phase 10: discovery-only sizing grid
      └─ Module 4: frozen contract, reconciliation, trade sheet, curves, figures, hashes
```

The wrapper deliberately imports no private strategy implementation. It reads the published Phase
8–10 audit outputs, joins the selected profile to the exact-lot cost surface, and independently
reconciles gross P&L, total costs, net P&L, turnover, margin, and cost-reserved risk.

## Economic ordering

At each event, Phase 10 applies the frozen quality switch first. Eligible events receive the lesser
of the entry-SPAN lot cap, the defined-loss-plus-q95-cost-reserve lot cap, and the 76-lot research
capacity ceiling. Exact costs are then selected from the quantity surface for that integer lot
count. Equity changes only at the 60-minute fixed exit.

## Data contracts

- `audit/phase9_scored_events.csv` fixes the causal confidence inputs.
- `audit/phase10_fly_cost_surface.parquet` fixes exact-lot gross, charge, slippage, margin, and loss
  economics for 1–100 lots.
- `audit/phase10_selected_profile_tradebook.csv` fixes the event-by-event policy path.
- `audit/phase10_selected_profiles.csv` fixes profile-level tear-sheet statistics.
- `contracts/strategy.json` fixes the only candidate allowed into forward shadow evaluation.

## Reproducibility layers

1. The Phase runners regenerate their own audit outputs from the local gold data.
2. Module 4 regenerates presentation artifacts from the preserved compact evidence.
3. The manifest detects changes to every source, implementation, document, or result member.
4. Tests reconcile the selected policy and enforce its margin/risk/acceptance boundary.
