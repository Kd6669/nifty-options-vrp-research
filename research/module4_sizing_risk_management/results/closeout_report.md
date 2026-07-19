# Module 4 — sizing and risk-management closeout

## Decision

**Forward-shadow candidate; not deployment approved.** Module 3 rejected normalized intraday VRP
as a standalone 60–180 minute defined-risk entry rule. Module 4 preserves one post-hoc gated
upper-tail short-iron-fly candidate, tests whether confidence can rank its one-lot outcomes, and
then freezes a capital-efficient sizing rule. The historical economics are presentable, but the
Phase 9 rank bootstrap gate failed and the score is not clean out-of-sample evidence.

## Frozen strategy contract

- NIFTY nearest-weekly proxy; legs inside ATM±3 at entry.
- Upper-85 normalized intraday VRP crossing upward; 60-minute fixed exit.
- Short iron fly; exact integer lots and date-aware Groww charges.
- Base depth/staleness slippage plus quantity-aware ladder, volume, and OI impact.
- Timestamp-aware joined SPAN at entry; ATM IV substitutes for missing India VIX.
- Entry gates: IV 5m > −0.046817 vol points, IV 15m > −0.114873 vol points, and normalized RV 5m > −0.02651714.
- Frozen quality switch: confidence score strictly above 40%; hard switch (power 0).
- Sizing: at most 35% of current equity in entry SPAN and 4% in defined max loss plus the
  discovery q95 exact round-trip cost reserve; 76-lot capacity ceiling.
- No fitted drawdown or losing-streak brake.

## Recommended historical profile

| Metric | Result |
|---|---:|
| Candidate signals | 132 |
| Executed trades | 86 |
| Average / maximum lots | 6.50 / 14 |
| Gross P&L | ₹153,012 |
| Total costs | ₹44,029 |
| Net P&L | ₹108,983 |
| Ending equity | ₹1,108,983 |
| Total return / CAGR | 10.90% / 1.99% |
| Win rate | 66.28% |
| Profit factor | 2.537 |
| Maximum drawdown | -₹7,982 (-0.76%) |
| 5% trade CVaR | -₹6,142 |
| Turnover | ₹6,670,528 |
| Average / maximum margin use | 31.93% / 34.97% |
| Average / maximum cost-reserved risk | 2.79% / 3.98% |

Calendar net P&L: 2021–23 discovery ₹70,835; 2024
validation ₹15,113; 2025–26 confirmation
₹23,036; combined later period
₹38,148. The deepest daily-close drawdown episode bottoms on
2023-05-25 at -₹7,982 (-0.76%).

## What passed and what did not

The 17,640-policy sizing grid had 4,392 discovery-eligible
policies. 58.8% were positive in the combined later
period, 45.8% were positive in both later
slices, and discovery-versus-holdout policy net rank correlation was
0.649. The score-floor-40% / low-margin region
forms a useful neighborhood rather than a single isolated optimizer cell.

However, the frozen composite score's combined-holdout one-lot net-P&L Spearman rho was
0.214, with bootstrap 95% interval
[-0.066, 0.484]. Because the lower
bound crosses zero, the sizing score did **not** pass its preregistered confidence gate. Positive
historical capital results therefore support only a frozen forward shadow test.

## Preserved evidence

- `trades/recommended_trade_sheet.csv`: all 132 signals, including 46 explicit skips, exact lots,
  binding cap, margin, cost-reserved loss, complete charge/slippage attribution, P&L, and equity.
- `curves/`: business-day equity, monthly returns, and drawdown episodes.
- `diagnostics/`: candidate profiles, cost breakdown, rank tests, score quintiles, regime results,
  and grid-neighborhood summaries.
- `exploration/sizing_grid.csv.gz`: the complete deterministic 17,640-policy grid.
- `visualizations/`: equity/drawdown, selected-profile frontier, and sizing-neighborhood figures.
- `manifest.json`: SHA-256 lineage over the contracts, implementations, source evidence, and results.

## Research boundary

These results do not observe intratrade MTM drawdown, SPAN expansion, forced liquidation, or
stop-loss performance. They cannot validate multi-day, multi-expiry, or horizons beyond the
rolling-chain coverage. Do not re-optimize the quality score or choose a new profile using the
same later-period outcomes. Freeze config 5628 and acquire untouched forward observations.

## Reproduce and verify

```powershell
python -m research.phase8.run_gated_capital_backtest
python -m research.phase9.run_confidence_sizing
python -m research.phase10.run_sizing_exploration
python -m research.module4_sizing_risk_management.run build
python -m research.module4_sizing_risk_management.run verify
python -m pytest tests/test_phase8_gated_capital.py tests/test_phase9_confidence_sizing.py tests/test_phase10_sizing_exploration.py tests/test_module4_sizing_risk_management.py -q
```

The Phase 8–10 reruns require the local gold observation Parquets documented in the runbook.
Module 4 itself rebuilds from the preserved compact audit outputs and exact-lot cost surface.
