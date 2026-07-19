# Phase 3 full-strategy tear sheet

## Decision

The frozen first-daily VRP cross-up short iron condor **fails cost-inclusive economic
viability** at one historical exchange lot per signal.

The gross signal is reproduced exactly: 895 trades, `+0.4726` option points per trade,
`65.81%` gross winners, and ₹20,226.25 aggregate one-lot gross P&L. But the average gross
P&L is only ₹22.60 while the average round-trip execution bill is ₹271.14. Base net P&L is
therefore **-₹222,440.67**, or **-₹248.54 per trade**.

This is not a portfolio simulation. There is no fixed capital pool, compounding, leverage,
position-sizing rule, overlapping-position allocator, or risk-management overlay. Every signal
is evaluated independently at exactly **one historical NIFTY lot**.

### Checkpoint closure

The first sustained normalized-VRP zero-cross-up is therefore **rejected as an executable trading
rule under the pinned Phase 3 friction model**. Lot scaling cannot rescue it: after fixed
brokerage is removed, its average quantity-scaled cost still exceeds its average gross edge.
This closes the zero-cross checkpoint without changing the earlier preregistered formulation.

The next diagnostic tests first daily upper-tail percentile crossings at 70%, 75%, 80%, 85%,
90%, and 95% in both directions, one lot per event. See
[`PHASE3_TAIL_PERCENTILE_TEAR_SHEET.md`](PHASE3_TAIL_PERCENTILE_TEAR_SHEET.md).

## Headline one-lot results

| Metric | Gross marks | Base costs | 1.5x slippage |
|---|---:|---:|---:|
| Trades | 895 | 895 | 895 |
| Aggregate P&L | ₹20,226.25 | **-₹222,440.67** | **-₹248,909.65** |
| Mean P&L per trade | ₹22.60 | **-₹248.54** | **-₹278.11** |
| Median P&L per trade | ₹27.50 | -₹227.85 | -₹247.18 |
| Win rate | 65.81% | **9.39%** | **8.72%** |
| 5th percentile | -₹297.50 | -₹627.97 | -₹706.10 |
| 5% CVaR | -₹704.83 | -₹984.88 | -₹1,060.84 |
| Profit factor | not used | 0.0700 | 0.0586 |
| Maximum sequential drawdown | not capitalized | -₹222,440.67 | -₹248,909.65 |
| Positive months | not used | **0 / 65** | **0 / 65** |

The base cost hurdle averages **5.4001 option points per trade**, compared with only `+0.4726`
gross points. Aggregate costs are 12.00 times aggregate gross profit. Only 9.39% of trades have
gross P&L large enough to clear their own base execution bill.

## Margin and return on margin

SPAN Model-A is applied to the selected entry-time slot for each four-leg basket.

| One-trade capital metric | Base result |
|---|---:|
| Mean required margin | ₹55,154.72 |
| Median required margin | ₹46,444.89 |
| 5th / 95th percentile | ₹28,364.91 / ₹91,433.23 |
| Maximum required margin | ₹115,767.11 |
| Mean net return on trade margin | **-0.4859%** |
| Median net return on trade margin | **-0.4742%** |
| Bootstrap 95% CI for mean net ROM | **-0.5183% to -0.4548%** |
| 5th percentile net ROM | -1.0793% |
| 5% CVaR net ROM | -1.7147% |

At 1.5x slippage, mean net return on margin falls to `-0.5374%`; its bootstrap 95% interval is
`-0.5704%` to `-0.5053%`. Both intervals are wholly below zero.

These are per-trade capital-efficiency observations. Summing the return-on-margin series is not
a portfolio return because no capital pool or concurrency rule has been specified.

## Gross-to-net attribution

| Component | Aggregate | Mean per trade | Share of total cost |
|---|---:|---:|---:|
| Brokerage: eight ₹20 orders | ₹143,200.00 | ₹160.00 | 59.01% |
| Synthetic adverse slippage | ₹52,963.68 | ₹59.18 | 21.83% |
| GST | ₹27,198.16 | ₹30.39 | 11.21% |
| Sell-side STT | ₹11,071.06 | ₹12.37 | 4.56% |
| Exchange transaction charges | ₹7,767.85 | ₹8.68 | 3.20% |
| Stamp duty | ₹333.11 | ₹0.37 | 0.14% |
| IPFT | ₹110.87 | ₹0.12 | 0.05% |
| SEBI turnover fees | ₹22.17 | ₹0.02 | 0.01% |
| **Total charges excluding slippage** | **₹189,703.24** | **₹211.96** | **78.17%** |
| **Total execution cost** | **₹242,666.92** | **₹271.14** | **100.00%** |

The dominant problem is structural: four legs entered and four legs exited create ₹160 of fixed
brokerage before slippage or statutory charges. The gross edge is too small for this order count.

## Turnover

| One-lot premium turnover | Result |
|---|---:|
| Aggregate entry-plus-exit turnover | ₹22,174,859.77 |
| Mean per trade | ₹24,776.38 |
| Median per trade | ₹18,349.84 |
| 5th / 95th percentile | ₹2,762.76 / ₹72,927.08 |
| Median all-in cost | 130.96 bps of premium turnover |

Turnover uses the adverse modeled fill price and the historical contract lot size. It is option
premium turnover, not underlying notional turnover.

## Exact inverse comparator

The long-condor inverse loses before and after costs:

| Metric | Short condor | Exact inverse long condor |
|---|---:|---:|
| Gross aggregate | +₹20,226.25 | -₹20,226.25 |
| Base net aggregate | **-₹222,440.67** | **-₹262,873.61** |
| Base mean per trade | -₹248.54 | -₹293.71 |
| Base net win rate | 9.39% | 6.15% |
| Mean net return on margin | -0.4859% | -0.6073% |

The short structure outperforms its exact inverse by ₹45.18 per trade after base costs, but both
are economically negative. Relative outperformance does not rescue the primary hypothesis.

## Calendar stability

Every year is net negative:

| Year | Trades | Gross P&L | Total cost | Net P&L | Mean net | Win rate | Mean net ROM |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2021 | 115 | ₹1,791.25 | ₹30,393.68 | -₹28,602.43 | -₹248.72 | 6.09% | -0.5073% |
| 2022 | 185 | ₹3,152.50 | ₹53,832.68 | -₹50,680.18 | -₹273.95 | 4.32% | -0.6020% |
| 2023 | 168 | ₹1,245.00 | ₹38,060.42 | -₹36,815.42 | -₹219.14 | 5.95% | -0.4841% |
| 2024 | 183 | ₹6,305.00 | ₹43,533.79 | -₹37,228.79 | -₹203.44 | 8.20% | -0.5652% |
| 2025 | 163 | ₹7,553.75 | ₹47,794.44 | -₹40,240.69 | -₹246.88 | 18.40% | -0.2735% |
| 2026 through 14 July | 81 | ₹178.75 | ₹29,051.91 | -₹28,873.16 | -₹356.46 | 17.28% | -0.4426% |

Only 12 of 276 weeks are positive after base costs (`4.35%`). No calendar month is positive.
The best month still loses ₹195.77; the worst loses ₹7,703.27.

## Entry-time and DTE diagnostics

Every populated entry hour is negative after costs. The least-negative bucket is 14:xx, but it
contains only five trades and is not usable evidence. The 10:xx bucket has 297 trades and loses
₹219.15 per trade; 11:xx has 376 and loses ₹260.26; 12:xx has 155 and loses ₹275.44; 13:xx has
62 and loses ₹259.18.

All DTE buckets are negative. The seven trades with DTE at most seven lose ₹458.74 on average;
the 7–10, 10–14, and 14+ buckets lose ₹219.62, ₹309.80, and ₹241.92 respectively.

## Execution data and assumptions

### Volatility input to slippage

India VIX is used where observed. For 56 trades, representing 224 entry leg rows and 224 exit
leg rows, India VIX is unavailable. Those rows use the contemporaneous independently
reconstructed ATM IV multiplied by 100 so it is on the same volatility-point scale as India VIX.
No baseline-VIX fallback remains. Volume, OI, and ATM IV are complete on all 3,580 leg rows.

No retained leg crosses the pinned model's low volume/OI-turnover stale threshold. The time and
depth multipliers still apply to every fill.

### Timestamp-aware SPAN selection

Margin uses the six-slot research release
`nifty_gold_span_six_slot_research_20210101_20260715/version=2.1.0`. For each entry the latest
matched reference-price slot not later than entry is selected, with fallback to the preceding
matched slot:

| Selected slot | Trades |
|---|---:|
| BOD | 298 |
| ID1 | 473 |
| ID2 | 119 |
| ID3 | 5 |

The research schedule uses ID1 `11:00`, ID2 `12:30`, and ID3 `14:00`. BOD has no published
reference timestamp in the retained schedule, so it is explicitly assumed available at the 09:15
session open until ID1. All 3,580 selected leg arrays are matched and their research reference
time is not later than entry.

This is the requested timestamp-nuanced research treatment, but it remains a reference-price
schedule rather than proof of historical file arrival. The strict point-in-time representation
contains no usable historical SPAN rows because file-arrival timestamps were not proven.

## Coverage and invariants

- Primary signals: 895, from 31 March 2021 through 14 July 2026.
- One trade maximum per session and exactly one historical exchange lot per trade.
- Leg rows: 3,580 entry plus the same exact contracts at 60-minute exit.
- Missing entry/exit close, volume, OI, ATM IV, lot size, strike, or selected SPAN scenarios: zero.
- Duplicate trade-leg keys: zero.
- Maximum gross P&L reconciliation error versus Phase 2: `0.0` points.
- Expiry-day trades: zero; observed DTE ranges from approximately 6.1 to 20.2 days.
- Historical lot sizes represented: 25, 50, 65, and 75 units.

## Interpretation

The Phase-2 gross directional relationship is real in the reconstructed marks but is not large
enough to trade as the frozen eight-order one-lot structure. Costs do not merely weaken the
result; they reverse every calendar month and push the entire bootstrap interval for mean net
return on margin below zero.

The economically correct checkpoint decision is therefore:

> **Do not promote the frozen H1 structure into a live strategy.** Preserve it as a documented
> gross signal result, but reject the present four-leg, 60-minute, one-lot implementation after
> Groww charges, calibrated slippage, and SPAN capital.

No claim of prospective confirmation is possible because every trade predates the 18 July 2026
hypothesis freeze. Matched non-crossing and scheduled-entry controls also require separately
frozen matching and scheduling rules; they are not invented after seeing this cost result.

## Reproduce

```powershell
python -m research.phase3.run_full_strategy_backtest `
  --gold-root "<external-data-root>\nifty_gold_span_six_slot_research_20210101_20260715\version=2.1.0\gold"
```

Generated artifacts:

- `audit/phase3_full_strategy_tradebook.csv`: one row per trade and structure, with base and 1.5x fields.
- `audit/phase3_full_strategy_legbook.csv`: exact entry/exit contract observations and slippage components.
- `audit/phase3_full_strategy_tearsheet.json`: machine-readable distributions and breakdowns.
- `audit/phase3_full_strategy_manifest.json`: code, input, and output hashes.
