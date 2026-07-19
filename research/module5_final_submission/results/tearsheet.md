# Final NIFTY VRP Research Tear Sheet

## Decision

**Shadow only; no live capital approval.** The preregistered standalone intraday VRP rules fail net
of one-lot costs. The later upper-85 short-iron-fly result is a post-hoc forward-shadow candidate,
not a clean out-of-sample discovery.

## Headline economics

| Metric | Result |
|---|---:|
| Starting capital | ₹1,000,000 |
| Ending equity | ₹1,108,983 |
| Net profit | ₹108,983 |
| Total return | 10.90% |
| CAGR | 2.04% |
| Annualized volatility | 1.43% |
| Sharpe / Sortino | 1.37 / 2.75 |
| Maximum drawdown | ₹7,982 (0.76%) |
| Max recovery duration | 184 calendar days |
| Signals / executed / skipped | 132 / 86 / 46 |
| Hit rate | 66.28% |
| Average win / loss | ₹3,156 / ₹-2,445 |
| Worst trade / day / week | ₹-7,982 / ₹-7,982 / ₹-7,982 |
| Trade CVaR 5% | ₹-6,142 |
| Gross / costs / net | ₹153,012 / ₹44,029 / ₹108,983 |
| Cost drag / gross | 28.77% |
| Turnover | ₹6,670,528 |

## Mandatory robustness extensions

- Execution decay: aggregate net P&L reaches zero at approximately
  **5.92×** modeled slippage.
- November 2024 break: post-minus-pre mean difference is
  **₹-72 per selected lot**
  (95% bootstrap CI ₹-378 to
  ₹262); the two-sided permutation p-value is
  **0.639**.
- Event conditioning uses a frozen ±5-business-day window around RBI MPC,
  Union Budget and 2024 national-election dates. Small cells are descriptive only.
- Capacity is an execution diagnostic over 1–100 equal lots, not an authorization to breach the
  ₹10 lakh margin and cash-risk controls.

## Why this might be fake

The positive curve was discovered after the base VRP hypothesis failed; the same history influenced
the gate, structure and sizing work. Calendar 2024 is weak, Phase 9's holdout rank-correlation
bootstrap gate fails, quotes are model-filled rather than historical bid/ask, and the rolling chain
cannot validate holds beyond 180 minutes. Sparse event and post-break samples cannot rescue those
identification problems.

## Live decision and kill-switch

Allocate **₹0 live** today. Run the exact rule in shadow at one-lot telemetry, without silently
retuning. Promotion requires at least 100 new non-overlapping trades over 12 months, positive net
P&L after observed costs in both six-month halves, stable concentration, and a positive resampled
score/P&L relationship. Stop new entries immediately for stale/missing quotes, absent point-in-time
SPAN, modeled-cost breach, margin above 35%, cash risk above 4%, daily loss above 0.75%, or total
drawdown above 1.5%.
