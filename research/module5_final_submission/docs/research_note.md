# Research note — NIFTY intraday VRP in defined-risk structures

## Question and hypothesis

Can the level and direction of matched intraday VRP identify profitable 60-minute defined-risk
NIFTY option trades, net of executable costs? The preregistered signal is the time-of-day-ranked
curve `IV² − RV²`; structures are nearest-weekly, entered within ATM±3, and never naked.

## IV, RV and VRP

Contract IV is independently inverted/recomputed from cleaned option prices and BSM inputs; chain IV
is not accepted at face value. Local ATM±3 IV, ATM IV, call/put IV, skew and curvature are calculated
causally at the timestamp. IV is rescaled to the intraday holding window.

RV is a trailing, entry-time-known return estimator with the same ACT/365 variance convention and
intraday horizon normalization. VRP is the difference of matched variances, not a comparison of a
raw annual IV percentage with an unmatched daily realized-vol number. Percentiles are formed
through time and by minute-of-day without future rows.

## Execution and capital

Every leg pays date-aware brokerage, STT, exchange, SEBI, stamp, IPFT and GST. Historical bid/ask is
unavailable, so the model combines a liquidity/staleness base penalty with volume/OI participation.
The corrected impact term is

```text
impact/base = (lots - 1)/59
              + sqrt(incremental quantity / minute volume)
              + sqrt(incremental quantity / open interest)
```

The former aggressive 10%-per-lot ladder is removed. The smooth ladder reaches one additional base
slippage unit around 60 lots; observed participation terms carry most of the incremental economic
meaning. Entry margin comes from the timestamp-aware SPAN join. The ₹10 lakh policy limits entry
margin to 35% and cost-reserved maximum loss to 4%.

## Results and interpretation

The base hypothesis fails: 60-minute defined-risk structures move too little to overcome four-leg
execution costs. Zero crossings, tail crossings and tail reversals remain non-viable through the
180-minute data boundary. The apparent improvement at longer horizons is a motivation for new data,
not permission to extrapolate beyond the archive.

The post-hoc gated upper-85 short iron fly produces ₹108,983 net on ₹10 lakh over 86 executions,
after ₹44,029 total costs. Maximum drawdown is below 1%, but the curve is sparse and daily Sharpe is
not treated as proof. The result deteriorates after the November-2024 market-structure break and in
event weeks. Neither difference is a causal estimate.

## Why this might be fake

- The positive candidate was found after the original hypothesis failed.
- Structure, gates and sizing were influenced by the same finite history.
- The frozen Phase 9 rank-correlation confidence interval crosses zero.
- Calendar 2024 is weak and post-break sample size is limited.
- Modeled slippage cannot recreate queue position or hidden liquidity.
- Rolling ATM±10 data censors fixed-contract paths beyond 180 minutes.
- Event-week samples are small and heavy-results weeks lack a defensible index calendar.

## Decision

Do not deploy. Shadow one lot, log predicted and realized costs, and wait for at least 100 new
non-overlapping trades across 12 months. Promote only if the predeclared forward gates in
`live_monitoring.md` pass without parameter changes.

