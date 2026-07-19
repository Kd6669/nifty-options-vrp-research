# Phase 6 — post-hoc VRP tail-reversal protocol

## Status

This is one final, explicitly post-hoc falsification requested after the Phase 5 closure. It is
not pristine OOS evidence and cannot be used to tune another rule on the same sample.

The test asks whether a causal VRP curve that first reaches an extreme and then starts reverting
toward zero predicts a defined-risk payoff.

## Frozen event

The state variable is the trailing five-minute median of the causal same-time-of-day percentile
of normalized 60-minute VRP, `q5`. Normalized VRP is still:

```text
V = ATM_IV^2 - trailing_60m_RV_annual^2
```

For each session, calculate the prior running maximum and minimum of `q5`.

Top-tail reversal:

```text
prior_running_max(q5) >= 0.90
current_q5 <= prior_running_max(q5) - 0.10
five_minute_q_velocity < 0
current_V > 0
```

Bottom-tail reversal:

```text
prior_running_min(q5) <= 0.10
current_q5 >= prior_running_min(q5) + 0.10
five_minute_q_velocity > 0
current_V < 0
```

The sign condition ensures that the curve has started reverting but has not already reached or
crossed zero. Only the first chronological qualifying reversal of either type is retained per
session. Entry is at the next exact minute.

The tail boundary and 10-point reversal confirmation are fixed once. No 70/75/80/85/95 sweep is
permitted.

## Requested trade and exact inverse

Requested mapping:

- top-tail reversal toward zero → **long** ATM±1 iron condor protected at ATM±3;
- bottom-tail reversal toward zero → **short** ATM±1 iron condor protected at ATM±3.

Exact inverse comparator:

- top-tail reversal → short iron condor;
- bottom-tail reversal → long iron condor.

Both mappings use the same events. This distinguishes whether any result comes from reversal
timing or merely from choosing the opposite structure direction after seeing outcomes.

## Execution and horizons

- NIFTY nearest-weekly options.
- Frozen entry contracts, all legs within ATM±3.
- One historical exchange lot.
- Primary horizon: 60 minutes.
- Prespecified sensitivities: 120 and 180 minutes.
- No overnight holding and no stale/synthetic exit substitution.
- Date-aware STT, brokerage, statutory charges, modeled entry/exit slippage, ATM-IV fallback for
  missing INDIA VIX, and timestamp-aware SPAN margin.

## Reversion diagnostics

For every event/horizon, report:

- whether normalized VRP touches/crosses zero before the fixed exit;
- minutes to first zero touch when observed;
- whether absolute distance from zero is smaller at the fixed exit;
- exact-contract structure-path coverage.

These diagnostics test the premise before judging the option trade.

## Acceptance

The requested mapping passes only if the 60-minute aggregate across both reversal types has:

1. at least 100 complete non-overlapping daily events;
2. positive mean net P&L;
3. positive aggregate net P&L;
4. a trade-date block-bootstrap 95% mean-net lower bound above zero;
5. positive results for both top- and bottom-reversal subgroups;
6. at least 80% exact-contract coverage;
7. at least 60% positive populated months.

The inverse and longer horizons are diagnostics only and cannot replace a failed primary result.

## Closure

Failure leaves the Phase 5 closure unchanged. No further VRP rule is to be created on this
sample after observing this result.
