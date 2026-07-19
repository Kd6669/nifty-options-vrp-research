# Phase 7 — unified 180-minute signal comparison protocol

## Horizon boundary

The reversal inverse diagnostic improves economically as the holding period increases:

- 60 minutes: +₹2.71 gross and −₹288.71 net per trade;
- 120 minutes: +₹42.32 gross and −₹247.02 net;
- 180 minutes: +₹104.60 gross and −₹181.43 net.

This is evidence that the effect, if any, may operate more slowly than 60 minutes. It is not
evidence of profitability: every aggregate cell remains net negative. Exact-contract coverage
simultaneously falls from 86.99% to 49.19%.

The current rolling nearest-weekly ATM±10 dataset therefore supports 180 minutes only as a
low-coverage diagnostic and cannot identify multi-session or longer-horizon performance without
non-random survivor bias. Research at longer horizons is a justified future direction requiring
full fixed-contract history, not an extrapolation from the present sample.

## Purpose

Compare every already-tested VRP event definition on one fixed 180-minute horizon using one
execution model. No new threshold is introduced.

## Event families

1. First daily normalized-VRP zero cross upward.
2. First daily normalized-VRP zero cross downward.
3. First daily causal-percentile crossings at 70%, 75%, 80%, 85%, 90%, and 95%, in both upward
   and downward directions.
4. First daily top-tail-to-zero and bottom-tail-to-zero reversal events from Phase 6.

The event builders are the same implementations used in the corresponding original tests.

## Structure mappings

Historical/requested mapping:

- zero up → short iron condor;
- zero down → long iron condor;
- every 70–95 percentile crossing → short iron condor, matching the Phase 3 threshold test;
- top reversal → long iron condor;
- bottom reversal → short iron condor.

The exact inverse reverses every structure direction on the same event.

## Execution

- NIFTY nearest-weekly options.
- ATM±1 inner legs protected at ATM±3.
- Entry at the next exact minute after the signal.
- Frozen expiry, strike, and option type through an exact 180-minute exit.
- One historical exchange lot.
- Date-aware STT, brokerage, statutory charges, modeled slippage, ATM-IV fallback for missing
  INDIA VIX, and timestamp-aware SPAN margin.
- No stale exit substitution or synthetic repricing.

Identical entry timestamps across signal variants are priced once and then mapped back to each
signal. Threshold cells overlap and must never be summed into a portfolio.

## Interpretation

For every signal/mapping cell report events, exact path coverage, gross and net P&L, net win rate,
SPAN return, and a trade-date block-bootstrap mean-net interval.

A cell is only an economically credible lead if it has at least 100 trades, at least 80% coverage,
positive mean net P&L, and a bootstrap lower bound above zero. Anything else is descriptive and
cannot reopen the closed hypothesis family.
