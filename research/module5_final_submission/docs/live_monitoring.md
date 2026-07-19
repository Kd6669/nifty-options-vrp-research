# Live shadow monitoring and kill-switch specification

## Current size

**Live capital: ₹0. Shadow telemetry: one lot.** The historical ₹10 lakh sizing curve is a research
diagnostic, not a deployment authorization.

## Pre-trade panel

Record timestamp, expiry/DTE, spot, all four leg quotes and ages, minute volume/OI, ATM and local
ATM±3 IV, trailing matched RV, VRP percentile/slope/acceleration, score components, SPAN slot/time,
expected margin, cost-reserved maximum loss and modeled fill hurdle. A trade is either completely
accepted or completely rejected; partial feature availability cannot be silently filled.

## Hard no-entry switches

- underlying or any leg quote older than two minutes;
- missing/invalid timestamp-aware SPAN or margin utilization above 35%;
- cost-reserved maximum loss above 4% of current shadow capital;
- expiry/lot-size/contract-rule mismatch;
- incomplete feature gate or score at/below 40%;
- modeled total cost at/above predicted gross P&L;
- unresolved market-wide feed, order or reconciliation incident.

## Session and portfolio stops

- stop new entries after a 0.75% one-day capital loss;
- flatten/review after a 1.5% peak-to-trough drawdown;
- stop if realized slippage averages above 1.5× model over 20 fills, or any fill exceeds 3×;
- stop after three consecutive reconciliation failures or unexplained cash/margin differences;
- no automatic parameter retuning after a stop.

## Promotion gate

Require at least 100 new non-overlapping trades, at least 12 calendar months and eight active months.
Net P&L after observed charges/fills must be positive overall and in both chronological six-month
halves; no month may contribute more than 40% of total profit; maximum drawdown must remain below
1.5%; and the frozen confidence score must have positive rank correlation with per-lot P&L with a
bootstrap 95% lower bound above zero. Failure keeps allocation at zero.

## Daily dashboard sketch

The workbook `Live Monitor` sheet is the static specification. A production panel should show:
data freshness, rule version/hash, signal state, current/peak equity, day and total drawdown, margin
and cash-risk utilization, predicted versus realized costs, rolling 20-trade hit/mean/CVaR,
event-week flag, structural regime, and every hard-switch state. Alerts must be retained with the
decision and source timestamps.

