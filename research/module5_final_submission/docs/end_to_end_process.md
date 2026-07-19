# End-to-end research process

## 1. Extraction and archives

The acquisition layer retrieves NIFTY spot, INDIA VIX, rolling nearest-expiry option-chain slices,
active futures and NSE SPAN files. Provider responses and request manifests enter immutable bronze;
typed provider-specific records enter silver; point-in-time joins, contract rules and independent
BSM fields enter gold. Credentials are environment-only and full datasets are excluded from Git.

The final local corpus covers 2021-01-01 through 2026-07-15. The Git repository carries a small
deterministic Parquet sample plus manifests and audits, not the multi-gigabyte archive.

## 2. Cleaning and processing

Cleaning normalizes timestamps, symbols, expiries, strikes, option type and quantities; rejects
invalid or crossed records; applies the reviewed NSE session calendar; and preserves exception
tables. The BSM layer recomputes price/IV/Greeks rather than trusting chain IV at face value. INDIA
VIX is used when available; the pinned and disclosed fallback is entry ATM IV.

SPAN is joined by the timestamp-aware dataset now in the project. Source slots remain visible so a
BOD reconstruction cannot masquerade as a proven intraday margin observation. Contract rules use
date-aware lot sizes and expiry conventions.

## 3. Playable-universe audit

The archive only follows rolling expiry ATM±10. Fixed-contract MTM labels decay with moneyness and
horizon. The unconditional coverage audit—including entries at any observed minute on every
available session—supports the bounded research universe: nearest weekly expiry, entry legs within
ATM±3 and a maximum 180-minute fixed horizon. Longer or multi-day paths are not imputed as observed.

## 4. Hypothesis formulation

Intraday implied and realized volatility are put on a matched variance/time basis. IV is a
local-chain measure from recomputed contract IV, normalized to the intraday horizon. RV uses only
trailing returns known at the signal timestamp and the same annualization/day-count convention.
The signal is `VRP = IV² − RV²`, with time-of-day percentile, slope and acceleration features.

The frozen base claim was falsifiable: VRP curve level and direction should predict 60-minute net
P&L in defined-risk nearest-weekly NIFTY structures entered within ATM±3. Tests included zero
crossings, directional 70/75/80/85/90/95 tail crossings, velocity/acceleration and tail reversals,
for long and short structures.

## 5. Hypothesis testing

Module 3 applies date-aware Groww charges, modeled slippage and timestamp-aware SPAN. Zero crossings,
tail crossings and reversals fail after costs. At 180 minutes gross P&L is larger, but no requested
cell passes both economics and coverage; signals lacking a valid 180-minute exit are reported as
unevaluated, never as zero-P&L trades. The standalone intraday VRP hypothesis is rejected.

## 6. Post-hoc validation and sizing

Modules 8–10 ask a different, explicitly post-hoc question: whether the upper-85 short-iron-fly
candidate can be screened and sized. Frozen causal gates use only entry-time IV/RV/DTE/time
features. Exact quantity-aware charges, slippage and SPAN are recomputed at each integer lot count.
The selected profile uses a 40% quality switch, 35% margin ceiling and 4% cost-reserved maximum-risk
ceiling on ₹10 lakh. It executes 86 of 132 signals.

The full historical curve is positive, but 2024 is weak and the Phase 9 score/P&L bootstrap gate
fails. Parameter exploration is therefore evidence about a shadow candidate—not clean OOS proof.

## 7. Final robustness and decision

Module 5 adds the four required extensions: slippage execution-decay; a formal 20-Nov-2024
permutation/bootstrap break test; RBI/Budget/election event-week conditioning; and a live shadow
monitoring design. It also publishes the final trade sheet, daily curve, drawdown autopsy, cost
attribution, capacity grid, workbook and memo.

The decision is zero live capital today. Preserve the exact rule in a forward shadow and require new
non-overlapping observations before reconsidering it.

