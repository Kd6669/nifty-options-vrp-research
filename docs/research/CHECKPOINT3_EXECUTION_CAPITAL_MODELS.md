# Checkpoint 3 — execution costs, slippage, and margin

## Status

Checkpoint 3 imports the already-built models behind one research-facing package,
`nifty_execution`. The source commits are immutable, their file hashes are recorded in
`docs/source_evidence/checkpoint3/SOURCE_MANIFEST.json`, and no credentials, generated
artifacts, or unrelated repository history are included.

## Source resolution

| Model | Pinned GitHub source | Local integration |
|---|---|---|
| Groww F&O charges | `groww-margin-charges-model@b9de06a` | Existing `nifty_span.broker_accounting` file is byte-identical; checkpoint 3 adds a defined-risk basket/round-trip facade. |
| SPAN Model-A margin | `groww-margin-charges-model@b9de06a` | Existing `nifty_span.span.margin_model_a` is byte-identical; checkpoint 3 adds a NIFTY-only research adapter and return-on-margin helper. |
| NIFTY slippage | `deployment-live-model@dc3f56d` | Exact NIFTY calibration is modularized with component-level output. |

## Slippage model

For option premium `P`, volume `V`, open interest `OI`, minutes to expiry `M`, and India
VIX `X`, the pinned NIFTY calibration is:

```text
vix_multiplier   = (max(X, 0) / 15)^1.5
base_spread      = max(0.05, P * 0.001599 * vix_multiplier)
time_multiplier  = 1 + 0.045543 / sqrt(max(M / 60, 0.1))
turnover_ratio   = max(V, 0) / (max(OI, 0) + 1)
stale_multiplier = 1.5 if turnover_ratio < 0.001 else 1.0
depth_multiplier = 1 + 1.501812 / max(log(max(OI, 0) + 1), 0.1)
slippage         = base_spread * time_multiplier * stale_multiplier * depth_multiplier
```

The value is a **one-sided adverse distance per option unit**. A four-leg entry therefore
pays the sum of `quantity × leg_slippage`; the fixed-horizon exit pays it again using the
exit observation. Component fields are an exact sequential decomposition, so the total is
`base_spread + time_penalty + stale_penalty + depth_penalty`.

Missing volume/OI map to zero and therefore receive both stale and maximum depth penalties.
The source model's generic missing-VIX default is 15, but the Phase 4 research adapter instead
passes same-timestamp ATM IV × 100 whenever INDIA VIX is missing. Synthetic bid/ask values are
audit proxies, not observed quotes. If
slippage is at least the premium, `is_executable_proxy` is false and the candidate should be
rejected rather than clipped into an artificial fill.

Here, "stale" is the upstream model's low volume/OI-turnover proxy; it is not elapsed quote
age. If a later dataset exposes exchange quote timestamps, quote-age staleness must be added as
a separately calibrated overlay rather than being implied by this field.

## Cost model

The imported Groww estimator decomposes brokerage, sell-side STT/CTT, buy-side stamp duty,
exchange transaction charges, SEBI turnover fees, IPFT, and GST. Checkpoint 3 keeps public
estimated charges separate from the Groww margin-API reserve or broker-reported aggregate;
they must not be double-counted.

`estimate_round_trip_execution_cost` applies public charges and slippage separately to entry
and exit, charging every order in a four-leg defined-risk basket on both sides of the
60-minute holding period.

Phase 4 adds date-aware option STT regimes (0.05%, 0.0625%, 0.10%, and 0.15% at their respective
effective dates) while retaining current brokerage as an explicitly deployable-current
assumption. It also adds an uncalibrated quantity-aware capacity overlay using the recovered
lot ladder plus submitted quantity relative to volume and OI. That recovered ladder was later
audited as too aggressive and retired. The current overlay anchors ladder parity at 60 lots and
adds separate square-root incremental-volume and incremental-OI terms. It remains an explicit
sensitivity pending order-book or realized-fill calibration. Full formulas and results are in
`PHASE4_COST_AWARE_VRP_DISCOVERY.md`.

The hypothesis exits intraday and does not intentionally settle options. Expiry-exercise STT
is therefore not silently approximated. Any held-to-settlement variant must add a dated,
verified settlement-STT rule before it is eligible for testing.

## Margin model

`estimate_defined_risk_margin` delegates to SPAN Model-A using the SPAN file selected for the
same trading date. It returns scenario scan loss, net option value, long premium, ELM,
add-ons, obligations, benefits, floors, selected SPAN slot, and total margin. The engine
includes the extra index-option ELM on expiry day.

Capital efficiency is `net_pnl_after_costs / required_margin`. A backtest must not substitute
premium-at-risk, maximum payoff loss, or a broker reserve quote for required SPAN margin
without labelling it as an approximation.

## Integration boundary for the next checkpoint

This checkpoint installs and verifies the engines; it does not retroactively change phase-2
gross-P&L evidence. The strategy backtest should next construct the four legs, compute entry
costs, load date/slot-correct SPAN, compute exit costs at 60 minutes, and report gross P&L,
every cost component, net P&L, required margin, and return on margin.

## Reproduction

```powershell
python -m pytest -q tests/test_nifty_execution.py tests/test_span.py
python -m ruff check src/nifty_execution tests/test_nifty_execution.py
```
