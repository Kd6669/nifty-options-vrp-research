# Repository purpose

This repository exists to complete the data-foundation step for the take-home research question:

> Is there a systematically harvestable variance or volatility risk premium in short-dated NIFTY
> index options that a small desk could capture after costs, slippage, margin, and the current
> regulatory regime?

The original brief is preserved verbatim at
[`docs/task/quant-research-task.md`](task/quant-research-task.md).

This repository first stopped at the accepted data audit. Phase 2 now adds volatility diagnostics
and a preregistered hypothesis while preserving the Phase 1 data modules and limitations. It still
stops before claiming a net-of-cost, out-of-sample strategy edge.

Phase 1 deliverables are:

1. Reusable acquisition, cleaning, BSM, SPAN, joining, and release code.
2. Explicit bronze/silver/gold archive contracts.
3. Full-corpus audit evidence and an honest readiness decision.
4. A small deterministic Parquet sample for tests and reviewer inspection.
5. Reproduction, validation, provenance, and source-evidence documentation.

The audit was accepted with its caveats. Phase 2 hypothesis formulation is now closed under
[`docs/research/FINAL_HYPOTHESIS.md`](research/FINAL_HYPOTHESIS.md) and the modular
`nifty_hypothesis` pipeline. Later phases must continue to carry the unresolved expiry identity,
historical bid/ask, and SPAN timing limitations and may not retune the frozen hypothesis silently.
