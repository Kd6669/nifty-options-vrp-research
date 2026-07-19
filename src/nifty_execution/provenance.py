from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ModelSourcePin:
    model: str
    repository: str
    commit: str
    source_paths: tuple[str, ...]
    integration: str

    @property
    def commit_url(self) -> str:
        return f"https://github.com/{self.repository}/commit/{self.commit}"


GROWW_COST_MARGIN_SOURCE = ModelSourcePin(
    model="groww_cost_and_span_margin",
    repository="Goyal-Dedhia-Capital/groww-margin-charges-model",
    commit="b9de06a2b6f6d7e13489c1e42ba4ddfc8621bb6b",
    source_paths=(
        "src/robs_live/broker_accounting/groww_charges.py",
        "src/robs_live/oms/prices.py",
        "src/robs_live/span/margin_model_a.py",
        "src/robs_live/span/groww_margin_parity.py",
    ),
    integration=(
        "The first three implementation files are byte-identical in nifty_span. "
        "The parity adapter only changes the package namespace from robs_live to nifty_span."
    ),
)


NIFTY_SLIPPAGE_SOURCE = ModelSourcePin(
    model="nifty_volume_oi_slippage",
    repository="Goyal-Dedhia-Capital/deployment-live-model",
    commit="dc3f56d1a1d602d15e11463521b3604e1c997411",
    source_paths=("Deployment_live_model/scripts/build_groww_non_span_enrichment.py",),
    integration=(
        "The calibrated NIFTY formula and constants are preserved. The checkpoint-3 module "
        "adds an algebraically exact component breakdown and scalar validation."
    ),
)


MODEL_SOURCE_PINS = (GROWW_COST_MARGIN_SOURCE, NIFTY_SLIPPAGE_SOURCE)


__all__ = [
    "GROWW_COST_MARGIN_SOURCE",
    "MODEL_SOURCE_PINS",
    "NIFTY_SLIPPAGE_SOURCE",
    "ModelSourcePin",
]
