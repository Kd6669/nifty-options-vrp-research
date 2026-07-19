"""Named stage and artifact contracts for hypothesis-formulation evidence."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .config import ResearchConfig


STAGE_ORDER = (
    "playable_universe",
    "moneyness_horizon",
    "unconditional_coverage",
    "wide_wing_sensitivity",
    "intraday_surface",
    "volatility_regimes",
    "matched_variance",
    "defined_risk_paths",
    "event_summary",
    "curve_crossings",
    "hypothesis_closeout",
    "manifest",
)


STAGE_DESCRIPTIONS = {
    "playable_universe": "Audit exact and stale-quote structure availability.",
    "moneyness_horizon": "Measure the moneyness-by-horizon research boundary.",
    "unconditional_coverage": "Recompute all-session ATM-offset and horizon coverage.",
    "wide_wing_sensitivity": "Compare +/-3 with +/-5, +/-7, and +/-9 wing support.",
    "intraday_surface": "Build parity-forward Black-76 IV, skew, RV, and VRP surfaces.",
    "volatility_regimes": "Build causal IV/skew percentile and time-of-day regimes.",
    "matched_variance": "Reconcile intraday, expiry, and daily RV clocks with IV.",
    "defined_risk_paths": "Mark frozen-contract bounded-risk structures by causal VRP state.",
    "event_summary": "Summarize tail, zero-crossing, and narrowed-boundary events.",
    "curve_crossings": "Test causal VRP-curve level, velocity, acceleration, and crossings.",
    "hypothesis_closeout": "Freeze the final level-and-direction hypothesis and evidence summary.",
    "manifest": "Hash and row-count every hypothesis-formulation artifact.",
}


STAGE_DEPENDENCIES = {
    "playable_universe": (),
    "moneyness_horizon": (),
    "unconditional_coverage": (),
    "wide_wing_sensitivity": ("unconditional_coverage",),
    "intraday_surface": (),
    "volatility_regimes": ("intraday_surface",),
    "matched_variance": ("intraday_surface",),
    "defined_risk_paths": ("intraday_surface", "matched_variance"),
    "event_summary": ("defined_risk_paths",),
    "curve_crossings": ("defined_risk_paths", "event_summary"),
    "hypothesis_closeout": (
        "unconditional_coverage",
        "matched_variance",
        "defined_risk_paths",
        "event_summary",
        "curve_crossings",
    ),
    "manifest": (
        "playable_universe",
        "moneyness_horizon",
        "unconditional_coverage",
        "wide_wing_sensitivity",
        "intraday_surface",
        "volatility_regimes",
        "matched_variance",
        "defined_risk_paths",
        "event_summary",
        "curve_crossings",
        "hypothesis_closeout",
    ),
}


@dataclass(frozen=True)
class ArtifactSet:
    """Canonical local outputs produced by every pipeline stage."""

    output_dir: Path
    session_mode: str
    entry_offset_source: str

    @classmethod
    def from_config(cls, config: ResearchConfig) -> ArtifactSet:
        return cls(
            output_dir=config.output_dir,
            session_mode=config.session_mode,
            entry_offset_source=config.entry_offset_source,
        )

    @property
    def unconditional_stem(self) -> str:
        return f"{self.session_mode}_{self.entry_offset_source}"

    def stage_outputs(self, stage: str) -> tuple[Path, ...]:
        root = self.output_dir
        mapping = {
            "playable_universe": (root / "phase2_playable_universe.json",),
            "moneyness_horizon": (root / "phase2_moneyness_horizon_boundary.json",),
            "unconditional_coverage": (
                root / f"phase2_unconditional_{self.unconditional_stem}.json",
                root
                / f"phase2_unconditional_start_time_matrix_{self.unconditional_stem}.parquet",
            ),
            "wide_wing_sensitivity": (root / "phase2_unconditional_wings_5_7_9.json",),
            "intraday_surface": (
                root / "phase2_intraday_volatility.json",
                root / "phase2_intraday_iv_surface.parquet",
                root / "phase2_intraday_rv_vrp_labels.parquet",
                root / "phase2_daily_iv_rv_vrp.parquet",
            ),
            "volatility_regimes": (
                root / "phase2_intraday_volatility_regimes.json",
                root / "phase2_intraday_iv_surface_ranked.parquet",
            ),
            "matched_variance": (
                root / "phase2_matched_realized_variance.json",
                root / "phase2_expiry_matched_vrp_1015.parquet",
                root / "phase2_daily_horizon_rv_vrp_1015.parquet",
            ),
            "defined_risk_paths": (
                root / "phase2_defined_risk_vrp.json",
                root / "phase2_vrp_state_60m.parquet",
                root / "phase2_local_chain_iv.parquet",
                root / "phase2_defined_risk_structure_paths.parquet",
            ),
            "event_summary": (root / "phase2_defined_risk_vrp_events.json",),
            "curve_crossings": (
                root / "phase2_vrp_curve_crossings.json",
                root / "phase2_vrp_session_curve_features.parquet",
                root / "phase2_vrp_percentile_crossing_events.parquet",
            ),
            "hypothesis_closeout": (root / "phase2_final_hypothesis_closeout.json",),
            "manifest": (root / "phase2_hypothesis_evidence_manifest.json",),
        }
        try:
            return mapping[stage]
        except KeyError as error:
            raise ValueError(f"unknown stage: {stage}") from error

    def all_outputs(self, *, include_manifest: bool = True) -> tuple[Path, ...]:
        stages = STAGE_ORDER if include_manifest else STAGE_ORDER[:-1]
        return tuple(path for stage in stages for path in self.stage_outputs(stage))
