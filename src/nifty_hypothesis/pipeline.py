"""Orchestration for every analysis used to formulate the Phase 2 hypothesis."""

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, Callable

from research.phase2 import analyze_defined_risk_vrp
from research.phase2 import analyze_intraday_volatility
from research.phase2 import analyze_matched_realized_variance
from research.phase2 import analyze_vrp_curve_crossings
from research.phase2 import audit_moneyness_horizon_boundary
from research.phase2 import audit_playable_universe
from research.phase2 import audit_unconditional_moneyness_horizon
from research.phase2 import close_hypothesis_formulation
from research.phase2 import extract_wings_5_7_9
from research.phase2 import summarize_defined_risk_vrp
from research.phase2 import summarize_intraday_volatility

from .config import ResearchConfig
from .contracts import (
    STAGE_DEPENDENCIES,
    STAGE_DESCRIPTIONS,
    STAGE_ORDER,
    ArtifactSet,
)
from .validation import inspect_artifact, sha256, validate_inputs


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def select_stages(
    *,
    start: str | None = None,
    through: str | None = None,
    only: tuple[str, ...] = (),
) -> tuple[str, ...]:
    unknown = set(only) - set(STAGE_ORDER)
    if unknown:
        raise ValueError(f"unknown stages: {', '.join(sorted(unknown))}")
    if only:
        return tuple(stage for stage in STAGE_ORDER if stage in only)
    start_index = STAGE_ORDER.index(start) if start else 0
    end_index = STAGE_ORDER.index(through) + 1 if through else len(STAGE_ORDER)
    if start_index >= end_index:
        raise ValueError("start stage must precede through stage")
    return STAGE_ORDER[start_index:end_index]


def stage_plan(config: ResearchConfig, stages: tuple[str, ...]) -> list[dict[str, Any]]:
    artifacts = ArtifactSet.from_config(config)
    return [
        {
            "stage": stage,
            "description": STAGE_DESCRIPTIONS[stage],
            "dependencies": list(STAGE_DEPENDENCIES[stage]),
            "outputs": [str(path) for path in artifacts.stage_outputs(stage)],
            "complete": all(path.exists() for path in artifacts.stage_outputs(stage)),
        }
        for stage in stages
    ]


def _playable(config: ResearchConfig, artifacts: ArtifactSet) -> None:
    payload = audit_playable_universe.audit(config.gold_root)
    _write_json(artifacts.stage_outputs("playable_universe")[0], payload)


def _moneyness(config: ResearchConfig, artifacts: ArtifactSet) -> None:
    payload = audit_moneyness_horizon_boundary.audit(config.gold_root)
    _write_json(artifacts.stage_outputs("moneyness_horizon")[0], payload)


def _unconditional(config: ResearchConfig, artifacts: ArtifactSet) -> None:
    payload = audit_unconditional_moneyness_horizon.audit(
        config.gold_root,
        config.output_dir,
        session_mode=config.session_mode,
        entry_offset_source=config.entry_offset_source,
    )
    _write_json(artifacts.stage_outputs("unconditional_coverage")[0], payload)


def _wide_wings(config: ResearchConfig, artifacts: ArtifactSet) -> None:
    unconditional_json, start_time_parquet = artifacts.stage_outputs(
        "unconditional_coverage"
    )
    payload = extract_wings_5_7_9.build(
        unconditional_json,
        start_time_parquet,
        config.gold_root,
    )
    _write_json(artifacts.stage_outputs("wide_wing_sensitivity")[0], payload)


def _intraday(config: ResearchConfig, artifacts: ArtifactSet) -> None:
    payload = analyze_intraday_volatility.analyze(
        config.gold_root,
        config.expiry_calendar,
        config.output_dir,
    )
    _write_json(artifacts.stage_outputs("intraday_surface")[0], payload)


def _volatility_regimes(config: ResearchConfig, artifacts: ArtifactSet) -> None:
    intraday = artifacts.stage_outputs("intraday_surface")
    output, ranked = artifacts.stage_outputs("volatility_regimes")
    payload = summarize_intraday_volatility.summarize(
        intraday[1],
        intraday[2],
        intraday[3],
        ranked,
    )
    _write_json(output, payload)


def _matched_variance(config: ResearchConfig, artifacts: ArtifactSet) -> None:
    intraday = artifacts.stage_outputs("intraday_surface")
    payload = analyze_matched_realized_variance.analyze(
        intraday[1],
        intraday[2],
        config.output_dir,
    )
    _write_json(artifacts.stage_outputs("matched_variance")[0], payload)


def _defined_risk(config: ResearchConfig, artifacts: ArtifactSet) -> None:
    intraday = artifacts.stage_outputs("intraday_surface")
    analyze_defined_risk_vrp.analyze(
        config.gold_root,
        intraday[1],
        intraday[2],
        config.output_dir,
        reuse_local_chain=False,
    )


def _events(config: ResearchConfig, artifacts: ArtifactSet) -> None:
    del config
    defined = artifacts.stage_outputs("defined_risk_paths")
    summarize_defined_risk_vrp.summarize(
        defined[0],
        defined[3],
        artifacts.stage_outputs("event_summary")[0],
    )


def _curve_crossings(config: ResearchConfig, artifacts: ArtifactSet) -> None:
    defined = artifacts.stage_outputs("defined_risk_paths")
    analyze_vrp_curve_crossings.analyze(defined[3], config.output_dir)


def _hypothesis_closeout(config: ResearchConfig, artifacts: ArtifactSet) -> None:
    intraday = artifacts.stage_outputs("intraday_surface")
    defined = artifacts.stage_outputs("defined_risk_paths")
    close_hypothesis_formulation.close(
        config.repo_root / "research" / "phase2" / "final_hypothesis.json",
        defined[3],
        artifacts.stage_outputs("unconditional_coverage")[0],
        intraday[0],
        artifacts.stage_outputs("matched_variance")[0],
        artifacts.stage_outputs("event_summary")[0],
        artifacts.stage_outputs("curve_crossings")[0],
        artifacts.stage_outputs("hypothesis_closeout")[0],
    )


def build_manifest(config: ResearchConfig, artifacts: ArtifactSet) -> dict[str, Any]:
    input_validation = validate_inputs(config)
    dataset_audit_hash = sha256(config.dataset_audit) if config.dataset_audit.exists() else None
    expiry_calendar_hash = (
        sha256(config.expiry_calendar) if config.expiry_calendar.exists() else None
    )
    source_paths = sorted(
        [
            *config.repo_root.glob("src/nifty_hypothesis/*.py"),
            *config.repo_root.glob("research/phase2/*.py"),
            *config.repo_root.glob("docs/research/*.md"),
            config.repo_root / "research" / "phase2" / "final_hypothesis.json",
            config.repo_root / "research" / "phase2" / "hypothesis_formulation.example.json",
            config.repo_root / "tests" / "test_nifty_hypothesis.py",
        ],
        key=lambda path: path.as_posix(),
    )
    source_files = [
        {
            "path": path.relative_to(config.repo_root).as_posix(),
            "sha256": sha256(path),
        }
        for path in source_paths
    ]
    entries = []
    for stage in STAGE_ORDER[:-1]:
        for path in artifacts.stage_outputs(stage):
            record = inspect_artifact(path, include_hash=True)
            record["stage"] = stage
            entries.append(record)
    manifest = {
        "schema_version": "phase2-hypothesis-evidence-manifest/v1",
        "created_at_utc": datetime.now(UTC).isoformat(),
        "configuration": config.as_dict(),
        "dataset_release": input_validation.get("dataset_release"),
        "dataset_audit_sha256": dataset_audit_hash,
        "expiry_calendar_sha256": expiry_calendar_hash,
        "source_files": source_files,
        "inputs_valid": input_validation["ok"],
        "artifacts": entries,
        "complete": all(record.get("exists", False) for record in entries),
    }
    _write_json(artifacts.stage_outputs("manifest")[0], manifest)
    return manifest


def _manifest(config: ResearchConfig, artifacts: ArtifactSet) -> None:
    build_manifest(config, artifacts)


RUNNERS: dict[str, Callable[[ResearchConfig, ArtifactSet], None]] = {
    "playable_universe": _playable,
    "moneyness_horizon": _moneyness,
    "unconditional_coverage": _unconditional,
    "wide_wing_sensitivity": _wide_wings,
    "intraday_surface": _intraday,
    "volatility_regimes": _volatility_regimes,
    "matched_variance": _matched_variance,
    "defined_risk_paths": _defined_risk,
    "event_summary": _events,
    "curve_crossings": _curve_crossings,
    "hypothesis_closeout": _hypothesis_closeout,
    "manifest": _manifest,
}


def _require_dependencies(stage: str, artifacts: ArtifactSet) -> None:
    missing = [
        path
        for dependency in STAGE_DEPENDENCIES[stage]
        for path in artifacts.stage_outputs(dependency)
        if not path.exists()
    ]
    if missing:
        lines = "\n".join(f"- {path}" for path in missing)
        raise FileNotFoundError(f"stage {stage} is missing dependency artifacts:\n{lines}")


def run_pipeline(
    config: ResearchConfig,
    stages: tuple[str, ...],
    *,
    resume: bool = False,
) -> list[dict[str, Any]]:
    input_validation = validate_inputs(config)
    if not input_validation["ok"]:
        failed = [check for check in input_validation["checks"] if not check["ok"]]
        raise ValueError(f"input validation failed: {failed}")

    artifacts = ArtifactSet.from_config(config)
    config.output_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for stage in stages:
        outputs = artifacts.stage_outputs(stage)
        if resume and stage != "manifest" and all(path.exists() for path in outputs):
            results.append({"stage": stage, "status": "reused", "outputs": list(map(str, outputs))})
            continue
        _require_dependencies(stage, artifacts)
        RUNNERS[stage](config, artifacts)
        missing = [str(path) for path in outputs if not path.exists()]
        if missing:
            raise RuntimeError(f"stage {stage} did not produce expected outputs: {missing}")
        results.append({"stage": stage, "status": "completed", "outputs": list(map(str, outputs))})
    return results
