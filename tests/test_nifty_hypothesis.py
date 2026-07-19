from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nifty_hypothesis.config import ResearchConfig
from nifty_hypothesis.contracts import STAGE_ORDER, ArtifactSet
from nifty_hypothesis import pipeline
from nifty_hypothesis.pipeline import run_pipeline, select_stages, stage_plan
from nifty_hypothesis.validation import inspect_artifact, validate_inputs
from research.phase2.close_hypothesis_formulation import build_first_daily_crossings


def _write_config(root: Path, gold_root: str) -> Path:
    config = {
        "schema_version": "phase2-hypothesis-evidence/v1",
        "repo_root": ".",
        "gold_root": gold_root,
        "expiry_calendar": "expiry.parquet",
        "dataset_audit": "audit.json",
        "output_dir": "audit",
        "session_mode": "observed",
        "entry_offset_source": "computed",
    }
    path = root / "config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def test_config_expands_environment_and_resolves_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gold = tmp_path / "gold"
    monkeypatch.setenv("TEST_ENDOVIA_GOLD", str(gold))
    config = ResearchConfig.from_json(_write_config(tmp_path, "${TEST_ENDOVIA_GOLD}"))

    assert config.gold_root == gold.resolve()
    assert config.expiry_calendar == (tmp_path / "expiry.parquet").resolve()
    assert config.output_dir == (tmp_path / "audit").resolve()


def test_stage_selection_preserves_canonical_order() -> None:
    assert select_stages(start="intraday_surface", through="matched_variance") == (
        "intraday_surface",
        "volatility_regimes",
        "matched_variance",
    )
    assert select_stages(only=("event_summary", "intraday_surface")) == (
        "intraday_surface",
        "event_summary",
    )
    with pytest.raises(ValueError, match="unknown stages"):
        select_stages(only=("not_a_stage",))
    assert STAGE_ORDER[-3:] == ("curve_crossings", "hypothesis_closeout", "manifest")


def test_artifact_contract_declares_every_stage(tmp_path: Path) -> None:
    artifacts = ArtifactSet(tmp_path, "observed", "computed")
    for stage in STAGE_ORDER:
        assert artifacts.stage_outputs(stage)
        assert all(path.parent == tmp_path for path in artifacts.stage_outputs(stage))
    assert artifacts.stage_outputs("defined_risk_paths")[-1].name == (
        "phase2_defined_risk_structure_paths.parquet"
    )


def test_validation_checks_dataset_audit_identity(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text("[project]\nname='test'\n", encoding="utf-8")
    gold = tmp_path / "gold"
    gold.mkdir()
    pq.write_table(pa.table({"value": [1]}), gold / "part.parquet")
    pq.write_table(pa.table({"expiry": [1]}), tmp_path / "expiry.parquet")
    (tmp_path / "audit.json").write_text(
        json.dumps(
            {
                "dataset_release": "test/version=1",
                "dataset_root_local": str(gold.resolve()),
            }
        ),
        encoding="utf-8",
    )
    config = ResearchConfig.from_json(_write_config(tmp_path, "gold"))

    result = validate_inputs(config)

    assert result["ok"] is True
    assert result["dataset_release"] == "test/version=1"


def test_artifact_inspection_reports_parquet_rows(tmp_path: Path) -> None:
    path = tmp_path / "artifact.parquet"
    pq.write_table(pa.table({"value": [1, 2, 3]}), path)

    record = inspect_artifact(path, include_hash=True)

    assert record["exists"] is True
    assert record["rows"] == 3
    assert len(record["sha256"]) == 64


def test_plan_marks_existing_outputs_complete(tmp_path: Path) -> None:
    config = ResearchConfig(
        repo_root=tmp_path,
        gold_root=tmp_path / "gold",
        expiry_calendar=tmp_path / "expiry.parquet",
        dataset_audit=tmp_path / "audit.json",
        output_dir=tmp_path / "audit",
    )
    artifacts = ArtifactSet.from_config(config)
    output = artifacts.stage_outputs("playable_universe")[0]
    output.parent.mkdir()
    output.write_text("{}", encoding="utf-8")

    plan = stage_plan(config, ("playable_universe", "moneyness_horizon"))

    assert plan[0]["complete"] is True
    assert plan[1]["complete"] is False


def test_resume_always_rebuilds_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = ResearchConfig(
        repo_root=tmp_path,
        gold_root=tmp_path / "gold",
        expiry_calendar=tmp_path / "expiry.parquet",
        dataset_audit=tmp_path / "audit.json",
        output_dir=tmp_path / "audit",
    )
    artifacts = ArtifactSet.from_config(config)
    for stage in STAGE_ORDER:
        for output in artifacts.stage_outputs(stage):
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text("stale", encoding="utf-8")

    called = []

    def rebuild_manifest(_config: ResearchConfig, _artifacts: ArtifactSet) -> None:
        called.append(True)
        _artifacts.stage_outputs("manifest")[0].write_text("fresh", encoding="utf-8")

    monkeypatch.setattr(pipeline, "validate_inputs", lambda _config: {"ok": True})
    monkeypatch.setitem(pipeline.RUNNERS, "manifest", rebuild_manifest)

    result = run_pipeline(config, ("manifest",), resume=True)

    assert called == [True]
    assert result[0]["status"] == "completed"
    assert artifacts.stage_outputs("manifest")[0].read_text(encoding="utf-8") == "fresh"


def test_closeout_uses_first_daily_crossing_and_exact_next_minute(tmp_path: Path) -> None:
    timestamps = pd.date_range("2026-01-02 10:00", periods=6, freq="min", tz="Asia/Kolkata")
    table = pa.table(
        {
            "horizon_minutes": [60] * 6,
            "entry_ts": timestamps,
            "trade_date": ["2026-01-02"] * 6,
            "entry_time": [timestamp.strftime("%H:%M") for timestamp in timestamps],
            "vrp_crossing": [
                "cross_up",
                "no_cross",
                "cross_up",
                "no_cross",
                "cross_down",
                "no_cross",
            ],
            "vrp_tod_percentile": [0.6, 0.7, 0.8, 0.7, 0.3, 0.2],
            "signal_vrp_var_act365": [0.01, 0.02, 0.03, 0.02, -0.01, -0.02],
            "short_iron_condor__pnl_points": [0.0, 1.25, 0.0, 9.0, 0.0, -2.0],
            "short_iron_condor__return_on_max_loss": [0.0, 0.1, 0.0, 0.9, 0.0, -0.2],
            "long_iron_condor__pnl_points": [0.0, -1.25, 0.0, -9.0, 0.0, 2.0],
            "long_iron_condor__return_on_max_loss": [0.0, -0.1, 0.0, -0.9, 0.0, 0.2],
        }
    )
    path = tmp_path / "structures.parquet"
    pq.write_table(table, path)

    events = build_first_daily_crossings(path)

    assert list(events["vrp_crossing"]) == ["cross_up", "cross_down"]
    assert events.iloc[0]["next_short_iron_condor__pnl_points"] == 1.25
    assert events.iloc[1]["next_long_iron_condor__pnl_points"] == 2.0
